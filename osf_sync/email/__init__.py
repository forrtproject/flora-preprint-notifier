from __future__ import annotations

import logging
import os
import random
import time
import uuid
from collections import deque
from typing import Any, Dict, Optional

from ..dynamo.preprints_repo import PreprintsRepo
from ..dynamo.suppression_repo import SuppressionRepo
from .data_assembly import assemble_email_context, _build_greeting
from .gmail import send_email
from .inbox import process_inbox
from .suppression import is_suppressed
from .template import render_email
from .validation import validate_recipient, repair_email_tld

logger = logging.getLogger(__name__)


class _RateLimiter:
    """Simple sliding-window rate limiter tracking send timestamps in-memory."""

    def __init__(self):
        self.per_minute = int(os.environ.get("EMAIL_RATE_PER_MINUTE", "10"))
        self.per_hour = int(os.environ.get("EMAIL_RATE_PER_HOUR", "100"))
        self.per_day = int(os.environ.get("EMAIL_RATE_PER_DAY", "500"))
        self._timestamps: deque[float] = deque()

    def _prune(self, now: float) -> None:
        day_ago = now - 86400
        while self._timestamps and self._timestamps[0] < day_ago:
            self._timestamps.popleft()

    def _count_since(self, now: float, seconds: float) -> int:
        cutoff = now - seconds
        return sum(1 for ts in self._timestamps if ts >= cutoff)

    def wait_if_needed(self) -> bool:
        """Wait until sending is allowed. Returns False if daily limit reached."""
        while True:
            now = time.time()
            self._prune(now)

            day_count = len(self._timestamps)
            if day_count >= self.per_day:
                return False

            hour_count = self._count_since(now, 3600)
            if hour_count >= self.per_hour:
                sleep_time = self._timestamps[-self.per_hour] + 3600 - now + 0.1
                logger.info("Rate limit (hourly): sleeping %.1fs", sleep_time)
                time.sleep(max(sleep_time, 0.1))
                continue

            minute_count = self._count_since(now, 60)
            if minute_count >= self.per_minute:
                sleep_time = self._timestamps[-self.per_minute] + 60 - now + 0.1
                logger.info("Rate limit (per-minute): sleeping %.1fs", sleep_time)
                time.sleep(max(sleep_time, 0.1))
                continue

            return True

    def record_send(self) -> None:
        self._timestamps.append(time.time())


def process_email_batch(
    *,
    limit: int = 50,
    max_seconds: Optional[int] = None,
    spread_seconds: Optional[int] = None,
    dry_run: bool = False,
    osf_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Process a batch of eligible preprints for email sending.

    Each preprint may have multiple author recipients.  *limit* caps the
    total number of **recipients** contacted (not preprints).

    When *spread_seconds* is set the batch sleeps a randomised interval
    between sends so that emails are spread over roughly that many
    seconds rather than fired in a burst.

    Returns a summary dict with counts of sent, failed, skipped, etc.
    """
    # Process inbox for bounces/unsubscribes before sending
    inbox_stats = {}
    try:
        inbox_stats = process_inbox(dry_run=dry_run)
    except Exception:
        logger.warning("Inbox processing failed (non-fatal)", exc_info=True)

    repo = PreprintsRepo()
    suppression_repo = SuppressionRepo()
    rate_limiter = _RateLimiter()

    deadline = (time.monotonic() + max_seconds) if max_seconds and max_seconds > 0 else None

    # Select eligible preprints
    if osf_id:
        item = repo.t_preprints.get_item(Key={"osf_id": osf_id}).get("Item")
        preprints = [item] if item else []
    else:
        preprints = repo.select_for_email(limit=limit)
    claim_owner = f"email-batch:{uuid.uuid4().hex[:10]}"

    def _release_claim_safe(preprint_id: str) -> None:
        if not preprint_id:
            return
        try:
            repo.release_email_claim(preprint_id, owner=claim_owner)
        except Exception:
            logger.debug("Failed to release email claim", exc_info=True)

    # Pre-compute average inter-send delay when spreading is requested.
    # Cap per-email delay so that a large spread with few actual preprints
    # doesn't cause extremely long waits (e.g. spread sized for 50 emails
    # but only 3 found → divide by actual count, not limit).
    _MAX_AVG_DELAY = float(os.environ.get("EMAIL_MAX_SPREAD_DELAY", "480"))  # 8 min
    avg_delay = 0.0
    if spread_seconds and spread_seconds > 0 and len(preprints) > 1:
        avg_delay = min(spread_seconds / len(preprints), _MAX_AVG_DELAY)

    recipients_sent = 0
    failed = 0
    skipped_suppressed = 0
    skipped_invalid = 0
    skipped_no_context = 0

    for preprint in preprints:
        if deadline and time.monotonic() >= deadline:
            break
        if recipients_sent >= limit:
            break

        pid = preprint.get("osf_id")
        if not pid:
            continue
        if not repo.claim_email_item(pid, owner=claim_owner):
            continue

        # Assemble context
        context = assemble_email_context(pid, repo=repo)
        if not context:
            skipped_no_context += 1
            _release_claim_safe(pid)
            continue

        all_recipients = context.get("_recipients", [])

        # Filter recipients: remove suppressed and invalid per-address
        valid_addresses = []
        valid_first_names = []
        for recip in all_recipients:
            addr = repair_email_tld(recip["email"])
            if is_suppressed(addr, repo=suppression_repo):
                skipped_suppressed += 1
                logger.info("Skipped suppressed email", extra={"osf_id": pid, "email": addr})
                continue
            valid, err_msg = validate_recipient(addr)
            if not valid:
                skipped_invalid += 1
                logger.info("Skipped invalid email", extra={"osf_id": pid, "email": addr, "error": err_msg})
                continue
            valid_addresses.append(addr)
            valid_first_names.append(recip["first_name"])

        if not valid_addresses:
            repo.mark_email_validated(pid, "no valid recipients")
            _release_claim_safe(pid)
            continue

        repo.mark_email_validated(pid, "valid")

        # Enforce all-or-none per preprint under recipient budget.
        remaining_budget = limit - recipients_sent
        if len(valid_addresses) > remaining_budget:
            logger.info(
                "Skipping preprint due to recipient budget",
                extra={
                    "osf_id": pid,
                    "recipient_count": len(valid_addresses),
                    "remaining_budget": remaining_budget,
                },
            )
            _release_claim_safe(pid)
            continue

        # Rebuild greeting to reflect only valid recipients
        context["author_greeting"] = _build_greeting(valid_first_names)

        # Render email
        try:
            subject, html_body, plain_body = render_email(context)
        except Exception as exc:
            failed += len(valid_addresses)
            repo.mark_email_error(pid, f"template render: {exc}", owner=claim_owner)
            logger.exception("Template render failed", extra={"osf_id": pid})
            continue

        if dry_run:
            recipients_sent += len(valid_addresses)
            logger.info(
                "DRY RUN: would send email",
                extra={"osf_id": pid, "to": valid_addresses, "subject": subject},
            )
            _release_claim_safe(pid)
            continue

        # Random inter-send delay to spread emails over time
        if avg_delay > 0 and recipients_sent > 0:
            delay = random.uniform(0.5 * avg_delay, 1.5 * avg_delay)
            remaining = (deadline - time.monotonic()) if deadline else float("inf")
            delay = min(delay, max(remaining - 5, 0))  # keep 5s headroom
            if delay > 0:
                logger.info("Spread delay: sleeping %.1fs", delay)
                time.sleep(delay)
                if deadline and time.monotonic() >= deadline:
                    _release_claim_safe(pid)
                    break

        # Rate limiting
        if not rate_limiter.wait_if_needed():
            logger.warning("Daily rate limit reached, stopping batch")
            _release_claim_safe(pid)
            break

        # Send single email to all valid recipients
        try:
            result = send_email(to=valid_addresses, subject=subject, html_body=html_body, plain_body=plain_body)
            message_id = result.get("id", "")
            marked = repo.mark_email_sent(
                pid,
                recipient=", ".join(valid_addresses),
                message_id=message_id,
                originals=context.get("originals") or [],
                owner=claim_owner,
            )
            if not marked:
                logger.warning(
                    "Email sent but claim ownership changed before marking state",
                    extra={"osf_id": pid},
                )
            rate_limiter.record_send()
            recipients_sent += len(valid_addresses)
        except Exception as exc:
            failed += len(valid_addresses)
            marked = repo.mark_email_error(pid, str(exc), owner=claim_owner)
            if not marked:
                logger.warning(
                    "Email send failed but claim ownership changed before marking error",
                    extra={"osf_id": pid},
                )
            logger.exception("Email send failed", extra={"osf_id": pid, "to": valid_addresses})

    result = {
        "stage": "email",
        "sent": recipients_sent,
        "failed": failed,
        "skipped_suppressed": skipped_suppressed,
        "skipped_invalid": skipped_invalid,
        "skipped_no_context": skipped_no_context,
        "dry_run": dry_run,
        "stopped_due_to_time": bool(deadline and time.monotonic() >= deadline),
    }
    if inbox_stats:
        result["inbox"] = inbox_stats
    return result
