#!/usr/bin/env python3
"""Backfill immutable original-study snapshots from the Gmail Sent archive.

The historical email sender stored recipients and a local send token, but not
the original studies included in each message. This script matches production
preprint records to the actual sent messages using recipients, preprint title,
and send time, then stores the parsed ``email_originals`` on each preprint.

Dry run (default):
    python scripts/backfill_email_originals.py

Apply the verified one-to-one matches:
    python scripts/backfill_email_originals.py --apply

The script never prints recipient addresses or message bodies.
"""
from __future__ import annotations

import argparse
import email
import email.policy
import imaplib
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

NOTIFICATION_SUBJECT_PREFIXES = (
    "A replication attempt related to ",
    "Replication attempts related to ",
)
DOI_URL_RE = re.compile(r"https?://doi\.org/([^\s>]+)", re.IGNORECASE)
REPLICATION_LINE_RE = re.compile(r"^Replication(?:\s+\d+)?:\s*(.*)$", re.IGNORECASE)


def _ddb_resource():
    region = os.getenv("AWS_REGION", "eu-north-1")
    return boto3.resource(
        "dynamodb",
        region_name=region,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )


def _normalise_text(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def _recipient_key(value: Any) -> Tuple[str, ...]:
    if isinstance(value, str):
        addresses = [part.strip().lower() for part in value.split(",")]
    else:
        addresses = [str(part).strip().lower() for part in (value or [])]
    return tuple(sorted(address for address in addresses if address))


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decode_subject(message: email.message.Message) -> str:
    return str(make_header(decode_header(message.get("Subject", ""))))


def _plain_text(message: email.message.Message) -> str:
    parts: List[str] = []
    for part in message.walk():
        if part.get_content_type() != "text/plain":
            continue
        payload = part.get_payload(decode=True) or b""
        parts.append(payload.decode(part.get_content_charset() or "utf-8", errors="replace"))
    return "\n".join(parts)


def _parse_reference_payload(payload: str) -> Tuple[str, str]:
    """Extract the displayed reference and DOI from one rendered plain-text line."""
    value = " ".join(payload.strip().split())
    match = DOI_URL_RE.search(value)
    if not match:
        return value, ""
    doi = unquote(match.group(1)).lower().rstrip(".,; ")
    # The rendered URL is wrapped in parentheses, while valid DOI suffixes can
    # themselves contain balanced parentheses. Remove only unmatched closing
    # delimiters belonging to the surrounding email markup.
    while doi.endswith(")") and doi.count(")") > doi.count("("):
        doi = doi[:-1]
    citation = value[:match.start()].rstrip(" (")
    if citation.casefold().endswith(doi.casefold()):
        citation = citation[:-len(doi)].rstrip(" ([")
    return citation, doi


def parse_originals_from_plain_text(text: str) -> List[Dict[str, Any]]:
    """Parse the exact cited originals and replication DOIs from a sent email."""
    originals: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        while line.startswith(">"):
            line = line[1:].lstrip()
        if line.startswith("Cited:"):
            citation, doi = _parse_reference_payload(line[len("Cited:"):])
            current = {
                "full_reference": citation or doi or "(unknown reference)",
                "doi": doi,
                "replications": [],
            }
            originals.append(current)
            continue
        replication_match = REPLICATION_LINE_RE.match(line)
        if replication_match and current is not None:
            citation, doi = _parse_reference_payload(replication_match.group(1))
            current["replications"].append({
                "full_reference": citation or doi or "(unknown)",
                "doi": doi,
            })
    return originals


def _chunks(values: Sequence[bytes], size: int) -> Iterable[Sequence[bytes]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def fetch_sent_notifications(*, since: str, mailbox: str) -> List[Dict[str, Any]]:
    sender = os.environ.get("GMAIL_SENDER_ADDRESS", "flora@replications.forrt.org")
    password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not password:
        raise RuntimeError("GMAIL_APP_PASSWORD is required")

    client = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    try:
        client.login(sender, password)
        status, _ = client.select(f'"{mailbox}"', readonly=True)
        if status != "OK":
            raise RuntimeError(f"Could not select Gmail mailbox {mailbox!r}")
        status, data = client.uid("search", None, "SINCE", since)
        if status != "OK":
            raise RuntimeError("Gmail search failed")
        uids = data[0].split() if data else []

        notification_uids: List[bytes] = []
        for chunk in _chunks(uids, 100):
            status, parts = client.uid(
                "fetch",
                b",".join(chunk).decode(),
                "(BODY.PEEK[HEADER.FIELDS (SUBJECT)])",
            )
            if status != "OK":
                raise RuntimeError("Gmail header fetch failed")
            for part in parts or []:
                if not isinstance(part, tuple):
                    continue
                uid_match = re.search(rb"UID (\d+)", part[0])
                header = email.message_from_bytes(part[1])
                subject = _decode_subject(header)
                if uid_match and subject.startswith(NOTIFICATION_SUBJECT_PREFIXES):
                    notification_uids.append(uid_match.group(1))

        messages: List[Dict[str, Any]] = []
        for chunk in _chunks(notification_uids, 40):
            status, parts = client.uid("fetch", b",".join(chunk).decode(), "(BODY.PEEK[])")
            if status != "OK":
                raise RuntimeError("Gmail message fetch failed")
            for part in parts or []:
                if not isinstance(part, tuple):
                    continue
                uid_match = re.search(rb"UID (\d+)", part[0])
                message = email.message_from_bytes(part[1], policy=email.policy.default)
                text = _plain_text(message)
                try:
                    sent_at = parsedate_to_datetime(message.get("Date")).astimezone(timezone.utc)
                except (TypeError, ValueError, AttributeError):
                    sent_at = None
                recipients = tuple(sorted(
                    address.lower()
                    for _, address in getaddresses(message.get_all("To", []))
                    if address
                ))
                messages.append({
                    "uid": uid_match.group(1).decode() if uid_match else "",
                    "message_id": str(message.get("Message-ID") or ""),
                    "recipients": recipients,
                    "sent_at": sent_at,
                    "normalised_body": _normalise_text(text),
                    "originals": parse_originals_from_plain_text(text),
                })
        return messages
    finally:
        try:
            client.logout()
        except Exception:
            pass


def fetch_sent_preprints(table) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {
        "IndexName": "by_queue_email",
        "KeyConditionExpression": Key("queue_email").eq("done"),
        "ProjectionExpression": (
            "osf_id, email_sent_at, email_recipient, email_message_id, "
            "email_originals, title"
        ),
    }
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        if not response.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def match_preprints_to_messages(
    preprints: Sequence[Dict[str, Any]],
    messages: Sequence[Dict[str, Any]],
    *,
    max_hours: float = 36,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Create one-to-one archive matches without exposing recipient addresses."""
    messages_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for message in messages:
        message_id = str(message.get("message_id") or "").strip().casefold()
        if message_id:
            messages_by_id[message_id].append(message)
    preprints_by_message_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for preprint in preprints:
        message_id = str(preprint.get("email_message_id") or "").strip().casefold()
        if message_id:
            preprints_by_message_id[message_id].append(preprint)

    matched: List[Dict[str, Any]] = []
    used_uids = set()
    matched_preprint_ids = set()
    for message_id, id_preprints in preprints_by_message_id.items():
        id_messages = messages_by_id.get(message_id, [])
        while id_preprints and id_messages:
            pairs = []
            for preprint in id_preprints:
                title = _normalise_text(preprint.get("title"))
                sent_at = _parse_datetime(preprint.get("email_sent_at"))
                for message in id_messages:
                    title_match = bool(title and title in message["normalised_body"])
                    if sent_at and message.get("sent_at"):
                        delta = abs((message["sent_at"] - sent_at).total_seconds())
                    else:
                        delta = float("inf")
                    pairs.append((0 if title_match else 1, delta, preprint, message))
            _title_penalty, delta, preprint, message = min(
                pairs,
                key=lambda pair: (pair[0], pair[1], str(pair[2].get("osf_id")), pair[3]["uid"]),
            )
            matched.append({
                "preprint": preprint,
                "message": message,
                "method": "message_id",
                "delta_seconds": delta,
            })
            used_uids.add(message["uid"])
            matched_preprint_ids.add(preprint["osf_id"])
            id_preprints.remove(preprint)
            id_messages.remove(message)

    remaining_preprints = [
        preprint for preprint in preprints
        if preprint.get("osf_id") not in matched_preprint_ids
    ]

    messages_by_recipients: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    for message in messages:
        if message["uid"] not in used_uids:
            messages_by_recipients[message["recipients"]].append(message)

    unmatched: List[Dict[str, Any]] = []
    preprints_by_recipients: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    for preprint in remaining_preprints:
        preprints_by_recipients[_recipient_key(preprint.get("email_recipient"))].append(preprint)

    max_seconds = max_hours * 3600
    for recipient_key, group in preprints_by_recipients.items():
        available = list(messages_by_recipients.get(recipient_key, []))
        pending = list(group)
        while pending and available:
            pairs = []
            for preprint in pending:
                sent_at = _parse_datetime(preprint.get("email_sent_at"))
                title = _normalise_text(preprint.get("title"))
                for message in available:
                    title_match = bool(title and title in message["normalised_body"])
                    if sent_at and message.get("sent_at"):
                        delta = abs((message["sent_at"] - sent_at).total_seconds())
                    else:
                        delta = float("inf")
                    pairs.append((0 if title_match else 1, delta, preprint, message))
            title_penalty, delta, preprint, message = min(
                pairs,
                key=lambda pair: (pair[0], pair[1], str(pair[2].get("osf_id")), pair[3]["uid"]),
            )
            if delta > max_seconds:
                break
            matched.append({
                "preprint": preprint,
                "message": message,
                "method": "recipients_title_time" if title_penalty == 0 else "recipients_time",
                "delta_seconds": delta,
            })
            pending.remove(preprint)
            available.remove(message)
        unmatched.extend(pending)

    return matched, unmatched


def persist_matches(table, matches: Sequence[Dict[str, Any]], *, overwrite: bool = False) -> Counter:
    results = Counter()
    now = datetime.now(timezone.utc).isoformat()
    for match in matches:
        preprint = match["preprint"]
        message = match["message"]
        if preprint.get("email_originals") and not overwrite:
            results["already_present"] += 1
            continue
        values = {
            ":originals": message["originals"],
            ":source": "gmail_sent_archive",
            ":recorded": now,
            ":message_id": message.get("message_id") or message.get("uid") or "unknown",
            ":true": True,
        }
        try:
            table.update_item(
                Key={"osf_id": preprint["osf_id"]},
                UpdateExpression=(
                    "SET email_originals=:originals, email_snapshot_source=:source, "
                    "email_snapshot_recorded_at=:recorded, "
                    "email_archive_message_id=:message_id"
                ),
                ConditionExpression=(
                    "email_sent=:true" if overwrite
                    else "email_sent=:true AND attribute_not_exists(email_originals)"
                ),
                ExpressionAttributeValues=values,
            )
            results["written"] += 1
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                results["condition_skipped"] += 1
                continue
            raise
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write verified snapshots to DynamoDB")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="With --apply, write complete one-to-one matches while leaving unmatched records untouched",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing snapshots (requires --apply)")
    parser.add_argument("--since", default="01-Mar-2026", help="IMAP SINCE date, e.g. 01-Mar-2026")
    parser.add_argument("--mailbox", default="[Gmail]/Sent Mail")
    args = parser.parse_args()
    if args.overwrite and not args.apply:
        parser.error("--overwrite requires --apply")
    if args.allow_partial and not args.apply:
        parser.error("--allow-partial requires --apply")

    table_name = os.environ.get("DDB_TABLE_PREPRINTS", "prod_preprints")
    table = _ddb_resource().Table(table_name)
    preprints = fetch_sent_preprints(table)
    messages = fetch_sent_notifications(since=args.since, mailbox=args.mailbox)
    matches, unmatched = match_preprints_to_messages(preprints, messages)

    methods = Counter(match["method"] for match in matches)
    invalid_originals = [
        match for match in matches
        if not match["message"].get("originals")
        or any(not original.get("doi") for original in match["message"]["originals"])
    ]
    unique_message_ids = {match["message"]["uid"] for match in matches}

    print(f"Sent preprints in DynamoDB:       {len(preprints)}")
    print(f"Notification messages in Gmail:  {len(messages)}")
    print(f"One-to-one matches:               {len(matches)}")
    print(f"  exact Message-ID:              {methods['message_id']}")
    print(f"  recipients + title + time:     {methods['recipients_title_time']}")
    print(f"  recipients + time:             {methods['recipients_time']}")
    print(f"Unmatched sent preprints:         {len(unmatched)}")
    print(f"Matched messages without complete original DOIs: {len(invalid_originals)}")
    print(f"Distinct Gmail messages matched: {len(unique_message_ids)}")

    matches_are_safe = len(unique_message_ids) == len(matches) and not invalid_originals
    complete = len(matches) == len(preprints) and not unmatched
    print(f"Audit result: {'PASS' if matches_are_safe and complete else 'PARTIAL' if matches_are_safe else 'FAIL'}")
    if not matches_are_safe:
        if unmatched:
            print("Unmatched OSF IDs: " + ", ".join(sorted(str(x.get("osf_id")) for x in unmatched)))
        print("No writes made.")
        return 1

    if unmatched:
        print("Unmatched OSF IDs: " + ", ".join(sorted(str(x.get("osf_id")) for x in unmatched)))
    if not complete and not (args.apply and args.allow_partial):
        print("No writes made; use --apply --allow-partial to store only verified matches.")
        return 1

    if not args.apply:
        print("Dry run only; pass --apply to store the snapshots.")
        return 0

    result = persist_matches(table, matches, overwrite=args.overwrite)
    print(
        "Write result: "
        + ", ".join(f"{key}={value}" for key, value in sorted(result.items()))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
