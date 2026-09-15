#!/usr/bin/env python3
"""Generate a Markdown dashboard summarising DynamoDB pipeline stats.

Usage:
    python scripts/generate_dashboard.py [OUTPUT_PATH]

Requires env vars for table names (DDB_TABLE_PREPRINTS, etc.) and AWS
credentials (AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or a
local DynamoDB endpoint (DYNAMO_LOCAL_URL).
"""

import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from urllib.parse import quote

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Attr, Key


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _get_dynamo_resource():
    local_url = os.getenv("DYNAMO_LOCAL_URL")
    region = os.getenv("AWS_REGION", "eu-central-1")
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
    if local_url:
        return boto3.resource(
            "dynamodb",
            region_name=region,
            endpoint_url=local_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
            config=cfg,
        )
    return boto3.resource("dynamodb", region_name=region, config=cfg)


def _count_by_gsi(table, index_name, key_name, key_value):
    """Count items on a GSI using Select='COUNT' (no data transferred)."""
    resp = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(key_name).eq(key_value),
        Select="COUNT",
    )
    total = resp["Count"]
    while resp.get("LastEvaluatedKey"):
        resp = table.query(
            IndexName=index_name,
            KeyConditionExpression=Key(key_name).eq(key_value),
            Select="COUNT",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        total += resp["Count"]
    return total


def _query_all_items(
    table,
    index_name,
    key_name,
    key_value,
    projection_expression=None,
):
    """Return all items from a GSI query (paginated)."""
    items = []
    kwargs = dict(
        IndexName=index_name,
        KeyConditionExpression=Key(key_name).eq(key_value),
    )
    if projection_expression:
        kwargs["ProjectionExpression"] = projection_expression
    resp = table.query(**kwargs)
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        resp = table.query(**kwargs)
        items.extend(resp["Items"])
    return items


def _scan_all(table, projection_expression):
    """Full paginated scan returning only projected attributes (small tables only)."""
    items = []
    resp = table.scan(ProjectionExpression=projection_expression)
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            ProjectionExpression=projection_expression,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp["Items"])
    return items


def _scan_count_with_filter(table, filter_expression):
    """Count table items matching a filter using paginated scans."""
    total = 0
    resp = table.scan(FilterExpression=filter_expression, Select="COUNT")
    total += resp.get("Count", 0)
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=filter_expression,
            Select="COUNT",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        total += resp.get("Count", 0)
    return total


def _scan_with_filter(table, filter_expression, projection_expression):
    """Full paginated scan returning projected attributes for items matching a filter."""
    items = []
    resp = table.scan(
        FilterExpression=filter_expression,
        ProjectionExpression=projection_expression,
    )
    items.extend(resp["Items"])
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(
            FilterExpression=filter_expression,
            ProjectionExpression=projection_expression,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp["Items"])
    return items


# ---------------------------------------------------------------------------
# Experiment analytics
# ---------------------------------------------------------------------------

PROVIDER_NAMES = {
    "osf": "OSF Preprints",
    "psyarxiv": "PsyArXiv",
    "socarxiv": "SocArXiv",
    "edarxiv": "EdArXiv",
    "metaarxiv": "MetaArXiv",
    "mediarxiv": "MediArXiv",
    "africarxiv": "AfricArXiv",
    "arabixiv": "Arabixiv",
    "biohackrxiv": "BioHackrXiv",
    "eartharxiv": "EarthArXiv",
    "ecoevorxiv": "EcoEvoRxiv",
    "frenxiv": "Frenxiv",
    "inarxiv": "INArxiv",
    "marxiv": "MarXiv",
    "sportrxiv": "SportRxiv",
    "thesiscommons": "Thesis Commons",
}


def _parse_timestamp(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _recipient_addresses(value):
    if isinstance(value, (list, tuple, set)):
        parts = value
    elif value:
        parts = str(value).split(",")
    else:
        parts = []
    return [str(part).strip().lower() for part in parts if str(part).strip()]


def _recipient_count(value):
    """Count deliveries; an address can appear in more than one notification."""
    return len(_recipient_addresses(value))


def _sparkline(values):
    """Render a compact zero-aware sparkline suitable for GitHub Markdown."""
    if not values:
        return "—"
    maximum = max(values)
    if maximum <= 0:
        return "·" * len(values)
    bars = "▁▂▃▄▅▆▇█"
    return "".join(
        "·" if value <= 0 else bars[min(len(bars) - 1, round(value / maximum * (len(bars) - 1)))]
        for value in values
    )


def _summarize_email_activity(sent_items):
    by_day = Counter()
    recipients_by_day = Counter()
    provider_counts = Counter()
    timestamp_missing = 0
    total_recipients = 0
    unique_recipient_addresses = set()

    for item in sent_items:
        addresses = _recipient_addresses(item.get("email_recipient"))
        recipients = len(addresses)
        total_recipients += recipients
        unique_recipient_addresses.update(addresses)
        provider_id = str(item.get("provider_id") or "unknown").lower()
        provider_counts[PROVIDER_NAMES.get(provider_id, provider_id)] += 1
        sent_at = _parse_timestamp(item.get("email_sent_at"))
        if not sent_at:
            timestamp_missing += 1
            continue
        day = sent_at.date()
        by_day[day] += 1
        recipients_by_day[day] += recipients

    if by_day:
        first_day, last_day = min(by_day), max(by_day)
        first_week = first_day - timedelta(days=first_day.weekday())
        last_week = last_day - timedelta(days=last_day.weekday())
        week_starts = []
        cursor = first_week
        while cursor <= last_week:
            week_starts.append(cursor)
            cursor += timedelta(days=7)
        by_week = Counter()
        for day, count in by_day.items():
            by_week[day - timedelta(days=day.weekday())] += count
        weekly_values = [by_week[week] for week in week_starts]
    else:
        first_day = last_day = None
        week_starts = []
        weekly_values = []

    monthly = defaultdict(lambda: {"notifications": 0, "recipients": 0})
    for day, count in by_day.items():
        key = day.strftime("%Y-%m")
        monthly[key]["notifications"] += count
        monthly[key]["recipients"] += recipients_by_day[day]

    cumulative = 0
    monthly_rows = []
    for month in sorted(monthly):
        cumulative += monthly[month]["notifications"]
        monthly_rows.append({
            "month": month,
            **monthly[month],
            "cumulative": cumulative,
        })

    return {
        "total_notifications": len(sent_items),
        "total_recipients": total_recipients,
        "unique_recipients": len(unique_recipient_addresses),
        "recipient_addresses": unique_recipient_addresses,
        "timestamp_missing": timestamp_missing,
        "first_day": first_day,
        "last_day": last_day,
        "week_starts": week_starts,
        "weekly_values": weekly_values,
        "monthly_rows": monthly_rows,
        "provider_counts": provider_counts,
        "active_send_days": len(by_day),
        "peak_day_count": max(by_day.values(), default=0),
    }


def _normalize_doi(value):
    text = str(value or "").strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    return text.rstrip(".,; ")


def _summarize_targeted_originals(sent_items):
    """Count immutable original-study snapshots stored when each email was sent."""
    counts = {}
    notifications_with_targets = 0
    multi_original_notifications = 0
    for item in sent_items:
        seen_in_notification = set()
        for original in item.get("email_originals") or []:
            doi = _normalize_doi(original.get("doi"))
            citation = " ".join(str(
                original.get("full_reference")
                or original.get("citation")
                or ""
            ).split())
            key = doi or citation.casefold()
            if not key:
                continue
            entry = counts.setdefault(key, {
                "doi": doi,
                "citation": citation or doi or "Unknown original",
                "notifications": 0,
                "replication_dois": set(),
            })
            if key not in seen_in_notification:
                seen_in_notification.add(key)
                entry["notifications"] += 1
            for replication in original.get("replications") or []:
                replication_doi = _normalize_doi(
                    (replication or {}).get("doi")
                    or (replication or {}).get("doi_r")
                )
                if replication_doi:
                    entry["replication_dois"].add(replication_doi)
        if seen_in_notification:
            notifications_with_targets += 1
        if len(seen_in_notification) > 1:
            multi_original_notifications += 1

    originals = sorted(
        counts.values(),
        key=lambda item: (-item["notifications"], item["citation"].casefold()),
    )
    for item in originals:
        item["known_replications"] = len(item.pop("replication_dois"))
    return {
        "originals": originals,
        "unique_originals": len(originals),
        "original_mentions": sum(item["notifications"] for item in originals),
        "notifications_with_targets": notifications_with_targets,
        "multi_original_notifications": multi_original_notifications,
    }


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect_stats():
    ddb = _get_dynamo_resource()

    preprints_name = os.environ.get("DDB_TABLE_PREPRINTS", "preprints")
    excluded_name = os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints")
    assignments_name = os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments")
    suppression_name = os.environ.get("DDB_TABLE_EMAIL_SUPPRESSION", "email_suppression")

    preprints = ddb.Table(preprints_name)
    excluded = ddb.Table(excluded_name)
    assignments = ddb.Table(assignments_name)
    suppression = ddb.Table(suppression_name)

    # Pipeline funnel — 8 GSI count queries
    queues = ["queue_pdf", "queue_grobid", "queue_extract", "queue_email"]
    funnel = {}
    for q in queues:
        index = f"by_{q}"
        pending = _count_by_gsi(preprints, index, q, "pending")
        done = _count_by_gsi(preprints, index, q, "done")
        funnel[q] = {"pending": pending, "done": done, "total": pending + done}

    total_preprints = funnel["queue_pdf"]["total"]

    # Exclusions — small table scan
    excl_items = _scan_all(excluded, "exclusion_reason")
    excl_counts = Counter(item.get("exclusion_reason", "unknown") for item in excl_items)
    total_excluded = sum(excl_counts.values())

    # Trial assignments — GSI query for assigned (need arm breakdown)
    assigned_items = _query_all_items(assignments, "by_status", "status", "assigned")
    arm_counts = Counter(item.get("arm", "unknown") for item in assigned_items)
    total_assigned = len(assigned_items)
    treatment_assigned = arm_counts.get("treatment", 0)

    # Randomization-excluded count
    randomization_excluded = _count_by_gsi(assignments, "by_status", "status", "excluded")

    # Email suppression and current send-error backlog
    suppression_items = _scan_all(suppression, "email, reason")
    suppression_counts = Counter(item.get("reason", "unknown") for item in suppression_items)
    total_suppressed = sum(suppression_counts.values())
    email_error_open = _scan_count_with_filter(
        preprints,
        Attr("email_error").exists(),
    )

    # Sent-notification records provide the experiment timeline and immutable
    # snapshots of the original studies included in each email.
    database_sent_items = _query_all_items(
        preprints,
        "by_queue_email",
        "queue_email",
        "done",
        "osf_id, email_sent_at, email_recipient, provider_id, email_originals",
    )
    sent_items = [item for item in database_sent_items if item.get("email_originals")]
    email_snapshot_missing = len(database_sent_items) - len(sent_items)
    email_activity = _summarize_email_activity(sent_items)
    targeted_originals = _summarize_targeted_originals(sent_items)
    sent_addresses = email_activity["recipient_addresses"]
    experiment_suppression_counts = Counter(
        item.get("reason", "unknown")
        for item in suppression_items
        if str(item.get("email") or "").strip().lower() in sent_addresses
    )

    contactable_by_arm = Counter()
    for item in assigned_items:
        arm = str(item.get("arm") or "unknown")
        contactable_by_arm[arm] += int(item.get("contactable_email_count") or 0)

    # FLoRA screening and email extraction coverage (active items only)
    active = Attr("excluded").not_exists() | Attr("excluded").eq(False)
    flora_screened = _scan_count_with_filter(
        preprints, Attr("flora_eligible").exists() & active,
    )
    email_extracted = _scan_count_with_filter(
        preprints, Attr("author_email_candidates").exists() & active,
    )

    # FLoRA matching — preprints with flora_eligible = True
    flora_eligible_items = _scan_with_filter(
        preprints,
        Attr("flora_eligible").eq(True),
        "osf_id, flora_eligible_count, flora_citation_validation_pending, "
        "author_email_candidates, trial_assignment_status, excluded",
    )
    flora_total = len(flora_eligible_items)

    # Distribution of eligible reference counts
    eligible_counts = [
        int(item.get("flora_eligible_count", 0))
        for item in flora_eligible_items
    ]
    flora_median_refs = median(eligible_counts) if eligible_counts else 0
    flora_max_refs = max(eligible_counts) if eligible_counts else 0
    flora_multi_ref = sum(1 for c in eligible_counts if c > 1)
    flora_multi_ref_pct = (flora_multi_ref / flora_total * 100) if flora_total else 0

    # Make the current FLoRA-matched population mutually exclusive so its
    # categories reconcile exactly to flora_total.
    flora_current = Counter()
    for item in flora_eligible_items:
        if item.get("excluded") is True:
            flora_current["excluded"] += 1
        elif not item.get("author_email_candidates"):
            flora_current["missing_email"] += 1
        elif item.get("flora_citation_validation_pending") is True:
            flora_current["validation_pending"] += 1
        else:
            flora_current["assignable"] += 1

    flora_excluded = flora_current["excluded"]
    flora_missing_email = flora_current["missing_email"]
    flora_validation_pending = flora_current["validation_pending"]
    flora_total_assignable = flora_current["assignable"]
    flora_currently_assigned = sum(
        1 for item in flora_eligible_items
        if not item.get("excluded")
        and item.get("author_email_candidates")
        and not item.get("flora_citation_validation_pending")
        and item.get("trial_assignment_status") == "assigned"
    )
    assigned_not_currently_assignable = max(0, total_assigned - flora_currently_assigned)

    # Assignment pending: assignable but not yet assigned
    flora_assignment_pending = sum(
        1 for item in flora_eligible_items
        if not item.get("trial_assignment_status")
        and not item.get("excluded")
        and item.get("author_email_candidates")
        and not item.get("flora_citation_validation_pending")
    )

    queue_extract_done = funnel["queue_extract"]["done"]

    flora_screening_pending = max(0, queue_extract_done - flora_screened)
    funnel["flora_screening"] = {
        "done": flora_screened,
        "pending": flora_screening_pending,
        "total": flora_screened + flora_screening_pending,
    }
    author_extraction_pending = max(0, queue_extract_done - email_extracted)
    funnel["author_extraction"] = {
        "done": email_extracted,
        "pending": author_extraction_pending,
        "total": email_extracted + author_extraction_pending,
    }
    funnel["trial_assignment"] = {
        "done": total_assigned,
        "pending": flora_assignment_pending,
        "total": total_assigned + flora_assignment_pending,
    }

    return {
        "funnel": funnel,
        "total_preprints": total_preprints,
        "queue_extract_done": queue_extract_done,
        "excl_counts": excl_counts,
        "total_excluded": total_excluded,
        "arm_counts": arm_counts,
        "total_assigned": total_assigned,
        "treatment_assigned": treatment_assigned,
        "randomization_excluded": randomization_excluded,
        "suppression_counts": suppression_counts,
        "total_suppressed": total_suppressed,
        "email_error_open": email_error_open,
        "flora_screened": flora_screened,
        "email_extracted": email_extracted,
        "flora_total": flora_total,
        "flora_median_refs": flora_median_refs,
        "flora_max_refs": flora_max_refs,
        "flora_multi_ref": flora_multi_ref,
        "flora_multi_ref_pct": flora_multi_ref_pct,
        "flora_validation_pending": flora_validation_pending,
        "flora_excluded": flora_excluded,
        "flora_missing_email": flora_missing_email,
        "flora_total_assignable": flora_total_assignable,
        "flora_currently_assigned": flora_currently_assigned,
        "assigned_not_currently_assignable": assigned_not_currently_assignable,
        "flora_assignment_pending": flora_assignment_pending,
        "email_activity": email_activity,
        "targeted_originals": targeted_originals,
        "contactable_by_arm": contactable_by_arm,
        "experiment_suppression_counts": experiment_suppression_counts,
        "database_sent_count": len(database_sent_items),
        "email_snapshot_missing": email_snapshot_missing,
    }


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

STAGE_LABELS = {
    "queue_pdf":         "PDF Download",
    "queue_grobid":      "GROBID Processing",
    "queue_extract":     "Reference Extraction",
    "flora_screening":   "FLoRA Screening",
    "author_extraction": "Author Extraction",
    "trial_assignment":  "Trial Assignment",
    "queue_email":       "Email",
}


def _escape_markdown(value):
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def _shorten(value, limit=96):
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip(" ,;:.-") + "…"


def _format_month(value):
    return datetime.strptime(value, "%Y-%m").strftime("%b %Y")


def _progress_bar(done, total, width=18):
    if total <= 0:
        return "░" * width
    filled = min(width, round(done / total * width))
    return "█" * filled + "░" * (width - filled)


def _pct(numerator, denominator, digits=1):
    if not denominator:
        return "—"
    return f"{numerator / denominator * 100:.{digits}f}%"


def render_markdown(stats):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_identified = stats["total_preprints"] + stats["total_excluded"]
    email = stats["funnel"]["queue_email"]
    activity = stats["email_activity"]
    targets = stats["targeted_originals"]
    sent = activity["total_notifications"]
    recipients = activity["total_recipients"]
    unique_recipients = activity["unique_recipients"]
    treatment = stats["treatment_assigned"]
    control = stats["arm_counts"].get("control", 0)
    email_in_queue = email["pending"] + sent
    missing_from_queue = max(0, treatment - email_in_queue)
    treatment_contactable = stats["contactable_by_arm"].get("treatment", 0)
    control_contactable = stats["contactable_by_arm"].get("control", 0)
    balance = treatment - control
    contactable_balance = treatment_contactable - control_contactable

    def signed(value):
        return "even" if value == 0 else f"{value:+,}"

    lines = [
        "# FLoRA-Notify experiment dashboard",
        f"*Updated {now} · [Source code](https://github.com/forrtproject/flora_preprint_notifier)*",
        "",
        f"> **{sent:,} archive-verified assigned-preprint records** · **{recipients:,} recorded recipient deliveries** "
        f"to **{unique_recipients:,} distinct addresses**  ",
        f"> **{targets['unique_originals']:,} distinct targeted originals recorded** · "
        f"{email['pending']:,} queued · {stats['email_error_open']:,} open send errors",
        "",
        "## Experiment at a glance",
        "",
        "| | Treatment | Control | Difference |",
        "|---|---:|---:|---:|",
        f"| Assigned preprints | **{treatment:,}** | **{control:,}** | {signed(balance)} |",
        f"| Contactable author addresses | {treatment_contactable:,} | "
        f"{control_contactable:,} | {signed(contactable_balance)} |",
        "",
        f"**Treatment delivery**  `{_progress_bar(sent, treatment)}` "
        f"**{sent:,} / {treatment:,} ({_pct(sent, treatment)})**",
    ]

    if missing_from_queue:
        lines.extend([
            "",
            "> [!WARNING]  ",
            f"> **{missing_from_queue} treatment preprint{' is' if missing_from_queue == 1 else 's are'} "
            "neither archive-verified as sent nor pending in the email queue.** "
            "Review sent-state anomalies before re-queuing.",
        ])

    if stats["email_snapshot_missing"]:
        lines.extend([
            "",
            "> [!CAUTION]  ",
            f"> **{stats['email_snapshot_missing']:,} record{' is' if stats['email_snapshot_missing'] == 1 else 's are'} "
            "marked sent in DynamoDB but have no matching Gmail message.** "
            "They are excluded from verified delivery totals pending correction.",
        ])

    lines.extend(["", "## Canonical notification records over time", ""])
    if activity["week_starts"]:
        first_week = activity["week_starts"][0].strftime("%d %b %Y")
        last_week = activity["week_starts"][-1].strftime("%d %b %Y")
        lines.extend([
            f"**Weekly pace** &nbsp; `{_sparkline(activity['weekly_values'])}`  ",
            f"{first_week} → {last_week} · each mark is one week · peak day: "
            f"{activity['peak_day_count']:,} notifications",
            "",
            "| Month | Notifications | Recipient deliveries | Cumulative |",
            "|---|---:|---:|---:|",
        ])
        for row in activity["monthly_rows"]:
            lines.append(
                f"| {_format_month(row['month'])} | {row['notifications']:,} | "
                f"{row['recipients']:,} | {row['cumulative']:,} |"
            )
        lines.extend([
            "",
            f"First send: **{activity['first_day'].strftime('%d %b %Y')}** · "
            f"latest send: **{activity['last_day'].strftime('%d %b %Y')}** · "
            f"active send days: **{activity['active_send_days']:,}**",
        ])
    else:
        lines.append("No sent notifications with timestamps yet.")

    if activity["timestamp_missing"]:
        lines.append(
            f"\n_{activity['timestamp_missing']} sent notification(s) have no timestamp and are omitted from the timeline._"
        )

    lines.extend([
        "",
        "## Most frequently targeted originals",
        "",
        f"Recorded from the sent-email snapshot for **{targets['notifications_with_targets']:,} of {sent:,}** canonical records: "
        f"**{targets['original_mentions']:,} original-study mentions**, "
        f"**{targets['unique_originals']:,} unique originals**, and "
        f"**{targets['multi_original_notifications']:,} notifications with multiple originals**.",
        "",
    ])
    if targets["originals"]:
        lines.extend([
            "| Original study | DOI | Notifications | Share of sent | Known replications |",
            "|---|---|---:|---:|---:|",
        ])
        for item in targets["originals"][:10]:
            doi = item["doi"]
            doi_cell = (
                f"[{_escape_markdown(doi)}](https://doi.org/{quote(doi, safe='/')})"
                if doi else "—"
            )
            lines.append(
                f"| {_escape_markdown(_shorten(item['citation']))} | {doi_cell} | "
                f"{item['notifications']:,} | {_pct(item['notifications'], sent)} | "
                f"{item['known_replications']:,} |"
            )
    else:
        lines.append("No targeted original-study records found for sent notifications.")

    if targets["notifications_with_targets"] != sent:
        missing_snapshots = sent - targets["notifications_with_targets"]
        lines.extend([
            "",
            "> [!WARNING]  ",
            f"> **{missing_snapshots:,} sent notification{' has' if missing_snapshots == 1 else 's have'} "
            "no immutable original-study snapshot.** This is a data-integrity gap, not a zero-target email.",
        ])

    lines.extend([
        "",
        "## Delivery mix and health",
        "",
        "| Preprint server | Sent notifications | Share |",
        "|---|---:|---:|",
    ])
    for provider, count in activity["provider_counts"].most_common():
        lines.append(f"| {_escape_markdown(provider)} | {count:,} | {_pct(count, sent)} |")

    lines.extend([
        "",
        "| Email health | Count | Rate per distinct recipient |",
        "|---|---:|---:|",
        f"| Bounces among recipients | {stats['experiment_suppression_counts'].get('bounce', 0):,} | "
        f"{_pct(stats['experiment_suppression_counts'].get('bounce', 0), unique_recipients)} |",
        f"| Unsubscribes among recipients | {stats['experiment_suppression_counts'].get('unsubscribe', 0):,} | "
        f"{_pct(stats['experiment_suppression_counts'].get('unsubscribe', 0), unique_recipients)} |",
        f"| Open send errors | {stats['email_error_open']:,} | — |",
        f"| Database sent-state anomalies | {stats['email_snapshot_missing']:,} | — |",
        f"| All suppressions on file | {stats['total_suppressed']:,} | — |",
        "",
        "---",
        "",
        "## Pipeline operations",
        "",
        "### Preprint flow",
        "",
        "| | Count |",
        "|---|---:|",
        f"| **Preprints identified (OSF)** | **{total_identified:,}** |",
        f"| − Excluded | {stats['total_excluded']:,} |",
        f"| = Active in pipeline | {stats['total_preprints']:,} |",
    ])

    # Exclusion breakdown sorted by frequency
    lines.extend([
        "", "<details>", "<summary>Exclusion breakdown</summary>", "",
        "| Reason | Count |", "|---|---:|",
    ])
    for reason, count in stats["excl_counts"].most_common():
        lines.append(f"| `{_escape_markdown(reason)}` | {count:,} |")
    lines.extend(["", "</details>"])

    # Pipeline funnel
    lines.extend([
        "",
        "### Pipeline funnel",
        "",
        "| Stage | Pending | Done | Total |",
        "|-------|--------:|-----:|------:|",
    ])

    for q in [
        "queue_pdf", "queue_grobid", "queue_extract",
        "flora_screening", "author_extraction", "trial_assignment",
        "queue_email",
    ]:
        s = stats["funnel"][q]
        lines.append(f"| {STAGE_LABELS[q]} | {s['pending']:,} | {s['done']:,} | {s['total']:,} |")

    # FLoRA matching and assignment reconciliation
    lines.extend([
        "",
        "### Current FLoRA-match accounting",
        "",
        "These categories are mutually exclusive and sum to the current match total.",
        "",
        "| Current state | Count |",
        "|---|---:|",
        f"| Records with a FLoRA match | **{stats['flora_total']:,}** |",
        f"| − Excluded from the active pipeline | {stats['flora_excluded']:,} |",
        f"| − Active without a contactable author | {stats['flora_missing_email']:,} |",
        f"| − Active, pending citation confirmation | {stats['flora_validation_pending']:,} |",
        f"| = Currently assignable | **{stats['flora_total_assignable']:,}** |",
        "",
        f"**Historical assignments:** {stats['total_assigned']:,} = "
        f"{stats['flora_currently_assigned']:,} still currently assignable + "
        f"{stats['assigned_not_currently_assignable']:,} no longer currently assignable.  ",
        f"> **Decision-review cohort: {stats['assigned_not_currently_assignable']:,}.** "
        f"This is historical cohort drift and is distinct from the {stats['flora_excluded']:,} "
        "current matched exclusions above; the two populations can overlap and should not be added or subtracted.",
        "",
        f"**Currently assignable but awaiting assignment:** {stats['flora_assignment_pending']:,}.  ",
        f"**Excluded during randomisation:** {stats['randomization_excluded']:,}.",
        "",
        "**Current matched-reference distribution:**",
        f" Median: {stats['flora_median_refs']}, "
        f"Max: {stats['flora_max_refs']:,}, "
        f">1 match: {stats['flora_multi_ref']:,}/{stats['flora_total']:,}"
        f" ({stats['flora_multi_ref_pct']:.0f}%)",
    ])

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    output_path = sys.argv[1] if len(sys.argv) > 1 else "dashboard.md"
    stats = collect_stats()
    md = render_markdown(stats)

    if output_path == "/dev/stdout":
        sys.stdout.write(md)
    else:
        with open(output_path, "w") as f:
            f.write(md)
        print(f"Dashboard written to {output_path}")


if __name__ == "__main__":
    main()
