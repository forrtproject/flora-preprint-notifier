#!/usr/bin/env python3
"""Generate a privacy-safe HTML audit of FLoRA exclusions and cohort drift."""
from __future__ import annotations

import argparse
import html
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")


def _ddb_resource():
    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )


def _scan(table, *, projection: str, filter_expression=None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {"ProjectionExpression": projection}
    if filter_expression is not None:
        kwargs["FilterExpression"] = filter_expression
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        if not response.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def _query_assignments(table, status: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {
        "IndexName": "by_status",
        "KeyConditionExpression": Key("status").eq(status),
        "ProjectionExpression": "preprint_id, arm, assigned_at, cluster_id, #status",
        "ExpressionAttributeNames": {"#status": "status"},
    }
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        if not response.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def _batch_get(ddb, table_name: str, ids: Iterable[str], projection: str) -> Dict[str, Dict[str, Any]]:
    values = sorted(set(ids))
    output: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(values), 100):
        request: Dict[str, Any] = {
            table_name: {
                "Keys": [{"osf_id": value} for value in values[start:start + 100]],
                "ProjectionExpression": projection,
            }
        }
        while request:
            response = ddb.batch_get_item(RequestItems=request)
            for item in response.get("Responses", {}).get(table_name, []):
                output[item["osf_id"]] = item
            request = response.get("UnprocessedKeys") or {}
    return output


def _date(value: Any) -> str:
    return str(value or "")[:10] or "—"


def _escape(value: Any) -> str:
    return html.escape(str(value or "—"))


def _reason_label(reason: str) -> str:
    labels = {
        "superseded_by_newer_version": "Superseded version",
        "ingest_not_latest_version": "Not latest version",
        "no_author_contacts_extracted": "No author contact",
        "ingest_date_window": "Outside date window",
        "cross_arm_author_overlap": "Cross-arm author overlap",
        "non_real_match_post_validation": "Match rejected after validation",
        "self_replication_email_deviation": "Self-replication deviation",
    }
    return labels.get(reason, reason.replace("_", " ").capitalize())


def _current_state(item: Mapping[str, Any] | None) -> str:
    if item is None:
        return "Record missing"
    if item.get("excluded") is True:
        return "Now excluded"
    if item.get("flora_eligible") is not True:
        return "No longer FLoRA-eligible"
    if not item.get("author_email_candidates"):
        return "No contactable author"
    if item.get("flora_citation_validation_pending") is True:
        return "Citation validation pending"
    return "Still currently assignable"


def _decision_guidance(record: Mapping[str, Any]) -> tuple[str, str]:
    """Return a review priority and the concrete decision the record needs."""
    state = record["state"]
    if record.get("archive_anomaly"):
        return "Urgent", "Reconcile the sent-state against archive evidence; do not re-send automatically."
    if state == "Record missing":
        return "Urgent", "Restore or account for the missing source record before analysis."
    if state == "Citation validation pending":
        return "Resolve", "Complete citation validation, then confirm eligibility and cohort status."
    if state == "Now excluded" and record.get("email_archive_verified"):
        return "Adjudicate", "Decide analysis handling for a post-assignment exclusion after a verified email."
    if state == "Now excluded":
        return "Adjudicate", "Confirm the exclusion and document its treatment in the analysis cohort."
    if state == "No longer FLoRA-eligible":
        return "Review", "Verify why eligibility changed and decide whether the original assignment remains in analysis."
    if state == "No contactable author":
        return "Review", "Decide denominator handling for an assigned record that could not be contacted."
    return "None", "No cohort decision required."


def collect_audit() -> Dict[str, Any]:
    ddb = _ddb_resource()
    preprints_name = os.environ.get("DDB_TABLE_PREPRINTS", "prod_preprints")
    excluded_name = os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "prod_excluded_preprints")
    assignments_name = os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "prod_trial_preprint_assignments")
    preprints = ddb.Table(preprints_name)
    excluded = ddb.Table(excluded_name)
    assignments = ddb.Table(assignments_name)

    current_matches = _scan(
        preprints,
        filter_expression=Attr("flora_eligible").eq(True),
        projection=(
            "osf_id, title, provider_id, date_created, date_published, flora_eligible, "
            "flora_eligible_count, author_email_candidates, trial_assignment_status, "
            "trial_arm, trial_assigned_at, email_sent, email_sent_at, email_originals, "
            "email_archive_audit_status, excluded, excluded_at, excluded_reason, links"
        ),
    )
    matched_excluded = [item for item in current_matches if item.get("excluded") is True]
    current_match_states = Counter()
    for item in current_matches:
        if item.get("excluded") is True:
            current_match_states["excluded"] += 1
        elif not item.get("author_email_candidates"):
            current_match_states["missing_email"] += 1
        elif item.get("flora_citation_validation_pending") is True:
            current_match_states["validation_pending"] += 1
        else:
            current_match_states["assignable"] += 1
    assigned_rows = _query_assignments(assignments, "assigned")
    randomisation_excluded = _query_assignments(assignments, "excluded")
    assigned_by_id = {row["preprint_id"]: row for row in assigned_rows}
    randomisation_excluded_ids = {row["preprint_id"] for row in randomisation_excluded}

    exclusion_rows = _batch_get(
        ddb,
        excluded_name,
        [item["osf_id"] for item in matched_excluded],
        "osf_id, exclusion_reason, exclusion_stage, excluded_at, exclusion_details",
    )
    assigned_current = _batch_get(
        ddb,
        preprints_name,
        [row["preprint_id"] for row in assigned_rows],
        (
            "osf_id, title, provider_id, flora_eligible, excluded, excluded_reason, "
            "excluded_at, email_sent_at, "
            "author_email_candidates, flora_citation_validation_pending, email_sent, "
            "email_originals, email_archive_audit_status, links"
        ),
    )

    exclusion_records: List[Dict[str, Any]] = []
    for item in matched_excluded:
        osf_id = item["osf_id"]
        registry = exclusion_rows.get(osf_id, {})
        assignment = assigned_by_id.get(osf_id)
        reason = str(registry.get("exclusion_reason") or item.get("excluded_reason") or "unknown")
        details = registry.get("exclusion_details") or {}
        exclusion_records.append({
            "osf_id": osf_id,
            "title": item.get("title") or "Untitled",
            "url": (item.get("links") or {}).get("html") or f"https://osf.io/{osf_id.split('_')[0]}/",
            "provider": item.get("provider_id") or "unknown",
            "reason": reason,
            "reason_label": _reason_label(reason),
            "stage": registry.get("exclusion_stage") or "unknown",
            "excluded_at": registry.get("excluded_at") or item.get("excluded_at"),
            "historically_assigned": assignment is not None,
            "assigned_at": (assignment or {}).get("assigned_at") or item.get("trial_assigned_at"),
            "arm": (assignment or {}).get("arm") or item.get("trial_arm") or "—",
            "randomisation_excluded": osf_id in randomisation_excluded_ids,
            "email_sent_flag": item.get("email_sent") is True,
            "email_archive_verified": bool(item.get("email_originals")),
            "contact_count": len(item.get("author_email_candidates") or []),
            "match_count": int(item.get("flora_eligible_count") or 0),
            "detail": (
                details.get("superseded_by")
                or details.get("window_start")
                or details.get("reason")
                or ""
            ),
        })

    assigned_exceptions: List[Dict[str, Any]] = []
    assigned_later_excluded: List[Dict[str, Any]] = []
    sent_state_anomalies: List[Dict[str, Any]] = []
    for row in assigned_rows:
        item = assigned_current.get(row["preprint_id"])
        state = _current_state(item)
        osf_id = row["preprint_id"]
        record = {
            "osf_id": osf_id,
            "title": (item or {}).get("title") or "Record unavailable",
            "url": ((item or {}).get("links") or {}).get("html") or f"https://osf.io/{osf_id.split('_')[0]}/",
            "arm": row.get("arm") or "unknown",
            "assigned_at": row.get("assigned_at"),
            "state": state,
            "reason": (item or {}).get("excluded_reason") or "—",
            "excluded_at": (item or {}).get("excluded_at"),
            "email_sent_flag": (item or {}).get("email_sent") is True,
            "email_archive_verified": bool((item or {}).get("email_originals")),
            "archive_anomaly": (item or {}).get("email_archive_audit_status") == "no_matching_sent_message",
        }
        record["priority"], record["decision"] = _decision_guidance(record)
        if state != "Still currently assignable":
            assigned_exceptions.append(record)
        if state == "Now excluded":
            assigned_later_excluded.append(record)
        if record["archive_anomaly"]:
            sent_state_anomalies.append(record)

    reason_counts = Counter(record["reason"] for record in exclusion_records)
    stage_counts = Counter(record["stage"] for record in exclusion_records)
    assigned_exception_states = Counter(record["state"] for record in assigned_exceptions)
    matched_later_excluded = [record for record in exclusion_records if record["historically_assigned"]]
    emailed_then_excluded = [record for record in assigned_later_excluded if record["email_archive_verified"]]

    checks = [
        {
            "name": "Exclusion registry coverage",
            "value": f"{len(exclusion_rows)} / {len(matched_excluded)}",
            "status": "pass" if len(exclusion_rows) == len(matched_excluded) else "fail",
            "interpretation": "Every matched-but-excluded preprint should have an auditable exclusion record.",
        },
        {
            "name": "No-contact exclusions",
            "value": f"{sum(r['reason'] == 'no_author_contacts_extracted' and r['contact_count'] == 0 for r in exclusion_records)} / {reason_counts['no_author_contacts_extracted']}",
            "status": "pass" if all(r["contact_count"] == 0 for r in exclusion_records if r["reason"] == "no_author_contacts_extracted") else "review",
            "interpretation": "No-contact exclusions should still have zero contactable addresses.",
        },
        {
            "name": "Cross-arm safeguards",
            "value": f"{sum(r['randomisation_excluded'] and not r['email_sent_flag'] for r in exclusion_records if r['reason'] == 'cross_arm_author_overlap')} / {reason_counts['cross_arm_author_overlap']}",
            "status": "pass" if all(r["randomisation_excluded"] and not r["email_sent_flag"] for r in exclusion_records if r["reason"] == "cross_arm_author_overlap") else "fail",
            "interpretation": "Cross-arm overlaps should be excluded during randomisation and never emailed.",
        },
        {
            "name": "Accepted assignments later excluded",
            "value": str(len(assigned_later_excluded)),
            "status": "review" if assigned_later_excluded else "pass",
            "interpretation": "These records entered the assigned cohort before a later exclusion and require case-level review.",
        },
        {
            "name": "Verified emails later excluded",
            "value": str(len(emailed_then_excluded)),
            "status": "review" if emailed_then_excluded else "pass",
            "interpretation": "These authors received a notification before the record was later excluded.",
        },
        {
            "name": "Sent-state archive anomalies",
            "value": str(len(sent_state_anomalies)),
            "status": "fail" if sent_state_anomalies else "pass",
            "interpretation": "These records are marked sent but have no corresponding Gmail message.",
        },
    ]

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "matched_excluded": exclusion_records,
        "assigned_exceptions": assigned_exceptions,
        "reason_counts": reason_counts,
        "stage_counts": stage_counts,
        "assigned_exception_states": assigned_exception_states,
        "matched_later_excluded": matched_later_excluded,
        "assigned_later_excluded": assigned_later_excluded,
        "emailed_then_excluded": emailed_then_excluded,
        "sent_state_anomalies": sent_state_anomalies,
        "checks": checks,
        "total_assigned": len(assigned_rows),
        "currently_assignable_assigned": len(assigned_rows) - len(assigned_exceptions),
        "randomisation_excluded": len(randomisation_excluded),
        "current_match_states": current_match_states,
        "current_match_total": len(current_matches),
    }


def _bar_rows(counts: Counter, total: int) -> str:
    rows = []
    for reason, count in counts.most_common():
        width = count / total * 100 if total else 0
        rows.append(
            f'<div class="bar-row"><div class="bar-label">{_escape(_reason_label(reason))}</div>'
            f'<div class="bar-track"><span style="width:{width:.2f}%"></span></div>'
            f'<div class="bar-value">{count}</div></div>'
        )
    return "".join(rows)


def _check_rows(checks: Sequence[Mapping[str, Any]]) -> str:
    return "".join(
        f'<tr><td><span class="status {check["status"]}">{_escape(check["status"])}</span></td>'
        f'<td><strong>{_escape(check["name"])}</strong><small>{_escape(check["interpretation"])}</small></td>'
        f'<td class="num">{_escape(check["value"])}</td></tr>'
        for check in checks
    )


def _exclusion_rows(records: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    for record in sorted(records, key=lambda x: (x["reason"], x["osf_id"])):
        state = "Later exclusion" if record["historically_assigned"] else "Before accepted assignment"
        rows.append(
            f'<tr data-reason="{_escape(record["reason"])}" data-state="{_escape(state)}">'
            f'<td><a href="{_escape(record["url"])}">{_escape(record["osf_id"])}</a>'
            f'<small>{_escape(record["title"])}</small></td>'
            f'<td>{_escape(record["reason_label"])}</td><td>{_escape(record["stage"])}</td>'
            f'<td>{_escape(state)}</td><td>{_escape(record["arm"])}</td>'
            f'<td class="num">{record["match_count"]}</td><td class="num">{record["contact_count"]}</td>'
            f'<td>{"Yes" if record["email_archive_verified"] else "No"}</td>'
            f'<td>{_escape(_date(record["excluded_at"]))}</td></tr>'
        )
    return "".join(rows)


def _exception_rows(records: Sequence[Mapping[str, Any]]) -> str:
    rows = []
    priority_order = {"Urgent": 0, "Resolve": 1, "Adjudicate": 2, "Review": 3, "None": 4}
    for record in sorted(
        records,
        key=lambda x: (priority_order.get(x["priority"], 9), x["state"], x["osf_id"]),
    ):
        rows.append(
            f'<tr><td><a href="{_escape(record["url"])}">{_escape(record["osf_id"])}</a>'
            f'<small>{_escape(record["title"])}</small></td><td>{_escape(record["arm"])}</td>'
            f'<td>{_escape(record["state"])}</td><td>{_escape(_reason_label(record["reason"]))}</td>'
            f'<td><span class="priority {_escape(record["priority"].lower())}">{_escape(record["priority"])}</span>'
            f'<small>{_escape(record["decision"])}</small></td>'
            f'<td>{"Verified" if record["email_archive_verified"] else "No verified message" if record["email_sent_flag"] else "Not sent"}</td>'
            f'<td>{_escape(_date(record.get("assigned_at")))}</td>'
            f'<td>{_escape(_date(record.get("excluded_at")))}</td></tr>'
        )
    return "".join(rows)


def render_html(audit: Mapping[str, Any]) -> str:
    total_excluded = len(audit["matched_excluded"])
    later = len(audit["assigned_later_excluded"])
    matched_later = len(audit["matched_later_excluded"])
    exclusion_only = total_excluded - matched_later
    exceptions = len(audit["assigned_exceptions"])
    decision_only = exceptions - matched_later
    emailed_later_excluded = len(audit["emailed_then_excluded"])
    current = audit["current_match_states"]
    current_total = audit["current_match_total"]
    later_reason_counts = Counter(record["reason"] for record in audit["assigned_later_excluded"])
    later_reason_text = ", ".join(
        f"{count} {_reason_label(reason).lower()}"
        for reason, count in later_reason_counts.most_common()
    )
    verified_later = sum(record["email_archive_verified"] for record in audit["assigned_later_excluded"])
    not_sent_later = sum(not record["email_sent_flag"] for record in audit["assigned_later_excluded"])
    no_longer_match_and_excluded = later - matched_later
    routine_reasons = {
        "superseded_by_newer_version",
        "no_author_contacts_extracted",
        "ingest_date_window",
        "cross_arm_author_overlap",
        "ingest_not_latest_version",
    }
    routine_exclusions = sum(
        count for reason, count in audit["reason_counts"].items() if reason in routine_reasons
    )
    substantive_exclusions = total_excluded - routine_exclusions
    reason_options = "".join(
        f'<option value="{_escape(reason)}">{_escape(_reason_label(reason))} ({count})</option>'
        for reason, count in audit["reason_counts"].most_common()
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FLoRA-Notify exclusion audit</title>
<style>
:root{{--paper:#f6f8f7;--ink:#17242b;--muted:#59676d;--line:#cbd5d7;--blue:#324d73;--blue-soft:#e5edf4;--amber:#9a6417;--amber-soft:#fff2d7;--red:#9d372f;--red-soft:#fbe8e5;--green:#25684f;--green-soft:#e2f1e9}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--paper);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;line-height:1.55}}
main{{max-width:1120px;margin:auto;padding:54px 28px 90px}} header{{border-top:7px solid var(--blue);padding-top:26px;margin-bottom:42px}}
h1,h2{{font-family:Charter,"Bitstream Charter",Georgia,serif;letter-spacing:-.025em}} h1{{font-size:clamp(2.5rem,6vw,5rem);line-height:.98;max-width:850px;margin:.15em 0 .28em}} h2{{font-size:2rem;margin:2.2em 0 .55em}} h3{{font-size:1.06rem;margin:0 0 .4em}}
p{{max-width:76ch}} .meta{{color:var(--muted);font-size:.88rem}} .lede{{font-family:Charter,Georgia,serif;font-size:1.35rem;max-width:790px}}
.verdict{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:1px;background:var(--line);border:1px solid var(--line);margin:30px 0}}
.verdict>div{{background:white;padding:22px}} .verdict strong{{display:block;font:700 2.35rem/1 Charter,Georgia,serif;margin-bottom:7px}} .verdict .primary{{background:var(--blue);color:white}}
.reconcile{{display:flex;height:62px;border-radius:4px;overflow:hidden;margin:22px 0 10px;color:white;font-weight:700}} .reconcile span{{display:flex;align-items:center;justify-content:center;min-width:2px}}
.assignable{{background:var(--green)}} .excluded{{background:var(--blue)}} .pending{{background:var(--amber)}} .legend{{display:flex;gap:20px;flex-wrap:wrap;color:var(--muted);font-size:.9rem}} .dot{{width:10px;height:10px;display:inline-block;border-radius:50%;margin-right:6px}}
.bar-row{{display:grid;grid-template-columns:minmax(180px,280px) 1fr 40px;gap:12px;align-items:center;margin:10px 0}} .bar-label{{font-size:.92rem}} .bar-track{{height:12px;background:#e2e7e7;border-radius:10px;overflow:hidden}} .bar-track span{{display:block;height:100%;background:var(--blue)}} .bar-value{{font-variant-numeric:tabular-nums;text-align:right}}
.callout{{border-left:5px solid var(--amber);background:var(--amber-soft);padding:18px 20px;margin:24px 0}} .callout.danger{{border-color:var(--red);background:var(--red-soft)}}
.table-wrap{{overflow:auto;border:1px solid var(--line);background:white}} table{{width:100%;border-collapse:collapse;font-size:.9rem}} th,td{{padding:11px 13px;border-bottom:1px solid #e2e7e7;text-align:left;vertical-align:top}} th{{position:sticky;top:0;background:#edf1f1;color:#39494f;font-weight:650}} td.num{{text-align:right;font-variant-numeric:tabular-nums}} td small{{display:block;color:var(--muted);max-width:52ch;margin-top:3px}} a{{color:#244e80;text-decoration-thickness:1px;text-underline-offset:2px}}
.status{{display:inline-block;border-radius:99px;padding:3px 9px;font-size:.75rem;font-weight:750;text-transform:uppercase;letter-spacing:.04em}} .status.pass{{background:var(--green-soft);color:var(--green)}} .status.review{{background:var(--amber-soft);color:var(--amber)}} .status.fail{{background:var(--red-soft);color:var(--red)}}
.priority{{display:inline-block;border-radius:3px;padding:2px 7px;margin-bottom:4px;font-size:.72rem;font-weight:780;text-transform:uppercase;letter-spacing:.04em;background:var(--blue-soft);color:var(--blue)}} .priority.urgent,.priority.adjudicate{{background:var(--red-soft);color:var(--red)}} .priority.resolve{{background:var(--amber-soft);color:var(--amber)}}
.sets{{display:grid;grid-template-columns:minmax(220px,1fr) minmax(140px,.55fr) minmax(220px,1fr);gap:8px;margin:22px 0}} .set{{padding:20px;background:white;border:1px solid var(--line)}} .set strong{{display:block;font:700 2rem/1 Charter,Georgia,serif;margin-bottom:6px}} .set small{{display:block;color:var(--muted)}} .set.overlap{{background:var(--amber-soft);border-color:#dfc184}}
.filters{{display:flex;gap:10px;flex-wrap:wrap;margin:14px 0}} select,input{{font:inherit;padding:9px 11px;border:1px solid #aebabc;border-radius:4px;background:white}} input{{min-width:250px;flex:1}} details{{margin-top:25px}} summary{{cursor:pointer;font-weight:700;color:var(--blue)}} footer{{margin-top:60px;padding-top:18px;border-top:1px solid var(--line);color:var(--muted);font-size:.85rem}}
@media(max-width:720px){{main{{padding:30px 16px 70px}}.verdict,.sets{{grid-template-columns:1fr}}.bar-row{{grid-template-columns:150px 1fr 32px}}th,td{{padding:9px}}}}
@media(prefers-reduced-motion:reduce){{*{{scroll-behavior:auto!important}}}}
</style></head><body><main>
<header><div class="meta">FLoRA-Notify · exclusion integrity review · generated {audit['generated_at']}</div>
<h1>The {exceptions} changed assigned records are the decision set</h1>
<p class="lede">The {total_excluded} current matched exclusions describe screening flow. The {exceptions} historical assignments that are no longer currently assignable are the records that can change cohort handling and therefore need decisions. The groups overlap by only {matched_later} records.</p>
<div class="verdict"><div class="primary"><strong>{exceptions}</strong>assigned records need review</div><div><strong>{later}</strong>now explicitly excluded</div><div><strong>{emailed_later_excluded}</strong>verified emails then excluded</div><div><strong>{len(audit['sent_state_anomalies'])}</strong>sent-state anomalies</div></div></header>

<section><h2>Two populations, two purposes</h2><p>The counts should not be compared as if one were a subset of the other. The {total_excluded} is a current screening-state count; the {exceptions} is a historical-cohort audit. Their exact relationship is:</p>
<div class="sets"><div class="set"><strong>{exclusion_only}</strong>in the {total_excluded} only<small>Currently matched and excluded, never accepted into the assigned cohort.</small></div><div class="set overlap"><strong>{matched_later}</strong>in both groups<small>Assigned, still a FLoRA match, and now excluded.</small></div><div class="set"><strong>{decision_only}</strong>in the {exceptions} only<small>Historical assignments whose current state changed for another reason, including {no_longer_match_and_excluded} excluded records that are no longer FLoRA-eligible.</small></div></div>
<div class="callout"><h3>Where choices are required</h3><p>Review all {exceptions} historical exceptions below. Resolve the missing record and pending validations first; then adjudicate the {later} explicit post-assignment exclusions and document how changed eligibility affects the remaining assigned records.</p></div></section>

<section><h2>How {current_total:,} current matches reconcile</h2><p>The dashboard now uses mutually exclusive states. This prevents contactless records that were already excluded from being subtracted twice.</p>
<div class="reconcile"><span class="assignable" style="width:{current['assignable']/current_total*100:.2f}%">{current['assignable']:,} assignable</span><span class="excluded" style="width:{current['excluded']/current_total*100:.2f}%">{current['excluded']:,} excluded</span><span class="pending" style="width:{current['validation_pending']/current_total*100:.2f}%" title="{current['validation_pending']} pending"></span></div>
<div class="legend"><span><i class="dot assignable"></i>Currently assignable: {current['assignable']:,}</span><span><i class="dot excluded"></i>Excluded: {current['excluded']:,}</span><span><i class="dot pending"></i>Pending validation: {current['validation_pending']:,}</span><span>Active without contact: {current['missing_email']:,}</span></div></section>

<section><h2>Screening audit: why the {total_excluded} were excluded</h2><p>This section validates pipeline screening; it is not the main decision list. Version control, absent contacts, the registered date window, and the cross-arm safeguard account for {routine_exclusions} of {total_excluded} exclusions. {substantive_exclusions} followed substantive post-processing review.</p>{_bar_rows(audit['reason_counts'], total_excluded)}</section>

<section><h2>Integrity checks</h2><div class="table-wrap"><table><thead><tr><th>Result</th><th>Check</th><th>Observed</th></tr></thead><tbody>{_check_rows(audit['checks'])}</tbody></table></div></section>

<section><h2>Decision register: all {exceptions} changed assignments</h2><p>{audit['total_assigned']:,} preprints were historically assigned. Of these, {audit['currently_assignable_assigned']:,} remain currently assignable and {exceptions} do not. The {exceptions} explain the difference between 1,777 historical assignments and 1,729 current assignable records.</p>
<div class="callout"><h3>These are cohort decisions, not unassigned cases</h3><p>{'; '.join(f'{count} {_escape(state).lower()}' for state,count in audit['assigned_exception_states'].most_common())}. Historical assignment remains in the audit trail; the decision is how each changed record is handled in analysis and reporting.</p></div>
<p><strong>Protocol anchor.</strong> The primary analysis is intention-to-treat among randomised, eligible, contactable preprints. The protocol separately specifies exclusion of withdrawn or removed preprints at follow-up and postprints identified during outcome collection. Each changed record therefore needs an explicit, documented mapping to a prespecified rule; it should not disappear from the cohort solely because its current database state changed.</p>
<div class="table-wrap"><table><thead><tr><th>Preprint</th><th>Arm</th><th>Current state</th><th>Reason if excluded</th><th>Decision needed</th><th>Email archive</th><th>Assigned</th><th>Excluded</th></tr></thead><tbody>{_exception_rows(audit['assigned_exceptions'])}</tbody></table></div></section>

<section><h2>Case review: accepted assignments later excluded</h2><p>These {later} records are the subset that warrants substantive review: {_escape(later_reason_text)}. {matched_later} remain marked as FLoRA matches; {later-matched_later} are no longer FLoRA-eligible. {verified_later} records have a verified sent email, while {not_sent_later} were not emailed.</p>
<div class="table-wrap"><table><thead><tr><th>Preprint</th><th>Arm</th><th>Current state</th><th>Reason</th><th>Decision needed</th><th>Email archive</th><th>Assigned</th><th>Excluded</th></tr></thead><tbody>{_exception_rows(audit['assigned_later_excluded'])}</tbody></table></div></section>

<section><h2>Operational audit: sent-state anomalies</h2><p>These {len(audit['sent_state_anomalies'])} records are separate from the 48-record cohort decision set unless their eligibility state also changed. DynamoDB marks them as sent, but no corresponding Gmail message exists. They should not be counted as delivered or re-sent without resolving the duplicated Message-ID history.</p>
<div class="table-wrap"><table><thead><tr><th>Preprint</th><th>Arm</th><th>Current state</th><th>Reason</th><th>Action needed</th><th>Email archive</th><th>Assigned</th><th>Excluded</th></tr></thead><tbody>{_exception_rows(audit['sent_state_anomalies'])}</tbody></table></div></section>

<details><summary>Inspect all {total_excluded} matched-but-excluded records</summary><div class="filters"><select id="reason"><option value="">All reasons</option>{reason_options}</select><select id="state"><option value="">All timing states</option><option>Later exclusion</option><option>Before accepted assignment</option></select><input id="search" type="search" placeholder="Search public OSF ID or title"></div>
<div class="table-wrap"><table id="all-records"><thead><tr><th>Preprint</th><th>Reason</th><th>Stage</th><th>Assignment timing</th><th>Arm</th><th>Matches</th><th>Contacts</th><th>Verified email</th><th>Excluded</th></tr></thead><tbody>{_exclusion_rows(audit['matched_excluded'])}</tbody></table></div></details>

<footer>Source: production DynamoDB preprint, exclusion, and assignment tables, plus immutable Gmail-sent snapshots. No recipient addresses or message bodies appear in this report. Counts describe current production state at generation time.</footer>
</main><script>
const reason=document.querySelector('#reason'),state=document.querySelector('#state'),search=document.querySelector('#search');
function filterRows(){{const q=search.value.trim().toLowerCase();document.querySelectorAll('#all-records tbody tr').forEach(row=>{{const show=(!reason.value||row.dataset.reason===reason.value)&&(!state.value||row.dataset.state===state.value)&&(!q||row.textContent.toLowerCase().includes(q));row.hidden=!show;}})}}
[reason,state,search].forEach(el=>el.addEventListener('input',filterRows));
</script></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", default="validation/exclusion_audit_2026-09-15.html")
    args = parser.parse_args()
    audit = collect_audit()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_html(audit), encoding="utf-8")
    print(f"Audit report written to {output}")
    print(f"Matched-but-excluded records: {len(audit['matched_excluded'])}")
    print(f"Historical assignments not currently assignable: {len(audit['assigned_exceptions'])}")
    print(f"Accepted assignments later excluded: {len(audit['assigned_later_excluded'])}")
    print(f"Verified emails later excluded: {len(audit['emailed_then_excluded'])}")
    print(f"Sent-state anomalies: {len(audit['sent_state_anomalies'])}")


if __name__ == "__main__":
    main()
