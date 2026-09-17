#!/usr/bin/env python3
"""Repair audited post-assignment state drift without sending email.

The script is dry-run by default.  ``--apply`` performs only conditional,
ID-scoped writes after rechecking every invariant against production data and
the current official FLoRA export.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from osf_sync.augmentation.flora_original_lookup import (  # noqa: E402
    _ensure_fresh_flora_csv,
    _load_flora_pairs_by_original,
    _resolve_flora_csv_path,
    normalize_doi,
)
from osf_sync.dynamo.preprints_repo import PreprintsRepo  # noqa: E402
from osf_sync.fetch_one import fetch_preprint_by_id, upsert_one_preprint  # noqa: E402


EMAILED_FLORA_IDS = (
    "2kfw3_v2", "352qa_v1", "7ha9w_v1", "97tkx_v1", "9rjux_v1",
    "9w3px_v1", "aj6my_v1", "d2zac_v1", "efj2t_v1", "fup3k_v1",
    "hp5tn_v1", "j7m4x_v1", "k8n7c_v2", "kaje7_v1", "kuem5_v1",
    "p6b2e_v1", "pr4u6_v5", "q8xdy_v1", "q9rkn_v1", "ur9dk_v3",
    "xa5v2_v1", "y9f8w_v1", "yfmgk_v1", "za7yx_v1", "zhx8k_v3",
)

STALE_CONTROL_IDS = (
    "cg4h2_v1", "jwmqv_v2", "k8se5_v2", "qayk9_v1", "tehqg_v2", "zkc65_v1",
)

POST_VALIDATION_FALSE_IDS = ("3aksc_v1", "txhgr_v4", "yc6wn_v1")

DT_RESTORE_ID = "dt7gh_v2"
BOUNCE_READMIT_ID = "m7d4j_v1"
DROP_ID = "d8me6_v1"
DROP_REASON = "unverifiable_historical_flora_assignment"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _query_refs(repo: PreprintsRepo, osf_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": "osf_id = :oid",
            "ExpressionAttributeValues": {":oid": osf_id},
            "ConsistentRead": True,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        response = repo.t_refs.query(**kwargs)
        rows.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return rows


def _get_preprint(repo: PreprintsRepo, osf_id: str) -> dict[str, Any]:
    return repo.t_preprints.get_item(
        Key={"osf_id": osf_id}, ConsistentRead=True
    ).get("Item") or {}


def _get_exclusion(repo: PreprintsRepo, osf_id: str) -> dict[str, Any]:
    return repo.t_excluded.get_item(
        Key={"osf_id": osf_id}, ConsistentRead=True
    ).get("Item") or {}


def _restore_emailed_flora(
    repo: PreprintsRepo,
    flora: dict[str, list[dict[str, Any]]],
    *,
    apply: bool,
    now: str,
) -> tuple[int, int]:
    preprints = 0
    refs_restored = 0
    for osf_id in EMAILED_FLORA_IDS:
        item = _get_preprint(repo, osf_id)
        _require(item.get("email_sent") is True, f"{osf_id}: expected email_sent=True")
        _require(item.get("trial_arm") == "treatment", f"{osf_id}: expected treatment arm")
        snapshots = item.get("email_originals") or []
        _require(bool(snapshots), f"{osf_id}: immutable email snapshot missing")
        refs = _query_refs(repo, osf_id)
        refs_by_doi: dict[str, list[dict[str, Any]]] = {}
        for ref in refs:
            if doi := normalize_doi(ref.get("doi")):
                refs_by_doi.setdefault(doi, []).append(ref)

        repaired_for_preprint = 0
        for original in snapshots:
            doi_o = normalize_doi(original.get("doi"))
            _require(bool(doi_o), f"{osf_id}: snapshot original DOI missing")
            matching_refs = refs_by_doi.get(str(doi_o), [])
            _require(len(matching_refs) == 1, f"{osf_id}: expected one current ref for {doi_o}")
            latest_pairs = flora.get(str(doi_o)) or []
            latest_by_r = {
                normalize_doi(pair.get("doi_r")): pair
                for pair in latest_pairs
                if normalize_doi(pair.get("doi_r"))
            }
            snapshot_replications = original.get("replications") or []
            pair_rows: list[dict[str, Any]] = []
            for replication in snapshot_replications:
                doi_r = normalize_doi(replication.get("doi"))
                _require(bool(doi_r), f"{osf_id}: snapshot replication DOI missing")
                _require(doi_r in latest_by_r, f"{osf_id}: emailed pair no longer in current FLoRA")
                pair_rows.append(latest_by_r[str(doi_r)])
            _require(bool(pair_rows), f"{osf_id}: snapshot has no current FLoRA replication")
            ref = matching_refs[0]
            if apply:
                repo.t_refs.update_item(
                    Key={"osf_id": osf_id, "ref_id": ref["ref_id"]},
                    ConditionExpression="attribute_exists(osf_id) AND doi = :doi",
                    UpdateExpression=(
                        "SET flora_ref_pairs=:pairs, flora_ref_pairs_count=:count, "
                        "flora_replication_cited=:false, flora_screened_at=:now, updated_at=:now"
                    ),
                    ExpressionAttributeValues={
                        ":doi": ref["doi"], ":pairs": pair_rows, ":count": len(pair_rows),
                        ":false": False, ":now": now,
                    },
                )
            repaired_for_preprint += 1
            refs_restored += 1

        if apply:
            repo.t_preprints.update_item(
                Key={"osf_id": osf_id},
                ConditionExpression="email_sent = :true AND trial_arm = :treatment",
                UpdateExpression=(
                    "SET flora_eligible=:true, flora_eligible_count=:count, "
                    "flora_last_checked=:now, updated_at=:now"
                ),
                ExpressionAttributeValues={
                    ":true": True, ":treatment": "treatment",
                    ":count": repaired_for_preprint, ":now": now,
                },
            )
        preprints += 1
    return preprints, refs_restored


def _clear_stale_controls(
    repo: PreprintsRepo,
    flora: dict[str, list[dict[str, Any]]],
    *,
    apply: bool,
    now: str,
) -> tuple[int, int]:
    refs_cleared = 0
    for osf_id in STALE_CONTROL_IDS:
        item = _get_preprint(repo, osf_id)
        _require(item.get("trial_arm") == "control", f"{osf_id}: expected control arm")
        _require(item.get("email_sent") is not True, f"{osf_id}: unexpectedly emailed")
        refs = _query_refs(repo, osf_id)
        current_matches = [r for r in refs if normalize_doi(r.get("doi")) in flora]
        _require(not current_matches, f"{osf_id}: now has a current FLoRA match")
        stale_refs = [r for r in refs if r.get("flora_ref_pairs")]
        _require(bool(stale_refs), f"{osf_id}: expected stale FLoRA reference state")
        for ref in stale_refs:
            if apply:
                repo.t_refs.update_item(
                    Key={"osf_id": osf_id, "ref_id": ref["ref_id"]},
                    ConditionExpression="attribute_exists(flora_ref_pairs)",
                    UpdateExpression=(
                        "SET updated_at=:now REMOVE flora_ref_pairs, flora_ref_pairs_count, "
                        "flora_replication_cited, flora_screened_at, citation_distance, "
                        "citation_apa_resolved, citation_validation_status, "
                        "citation_validation_updated_at"
                    ),
                    ExpressionAttributeValues={":now": now},
                )
            refs_cleared += 1
        if apply:
            repo.t_preprints.update_item(
                Key={"osf_id": osf_id},
                ConditionExpression="trial_arm = :control AND (attribute_not_exists(email_sent) OR email_sent = :false)",
                UpdateExpression=(
                    "SET flora_eligible=:false, flora_eligible_count=:zero, "
                    "flora_last_checked=:now, updated_at=:now"
                ),
                ExpressionAttributeValues={
                    ":control": "control", ":false": False, ":zero": 0, ":now": now,
                },
            )
    return len(STALE_CONTROL_IDS), refs_cleared


def _repair_post_validation(repo: PreprintsRepo, *, apply: bool, now: str) -> int:
    for osf_id in POST_VALIDATION_FALSE_IDS:
        item = _get_preprint(repo, osf_id)
        exclusion = _get_exclusion(repo, osf_id)
        _require(item.get("trial_assignment_status") == "assigned", f"{osf_id}: not assigned")
        _require(
            exclusion.get("exclusion_reason") == "non_real_match_post_validation",
            f"{osf_id}: post-validation exclusion missing",
        )
        if apply:
            repo.t_preprints.update_item(
                Key={"osf_id": osf_id},
                ConditionExpression="excluded_reason = :reason",
                UpdateExpression=(
                    "SET flora_eligible=:false, flora_eligible_count=:zero, updated_at=:now"
                ),
                ExpressionAttributeValues={
                    ":reason": "non_real_match_post_validation", ":false": False,
                    ":zero": 0, ":now": now,
                },
            )
    return len(POST_VALIDATION_FALSE_IDS)


def _readmit_bounced_record(repo: PreprintsRepo, *, apply: bool, now: str) -> int:
    item = _get_preprint(repo, BOUNCE_READMIT_ID)
    exclusion = _get_exclusion(repo, BOUNCE_READMIT_ID)
    _require(item.get("email_sent") is True, f"{BOUNCE_READMIT_ID}: email was not sent")
    _require(
        item.get("excluded_reason") == "no_author_contacts_extracted"
        and exclusion.get("exclusion_reason") == "no_author_contacts_extracted",
        f"{BOUNCE_READMIT_ID}: expected author-contact exclusion",
    )
    if apply:
        repo.t_preprints.update_item(
            Key={"osf_id": BOUNCE_READMIT_ID},
            ConditionExpression="email_sent = :true AND excluded_reason = :reason",
            UpdateExpression=(
                "SET excluded=:false, updated_at=:now REMOVE excluded_reason, excluded_at, "
                "excluded_date, excluded_stage"
            ),
            ExpressionAttributeValues={
                ":true": True, ":false": False,
                ":reason": "no_author_contacts_extracted", ":now": now,
            },
        )
        repo.t_excluded.delete_item(
            Key={"osf_id": BOUNCE_READMIT_ID},
            ConditionExpression="exclusion_reason = :reason",
            ExpressionAttributeValues={":reason": "no_author_contacts_extracted"},
        )
    return 1


def _restore_orphaned_record(repo: PreprintsRepo, *, apply: bool, now: str) -> int:
    _require(not _get_preprint(repo, DT_RESTORE_ID), f"{DT_RESTORE_ID}: preprint row already exists")
    exclusion = _get_exclusion(repo, DT_RESTORE_ID)
    _require(
        exclusion.get("exclusion_reason") == "docx_to_pdf_conversion_failed",
        f"{DT_RESTORE_ID}: unexpected exclusion state",
    )
    assignment = repo.t_trial_assignments.get_item(
        Key={"preprint_id": DT_RESTORE_ID}, ConsistentRead=True
    ).get("Item") or {}
    _require(assignment.get("status") == "assigned", f"{DT_RESTORE_ID}: assignment missing")
    _require(assignment.get("arm") == "treatment", f"{DT_RESTORE_ID}: arm changed")
    refs = _query_refs(repo, DT_RESTORE_ID)
    eligible_refs = [
        r for r in refs
        if r.get("flora_ref_pairs") and r.get("flora_replication_cited") is False
    ]
    _require(len(eligible_refs) == 1, f"{DT_RESTORE_ID}: expected one preserved eligible ref")
    osf_record = fetch_preprint_by_id(DT_RESTORE_ID)
    _require(bool(osf_record), f"{DT_RESTORE_ID}: no longer available from OSF")

    if apply:
        _require(upsert_one_preprint(osf_record) == 1, f"{DT_RESTORE_ID}: OSF upsert failed")
        repo.t_preprints.update_item(
            Key={"osf_id": DT_RESTORE_ID},
            ConditionExpression="attribute_exists(osf_id) AND attribute_not_exists(email_sent)",
            UpdateExpression=(
                "SET trial_assignment_status=:status, trial_arm=:arm, "
                "trial_cluster_id=:cluster, trial_assigned_at=:assigned, "
                "trial_assignment_run_id=:run, trial_matched_cluster_ids=:matched, "
                "flora_eligible=:true, flora_eligible_count=:one, "
                "manual_review_hold=:true, manual_review_reason=:review_reason, "
                "excluded=:false, restored_at=:now, updated_at=:now "
                "REMOVE excluded_reason, excluded_at, excluded_date, excluded_stage, "
                "queue_pdf, queue_grobid, queue_extract, queue_email, "
                "claim_pdf_owner, claim_pdf_until, claim_grobid_owner, claim_grobid_until, "
                "claim_extract_owner, claim_extract_until, claim_email_owner, claim_email_until"
            ),
            ExpressionAttributeValues={
                ":status": assignment["status"], ":arm": assignment["arm"],
                ":cluster": assignment["cluster_id"], ":assigned": assignment["assigned_at"],
                ":run": assignment["run_id"],
                ":matched": assignment.get("matched_cluster_ids") or [],
                ":true": True, ":false": False, ":one": 1,
                ":review_reason": "restored_after_transient_docx_conversion_failure",
                ":now": now,
            },
        )
        repo.t_excluded.delete_item(
            Key={"osf_id": DT_RESTORE_ID},
            ConditionExpression="exclusion_reason = :reason",
            ExpressionAttributeValues={":reason": "docx_to_pdf_conversion_failed"},
        )
    return 1


def _drop_unverifiable_assignment(repo: PreprintsRepo, *, apply: bool) -> int:
    item = _get_preprint(repo, DROP_ID)
    _require(item.get("trial_assignment_status") == "assigned", f"{DROP_ID}: not assigned")
    _require(item.get("trial_arm") == "control", f"{DROP_ID}: expected control arm")
    _require(item.get("email_sent") is not True, f"{DROP_ID}: unexpectedly emailed")
    _require(item.get("flora_eligible") is False, f"{DROP_ID}: unexpectedly eligible")
    _require(not _get_exclusion(repo, DROP_ID), f"{DROP_ID}: already excluded")
    if apply:
        marked = repo.mark_preprint_excluded(
            osf_id=DROP_ID,
            reason=DROP_REASON,
            stage="post_assignment_audit",
            details={
                "email_sent": False,
                "audit_conclusion": "No current or reconstructable historical FLoRA match",
            },
        )
        _require(marked, f"{DROP_ID}: exclusion write was not accepted")
    return 1


def _verify(repo: PreprintsRepo) -> None:
    for osf_id in EMAILED_FLORA_IDS:
        item = _get_preprint(repo, osf_id)
        _require(item.get("flora_eligible") is True, f"{osf_id}: eligibility not restored")
    for osf_id in STALE_CONTROL_IDS + POST_VALIDATION_FALSE_IDS:
        item = _get_preprint(repo, osf_id)
        _require(item.get("flora_eligible") is False, f"{osf_id}: should be ineligible")
    bounced = _get_preprint(repo, BOUNCE_READMIT_ID)
    _require(bounced.get("excluded") is False, f"{BOUNCE_READMIT_ID}: still excluded")
    _require(not _get_exclusion(repo, BOUNCE_READMIT_ID), f"{BOUNCE_READMIT_ID}: registry remains")
    restored = _get_preprint(repo, DT_RESTORE_ID)
    _require(restored.get("manual_review_hold") is True, f"{DT_RESTORE_ID}: hold missing")
    _require(restored.get("trial_assignment_status") == "assigned", f"{DT_RESTORE_ID}: assignment missing")
    dropped = _get_preprint(repo, DROP_ID)
    _require(dropped.get("excluded_reason") == DROP_REASON, f"{DROP_ID}: drop not recorded")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply conditional production writes")
    args = parser.parse_args()

    repo = PreprintsRepo()
    flora_path = _resolve_flora_csv_path(None)
    _ensure_fresh_flora_csv(flora_path)
    flora = _load_flora_pairs_by_original(flora_path)
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    print("mode:", "APPLY" if args.apply else "DRY RUN")
    emailed, restored_refs = _restore_emailed_flora(repo, flora, apply=args.apply, now=now)
    controls, cleared_refs = _clear_stale_controls(repo, flora, apply=args.apply, now=now)
    validated = _repair_post_validation(repo, apply=args.apply, now=now)
    bounced = _readmit_bounced_record(repo, apply=args.apply, now=now)
    orphaned = _restore_orphaned_record(repo, apply=args.apply, now=now)
    dropped = _drop_unverifiable_assignment(repo, apply=args.apply)
    print(f"emailed FLoRA restored: {emailed} preprints / {restored_refs} references")
    print(f"stale control state cleared: {controls} preprints / {cleared_refs} references")
    print(f"post-validation flags corrected: {validated}")
    print(f"bounced sent record readmitted: {bounced}")
    print(f"orphaned record restored with hold: {orphaned}")
    print(f"unverifiable control assignment dropped: {dropped}")
    if args.apply:
        _verify(repo)
        print("post-write verification: PASS")
    else:
        print("dry-run invariant checks: PASS; no database writes performed")


if __name__ == "__main__":
    try:
        main()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise SystemExit(f"AWS write/check failed: {code}") from exc
