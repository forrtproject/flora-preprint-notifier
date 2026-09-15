from .client import get_dynamo_resource
from ..logging_setup import get_logger, with_extras
from ..version_utils import parse_version, sibling_ids
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from typing import List, Dict, Optional, Any, Iterable, Set
import datetime as dt
import functools
import os
import re
import urllib.parse

import requests


def _strip_nones(d: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of dict without None values (DynamoDB rejects None)."""
    return {k: v for k, v in d.items() if v is not None}


_DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)
_CROSSREF_TRANSFORM_DOI_RE = re.compile(r"/works/([^/]+)/transform", re.IGNORECASE)
_CANONICALIZE_ALIAS_PREFIXES = tuple(
    p.strip().lower()
    for p in os.environ.get("DOI_CANONICALIZE_ALIAS_PREFIXES", "10.2307/").split(",")
    if p.strip()
)
_CANONICALIZE_TIMEOUT_SECONDS = float(os.environ.get("DOI_CANONICALIZE_TIMEOUT_SECONDS", "8"))


@functools.lru_cache(maxsize=8192)
def _canonicalize_alias_doi(doi: str) -> str:
    """
    Canonicalize alias-style DOIs via doi.org redirect target when available.

    This is prefix-gated to avoid expensive per-DOI network requests.
    """
    value = str(doi or "").strip().lower()
    if not value:
        return value
    if _CANONICALIZE_ALIAS_PREFIXES and not any(value.startswith(p) for p in _CANONICALIZE_ALIAS_PREFIXES):
        return value
    try:
        safe = urllib.parse.quote(value, safe="")
        resp = requests.get(
            f"https://doi.org/{safe}",
            headers={"Accept": "text/x-bibliography; style=apa"},
            allow_redirects=True,
            timeout=(4, max(4.0, _CANONICALIZE_TIMEOUT_SECONDS)),
        )
        final_url = str(resp.url or "")
    except Exception:
        return value
    match = _CROSSREF_TRANSFORM_DOI_RE.search(final_url)
    if not match:
        return value
    try:
        canonical = urllib.parse.unquote(match.group(1)).strip().lower()
    except Exception:
        return value
    if canonical.startswith("10.") and _DOI_RE.fullmatch(canonical):
        return canonical
    return value


def _normalize_reference_doi(value: Any, *, source: Optional[str] = None) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    for pref in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "dx.doi.org/",
        "doi.org/",
        "doi:",
    ):
        if raw.startswith(pref):
            raw = raw[len(pref):]
            break
    first_doi = raw.find("10.")
    if first_doi == -1:
        return None
    raw = raw[first_doi:]
    m = _DOI_RE.search(raw)
    if m:
        raw = m.group(0)
    raw = raw.split("?", 1)[0].split("#", 1)[0].strip()
    raw = raw.strip(" \t\r\n\"'<>[]{}")
    source_key = (source or "").lower()
    if source_key not in {"crossref", "openalex"}:
        raw = raw.strip(".,;:")
        while raw.endswith(")") and raw.count("(") < raw.count(")"):
            raw = raw[:-1].rstrip()
    if not raw or not raw.startswith("10."):
        return None
    if not _DOI_RE.fullmatch(raw):
        return None
    if source_key in {"tei", "crossref", "openalex"}:
        raw = _canonicalize_alias_doi(raw)
        if not raw or not raw.startswith("10.") or not _DOI_RE.fullmatch(raw):
            return None
    return raw

log = get_logger(__name__)


_STAGE_CLAIM_FIELDS = {
    "pdf": ("queue_pdf", "claim_pdf_owner", "claim_pdf_until"),
    "grobid": ("queue_grobid", "claim_grobid_owner", "claim_grobid_until"),
    "extract": ("queue_extract", "claim_extract_owner", "claim_extract_until"),
}


class PreprintsRepo:
    def __init__(self):
        ddb = get_dynamo_resource()
        self.ddb = ddb
        self.t_preprints = ddb.Table(os.environ.get("DDB_TABLE_PREPRINTS", "preprints"))
        self.t_refs = ddb.Table(os.environ.get("DDB_TABLE_REFERENCES", "preprint_references"))
        self.flora_recheck_days = int(os.environ.get("DDB_REFS_FLORA_RECHECK_DAYS", "30"))
        self.t_tei = ddb.Table(os.environ.get("DDB_TABLE_TEI", "preprint_tei"))
        self.t_sync = ddb.Table(os.environ.get("DDB_TABLE_SYNCSTATE", "sync_state"))
        self.t_excluded = ddb.Table(os.environ.get("DDB_TABLE_EXCLUDED_PREPRINTS", "excluded_preprints"))
        self.t_trial_nodes = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_AUTHOR_NODES", "trial_author_nodes"))
        self.t_trial_tokens = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_AUTHOR_TOKENS", "trial_author_tokens"))
        self.t_trial_clusters = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_CLUSTERS", "trial_clusters"))
        self.t_trial_assignments = ddb.Table(os.environ.get("DDB_TABLE_TRIAL_ASSIGNMENTS", "trial_preprint_assignments"))

    # --- cursors (sync_state) ---
    def get_cursor(self, source_key: str) -> Optional[str]:
        r = self.t_sync.get_item(Key={"source_key": source_key}).get("Item")
        return r.get("last_seen_published") if r else None

    def set_cursor(self, source_key: str, last_seen_iso: str) -> None:
        now = dt.datetime.utcnow().isoformat()
        self.t_sync.put_item(Item={
            "source_key": source_key,
            "last_seen_published": last_seen_iso,
            "last_run_at": now
        })

    def get_sync_item(self, source_key: str) -> Dict[str, Any]:
        return self.t_sync.get_item(Key={"source_key": source_key}).get("Item") or {}

    def put_sync_item(self, item: Dict[str, Any]) -> None:
        self.t_sync.put_item(Item=item)

    def mark_preprint_excluded(
        self,
        *,
        osf_id: str,
        reason: str,
        stage: Optional[str] = None,
        occurred_at: Optional[dt.datetime] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> bool:
        osf_id_val = str(osf_id or "").strip()
        if not osf_id_val:
            raise ValueError("osf_id is required")
        reason_val = str(reason or "").strip()
        if not reason_val:
            raise ValueError("reason is required")
        ts = occurred_at or dt.datetime.now(dt.timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        else:
            ts = ts.astimezone(dt.timezone.utc)
        excluded_at = ts.isoformat()
        exclusion_date = ts.date().isoformat()
        item: Dict[str, Any] = {
            "osf_id": osf_id_val,
            "excluded_at": excluded_at,
            "exclusion_date": exclusion_date,
            "exclusion_reason": reason_val,
            "created_at": excluded_at,
            "updated_at": excluded_at,
        }
        if stage:
            item["exclusion_stage"] = stage
        if details:
            item["exclusion_details"] = details

        try:
            self.t_excluded.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(osf_id)",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

        expr_values = {
            ":true": True,
            ":reason": reason_val,
            ":at": excluded_at,
            ":date": exclusion_date,
            ":t": excluded_at,
        }
        set_exprs = [
            "excluded=:true",
            "excluded_reason=:reason",
            "excluded_at=:at",
            "excluded_date=:date",
            "updated_at=:t",
        ]
        if stage:
            expr_values[":stage"] = stage
            set_exprs.append("excluded_stage=:stage")

        # If the email was already sent, keep queue_email intact so the
        # dashboard count stays correct and we never lose send tracking.
        remove_exprs = [
            "queue_pdf", "queue_grobid", "queue_extract",
            "claim_pdf_owner", "claim_pdf_until",
            "claim_grobid_owner", "claim_grobid_until",
            "claim_extract_owner", "claim_extract_until",
        ]
        # Two-pass: first try with email_sent=True condition (preserve queue_email),
        # then fall back to removing queue_email (email not sent).
        for include_queue_email in (False, True):
            removes = list(remove_exprs)
            if include_queue_email:
                removes.append("queue_email")
            condition = "attribute_exists(osf_id)"
            if not include_queue_email:
                # Only match if email_sent IS true (preserve queue_email)
                condition += " AND email_sent = :true"
            try:
                self.t_preprints.update_item(
                    Key={"osf_id": osf_id_val},
                    ConditionExpression=condition,
                    UpdateExpression=(
                        "SET " + ", ".join(set_exprs) + " "
                        "REMOVE " + ", ".join(removes)
                    ),
                    ExpressionAttributeValues=expr_values,
                )
                if not include_queue_email:
                    with_extras(log, osf_id=osf_id_val, reason=reason_val).warning(
                        "excluding already-emailed preprint; preserving queue_email=done"
                    )
                break  # success
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code == "ConditionalCheckFailedException" and not include_queue_email:
                    continue  # email_sent != True, try fallback
                if code != "ConditionalCheckFailedException":
                    raise
                break  # item doesn't exist, nothing to update

        return True

    def is_preprint_excluded(self, osf_id: str) -> bool:
        if not osf_id:
            return False
        item = self.t_excluded.get_item(Key={"osf_id": osf_id}).get("Item")
        return bool(item)

    def summarize_excluded_preprints(self) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        total = 0
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {"ProjectionExpression": "osf_id, exclusion_reason"}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_excluded.scan(**kwargs)
            items = resp.get("Items", [])
            for item in items:
                total += 1
                reason = str(item.get("exclusion_reason") or "unknown")
                counts[reason] = counts.get(reason, 0) + 1
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return {"total_excluded_preprints": total, "by_reason": counts}

    # --- trial allocation state ---
    def select_unassigned_preprints(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "FilterExpression": Attr("trial_assignment_status").not_exists() & (
                    Attr("excluded").not_exists() | Attr("excluded").eq(False)
                ) & Attr("flora_eligible").eq(True),
                "ProjectionExpression": "osf_id, provider_id, date_created, date_published, author_email_candidates, flora_eligible",
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            if limit:
                kwargs["Limit"] = max(1, limit - len(out))

            resp = self.t_preprints.scan(**kwargs)
            out.extend(resp.get("Items", []))
            if limit and len(out) >= limit:
                return out[:limit]
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                return out

    def list_trial_clusters(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_trial_clusters.scan(**kwargs)
            out.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                return out

    def list_trial_nodes(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_trial_nodes.scan(**kwargs)
            out.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                return out

    def get_trial_nodes(self, node_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        wanted = [n for n in node_ids if n]
        if not wanted:
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for chunk in _chunks(wanted, 100):
            keys = [{"node_id": nid} for nid in chunk]
            request = {self.t_trial_nodes.name: {"Keys": keys}}
            resp = self.ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(self.t_trial_nodes.name, []):
                node_id = item.get("node_id")
                if node_id:
                    out[node_id] = item
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = self.ddb.batch_get_item(RequestItems=unprocessed)
                for item in resp.get("Responses", {}).get(self.t_trial_nodes.name, []):
                    node_id = item.get("node_id")
                    if node_id:
                        out[node_id] = item
                unprocessed = resp.get("UnprocessedKeys") or {}
        return out

    def get_trial_token_map(self, tokens: Iterable[str]) -> Dict[str, str]:
        wanted = [t for t in tokens if t]
        if not wanted:
            return {}
        out: Dict[str, str] = {}
        for chunk in _chunks(wanted, 100):
            keys = [{"token": token} for token in chunk]
            request = {self.t_trial_tokens.name: {"Keys": keys}}
            resp = self.ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(self.t_trial_tokens.name, []):
                token = item.get("token")
                node_id = item.get("node_id")
                if token and node_id:
                    out[token] = node_id
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = self.ddb.batch_get_item(RequestItems=unprocessed)
                for item in resp.get("Responses", {}).get(self.t_trial_tokens.name, []):
                    token = item.get("token")
                    node_id = item.get("node_id")
                    if token and node_id:
                        out[token] = node_id
                unprocessed = resp.get("UnprocessedKeys") or {}
        return out

    def put_trial_nodes(self, nodes: Iterable[Dict[str, Any]]) -> None:
        with self.t_trial_nodes.batch_writer(overwrite_by_pkeys=["node_id"]) as bw:
            for node in nodes:
                node_id = node.get("node_id")
                if not node_id:
                    continue
                bw.put_item(Item=_strip_nones(node))

    def put_trial_token_map(self, token_to_node: Dict[str, str]) -> None:
        with self.t_trial_tokens.batch_writer(overwrite_by_pkeys=["token"]) as bw:
            for token, node_id in token_to_node.items():
                if not token or not node_id:
                    continue
                bw.put_item(Item={"token": token, "node_id": node_id, "updated_at": dt.datetime.utcnow().isoformat()})

    def put_trial_clusters(self, clusters: Iterable[Dict[str, Any]]) -> None:
        with self.t_trial_clusters.batch_writer(overwrite_by_pkeys=["cluster_id"]) as bw:
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id")
                if not cluster_id:
                    continue
                bw.put_item(Item=_strip_nones(cluster))

    def put_trial_assignments(self, assignments: Iterable[Dict[str, Any]]) -> None:
        with self.t_trial_assignments.batch_writer(overwrite_by_pkeys=["preprint_id"]) as bw:
            for row in assignments:
                preprint_id = row.get("preprint_id")
                if not preprint_id:
                    continue
                bw.put_item(Item=_strip_nones(row))

    def mark_preprint_trial_assignment(
        self,
        *,
        osf_id: str,
        status: str,
        assigned_at: str,
        arm: Optional[str] = None,
        cluster_id: Optional[str] = None,
        reason: Optional[str] = None,
        matched_cluster_ids: Optional[List[str]] = None,
        run_id: Optional[str] = None,
    ) -> bool:
        set_exprs = [
            "trial_assignment_status=:status",
            "trial_assigned_at=:assigned_at",
            "updated_at=:updated_at",
        ]
        eav: Dict[str, Any] = {
            ":status": status,
            ":assigned_at": assigned_at,
            ":updated_at": assigned_at,
        }
        if arm is not None:
            set_exprs.append("trial_arm=:arm")
            eav[":arm"] = arm
        if cluster_id is not None:
            set_exprs.append("trial_cluster_id=:cluster")
            eav[":cluster"] = cluster_id
        if reason is not None:
            set_exprs.append("trial_assignment_reason=:reason")
            eav[":reason"] = reason
        if matched_cluster_ids is not None:
            set_exprs.append("trial_matched_cluster_ids=:matched")
            eav[":matched"] = matched_cluster_ids
        if run_id is not None:
            set_exprs.append("trial_assignment_run_id=:run_id")
            eav[":run_id"] = run_id

        try:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression="SET " + ", ".join(set_exprs),
                ExpressionAttributeValues=eav,
                ConditionExpression="attribute_not_exists(trial_assignment_status)",
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                with_extras(log, osf_id=osf_id).info(
                    "trial assignment skipped; preprint already has trial_assignment_status"
                )
                return False
            raise

    # --- upsert preprints batch ---
    def upsert_preprints(self, rows: List[Dict]) -> int:
        skip_existing = os.environ.get("OSF_INGEST_SKIP_EXISTING", "false").lower() in {"1", "true", "yes"}
        if rows:
            ids = [r.get("id") for r in rows if r.get("id")]
            excluded = self._fetch_excluded_reasons(ids)
            if excluded:
                # Certain exclusions are intentionally re-admittable:
                # - ingest_date_window: anchor/window changes can backfill
                # - *_to_pdf_conversion_failed: transient (e.g. missing/locked LibreOffice)
                _READMITTABLE = {
                    "ingest_date_window",
                    "docx_to_pdf_conversion_failed",
                    "office_to_pdf_conversion_failed",
                }
                excluded_ids = {
                    osf_id for osf_id, reason in excluded.items()
                    if str(reason or "") not in _READMITTABLE
                }
                before = len(rows)
                rows = [r for r in rows if r.get("id") not in excluded_ids]
                skipped = before - len(rows)
                if skipped:
                    with_extras(log, skipped=skipped, incoming=before).info("skipping excluded preprints")
            if not rows:
                return 0
        if skip_existing and rows:
            ids = [r.get("id") for r in rows if r.get("id")]
            existing = self._fetch_existing_ids(ids)
            if existing:
                before = len(rows)
                rows = [r for r in rows if r.get("id") not in existing]
                skipped = before - len(rows)
                if skipped:
                    with_extras(log, skipped=skipped, incoming=before).info("skipping existing preprints")
            if not rows:
                return 0
        # Split into new vs existing preprints
        all_ids = [r.get("id") for r in rows if r.get("id")]
        existing_ids = self._fetch_existing_ids(all_ids) if all_ids else set()

        # --- Version-aware sibling handling ---
        versioned_rows = [r for r in rows if parse_version(r.get("id", ""))[1] is not None]
        if versioned_rows:
            sibling_states = self._fetch_sibling_states(versioned_rows)
            version_excluded_ids: Set[str] = set()
            for row in versioned_rows:
                osf_id = row.get("id", "")
                sibs = sibling_ids(osf_id)
                existing_sibs = {sid: sibling_states[sid] for sid in sibs if sid in sibling_states}
                if not existing_sibs:
                    continue
                # Check if any non-excluded sibling has been randomized or emailed
                protected_sib = None
                for sid, state in existing_sibs.items():
                    if state.get("excluded"):
                        continue
                    if state.get("trial_assignment_status") or state.get("email_sent") is True:
                        protected_sib = sid
                        break
                if protected_sib:
                    # Sibling already randomized/emailed — exclude the incoming version
                    version_excluded_ids.add(osf_id)
                    self.mark_preprint_excluded(
                        osf_id=osf_id,
                        reason="ingest_sibling_version_randomized",
                        stage="sync",
                        details={"randomized_version": protected_sib},
                    )
                    with_extras(log, osf_id=osf_id, randomized_version=protected_sib).info(
                        "excluding version: sibling already randomized/emailed"
                    )
                else:
                    # No sibling randomized/emailed — exclude older siblings, ingest new
                    newly_excluded = False
                    for sid, state in existing_sibs.items():
                        if state.get("excluded"):
                            continue
                        newly_excluded = True
                        self.mark_preprint_excluded(
                            osf_id=sid,
                            reason="superseded_by_newer_version",
                            stage="sync",
                            details={"superseded_by": osf_id},
                        )
                        with_extras(log, osf_id=sid, superseded_by=osf_id).info(
                            "excluding old version: superseded by newer"
                        )
                    if newly_excluded:
                        # First time superseding — treat as new (fresh put_item)
                        existing_ids.discard(osf_id)
            if version_excluded_ids:
                rows = [r for r in rows if r.get("id") not in version_excluded_ids]
                with_extras(log, excluded=len(version_excluded_ids)).info(
                    "version-excluded incoming preprints"
                )

        count = 0
        now_iso = dt.datetime.utcnow().isoformat()

        for obj in rows:
            try:
                a = (obj.get("attributes") or {})
                rid = obj.get("relationships") or {}
                rel_pf = (rid.get("primary_file") or {})
                has_primary = bool((rel_pf.get("data") or rel_pf.get("links")))
                is_published = bool(a.get("is_published"))
                osf_id = obj["id"]

                osf_fields = {
                    "type": obj.get("type"),
                    "provider_id": (rid.get("provider") or {}).get("data", {}).get("id"),
                    "title": a.get("title"),
                    "description": a.get("description"),
                    "doi": a.get("doi"),
                    "date_created": a.get("date_created"),
                    "date_modified": a.get("date_modified"),
                    "date_published": a.get("date_published") or "",
                    "is_published": is_published,
                    "version": a.get("version"),
                    "is_latest_version": a.get("is_latest_version"),
                    "reviews_state": a.get("reviews_state"),
                    "tags": a.get("tags") or [],
                    "subjects": a.get("subjects") or [],
                    "license_record": a.get("license_record"),
                    "links": obj.get("links"),
                    "raw": obj,
                    "updated_at": now_iso,
                }

                if osf_id in existing_ids:
                    # Update only OSF metadata fields, preserving pipeline state
                    set_exprs = []
                    eav = {}
                    ean = {}
                    for i, (k, v) in enumerate(osf_fields.items()):
                        if v is None:
                            continue
                        attr_name = f"#f{i}"
                        attr_val = f":v{i}"
                        ean[attr_name] = k
                        eav[attr_val] = v
                        set_exprs.append(f"{attr_name}={attr_val}")
                    if set_exprs:
                        self.t_preprints.update_item(
                            Key={"osf_id": osf_id},
                            UpdateExpression="SET " + ", ".join(set_exprs),
                            ExpressionAttributeValues=eav,
                            ExpressionAttributeNames=ean,
                        )
                else:
                    # New preprint — full put
                    item_full = {"osf_id": osf_id, **osf_fields,
                                 "pdf_downloaded": False,
                                 "tei_generated": False,
                                 "tei_extracted": False}
                    if is_published and has_primary:
                        item_full["queue_pdf"] = "pending"
                    self.t_preprints.put_item(Item=_strip_nones(item_full))
                count += 1
            except Exception as e:
                with_extras(log, osf_id=obj.get("id"), err=str(e)).warning("preprint upsert failed")
        return count

    def _fetch_existing_ids(self, ids: Iterable[str]) -> Set[str]:
        """
        Batch-get existing osf_id values to support skip-existing ingest.
        """
        key_schema = self.t_preprints.key_schema or []
        hash_key = next((k["AttributeName"] for k in key_schema if k.get("KeyType") == "HASH"), None)
        range_key = next((k["AttributeName"] for k in key_schema if k.get("KeyType") == "RANGE"), None)
        if not hash_key:
            with_extras(log, table=self.t_preprints.name).warning("preprints table missing hash key; skipping exists check")
            return set()
        if range_key:
            with_extras(log, table=self.t_preprints.name, range_key=range_key).warning(
                "preprints table has range key; skipping exists check"
            )
            return set()
        ddb = self.ddb
        table_name = self.t_preprints.name
        existing: Set[str] = set()
        id_list = [str(i) for i in ids if i]
        for chunk in _chunks(id_list, 100):
            keys = [{hash_key: i} for i in chunk]
            request = {table_name: {"Keys": keys, "ProjectionExpression": hash_key}}
            resp = ddb.batch_get_item(RequestItems=request)
            existing.update(_extract_key_values(resp.get("Responses", {}).get(table_name, []), hash_key))
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = ddb.batch_get_item(RequestItems=unprocessed)
                existing.update(_extract_key_values(resp.get("Responses", {}).get(table_name, []), hash_key))
                unprocessed = resp.get("UnprocessedKeys") or {}
        return existing

    def _fetch_excluded_reasons(self, ids: Iterable[str]) -> Dict[str, Optional[str]]:
        table_name = self.t_excluded.name
        existing: Dict[str, Optional[str]] = {}
        id_list = [str(i) for i in ids if i]
        for chunk in _chunks(id_list, 100):
            keys = [{"osf_id": i} for i in chunk]
            request = {table_name: {"Keys": keys, "ProjectionExpression": "osf_id, exclusion_reason"}}
            resp = self.ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(table_name, []):
                osf_id = item.get("osf_id")
                if osf_id:
                    existing[str(osf_id)] = item.get("exclusion_reason")
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = self.ddb.batch_get_item(RequestItems=unprocessed)
                for item in resp.get("Responses", {}).get(table_name, []):
                    osf_id = item.get("osf_id")
                    if osf_id:
                        existing[str(osf_id)] = item.get("exclusion_reason")
                unprocessed = resp.get("UnprocessedKeys") or {}
        return existing

    def _fetch_sibling_states(self, versioned_rows: List[Dict]) -> Dict[str, Dict]:
        """For incoming versioned preprints, batch-get existing sibling records.

        Returns {osf_id: {"trial_assignment_status": ..., "excluded": ...}} for
        siblings that actually exist in the DB.
        """
        candidate_ids: Set[str] = set()
        for row in versioned_rows:
            osf_id = row.get("id") or ""
            candidate_ids.update(sibling_ids(osf_id))
        if not candidate_ids:
            return {}

        table_name = self.t_preprints.name
        result: Dict[str, Dict] = {}
        id_list = sorted(candidate_ids)
        for chunk in _chunks(id_list, 100):
            keys = [{"osf_id": i} for i in chunk]
            request = {table_name: {
                "Keys": keys,
                "ProjectionExpression": "osf_id, trial_assignment_status, excluded, email_sent",
            }}
            resp = self.ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(table_name, []):
                result[item["osf_id"]] = item
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = self.ddb.batch_get_item(RequestItems=unprocessed)
                for item in resp.get("Responses", {}).get(table_name, []):
                    result[item["osf_id"]] = item
                unprocessed = resp.get("UnprocessedKeys") or {}
        return result

    # --- PDF / TEI flags ---
    def mark_pdf(self, osf_id: str, ok: bool, path: Optional[str] = None):
        now = dt.datetime.utcnow().isoformat()
        if ok and path:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression=(
                    "SET pdf_downloaded=:ok, pdf_path=:p, pdf_downloaded_at=:t, updated_at=:t, "
                    "queue_pdf=:done, queue_grobid=:pending "
                    "REMOVE claim_pdf_owner, claim_pdf_until"
                ),
                ExpressionAttributeValues={":ok": True, ":p": path, ":t": now, ":done": "done", ":pending": "pending"},
            )
        else:
            # Only set flag/timestamp, avoid writing None path
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression=(
                    "SET pdf_downloaded=:ok, pdf_downloaded_at=:t, updated_at=:t, queue_pdf=:done "
                    "REMOVE claim_pdf_owner, claim_pdf_until"
                ),
                ExpressionAttributeValues={":ok": bool(ok), ":t": now, ":done": "done"},
            )

    def mark_tei(self, osf_id: str, ok: bool, tei_path: Optional[str]):
        now = dt.datetime.utcnow().isoformat()
        if ok and tei_path:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression=(
                    "SET tei_generated=:ok, tei_path=:p, tei_generated_at=:t, updated_at=:t, "
                    "queue_grobid=:done, queue_extract=:pending "
                    "REMOVE claim_grobid_owner, claim_grobid_until"
                ),
                ExpressionAttributeValues={":ok": True, ":p": tei_path, ":t": now, ":done": "done", ":pending": "pending"},
            )
        else:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression=(
                    "SET tei_generated=:ok, tei_generated_at=:t, updated_at=:t, queue_grobid=:done "
                    "REMOVE claim_grobid_owner, claim_grobid_until"
                ),
                ExpressionAttributeValues={":ok": bool(ok), ":t": now, ":done": "done"},
            )

    def mark_extracted(self, osf_id: str):
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression=(
                "SET tei_extracted=:v, updated_at=:t, queue_extract=:done "
                "REMOVE claim_extract_owner, claim_extract_until"
            ),
            ExpressionAttributeValues={":v": True, ":t": dt.datetime.utcnow().isoformat(), ":done": "done"}
        )

    def claim_stage_item(self, stage: str, osf_id: str, *, owner: str, lease_seconds: int = 1800) -> bool:
        fields = _STAGE_CLAIM_FIELDS.get(stage)
        if not fields:
            raise ValueError(f"Unsupported claim stage: {stage}")
        queue_field, owner_field, until_field = fields
        now_epoch = int(dt.datetime.utcnow().timestamp())
        until_epoch = now_epoch + max(int(lease_seconds), 1)
        now_iso = dt.datetime.utcnow().isoformat()
        try:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression="SET #owner=:owner, #until=:until, updated_at=:t",
                ConditionExpression=(
                    "#queue = :pending AND ("
                    "attribute_not_exists(#owner) OR attribute_not_exists(#until) OR #until < :now"
                    ")"
                ),
                ExpressionAttributeNames={
                    "#queue": queue_field,
                    "#owner": owner_field,
                    "#until": until_field,
                },
                ExpressionAttributeValues={
                    ":pending": "pending",
                    ":owner": owner,
                    ":until": until_epoch,
                    ":now": now_epoch,
                    ":t": now_iso,
                },
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def release_stage_claim(self, stage: str, osf_id: str) -> None:
        fields = _STAGE_CLAIM_FIELDS.get(stage)
        if not fields:
            raise ValueError(f"Unsupported claim stage: {stage}")
        _, owner_field, until_field = fields
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression="SET updated_at=:t REMOVE #owner, #until",
            ExpressionAttributeNames={"#owner": owner_field, "#until": until_field},
            ExpressionAttributeValues={":t": dt.datetime.utcnow().isoformat()},
        )

    def record_stage_error(self, stage: str, osf_id: str, message: str) -> int:
        """Record a stage error, release the claim, and return the new retry count."""
        fields = _STAGE_CLAIM_FIELDS.get(stage)
        if not fields:
            raise ValueError(f"Unsupported claim stage: {stage}")
        _, owner_field, until_field = fields
        now = dt.datetime.utcnow().isoformat()
        retry_field = f"retry_count_{stage}"
        resp = self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression=(
                "SET last_error_stage=:stage, last_error_message=:msg, last_error_at=:t, updated_at=:t "
                "ADD #retry :inc "
                "REMOVE #owner, #until"
            ),
            ExpressionAttributeNames={
                "#owner": owner_field,
                "#until": until_field,
                "#retry": retry_field,
            },
            ExpressionAttributeValues={
                ":stage": stage,
                ":msg": str(message)[:2000],
                ":t": now,
                ":inc": 1,
            },
            ReturnValues="UPDATED_NEW",
        )
        return int(resp.get("Attributes", {}).get(retry_field, 1))

    # --- select queues ---
    def select_for_pdf(self, limit: int) -> List[str]:
        # Prefer GSI, fallback to scan
        try:
            return self._paginated_queue_query(
                index_name="by_queue_pdf",
                queue_field="queue_pdf",
                queue_value="pending",
                filter_expr=(
                    "(attribute_not_exists(pdf_downloaded) OR pdf_downloaded = :false) AND "
                    "(attribute_not_exists(excluded) OR excluded = :false)"
                ),
                eav={":false": False},
                limit=limit,
            )
        except Exception:
            with_extras(log, index="by_queue_pdf").warning("GSI query failed, falling back to scan")
            return self._paginated_scan(
                filter_expr=(
                    "is_published = :true AND (pdf_downloaded = :false OR attribute_not_exists(pdf_downloaded)) AND "
                    "(attribute_not_exists(excluded) OR excluded = :false)"
                ),
                eav={":true": True, ":false": False},
                limit=limit,
            )

    def select_for_grobid(self, limit: int) -> List[str]:
        try:
            return self._paginated_queue_query(
                index_name="by_queue_grobid",
                queue_field="queue_grobid",
                queue_value="pending",
                filter_expr=(
                    "pdf_downloaded = :true AND (attribute_not_exists(tei_generated) OR tei_generated = :false) AND "
                    "(attribute_not_exists(excluded) OR excluded = :false)"
                ),
                eav={":true": True, ":false": False},
                limit=limit,
            )
        except Exception:
            with_extras(log, index="by_queue_grobid").warning("GSI query failed, falling back to scan")
            return self._paginated_scan(
                filter_expr=(
                    "pdf_downloaded = :true AND (tei_generated = :false OR attribute_not_exists(tei_generated)) AND "
                    "(attribute_not_exists(excluded) OR excluded = :false)"
                ),
                eav={":true": True, ":false": False},
                limit=limit,
            )

    def select_for_extraction(self, limit: int) -> List[Dict[str, Any]]:
        try:
            return self._paginated_queue_query_items(
                index_name="by_queue_extract",
                queue_field="queue_extract",
                queue_value="pending",
                filter_expr=(
                    "pdf_downloaded = :true AND tei_generated = :true AND "
                    "(attribute_not_exists(tei_extracted) OR tei_extracted = :false) AND "
                    "(attribute_not_exists(excluded) OR excluded = :false)"
                ),
                eav={":true": True, ":false": False},
                limit=limit,
            )
        except Exception:
            with_extras(log, index="by_queue_extract").warning("GSI query failed, falling back to scan")
            return self._paginated_scan_items(
                filter_expr=(
                    "pdf_downloaded = :t AND tei_generated = :t AND "
                    "(attribute_not_exists(tei_extracted) OR tei_extracted = :f) AND "
                    "(attribute_not_exists(excluded) OR excluded = :f)"
                ),
                eav={":t": True, ":f": False},
                limit=limit,
            )

    def _paginated_queue_query(
        self,
        *,
        index_name: str,
        queue_field: str,
        queue_value: str,
        filter_expr: str,
        eav: Dict[str, Any],
        limit: int,
        page_size: int = 500,
    ) -> List[str]:
        """Query a queue GSI with pagination to collect enough results after filtering."""
        if limit <= 0:
            return []
        full_eav = {":q": queue_value, **eav}
        results: List[str] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "IndexName": index_name,
                "KeyConditionExpression": f"{queue_field} = :q",
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": full_eav,
                "Limit": page_size,
                "ScanIndexForward": True,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.query(**kwargs)
            for item in resp.get("Items", []):
                val = item.get("osf_id")
                if val:
                    results.append(val)
            if len(results) >= limit:
                return results[:limit]
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            # Defensive guard against pathological pagination loops.
            if next_key == last_key:
                with_extras(log, index=index_name).warning("queue query pagination key did not advance; stopping")
                break
            last_key = next_key
        return results[:limit]

    def _paginated_queue_query_items(
        self,
        *,
        index_name: str,
        queue_field: str,
        queue_value: str,
        filter_expr: str,
        eav: Dict[str, Any],
        limit: int,
        page_size: int = 500,
    ) -> List[Dict[str, Any]]:
        """Like _paginated_queue_query but returns full items."""
        if limit <= 0:
            return []
        full_eav = {":q": queue_value, **eav}
        results: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "IndexName": index_name,
                "KeyConditionExpression": f"{queue_field} = :q",
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": full_eav,
                "Limit": page_size,
                "ScanIndexForward": True,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.query(**kwargs)
            results.extend(resp.get("Items", []))
            if len(results) >= limit:
                return results[:limit]
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            if next_key == last_key:
                with_extras(log, index=index_name).warning("queue query pagination key did not advance; stopping")
                break
            last_key = next_key
        return results[:limit]

    def _paginated_scan(
        self,
        *,
        filter_expr: str,
        eav: Dict[str, Any],
        limit: int,
        page_size: int = 500,
    ) -> List[str]:
        """Scan with pagination to collect enough results after filtering."""
        if limit <= 0:
            return []
        results: List[str] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": eav,
                "Limit": page_size,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.scan(**kwargs)
            for item in resp.get("Items", []):
                val = item.get("osf_id")
                if val:
                    results.append(val)
            if len(results) >= limit:
                return results[:limit]
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            if next_key == last_key:
                with_extras(log).warning("scan pagination key did not advance; stopping")
                break
            last_key = next_key
        return results[:limit]

    def _paginated_scan_items(
        self,
        *,
        filter_expr: str,
        eav: Dict[str, Any],
        limit: int,
        page_size: int = 500,
    ) -> List[Dict[str, Any]]:
        """Like _paginated_scan but returns full items."""
        if limit <= 0:
            return []
        results: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "FilterExpression": filter_expr,
                "ExpressionAttributeValues": eav,
                "Limit": page_size,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.scan(**kwargs)
            results.extend(resp.get("Items", []))
            if len(results) >= limit:
                return results[:limit]
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            if next_key == last_key:
                with_extras(log).warning("scan pagination key did not advance; stopping")
                break
            last_key = next_key
        return results[:limit]

    # --- TEI / references writes ---
    def upsert_tei(self, osf_id: str, preprint: Dict) -> None:
        item_full = {"osf_id": osf_id, **preprint, "extracted_at": dt.datetime.utcnow().isoformat()}
        self.t_tei.put_item(Item=_strip_nones(item_full))

    def prepare_reference_item(self, osf_id: str, ref: Dict) -> Dict[str, Any]:
        """Build a complete DynamoDB item for a reference without writing it."""
        ref_clean = dict(ref)
        doi_norm = _normalize_reference_doi(ref_clean.get("doi"), source=ref_clean.get("doi_source"))
        if doi_norm:
            ref_clean["doi"] = doi_norm
            ref_clean["has_doi"] = True
        elif ref_clean.get("doi"):
            ref_clean["doi"] = None
            ref_clean["has_doi"] = False
            ref_clean.pop("doi_source", None)
        item_full = {"osf_id": osf_id, "ref_id": ref["ref_id"], **ref_clean,
                     "updated_at": dt.datetime.utcnow().isoformat()}
        return _strip_nones(item_full)

    def upsert_reference(self, osf_id: str, ref: Dict) -> None:
        self.t_refs.put_item(Item=self.prepare_reference_item(osf_id, ref))

    # --- utilities / other operations ---
    def delete_preprint(self, osf_id: str) -> None:
        self.t_preprints.delete_item(Key={"osf_id": osf_id})

    def exists_preprint(self, osf_id: str) -> bool:
        return bool(self.t_preprints.get_item(
            Key={"osf_id": osf_id}, ProjectionExpression="osf_id",
        ).get("Item"))

    def get_preprint_basic(self, osf_id: str) -> Optional[Dict[str, Any]]:
        it = self.t_preprints.get_item(
            Key={"osf_id": osf_id},
            ProjectionExpression="osf_id, provider_id, #raw",
            ExpressionAttributeNames={"#raw": "raw"},
        ).get("Item")
        if not it:
            return None
        return {"osf_id": it.get("osf_id"), "provider_id": it.get("provider_id"), "raw": it.get("raw")}

    def get_preprint_doi(self, osf_id: str) -> Optional[str]:
        """
        Return the DOI stored on the preprint record (if any).
        """
        it = self.t_preprints.get_item(
            Key={"osf_id": osf_id}, ProjectionExpression="doi",
        ).get("Item")
        if not it:
            return None
        return it.get("doi")

    def select_refs_missing_doi(
        self,
        limit: Optional[int],
        osf_id: Optional[str] = None,
        *,
        ref_id: Optional[str] = None,
        include_existing: bool = False,
        skip_checked_within_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        limit_val: Optional[int] = None if limit is None else int(limit)
        if limit_val is not None and limit_val <= 0:
            limit_val = None
        items: List[Dict[str, Any]] = []

        if osf_id:
            last_key = None
            while True:
                kwargs = {"KeyConditionExpression": Key("osf_id").eq(osf_id)}
                if last_key:
                    kwargs["ExclusiveStartKey"] = last_key
                resp = self.t_refs.query(**kwargs)
                chunk = resp.get("Items", [])
                items.extend(chunk)
                last_key = resp.get("LastEvaluatedKey")
                if not last_key or (limit_val and len(items) >= limit_val):
                    break
        else:
            fe = "(attribute_not_exists(doi) OR doi = :empty)"
            eav = {":empty": ""}
            last_key = None
            while True:
                scan_kwargs: Dict[str, Any] = {
                    "FilterExpression": fe,
                    "ExpressionAttributeValues": eav,
                }
                if last_key:
                    scan_kwargs["ExclusiveStartKey"] = last_key
                resp = self.t_refs.scan(**scan_kwargs)
                items.extend(resp.get("Items", []))
                if limit_val and len(items) >= limit_val:
                    break
                last_key = resp.get("LastEvaluatedKey")
                if not last_key:
                    break

        if ref_id:
            items = [it for it in items if it and it.get("ref_id") == ref_id]
            if include_existing and not items and osf_id:
                item = self.t_refs.get_item(Key={"osf_id": osf_id, "ref_id": ref_id}).get("Item")
                items = [item] if item else []

        if not include_existing:
            filtered = []
            for it in items:
                if not it:
                    continue
                doi_val = (it.get("doi") or "").strip()
                if not doi_val:
                    filtered.append(it)
            items = filtered

        # Skip refs that were recently checked and found no match
        if skip_checked_within_seconds and not include_existing:
            cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=skip_checked_within_seconds)).isoformat()
            items = [
                it for it in items
                if not it.get("doi_checked_at") or it["doi_checked_at"] < cutoff
            ]

        if limit_val:
            items = items[:limit_val]
        return items

    def select_osf_ids_with_refs_missing_doi(
        self,
        limit_preprints: int,
        *,
        skip_checked_within_seconds: Optional[int] = None,
    ) -> List[str]:
        if limit_preprints <= 0:
            return []

        cutoff: Optional[str] = None
        if skip_checked_within_seconds:
            cutoff = (
                dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=skip_checked_within_seconds)
            ).isoformat()

        out: List[str] = []
        seen: Set[str] = set()
        last_key = None
        fe = "(attribute_not_exists(doi) OR doi = :empty)"
        eav = {":empty": ""}
        while len(out) < limit_preprints:
            scan_kwargs: Dict[str, Any] = {
                "FilterExpression": fe,
                "ExpressionAttributeValues": eav,
                "ProjectionExpression": "osf_id,doi_checked_at",
            }
            if last_key:
                scan_kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_refs.scan(**scan_kwargs)
            for item in resp.get("Items", []):
                osf_id = (item or {}).get("osf_id")
                if not osf_id or osf_id in seen:
                    continue
                if cutoff:
                    checked_at = item.get("doi_checked_at")
                    if checked_at and checked_at >= cutoff:
                        continue
                seen.add(osf_id)
                out.append(osf_id)
                if len(out) >= limit_preprints:
                    break
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        # Filter out excluded preprints to avoid wasting Crossref API calls
        if out:
            excluded = self._fetch_excluded_reasons(out)
            out = [oid for oid in out if oid not in excluded]
        return out

    def filter_osf_ids_without_sent_email(self, osf_ids: Iterable[str]) -> Set[str]:
        """
        Return the subset of OSF ids that do not have email_sent=True.

        Missing preprint rows are treated as unsent so callers do not
        accidentally drop work because of incomplete denormalized state.
        """
        wanted = [oid for oid in osf_ids if oid]
        if not wanted:
            return set()

        sent_ids: Set[str] = set()
        for chunk in _chunks(wanted, 100):
            keys = [{"osf_id": oid} for oid in chunk]
            request = {
                self.t_preprints.name: {
                    "Keys": keys,
                    "ProjectionExpression": "osf_id, email_sent",
                }
            }
            resp = self.ddb.batch_get_item(RequestItems=request)
            for item in resp.get("Responses", {}).get(self.t_preprints.name, []):
                osf_id_val = item.get("osf_id")
                if osf_id_val and item.get("email_sent") is True:
                    sent_ids.add(osf_id_val)
            unprocessed = resp.get("UnprocessedKeys") or {}
            while unprocessed:
                resp = self.ddb.batch_get_item(RequestItems=unprocessed)
                for item in resp.get("Responses", {}).get(self.t_preprints.name, []):
                    osf_id_val = item.get("osf_id")
                    if osf_id_val and item.get("email_sent") is True:
                        sent_ids.add(osf_id_val)
                unprocessed = resp.get("UnprocessedKeys") or {}

        return {oid for oid in wanted if oid not in sent_ids}

    def update_reference_doi(self, osf_id: str, ref_id: str, doi: str, *, source: str) -> bool:
        # Only set if not already set
        doi_norm = _normalize_reference_doi(doi, source=source)
        if not doi_norm:
            with_extras(log, osf_id=osf_id, ref_id=ref_id, doi=doi, source=source).warning(
                "Rejected malformed DOI update"
            )
            return False
        now = dt.datetime.utcnow().isoformat()
        try:
            self.t_refs.update_item(
                Key={"osf_id": osf_id, "ref_id": ref_id},
                UpdateExpression=(
                    "SET doi=:d, has_doi=:hd, doi_source=:src, updated_at=:t "
                    "REMOVE doi_checked_at, flora_lookup_status, flora_checked_at, "
                    "flora_lookup_payload, flora_refs, flora_refs_count, flora_ref_pairs, flora_ref_pairs_count, "
                    "flora_replication_cited, flora_screened_at, flora_matching_replication_dois, "
                    "flora_queue_pk, flora_queue_sk"
                ),
                ExpressionAttributeValues={
                    ":d": doi_norm,
                    ":hd": True,
                    ":src": source,
                    ":t": now,
                    ":empty": "",
                },
                ConditionExpression="attribute_not_exists(doi) OR doi = :empty",
                ReturnValues="NONE",
            )
            # Clear preprint-level flora check so it gets rechecked
            try:
                self.t_preprints.update_item(
                    Key={"osf_id": osf_id},
                    UpdateExpression="REMOVE flora_last_checked",
                )
            except Exception:
                pass  # Best-effort; 30-day recheck will catch it
            return True
        except ClientError as e:
            # Conditional check failed or other issue
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def mark_reference_doi_checked(self, osf_id: str, ref_id: str) -> None:
        """Record that a reference was checked for DOI but no match was found."""
        now = dt.datetime.utcnow().isoformat()
        self.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression="SET doi_checked_at=:t, updated_at=:t",
            ExpressionAttributeValues={":t": now},
        )

    def update_reference_raw_citation_validity(self, osf_id: str, ref_id: str, validity: str) -> None:
        now = dt.datetime.utcnow().isoformat()
        self.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression="SET raw_citation_validity=:v, raw_citation_validity_updated_at=:t",
            ExpressionAttributeValues={":v": validity, ":t": now},
        )

    def update_preprint_flora_eligibility(
        self,
        osf_id: str,
        *,
        eligible: bool,
        eligible_count: int,
    ) -> None:
        now = dt.datetime.utcnow().isoformat()
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression=(
                "SET flora_eligible=:eligible, flora_eligible_count=:count, "
                "flora_last_checked=:t, flora_screened_at=:t, updated_at=:t"
            ),
            ExpressionAttributeValues={
                ":eligible": bool(eligible),
                ":count": int(eligible_count),
                ":t": now,
            },
        )

    def select_preprints_for_flora_check(
        self,
        *,
        limit: int = 0,
        osf_id: Optional[str] = None,
        only_unchecked: bool = True,
    ) -> List[str]:
        """Return osf_ids of preprints due for FLORA check.

        Scans the preprints table (small: ~10k rows) and returns ids where:
        - email_sent is not True
        - flora_last_checked is absent or older than flora_recheck_days
        """
        if osf_id:
            return [osf_id]

        cutoff = (
            dt.datetime.utcnow() - dt.timedelta(days=max(1, self.flora_recheck_days))
        ).isoformat()
        items: List[str] = []
        last_key = None
        while True:
            scan_kwargs: Dict[str, Any] = {
                "ProjectionExpression": "osf_id, flora_last_checked, email_sent, excluded",
            }
            if last_key:
                scan_kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.scan(**scan_kwargs)
            for item in resp.get("Items", []):
                if item.get("excluded") is True:
                    continue
                if item.get("email_sent") is True:
                    continue
                checked = item.get("flora_last_checked")
                if only_unchecked and checked and checked > cutoff:
                    continue
                oid = item.get("osf_id")
                if oid:
                    items.append(oid)
                if limit and len(items) >= limit:
                    break
            if limit and len(items) >= limit:
                break
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return items

    def update_reference_flora_result(
        self,
        osf_id: str,
        ref_id: str,
        *,
        ref_pairs: List[Dict[str, Any]],
        replication_cited: bool,
    ) -> None:
        """Write FLORA match + screening result in a single update (matched refs only)."""
        now = dt.datetime.utcnow().isoformat()
        self.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression=(
                "SET flora_ref_pairs=:p, flora_ref_pairs_count=:pc, "
                "flora_replication_cited=:rc, flora_screened_at=:t, updated_at=:t "
                "REMOVE flora_lookup_payload, flora_refs, flora_refs_count, "
                "flora_matching_replication_dois, flora_lookup_status, flora_checked_at, "
                "flora_queue_pk, flora_queue_sk"
            ),
            ExpressionAttributeValues={
                ":p": ref_pairs,
                ":pc": len(ref_pairs),
                ":rc": bool(replication_cited),
                ":t": now,
            },
        )

    def update_reference_citation_distance(
        self,
        osf_id: str,
        ref_id: str,
        *,
        distance: float,
        apa_citation: str,
        validation_status: str,
    ) -> None:
        """Store citation distance and validation status on a reference.

        *validation_status* should be ``"pass"`` or ``"pending_review"``.
        """
        from decimal import Decimal
        now = dt.datetime.utcnow().isoformat()
        self.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression=(
                "SET citation_distance=:d, citation_apa_resolved=:apa, "
                "citation_validation_status=:s, citation_validation_updated_at=:t, "
                "updated_at=:t"
            ),
            ExpressionAttributeValues={
                ":d": Decimal(str(round(distance, 6))),
                ":apa": apa_citation,
                ":s": validation_status,
                ":t": now,
            },
        )

    def update_reference_validation_decision(
        self,
        osf_id: str,
        ref_id: str,
        *,
        decision: str,
    ) -> None:
        """Record a reviewer's approve/reject decision for a reference.

        *decision* should be ``"approved"`` or ``"rejected"``.
        """
        now = dt.datetime.utcnow().isoformat()
        # Require the reference to already exist so a stale/typo target raises
        # ConditionalCheckFailedException instead of creating a sparse phantom item.
        self.t_refs.update_item(
            Key={"osf_id": osf_id, "ref_id": ref_id},
            UpdateExpression=(
                "SET citation_validation_status=:s, "
                "citation_validation_updated_at=:t, updated_at=:t"
            ),
            ConditionExpression="attribute_exists(osf_id)",
            ExpressionAttributeValues={":s": decision, ":t": now},
        )

    def set_citation_validation_pending(
        self,
        osf_id: str,
        *,
        pending: bool,
        review_id: Optional[str] = None,
    ) -> None:
        """Set or clear the preprint-level citation validation pending flag.

        When *pending* is True, preprint is excluded from email selection until
        all flagged references are reviewed.  Pass *review_id* to store the
        review identifier used to match incoming reviewer responses.
        """
        now = dt.datetime.utcnow().isoformat()
        if pending:
            expr = "SET flora_citation_validation_pending=:true, updated_at=:t"
            eav: Dict[str, Any] = {":true": True, ":t": now}
            if review_id:
                expr += ", citation_validation_review_id=:rid"
                eav[":rid"] = review_id
        else:
            expr = (
                "SET updated_at=:t "
                "REMOVE flora_citation_validation_pending, "
                "citation_validation_review_id"
            )
            eav = {":t": now}
        # Require the preprint to exist so this never creates a sparse phantom item
        # (e.g. from a decision applied to an orphan reference).
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression=expr,
            ConditionExpression="attribute_exists(osf_id)",
            ExpressionAttributeValues=eav,
        )

    def update_preprint_author_email_candidates(
        self,
        osf_id: str,
        candidates: List[Dict[str, Any]],
    ) -> None:
        now = dt.datetime.utcnow().isoformat()
        # Queueing for email is handled after treatment assignment in randomization.
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression="SET author_email_candidates=:c, updated_at=:t",
            ExpressionAttributeValues={":c": candidates, ":t": now},
        )

    # --- email stage ---
    def select_for_email(self, limit: int = 50) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_key = None
        page_limit = max(limit, 100)
        while True:
            kwargs: Dict[str, Any] = {
                "IndexName": "by_queue_email",
                "KeyConditionExpression": "queue_email = :q",
                "FilterExpression": (
                    "attribute_exists(author_email_candidates)"
                    " AND trial_arm = :treatment"
                    " AND (attribute_not_exists(excluded) OR excluded = :false)"
                    " AND (attribute_not_exists(flora_citation_validation_pending)"
                    "      OR flora_citation_validation_pending = :false)"
                ),
                "ExpressionAttributeValues": {
                    ":q": "pending",
                    ":treatment": "treatment",
                    ":false": False,
                },
                "Limit": page_limit,
                "ScanIndexForward": True,
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.t_preprints.query(**kwargs)
            out.extend(resp.get("Items", []))
            if len(out) >= limit:
                return out[:limit]
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                return out

    def claim_email_item(self, osf_id: str, *, owner: str, lease_seconds: int = 1800) -> bool:
        now = dt.datetime.utcnow()
        now_iso = now.isoformat()
        until_iso = (now + dt.timedelta(seconds=max(1, int(lease_seconds)))).isoformat()
        try:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                ConditionExpression=(
                    "queue_email = :pending "
                    "AND (attribute_not_exists(email_sent) OR email_sent = :false) "
                    "AND (attribute_not_exists(claim_email_until) OR claim_email_until < :now OR claim_email_owner = :owner)"
                ),
                UpdateExpression="SET claim_email_owner=:owner, claim_email_until=:until, updated_at=:now",
                ExpressionAttributeValues={
                    ":pending": "pending",
                    ":false": False,
                    ":owner": owner,
                    ":until": until_iso,
                    ":now": now_iso,
                },
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def release_email_claim(self, osf_id: str, *, owner: Optional[str] = None) -> bool:
        kwargs: Dict[str, Any] = {
            "Key": {"osf_id": osf_id},
            "UpdateExpression": "REMOVE claim_email_owner, claim_email_until",
        }
        if owner:
            kwargs["ConditionExpression"] = "claim_email_owner = :owner"
            kwargs["ExpressionAttributeValues"] = {":owner": owner}
        try:
            self.t_preprints.update_item(**kwargs)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def set_queue_email(self, osf_id: str) -> None:
        """Mark a preprint as ready for email sending via queue_email GSI."""
        now = dt.datetime.utcnow().isoformat()
        try:
            self.t_preprints.update_item(
                Key={"osf_id": osf_id},
                UpdateExpression="SET queue_email=:pending, updated_at=:t",
                ConditionExpression=(
                    "attribute_not_exists(queue_email) OR queue_email <> :done"
                ),
                ExpressionAttributeValues={":pending": "pending", ":t": now, ":done": "done"},
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise

    def clear_queue_email(self, osf_id: str) -> None:
        now = dt.datetime.utcnow().isoformat()
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression="SET updated_at=:t REMOVE queue_email, claim_email_owner, claim_email_until",
            ExpressionAttributeValues={":t": now},
        )

    def mark_email_sent(
        self,
        osf_id: str,
        *,
        recipient: str,
        message_id: str,
        originals: Optional[List[Dict[str, Any]]] = None,
        owner: Optional[str] = None,
    ) -> bool:
        now = dt.datetime.utcnow().isoformat()
        expr_values: Dict[str, Any] = {
            ":true": True,
            ":t": now,
            ":r": recipient,
            ":mid": message_id,
            ":done": "done",
        }
        set_exprs = [
            "email_sent=:true",
            "email_sent_at=:t",
            "email_recipient=:r",
            "email_message_id=:mid",
            "updated_at=:t",
            "queue_email=:done",
        ]
        if originals is not None:
            set_exprs.append("email_originals=:originals")
            expr_values[":originals"] = originals

        kwargs: Dict[str, Any] = {
            "Key": {"osf_id": osf_id},
            "UpdateExpression": (
                "SET " + ", ".join(set_exprs) + " "
                "REMOVE email_error, claim_email_owner, claim_email_until"
            ),
            "ExpressionAttributeValues": expr_values,
        }
        if owner:
            kwargs["ConditionExpression"] = "claim_email_owner = :owner"
            expr_values[":owner"] = owner
        try:
            self.t_preprints.update_item(**kwargs)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def mark_email_error(self, osf_id: str, error: str, *, owner: Optional[str] = None) -> bool:
        now = dt.datetime.utcnow().isoformat()
        expr_values: Dict[str, Any] = {
            ":err": str(error)[:2000],
            ":false": False,
            ":t": now,
        }
        kwargs: Dict[str, Any] = {
            "Key": {"osf_id": osf_id},
            "UpdateExpression": (
                "SET email_error=:err, email_sent=:false, updated_at=:t "
                "REMOVE claim_email_owner, claim_email_until"
            ),
            "ExpressionAttributeValues": expr_values,
        }
        if owner:
            kwargs["ConditionExpression"] = "claim_email_owner = :owner"
            expr_values[":owner"] = owner
        try:
            self.t_preprints.update_item(**kwargs)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def mark_email_validated(self, osf_id: str, status: str) -> None:
        now = dt.datetime.utcnow().isoformat()
        self.t_preprints.update_item(
            Key={"osf_id": osf_id},
            UpdateExpression="SET email_validation_status=:s, updated_at=:t",
            ExpressionAttributeValues={":s": status, ":t": now},
        )


def _chunks(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _extract_key_values(items: List[Dict[str, Any]], key_name: str) -> Set[str]:
    out: Set[str] = set()
    for item in items:
        val = item.get(key_name)
        if isinstance(val, dict) and "S" in val:
            out.add(val.get("S"))
        elif isinstance(val, str):
            out.add(val)
    return out
