from __future__ import annotations
from typing import Dict, List, Optional, Any
from ..dynamo.preprints_repo import PreprintsRepo
from .doi_multi_method_lookup import doi_resolves, normalize_doi
import logging, traceback, json, re

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger.setLevel(logging.INFO)

def _json_dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)

def _ensure_list(v: Any): return v if isinstance(v, list) else ([] if v is None else [v])
def _safe_str(v: Any, maxlen: int | None = None): 
    if v is None: return None
    s = str(v);  return s if maxlen is None or len(s) <= maxlen else s[:maxlen]
def _to_int_or_none(v: Any):
    if v is None: return None
    s = str(v).strip();  return int(s) if s.isdigit() else None


_RAW_DOI_RE = re.compile(r"10\.[0-9]{4,9}/\S+", re.IGNORECASE)


def _is_plausible_doi(doi: Optional[str]) -> bool:
    d = str(doi or "").strip().lower()
    if not d or "/" not in d:
        return False
    _, suffix = d.split("/", 1)
    if len(suffix) < 6:
        return False
    if suffix.endswith(("-", ".", "/")):
        return False
    if not suffix[-1].isalnum():
        return False
    return True


def _extract_dois_from_raw_citation(raw_citation: Optional[str]) -> List[str]:
    """
    Extract unique DOI candidates from raw citation text.
    """
    txt = _safe_str(raw_citation)
    if not txt:
        return []
    dois: list[str] = []
    seen: set[str] = set()
    for m in _RAW_DOI_RE.finditer(txt):
        d = normalize_doi(m.group(0), source="text")
        if not d or d in seen:
            continue
        if not _is_plausible_doi(d):
            continue
        seen.add(d)
        dois.append(d)
    return dois


def _extract_unique_doi_from_raw_citation(raw_citation: Optional[str]) -> Optional[str]:
    """
    Extract a DOI from raw citation text only when there is a single unique DOI candidate.
    """
    dois = _extract_dois_from_raw_citation(raw_citation)
    if len(dois) == 1:
        return dois[0]
    return None


def _repair_doi_suffix_if_needed(doi: str, *, max_trim: int = 8) -> Optional[str]:
    """
    Try to recover DOIs contaminated by trailing digits (common TEI parsing artifact),
    e.g. 10.xxxx/abc12345 -> 10.xxxx/abc123.
    """
    candidate = str(doi or "").strip().lower()
    if not candidate:
        return None
    trimmed = candidate
    for _ in range(max_trim):
        if not trimmed or not trimmed[-1].isdigit():
            break
        trimmed = trimmed[:-1]
        if len(trimmed) < 8:
            break
        if doi_resolves(trimmed) is True:
            return trimmed
    return None

def write_extraction(
    osf_id: str,
    preprint: Dict,
    references: List[Dict],
    *,
    raise_on_error: bool = False,
    log: Optional[logging.Logger] = None,
    allow_protected_overwrite: bool = False,
) -> Dict[str, int | str | bool]:

    _log = log or logger
    _log.info("TEI upsert start", extra={"osf_id": osf_id, "refs": len(references)})

    result = {"osf_id": osf_id, "tei_ok": False, "refs_total": len(references), "refs_upserted": 0, "refs_failed": 0}

    p_title = _safe_str(preprint.get("title"))
    p_doi = _safe_str(preprint.get("doi"))
    p_authors = _ensure_list(preprint.get("authors"))
    p_published_date = _safe_str(preprint.get("published_date"))

    repo = PreprintsRepo()

    # Reference rows are part of the randomized baseline once a preprint has
    # been assigned (and certainly once an email has been sent).  Replacing
    # them with batch_writer.put_item would discard downstream FLoRA and
    # citation-validation fields.  A deliberate repair can opt out, but the
    # routine pipeline must treat these rows as immutable.
    if not allow_protected_overwrite:
        state = repo.t_preprints.get_item(
            Key={"osf_id": osf_id},
            ProjectionExpression="trial_assignment_status, email_sent",
            ConsistentRead=True,
        ).get("Item") or {}
        if state.get("trial_assignment_status") or state.get("email_sent") is True:
            result["skipped_protected"] = True
            _log.warning(
                "Skipping TEI/reference overwrite for protected preprint",
                extra={"osf_id": osf_id},
            )
            return result

    try:
        # TEI summary upsert
        try:
            repo.upsert_tei(osf_id, {
                "title": p_title,
                "doi": p_doi,
                "authors": p_authors,
                "published_date": p_published_date,
                "has_title": bool(preprint.get("has_title")),
                "has_doi": bool(preprint.get("has_doi")),
                "has_authors": bool(preprint.get("has_authors")),
                "has_published_date": bool(preprint.get("has_published_date")),
            })
            result["tei_ok"] = True
            _log.info("TEI upserted", extra={"osf_id": osf_id, "title_snippet": (p_title or "")[:160]})
        except Exception as e:
            result["tei_ok"] = False
            _log.error("TEI upsert failed", extra={"osf_id": osf_id, "error": str(e)})
            if raise_on_error:
                raise

        # References — prepare all items, then batch-write.
        prepared_items: List[Dict] = []
        for idx, ref in enumerate(references):
            ref_id = ref.get("ref_id") or f"r{idx}"
            raw_citation = _safe_str(ref.get("raw_citation"))
            raw_citation_dois = _extract_dois_from_raw_citation(raw_citation)
            raw_citation_doi = raw_citation_dois[0] if len(raw_citation_dois) == 1 else None
            raw_ref_doi = _safe_str(ref.get("doi"))
            ref_doi = normalize_doi(raw_ref_doi)
            doi_source = None
            if raw_ref_doi and not ref_doi:
                _log.info(
                    "Malformed GROBID DOI, discarding",
                    extra={"osf_id": osf_id, "ref_id": ref_id, "doi": raw_ref_doi},
                )
            # TEI occasionally assigns one DOI to many refs; if raw citation has exactly one DOI and it
            # conflicts with the TEI idno DOI, trust raw-citation DOI for this reference.
            if ref_doi and raw_citation_dois and ref_doi not in raw_citation_dois:
                if raw_citation_doi:
                    _log.warning(
                        "TEI DOI conflicts with raw citation DOI; preferring raw DOI",
                        extra={
                            "osf_id": osf_id,
                            "ref_id": ref_id,
                            "tei_doi": ref_doi,
                            "raw_citation_doi": raw_citation_doi,
                        },
                    )
                    ref_doi = raw_citation_doi
                    doi_source = "tei_raw"
                else:
                    # Multiple DOI candidates in raw citation, and TEI DOI is not among them.
                    # Do not keep an inconsistent DOI; leave unresolved for enrichment.
                    _log.warning(
                        "TEI DOI not present among raw citation DOI candidates; discarding DOI",
                        extra={
                            "osf_id": osf_id,
                            "ref_id": ref_id,
                            "tei_doi": ref_doi,
                            "raw_doi_candidates": raw_citation_dois[:5],
                            "raw_doi_candidates_count": len(raw_citation_dois),
                        },
                    )
                    ref_doi = None
                    doi_source = None
            elif raw_citation_doi and not ref_doi:
                # Recover DOI from raw citation when TEI idno DOI is missing/malformed.
                ref_doi = raw_citation_doi
                doi_source = "tei_raw"
                _log.info(
                    "Recovered DOI from raw citation",
                    extra={"osf_id": osf_id, "ref_id": ref_id, "doi": ref_doi},
                )
            if ref_doi:
                resolve_state = doi_resolves(ref_doi)
                if resolve_state is False:
                    repaired = _repair_doi_suffix_if_needed(ref_doi)
                    if repaired:
                        _log.info(
                            "Repaired malformed DOI before validation",
                            extra={"osf_id": osf_id, "ref_id": ref_id, "doi_old": ref_doi, "doi_new": repaired},
                        )
                        ref_doi = repaired
                    else:
                        _log.info(
                            "GROBID DOI does not resolve, discarding",
                            extra={"osf_id": osf_id, "ref_id": ref_id, "doi": ref_doi},
                        )
                        ref_doi = None
                        doi_source = None
            if ref_doi and not doi_source:
                doi_source = "tei"
            item = {
                "ref_id": ref_id,
                "title": _safe_str(ref.get("title")),
                "authors": _ensure_list(ref.get("authors")),
                "journal": _safe_str(ref.get("journal")),
                "year": _to_int_or_none(ref.get("year")),
                "doi": ref_doi,
                "has_doi": bool(ref_doi),
                "has_title": bool(ref.get("has_title")),
                "has_authors": bool(ref.get("has_authors")),
                "has_journal": bool(ref.get("has_journal")),
                "has_year": bool(ref.get("has_year")),
                "doi_source": doi_source if ref_doi else None,
                "raw_citation": raw_citation,
            }
            try:
                prepared_items.append(repo.prepare_reference_item(osf_id, item))
            except Exception as e:
                result["refs_failed"] += 1
                _log.warning(
                    "Reference preparation failed",
                    extra={"osf_id": osf_id, "ref_idx": idx, "ref_id": ref_id, "error": str(e)},
                )
                if raise_on_error:
                    raise

        # Batch-write all prepared reference items (25 per DynamoDB batch request).
        try:
            with repo.t_refs.batch_writer() as batch:
                for item in prepared_items:
                    batch.put_item(Item=item)
            result["refs_upserted"] = len(prepared_items)
        except Exception as e:
            result["refs_failed"] += len(prepared_items) - result["refs_upserted"]
            _log.warning(
                "Batch reference upsert failed",
                extra={"osf_id": osf_id, "error": str(e)},
            )
            if raise_on_error:
                raise

        if result["tei_ok"]:
            repo.mark_extracted(osf_id)
            _log.info("Marked preprint as extracted", extra={"osf_id": osf_id})

        _log.info("TEI extraction write complete", extra=result)
        return result

    except Exception:
        _log.exception("Unexpected error during write_extraction", extra={"osf_id": osf_id})
        if raise_on_error:
            raise
        result["tei_ok"] = False
        return result
