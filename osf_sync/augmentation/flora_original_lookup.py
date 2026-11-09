from __future__ import annotations

import csv
import datetime as dt
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ..logging_setup import get_logger, with_extras
from ..runtime_config import RUNTIME_CONFIG

logger = get_logger(__name__)

FLORA_CSV_URL = RUNTIME_CONFIG.flora.csv_url
FLORA_CSV_PATH_DEFAULT = RUNTIME_CONFIG.flora.csv_path


def _info(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).info(msg)
    else:
        logger.info(msg)


def _warn(msg: str, **extras: Any) -> None:
    if extras:
        with_extras(logger, **extras).warning(msg)
    else:
        logger.warning(msg)


_DOI_PATTERN = re.compile(r"10\.\S+", re.IGNORECASE)
_ALLOWED_OUTCOMES = {"successful", "failed", "mixed"}
_OUTCOME_ALIASES = {
    "success": "successful",
    "successful": "successful",
    "failure": "failed",
    "failed": "failed",
    "mixed": "mixed",
}


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """
    Normalize DOI to lowercase and strip URL prefixes; return None if invalid.
    """
    if not doi or not isinstance(doi, str):
        return None
    v = doi.strip().lower()
    v = re.sub(r"^https?://(dx\.)?doi\.org/", "", v)
    m = _DOI_PATTERN.search(v)
    return m.group(0) if m else None


def _normalize_field_name(name: Optional[str]) -> str:
    return (name or "").replace("\ufeff", "").strip().strip('"').strip()


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    return value or None


def _valid_url(value: Any) -> str:
    """Return value if it looks like an http(s) URL, otherwise empty string."""
    txt = _clean_text(value) or ""
    return txt if txt.startswith(("http://", "https://")) else ""


def _normalize_outcome(value: Any) -> Optional[str]:
    txt = _clean_text(value)
    if not txt:
        return None
    normalized = _OUTCOME_ALIASES.get(txt.lower())
    if normalized in _ALLOWED_OUTCOMES:
        return normalized
    return None


def _resolve_flora_csv_path(cache_path: Optional[str]) -> Path:
    raw = cache_path or FLORA_CSV_PATH_DEFAULT
    return Path(raw).expanduser()


def _is_file_from_today(path: Path) -> bool:
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime)
    except FileNotFoundError:
        return False
    except Exception:
        return False
    return mtime.date() >= dt.date.today()


def _download_flora_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with requests.get(FLORA_CSV_URL, stream=True, timeout=(20, 180)) as resp:
            resp.raise_for_status()
            with tmp_path.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def _ensure_fresh_flora_csv(path: Path, *, debug: bool = False) -> Dict[str, Any]:
    if _is_file_from_today(path):
        return {"downloaded": False, "used_stale": False}

    try:
        _download_flora_csv(path)
        if debug:
            _info("Downloaded fresh FLORA CSV", path=str(path), source=FLORA_CSV_URL)
        return {"downloaded": True, "used_stale": False}
    except Exception as exc:
        if path.exists():
            _warn(
                "Failed to refresh FLORA CSV; using existing local copy",
                path=str(path),
                error=str(exc),
            )
            return {"downloaded": False, "used_stale": True}
        raise RuntimeError(f"Unable to download FLORA CSV to {path}") from exc


def _load_flora_pairs_by_original(path: Path) -> Dict[str, List[Dict[str, Optional[str]]]]:
    # The CSV at FLORA_CSV_URL is flora_filtered.csv, which is pre-filtered upstream in line
    # with the protocol: it excludes OSF registrations, Boyce et al. (2023) student
    # replications, and retracted replications (RetractionWatch). Replications without a DOI
    # are retained here but are skipped downstream in flora_screening.py when doi_r is None.
    # Filter script: https://github.com/forrtproject/FReD-data/blob/main/R/filter_flora.R
    pairs_by_original: Dict[str, List[Dict[str, Optional[str]]]] = {}
    seen_by_original: Dict[str, set] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return pairs_by_original
        field_map = {_normalize_field_name(name): name for name in reader.fieldnames}

        def _row_value(row: Dict[str, Any], key: str) -> Optional[str]:
            return _clean_text(row.get(field_map.get(key, key)))

        for row in reader:
            # FLoRA contains both replication and reproduction records.  This
            # intervention is specifically about notifying authors of
            # replications, so fail closed when the upstream type is absent or
            # has any other value.
            study_type = (_row_value(row, "type") or "").lower()
            if study_type != "replication":
                continue
            doi_o = normalize_doi(_row_value(row, "doi_o"))
            if not doi_o:
                continue
            outcome = _normalize_outcome(
                _row_value(row, "outcome")
                or _row_value(row, "outcome_r")
                or _row_value(row, "replication_outcome")
            )
            # Protocol-aligned outcomes are mandatory for replication rows.
            if outcome is None:
                continue
            rec = {
                "doi_o": doi_o,
                "doi_r": normalize_doi(_row_value(row, "doi_r")),
                "apa_ref_o": _row_value(row, "apa_ref_o"),
                "apa_ref_r": _row_value(row, "apa_ref_r"),
                "replication_outcome": outcome,
                "oa_url_r": _valid_url(_row_value(row, "oa_url_r")),
            }
            key = (
                rec["doi_o"],
                rec["doi_r"],
                rec["apa_ref_o"],
                rec["apa_ref_r"],
                rec["replication_outcome"],
            )
            seen = seen_by_original.setdefault(doi_o, set())
            if key in seen:
                continue
            seen.add(key)
            pairs_by_original.setdefault(doi_o, []).append(rec)

    return pairs_by_original
