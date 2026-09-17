from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import requests
from lxml import etree
from boto3.dynamodb.types import TypeDeserializer

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXTRACTION_DIR = Path(__file__).resolve().parent
if str(EXTRACTION_DIR) not in sys.path:
    sys.path.insert(0, str(EXTRACTION_DIR))

from osf_sync.dynamo.preprints_repo import PreprintsRepo
from osf_sync.dynamo.api_cache_repo import ApiCacheRepo
from osf_sync.email.blacklist import load_blacklist
from osf_sync.email.validation import validate_recipient, repair_email_tld
from osf_sync.email.suppression import is_suppressed
from osf_sync.exclusion_logging import log_preprint_exclusion
from osf_sync.iter_preprints import SESSION, OSF_API
from osf_sync.pdf import ensure_pdf_available_or_skip
import osf_sync.grobid as grobid
from get_mail_from_pdf import get_mail_from_pdf
from get_orcid_from_pdf import get_orcid_from_pdf

ORCID_API = os.environ.get("ORCID_API", "https://pub.orcid.org/v3.0")
ORCID_TOKEN_URL = os.environ.get("ORCID_TOKEN_URL", "https://orcid.org/oauth/token")
ORCID_TIMEOUT = float(os.environ.get("ORCID_TIMEOUT", "60"))
ORCID_MAX_RETRIES = int(os.environ.get("ORCID_MAX_RETRIES", "3"))
ORCID_BACKOFF = float(os.environ.get("ORCID_BACKOFF", "1.5"))
GROBID_MAX_RETRIES = int(os.environ.get("GROBID_MAX_RETRIES", "2"))
GROBID_BACKOFF = float(os.environ.get("GROBID_BACKOFF", "2.0"))
ORCID_CLIENT_ID = os.environ.get("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.environ.get("ORCID_CLIENT_SECRET")
ORCID_HEADERS = {"Accept": "application/json"}
OPENALEX_API = os.environ.get("OPENALEX_API", "https://api.openalex.org")
OPENALEX_MAILTO = os.environ.get("OPENALEX_MAILTO") or os.environ.get("OPENALEX_EMAIL")
OPENALEX_TIMEOUT = float(os.environ.get("OPENALEX_TIMEOUT", "20"))
OPENALEX_MAX_RETRIES = int(os.environ.get("OPENALEX_MAX_RETRIES", "2"))
OPENALEX_BACKOFF = float(os.environ.get("OPENALEX_BACKOFF", "1.5"))
AFFILIATION_DOMAIN_BONUS = float(os.environ.get("AFFILIATION_DOMAIN_BONUS", "0.20"))

TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}

CSV_COLUMNS = [
    "id",
    "name.surname",
    "name.given",
    "email",
    "affiliation",
    "n",
    "orcid.xml",
    "source",
    "osf.id",
    "osf.name",
    "orcid.osf",
    "orcid.pdf",
    "name.given.orcid",
    "name.surname.orcid",
    "orcid.name",
    "email.source",
    "orcid",
    "orcid.source",
    "pdf.email",
    "affiliation.orcid",
    "email.possible",
    "email.similarity",
    "review_needed",
]
DEFAULT_DEBUG_CSV = EXTRACTION_DIR / "authorList_ext.debug.csv"
PDF_EMAIL_MATCH_THRESHOLD = float(os.environ.get("PDF_EMAIL_MATCH_THRESHOLD", "0.75"))
PDF_ORCID_MATCH_THRESHOLD = float(os.environ.get("PDF_ORCID_MATCH_THRESHOLD", "0.75"))

# Persistent API cache (DynamoDB) — lazily initialised so env vars are
# resolved at first use rather than at module import time.
_api_cache: Optional["ApiCacheRepo"] = None


def _get_api_cache() -> "ApiCacheRepo":
    global _api_cache
    if _api_cache is None:
        _api_cache = ApiCacheRepo()
    return _api_cache
_ORCID_PERSON_TTL = 30 * 24 * 3600  # 30 days
_ORCID_EMPLOYMENT_TTL = 30 * 24 * 3600  # 30 days
_ORCID_NAME_SEARCH_TTL = 7 * 24 * 3600  # 7 days
_OSF_CONTRIBUTORS_TTL = 7 * 24 * 3600  # 7 days
_OPENALEX_INSTITUTION_TTL = 30 * 24 * 3600  # 30 days


def _log(msg: str):
    print(msg, flush=True)
    if _LOG_FH:
        _LOG_FH.write(msg + "\n")
        _LOG_FH.flush()

DEBUG = False
_LOG_FH = None
_ORCID_TOKEN: Optional[str] = None
_ORCID_TOKEN_EXPIRES_AT: Optional[float] = None


def _dbg(msg: str):
    if DEBUG:
        print(msg, flush=True)
        if _LOG_FH:
            _LOG_FH.write(msg + "\n")
            _LOG_FH.flush()


def _normalize_ws(text: str) -> str:
    return " ".join(text.split())


EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,15}$", re.IGNORECASE)
def _clean_email(raw: Optional[str], *, allow_blacklist: bool = False) -> Optional[str]:
    if not raw:
        return None
    txt = raw.strip().strip("<>\"'").strip().strip(";").strip(",").strip(".")
    if txt.lower().startswith("mailto:"):
        txt = txt[7:].strip()
    if not txt:
        return None
    if not EMAIL_RE.match(txt):
        return None
    # Strip trailing PDF annotation artifacts (e.g. .Link, .Highlights)
    txt = repair_email_tld(txt)
    local, _, domain = txt.rpartition("@")
    if not local or not domain:
        return None
    local_l = local.lower()
    domain_l = domain.lower()
    if not allow_blacklist:
        blacklist = load_blacklist()
        if domain_l in blacklist["domains"]:
            return None
        if local_l in blacklist["locals"]:
            return None
        if txt.lower() in blacklist["emails"]:
            return None
    return txt


def _orcid_headers() -> Dict[str, str]:
    headers = dict(ORCID_HEADERS)
    token = _get_orcid_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _orcid_request(method: str, url: str, **kwargs) -> requests.Response:
    attempt = 0
    backoff = ORCID_BACKOFF
    while True:
        attempt += 1
        try:
            return requests.request(method, url, timeout=ORCID_TIMEOUT, **kwargs)
        except requests.RequestException as exc:
            if attempt >= ORCID_MAX_RETRIES:
                raise
            _dbg(f"[orcid] retry {attempt} error={exc}")
            time.sleep(backoff)
            backoff *= 2


def _openalex_request(url: str, *, params: Dict[str, Any]) -> requests.Response:
    attempt = 0
    backoff = OPENALEX_BACKOFF
    while True:
        attempt += 1
        try:
            return requests.get(url, params=params, timeout=OPENALEX_TIMEOUT)
        except requests.RequestException as exc:
            if attempt >= OPENALEX_MAX_RETRIES:
                raise
            _dbg(f"[openalex] retry {attempt} error={exc}")
            time.sleep(backoff)
            backoff *= 2


def _get_orcid_token() -> Optional[str]:
    global _ORCID_TOKEN, _ORCID_TOKEN_EXPIRES_AT
    if not ORCID_CLIENT_ID or not ORCID_CLIENT_SECRET:
        return None
    now = time.time()
    if _ORCID_TOKEN and _ORCID_TOKEN_EXPIRES_AT and now < _ORCID_TOKEN_EXPIRES_AT:
        return _ORCID_TOKEN
    data = {
        "client_id": ORCID_CLIENT_ID,
        "client_secret": ORCID_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }
    try:
        r = _orcid_request("POST", ORCID_TOKEN_URL, data=data)
        if r.status_code >= 400:
            _dbg(f"[orcid] token status={r.status_code}")
            return None
        payload = r.json()
        _ORCID_TOKEN = payload.get("access_token")
        expires_in = float(payload.get("expires_in") or 0)
        _ORCID_TOKEN_EXPIRES_AT = now + max(0, expires_in - 30)
        return _ORCID_TOKEN
    except Exception as exc:
        _dbg(f"[orcid] token error={exc}")
        return None


def _filter_emails(emails: List[str]) -> Tuple[List[str], int]:
    valid: List[str] = []
    invalid = 0
    for raw in emails:
        val = _clean_email(raw)
        if val:
            valid.append(val)
        else:
            invalid += 1
    valid = list(dict.fromkeys(valid))
    return valid, invalid


def _inc(stats: Dict[str, int], key: str, n: int = 1) -> None:
    stats[key] = stats.get(key, 0) + n


def _normalize_name(text: str) -> str:
    if not text:
        return ""
    txt = unicodedata.normalize("NFKD", text)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.lower()
    txt = re.sub(r"[\W\d_]+", "", txt, flags=re.UNICODE)
    txt = re.sub(r"[^a-z]", "", txt)
    return txt


def _first_name(text: str) -> str:
    if not text:
        return ""
    return text.split()[0]


def _initials(text: str) -> str:
    if not text:
        return ""
    parts = re.split(r"\s+", text.strip())
    return "".join(p[0] for p in parts if p)


def _initials_only(text: str) -> str:
    if not text:
        return ""
    parts = [p for p in re.split(r"\s+", text.strip()) if p]
    initials = []
    for p in parts:
        p_clean = p.strip(".")
        if p_clean:
            initials.append(p_clean[0])
    return "".join(initials)


def _strip_middle_initials(text: str) -> str:
    if not text:
        return ""
    parts = [p for p in re.split(r"\s+", text.strip()) if p]
    cleaned = []
    for p in parts:
        p_clean = p.strip(".")
        if len(p_clean) == 1:
            continue
        cleaned.append(p)
    return " ".join(cleaned)


def _levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            insert = curr[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            curr.append(min(insert, delete, sub))
        prev = curr
    return prev[-1]


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    dist = _levenshtein_distance(a, b)
    denom = max(len(a), len(b))
    return 1.0 - (dist / denom if denom else 0.0)


def _normalize_orcid(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    txt = raw.strip()
    match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[0-9X])", txt, re.IGNORECASE)
    if not match:
        match = re.search(r"(\d{15}[0-9X])", txt, re.IGNORECASE)
    if not match:
        return None
    val = match.group(1).upper()
    if len(val) == 16:
        val = "-".join([val[i : i + 4] for i in range(0, 16, 4)])
    if not _valid_orcid(val):
        return None
    return val


def _valid_orcid(orcid: str) -> bool:
    if not orcid:
        return False
    digits = orcid.replace("-", "").upper()
    if not re.match(r"^\d{15}[0-9X]$", digits):
        return False
    total = 0
    for ch in digits[:-1]:
        total = (total + int(ch)) * 2
    remainder = total % 11
    result = (12 - remainder) % 11
    check = "X" if result == 10 else str(result)
    return digits[-1] == check


def _deserialize_raw(raw: Any) -> Optional[dict]:
    if raw is None:
        return None
    if isinstance(raw, dict) and set(raw.keys()) == {"M"}:
        des = TypeDeserializer()
        return des.deserialize(raw)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    if isinstance(raw, dict):
        return raw
    return None


def _provider_id_from_raw(raw: dict) -> Optional[str]:
    rel = (raw.get("relationships") or {}).get("provider") or {}
    data = rel.get("data") or {}
    return data.get("id")


def _ensure_pdf(
    osf_id: str,
    provider_id: str,
    raw: dict,
    dest_root: str,
) -> Optional[Path]:
    start = time.perf_counter()
    kind, path, _reason = ensure_pdf_available_or_skip(
        osf_id=osf_id,
        provider_id=provider_id,
        raw=raw,
        dest_root=dest_root,
    )
    _dbg(f"[{osf_id}] pdf: kind={kind} path={path} in {time.perf_counter() - start:.2f}s")
    if kind == "skipped" or not path:
        if _reason:
            log_preprint_exclusion(
                reason=_reason,
                osf_id=osf_id,
                stage="author",
                details={"provider_id": provider_id},
            )
        return None
    return Path(path)


def _ensure_tei(provider_id: str, osf_id: str, dest_root: str) -> Optional[Path]:
    tei_path = Path(dest_root) / provider_id / osf_id / "tei.xml"
    if tei_path.exists():
        _dbg(f"[{osf_id}] tei: found {tei_path}")
        return tei_path
    # Try S3 cache before expensive GROBID regeneration.
    from ..tei_cache import download_tei
    if download_tei(provider_id, osf_id, str(tei_path)):
        _dbg(f"[{osf_id}] tei: restored from S3 cache")
        return tei_path
    attempt = 0
    backoff = GROBID_BACKOFF
    while True:
        attempt += 1
        start = time.perf_counter()
        ok, out, err = grobid.process_pdf_to_tei(provider_id, osf_id)
        _dbg(
            f"[{osf_id}] tei: ok={ok} out={out} err={err} "
            f"in {time.perf_counter() - start:.2f}s (attempt {attempt})"
        )
        if ok and out:
            return Path(out)
        if attempt >= GROBID_MAX_RETRIES:
            return None
        # Retry on common transient errors
        err_s = (err or "").lower()
        if "timeout" in err_s or "connection" in err_s or "aborted" in err_s:
            time.sleep(backoff)
            backoff *= 2
            continue
        return None


def _cleanup_paths(*paths: Optional[Path]) -> None:
    for p in paths:
        if not p:
            continue
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass
    for p in paths:
        if not p:
            continue
        try:
            parent = p.parent
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        except Exception:
            pass


@dataclass
class TeiAuthor:
    given: Optional[str]
    surname: Optional[str]
    email: Optional[str]
    affiliation: Optional[str]
    orcid_xml: Optional[str]
    n: Optional[int]


def _extract_authors_from_tei(tei_path: Path) -> List[TeiAuthor]:
    start = time.perf_counter()
    authors: List[TeiAuthor] = []
    tree = etree.parse(str(tei_path))
    nodes = tree.xpath(
        "//tei:teiHeader//tei:sourceDesc//tei:analytic/tei:author",
        namespaces=TEI_NS,
    )
    if not nodes:
        nodes = tree.xpath(
            "//tei:teiHeader//tei:titleStmt/tei:author",
            namespaces=TEI_NS,
        )
    for idx, author in enumerate(nodes, start=1):
        surnames = [t.strip() for t in author.xpath(".//tei:surname/text()", namespaces=TEI_NS) if t.strip()]
        forenames = [t.strip() for t in author.xpath(".//tei:forename/text()", namespaces=TEI_NS) if t.strip()]
        surname = " ".join(surnames) if surnames else None
        given = " ".join(forenames) if forenames else None
        email_nodes = author.xpath(".//tei:email/text()", namespaces=TEI_NS)
        email = email_nodes[0].strip() if email_nodes else None
        if email and email.lower() == "false":
            email = None

        affil_nodes = author.xpath(".//tei:affiliation", namespaces=TEI_NS)
        affils: List[str] = []
        for affil in affil_nodes:
            txt = _normalize_ws(" ".join(affil.xpath(".//text()", namespaces=TEI_NS))).strip()
            if txt:
                affils.append(txt)
        affiliation = "; ".join(dict.fromkeys(affils)) if affils else None

        orcid = None
        idno_nodes = author.xpath(".//tei:idno", namespaces=TEI_NS)
        for node in idno_nodes:
            typ = (node.get("type") or "").lower()
            raw = _normalize_ws(" ".join(node.xpath(".//text()", namespaces=TEI_NS))).strip()
            if "orcid" in typ or "orcid" in raw.lower():
                cand = _normalize_orcid(raw)
                if cand:
                    orcid = cand
                    break
        if not orcid:
            ptrs = author.xpath(".//tei:ptr/@target", namespaces=TEI_NS)
            for raw in ptrs:
                cand = _normalize_orcid(raw)
                if cand:
                    orcid = cand
                    break

        authors.append(
            TeiAuthor(
                given=given,
                surname=surname,
                email=email if email else None,
                affiliation=affiliation,
                orcid_xml=orcid,
                n=idx,
            )
        )
    _dbg(f"[tei] parsed {len(authors)} authors from {tei_path} in {time.perf_counter() - start:.2f}s")
    return authors


def _fetch_contributors(osf_id: str) -> List[dict]:
    # Check DynamoDB persistent cache first
    db_key = f"osf_contributors::{osf_id}"
    db_item = _get_api_cache().get(db_key)
    if db_item and _get_api_cache().is_fresh(db_item, ttl_seconds=_OSF_CONTRIBUTORS_TTL):
        payload = db_item.get("payload")
        if isinstance(payload, list):
            _dbg(f"[{osf_id}] osf contributors from cache total={len(payload)}")
            return payload
    # Fetch from API
    out: List[dict] = []
    url = f"{OSF_API}/preprints/{osf_id}/contributors/"
    page = 0
    while url:
        page += 1
        r = SESSION.get(url, timeout=(10, 120))
        _dbg(f"[{osf_id}] osf contributors page={page} status={r.status_code}")
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("data", []))
        url = (data.get("links") or {}).get("next")
    _dbg(f"[{osf_id}] osf contributors total={len(out)}")
    # Persist to DynamoDB cache
    _get_api_cache().put(db_key, out, source="osf", ttl_seconds=_OSF_CONTRIBUTORS_TTL)
    return out


def _fetch_osf_user(user_id: str, cache: Dict[str, dict]) -> Optional[dict]:
    if user_id in cache:
        return cache[user_id]
    url = f"{OSF_API}/users/{user_id}/"
    r = SESSION.get(url, timeout=(10, 120))
    _dbg(f"[osf user] id={user_id} status={r.status_code}")
    if r.status_code >= 400:
        cache[user_id] = None
        return None
    data = r.json().get("data") or {}
    cache[user_id] = data
    return data


def _extract_osf_authors(osf_id: str, cache: Dict[str, dict]) -> List[Dict[str, Any]]:
    start = time.perf_counter()
    contributors = _fetch_contributors(osf_id)
    authors: List[Dict[str, Any]] = []
    for c in contributors:
        rel = (c.get("relationships") or {}).get("users") or {}
        user_id = ((rel.get("data") or {}) or {}).get("id")
        if not user_id:
            continue
        user = _fetch_osf_user(user_id, cache)
        if not user:
            continue
        attrs = user.get("attributes") or {}
        given = (attrs.get("given_name") or "").strip()
        middle = (attrs.get("middle_names") or "").strip()
        family = (attrs.get("family_name") or "").strip()
        full_given = f"{given} {middle}".strip()
        name = " ".join([full_given, family]).strip()

        social = attrs.get("social") or {}
        orcid = _normalize_orcid(social.get("orcid"))

        affiliations: List[str] = []
        for emp in attrs.get("employment") or []:
            end_year = emp.get("endYear")
            if end_year is None or str(end_year).strip() == "":
                inst = (emp.get("institution") or "").strip()
                if inst:
                    affiliations.append(inst)

        authors.append(
            {
                "osf.id": user_id,
                "osf.name": name if name else None,
                "osf.name.given": full_given if full_given else None,
                "osf.name.surname": family if family else None,
                "osf.affiliation": ";".join(affiliations) if affiliations else None,
                "github": (social.get("github") or "").strip() or None,
                "orcid.osf": orcid,
            }
        )
    _dbg(f"[{osf_id}] osf authors={len(authors)} in {time.perf_counter() - start:.2f}s")
    return authors


def _match_authors(
    tei_authors: List[TeiAuthor],
    osf_authors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    tei_pool = list(tei_authors)
    matched: List[Dict[str, Any]] = []
    for osf in osf_authors:
        best_idx = None
        best_dist = None

        osf_given = osf.get("osf.name.given") or ""
        osf_surname = osf.get("osf.name.surname") or ""
        osf_full = f"{osf_given} {osf_surname}".strip()

        osf_keys = [
            _normalize_name(osf_full),
            _normalize_name(f"{_first_name(osf_given)} {osf_surname}".strip()),
            _normalize_name(f"{_initials(osf_given)} {osf_surname}".strip()),
            _normalize_name(osf_given),
            _normalize_name(osf_surname),
        ]

        for idx, tei in enumerate(tei_pool):
            tei_full = f"{tei.given or ''} {tei.surname or ''}".strip()
            tei_keys = [
                _normalize_name(tei_full),
                _normalize_name(f"{_first_name(tei.given or '')} {tei.surname or ''}".strip()),
                _normalize_name(f"{_initials(tei.given or '')} {tei.surname or ''}".strip()),
                _normalize_name(tei.given or ""),
                _normalize_name(tei.surname or ""),
            ]
            for k in range(len(osf_keys)):
                a = osf_keys[k]
                b = tei_keys[k]
                if not a or not b:
                    continue
                dist = _levenshtein_distance(a, b)
                if dist <= 2 and (best_dist is None or dist < best_dist):
                    best_dist = dist
                    best_idx = idx
            if best_dist == 0:
                break

        row = dict(osf)
        if best_idx is not None:
            tei = tei_pool.pop(best_idx)
            row.update(
                {
                    "name.given": tei.given,
                    "name.surname": tei.surname,
                    "email": tei.email,
                    "affiliation": tei.affiliation,
                    "orcid.xml": tei.orcid_xml,
                    "n": tei.n,
                }
            )
        else:
            row.update(
                {
                    "name.given": None,
                    "name.surname": None,
                    "email": None,
                    "affiliation": None,
                    "orcid.xml": None,
                    "n": None,
                }
            )
        matched.append(row)
    return matched


def _format_r_list(values: List[str]) -> str:
    quoted = [f"\"{v}\"" for v in values]
    return ", ".join(quoted)


def _format_affiliations(values: List[str]) -> Optional[str]:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return f"c({_format_r_list(values)})"


def _fetch_orcid_person(orcid: str, cache: Dict[str, dict]) -> Optional[dict]:
    if orcid in cache:
        return cache[orcid]
    # L2: check DynamoDB persistent cache
    db_key = f"orcid_person::{orcid}"
    db_item = _get_api_cache().get(db_key)
    if db_item and _get_api_cache().is_fresh(db_item, ttl_seconds=_ORCID_PERSON_TTL):
        payload = db_item.get("payload")
        if isinstance(payload, dict) and payload.get("_none") is True:
            cache[orcid] = None
            return None
        cache[orcid] = payload
        return payload
    # L3: call API
    url = f"{ORCID_API}/{orcid}/person"
    r = _orcid_request("GET", url, headers=_orcid_headers())
    _dbg(f"[orcid] person {orcid} status={r.status_code}")
    if r.status_code >= 400:
        cache[orcid] = None
        _get_api_cache().put(db_key, {"_none": True}, source="orcid", ttl_seconds=_ORCID_PERSON_TTL)
        return None
    data = r.json()
    cache[orcid] = data
    _get_api_cache().put(db_key, data, source="orcid", ttl_seconds=_ORCID_PERSON_TTL)
    return data


def _fetch_orcid_employments(orcid: str, cache: Dict[str, List[str]]) -> List[str]:
    if orcid in cache:
        return cache[orcid]
    # L2: check DynamoDB persistent cache
    db_key = f"orcid_employments::{orcid}"
    db_item = _get_api_cache().get(db_key)
    if db_item and _get_api_cache().is_fresh(db_item, ttl_seconds=_ORCID_EMPLOYMENT_TTL):
        payload = db_item.get("payload")
        if isinstance(payload, list):
            cache[orcid] = payload
            return payload
    # L3: call API
    url = f"{ORCID_API}/{orcid}/employments"
    r = _orcid_request("GET", url, headers=_orcid_headers())
    _dbg(f"[orcid] employments {orcid} status={r.status_code}")
    if r.status_code >= 400:
        cache[orcid] = []
        _get_api_cache().put(db_key, [], source="orcid", ttl_seconds=_ORCID_EMPLOYMENT_TTL)
        return []
    data = r.json()
    insts: List[str] = []
    groups = data.get("affiliation-group") or []
    if isinstance(groups, dict):
        groups = [groups]
    for group in groups:
        summaries = group.get("summaries") or []
        for item in summaries:
            summary = item.get("employment-summary") or {}
            org = summary.get("organization") or {}
            name = (org.get("name") or "").strip()
            if name:
                insts.append(name)
    result = list(dict.fromkeys(insts))
    cache[orcid] = result
    _get_api_cache().put(db_key, result, source="orcid", ttl_seconds=_ORCID_EMPLOYMENT_TTL)
    return result


def _search_orcid_by_name(
    family: str,
    given: str,
    cache: Dict[Tuple[str, str], Optional[str]],
) -> Optional[str]:
    key = (family, given)
    if key in cache:
        return cache[key]
    fam = re.sub(r"[^\w\s]", "", family, flags=re.UNICODE)
    giv = re.sub(r"[^\w\s]", "", given, flags=re.UNICODE)
    if not fam or not giv:
        cache[key] = None
        return None
    # L2: check DynamoDB persistent cache
    db_key = f"orcid_name_search::{fam}::{giv}"
    db_item = _get_api_cache().get(db_key)
    if db_item and _get_api_cache().is_fresh(db_item, ttl_seconds=_ORCID_NAME_SEARCH_TTL):
        payload = db_item.get("payload")
        if isinstance(payload, dict) and payload.get("_none") is True:
            cache[key] = None
            return None
        if isinstance(payload, str):
            cache[key] = payload
            return payload
    # L3: call API
    q = f"family-name:{fam} AND given-names:{giv}"
    url = f"{ORCID_API}/search/"
    r = _orcid_request("GET", url, headers=_orcid_headers(), params={"q": q})
    _dbg(f"[orcid] search family={fam} given={giv} status={r.status_code}")
    if r.status_code >= 400:
        cache[key] = None
        _get_api_cache().put(db_key, {"_none": True}, source="orcid", ttl_seconds=_ORCID_NAME_SEARCH_TTL)
        return None
    data = r.json()
    results = data.get("result") or []
    if len(results) != 1:
        cache[key] = None
        _get_api_cache().put(db_key, {"_none": True}, source="orcid", ttl_seconds=_ORCID_NAME_SEARCH_TTL)
        return None
    identifier = results[0].get("orcid-identifier") or {}
    orcid = _normalize_orcid(identifier.get("path"))
    cache[key] = orcid
    if orcid:
        _get_api_cache().put(db_key, orcid, source="orcid", ttl_seconds=_ORCID_NAME_SEARCH_TTL)
    else:
        _get_api_cache().put(db_key, {"_none": True}, source="orcid", ttl_seconds=_ORCID_NAME_SEARCH_TTL)
    return orcid


def _assign_orcid_from_pdf(
    rows: List[Dict[str, Any]],
    pdf_orcids: List[str],
    orcid_cache: Dict[str, dict],
    email_validation_cache: Optional[Dict[str, bool]] = None,
    *,
    threshold: float = PDF_ORCID_MATCH_THRESHOLD,
) -> int:
    email_validation_cache = email_validation_cache or {}
    assigned = 0
    if not pdf_orcids:
        return 0
    # Skip ORCID PDF enrichment if there are no rows that still need email enrichment.
    if not any(
        (not _row_has_contactable_email(row, email_validation_cache))
        and (not row.get("orcid.osf"))
        and (not row.get("orcid.xml"))
        and (not row.get("orcid.pdf"))
        for row in rows
    ):
        return 0
    infos = []
    for raw in pdf_orcids:
        orcid = _normalize_orcid(raw)
        if not orcid:
            continue
        if any(r.get("orcid.osf") == orcid or r.get("orcid.xml") == orcid for r in rows):
            continue
        person = _fetch_orcid_person(orcid, orcid_cache)
        if not person:
            continue
        name = person.get("name") or {}
        given = (name.get("given-names") or {}).get("value")
        family = (name.get("family-name") or {}).get("value")
        if not family:
            continue
        infos.append({"orcid": orcid, "given": given, "family": family})

    for info in infos:
        best_idx = None
        best_sim = -1.0
        for idx, row in enumerate(rows):
            if _row_has_contactable_email(row, email_validation_cache):
                continue
            if row.get("orcid.osf") or row.get("orcid.xml") or row.get("orcid.pdf"):
                continue
            osf_surname = row.get("osf.name.surname") or ""
            osf_given = row.get("osf.name.given") or ""
            sim = max(
                _similarity(_normalize_name(osf_surname), _normalize_name(info["family"])),
                _similarity(
                    _normalize_name(f"{osf_surname} {osf_given}".strip()),
                    _normalize_name(f"{info['family']} {info['given'] or ''}".strip()),
                ),
            )
            if sim > best_sim:
                best_sim = sim
                best_idx = idx
        if best_idx is not None and best_sim >= threshold:
            rows[best_idx]["orcid.pdf"] = info["orcid"]
            rows[best_idx]["name.given.orcid"] = info["given"]
            rows[best_idx]["name.surname.orcid"] = info["family"]
            assigned += 1
    return assigned


def _assign_orcid_by_name(
    rows: List[Dict[str, Any]],
    name_cache: Dict[Tuple[str, str], Optional[str]],
    email_validation_cache: Optional[Dict[str, bool]] = None,
) -> int:
    email_validation_cache = email_validation_cache or {}
    assigned = 0
    for row in rows:
        if _row_has_contactable_email(row, email_validation_cache):
            continue
        if row.get("orcid.osf") or row.get("orcid.xml") or row.get("orcid.pdf"):
            continue
        family = row.get("osf.name.surname") or ""
        given = row.get("osf.name.given") or ""
        if not family or not given:
            continue
        orcid = _search_orcid_by_name(family, given, name_cache)
        if orcid:
            row["orcid.name"] = orcid
            assigned += 1
    return assigned


def _merge_orcids(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"osf": 0, "xml": 0, "pdf": 0, "name": 0, "osf;xml": 0, "none": 0}
    for row in rows:
        osf = row.get("orcid.osf")
        xml = row.get("orcid.xml")
        pdf = row.get("orcid.pdf")
        name = row.get("orcid.name")

        orcid = None
        source = None

        if osf:
            orcid = osf
            if xml and xml == osf:
                source = "osf;xml"
            else:
                source = "osf"
        elif xml:
            orcid = xml
            source = "xml"
        elif pdf:
            orcid = pdf
            source = "pdf"
        elif name:
            orcid = name
            source = "name"

        row["orcid"] = orcid
        row["orcid.source"] = source
        if source:
            counts[source] = counts.get(source, 0) + 1
        else:
            counts["none"] += 1
    return counts


def _fill_orcid_details(
    rows: List[Dict[str, Any]],
    orcid_cache: Dict[str, dict],
    email_validation_cache: Optional[Dict[str, bool]] = None,
) -> Tuple[int, int, int]:
    email_validation_cache = email_validation_cache or {}
    emails_added = 0
    names_added = 0
    emails_invalid = 0
    for row in rows:
        orcid = row.get("orcid")
        if not orcid:
            continue
        # Keep existing matched emails (TEI/PDF) as authoritative and skip ORCID person lookup.
        if _row_has_contactable_email(row, email_validation_cache):
            continue
        person = _fetch_orcid_person(orcid, orcid_cache)
        if not person:
            continue
        name = person.get("name") or {}
        given = (name.get("given-names") or {}).get("value")
        family = (name.get("family-name") or {}).get("value")
        if given and not row.get("name.given.orcid"):
            row["name.given.orcid"] = given
            names_added += 1
        if family and not row.get("name.surname.orcid"):
            row["name.surname.orcid"] = family
            names_added += 1
        emails = (person.get("emails") or {}).get("email") or []
        if emails and (not _row_has_contactable_email(row, email_validation_cache)):
            first = emails[0]
            val = (first or {}).get("email")
            clean = _clean_email(val, allow_blacklist=True)
            if clean:
                row["email"] = clean
                row["email.source"] = "orcid"
                emails_added += 1
            elif val:
                emails_invalid += 1
    return emails_added, names_added, emails_invalid


def _fill_orcid_affiliations(
    rows: List[Dict[str, Any]],
    affil_cache: Dict[str, List[str]],
    email_validation_cache: Optional[Dict[str, bool]] = None,
) -> int:
    email_validation_cache = email_validation_cache or {}
    filled = 0
    for row in rows:
        if _row_has_contactable_email(row, email_validation_cache):
            continue
        orcid = row.get("orcid")
        if not orcid:
            continue
        insts = _fetch_orcid_employments(orcid, affil_cache)
        row["affiliation.orcid"] = _format_affiliations(insts)
        if insts:
            filled += 1
    return filled


def _process_preprint(
    item: dict,
    dest_root: str,
    osf_user_cache: Dict[str, dict],
    orcid_cache: Dict[str, dict],
    orcid_name_cache: Dict[Tuple[str, str], Optional[str]],
    orcid_affil_cache: Dict[str, List[str]],
    delete_files: bool,
    stats: Dict[str, int],
) -> List[Dict[str, Any]]:
    osf_id = item.get("osf_id")
    if not osf_id:
        return []
    raw = _deserialize_raw(item.get("raw"))
    if not raw:
        return []
    provider_id = item.get("provider_id") or _provider_id_from_raw(raw) or "unknown"
    _dbg(f"[{osf_id}] start provider={provider_id}")

    pdf_path = None
    tei_path = None
    try:
        pdf_path = _ensure_pdf(osf_id, provider_id, raw, dest_root)
        if not pdf_path:
            _dbg(f"[{osf_id}] step pdf: skipped (missing)")
            return []
        _inc(stats, "preprints_with_pdf")
        _dbg(f"[{osf_id}] step pdf: used")

        pdf_emails_raw = get_mail_from_pdf(str(pdf_path)) if pdf_path else []
        pdf_orcids_raw = get_orcid_from_pdf(str(pdf_path)) if pdf_path else []
        _inc(stats, "pdf_emails_found", len(pdf_emails_raw))
        _inc(stats, "pdf_orcids_found", len(pdf_orcids_raw))
        pdf_emails, pdf_emails_invalid = _filter_emails([e for e in pdf_emails_raw if e])
        _inc(stats, "pdf_emails_invalid", pdf_emails_invalid)
        _inc(stats, "pdf_emails_valid", len(pdf_emails))
        pdf_orcids = []
        pdf_orcids_invalid = 0
        for raw_orcid in pdf_orcids_raw:
            cand = _normalize_orcid(raw_orcid)
            if cand:
                pdf_orcids.append(cand)
            else:
                pdf_orcids_invalid += 1
        pdf_orcids = list(dict.fromkeys([o for o in pdf_orcids if o]))
        _inc(stats, "pdf_orcids_invalid", pdf_orcids_invalid)
        _inc(stats, "pdf_orcids_valid", len(pdf_orcids))
        _dbg(f"[{osf_id}] step pdf emails: {'used' if pdf_emails else 'skipped'} ({len(pdf_emails)})")
        _dbg(f"[{osf_id}] step pdf orcids: {'used' if pdf_orcids else 'skipped'} ({len(pdf_orcids)})")

        tei_path = _ensure_tei(provider_id, osf_id, dest_root)
        if tei_path:
            _inc(stats, "preprints_with_tei")
            _dbg(f"[{osf_id}] step tei: used")
        else:
            _dbg(f"[{osf_id}] step tei: skipped (missing)")

        tei_authors = _extract_authors_from_tei(tei_path) if tei_path else []
        if tei_path:
            _dbg(f"[{osf_id}] step tei authors: {'used' if tei_authors else 'skipped'} ({len(tei_authors)})")
        for a in tei_authors:
            if a.email:
                _inc(stats, "tei_emails_found")
                clean = _clean_email(a.email)
                if clean:
                    a.email = clean
                    _inc(stats, "tei_emails_valid")
                else:
                    a.email = None
                    _inc(stats, "tei_emails_invalid")
        osf_authors = _extract_osf_authors(osf_id, osf_user_cache)
        if not osf_authors:
            _dbg(f"[{osf_id}] step osf contributors: skipped (none)")
            return []
        _inc(stats, "preprints_with_osf")
        _dbg(f"[{osf_id}] step osf contributors: used ({len(osf_authors)})")

        rows = _match_authors(tei_authors, osf_authors)
        _dbg(f"[{osf_id}] step name matching: {'used' if tei_authors else 'skipped'} ({len(rows)} rows)")
        for row in rows:
            row["id"] = osf_id
            row["source"] = provider_id
            if row.get("email"):
                row["email.source"] = "xml"

        assigned_pdf_emails = _assign_pdf_emails(rows, pdf_emails)
        if assigned_pdf_emails:
            _inc(stats, "pdf_emails_assigned", assigned_pdf_emails)

        email_validation_cache: Dict[str, bool] = {}
        has_preprint_contacts = _has_contactable_preprint_email(rows, email_validation_cache)
        if has_preprint_contacts:
            _dbg(f"[{osf_id}] step orcid gate: skipped (preprint-extracted contactable email present)")
        assigned_pdf = 0
        assigned_name = 0
        emails_added = 0
        names_added = 0
        emails_invalid = 0
        affil_filled = 0
        if not has_preprint_contacts:
            assigned_pdf = _assign_orcid_from_pdf(rows, pdf_orcids, orcid_cache, email_validation_cache)
            assigned_name = _assign_orcid_by_name(rows, orcid_name_cache, email_validation_cache)
        source_counts = _merge_orcids(rows)
        if not has_preprint_contacts:
            emails_added, names_added, emails_invalid = _fill_orcid_details(
                rows,
                orcid_cache,
                email_validation_cache,
            )
            if emails_added:
                _inc(stats, "orcid_emails_added", emails_added)
            if emails_invalid:
                _inc(stats, "orcid_emails_invalid", emails_invalid)
            affil_filled = _fill_orcid_affiliations(rows, orcid_affil_cache, email_validation_cache)
        _dbg(f"[{osf_id}] step orcid from pdf: {'used' if assigned_pdf else 'skipped'} ({assigned_pdf})")
        _dbg(f"[{osf_id}] step orcid by name: {'used' if assigned_name else 'skipped'} ({assigned_name})")
        _dbg(f"[{osf_id}] step orcid merge: sources={source_counts}")
        _dbg(
            f"[{osf_id}] step orcid person: {'used' if emails_added or names_added else 'skipped'} "
            f"(emails={emails_added} names={names_added})"
        )
        _dbg(f"[{osf_id}] step orcid affiliations: {'used' if affil_filled else 'skipped'} ({affil_filled})")

        pdf_email_val = "false" if not pdf_emails else _format_r_list(pdf_emails)
        for row in rows:
            row["pdf.email"] = pdf_email_val
            if row.get("name.given") or row.get("name.surname"):
                _inc(stats, "rows_matched_tei")
            else:
                _inc(stats, "rows_unmatched_tei")
        _dbg(f"[{osf_id}] done rows={len(rows)}")
        _inc(stats, "rows_total", len(rows))
        for k, v in source_counts.items():
            _inc(stats, f"orcid_source_{k}", v)
        return rows
    finally:
        if delete_files:
            _cleanup_paths(pdf_path, tei_path)


def _iter_items_by_ids(repo: PreprintsRepo, ids: List[str]) -> Iterable[dict]:
    for osf_id in ids:
        it = repo.t_preprints.get_item(Key={"osf_id": osf_id}).get("Item")
        if it and not it.get("excluded"):
            yield it


def _iter_items_scan(repo: PreprintsRepo, limit: Optional[int]) -> Iterable[dict]:
    last_key = None
    scanned = 0
    while True:
        kwargs: Dict[str, Any] = {
            "FilterExpression": "(attribute_not_exists(excluded) OR excluded = :false)",
            "ExpressionAttributeValues": {":false": False},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        if limit:
            kwargs["Limit"] = max(1, limit - scanned)
        resp = repo.t_preprints.scan(**kwargs)
        items = resp.get("Items", [])
        for it in items:
            yield it
            scanned += 1
            if limit and scanned >= limit:
                return
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return


def _row_for_csv(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in CSV_COLUMNS:
        val = row.get(key)
        out[key] = "NA" if val is None else val
    return out


EMAIL_EXTRACT_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    flags=re.IGNORECASE | re.MULTILINE | re.UNICODE,
)


def _extract_emails_from_value(val: Optional[str]) -> List[str]:
    if not val:
        return []
    if isinstance(val, str) and val.strip().upper() == "NA":
        return []
    return list(dict.fromkeys(EMAIL_EXTRACT_RE.findall(str(val))))


def _extract_domain_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        host = (urlparse(url).hostname or "").strip().lower()
    except Exception:
        return None
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _affiliation_values_from_row(row: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    for key in ["affiliation", "osf.affiliation", "affiliation.orcid"]:
        raw = row.get(key)
        if not raw:
            continue
        txt = str(raw).strip()
        if not txt or txt.upper() == "NA":
            continue
        # Parse R-style c("a","b") from ORCID affiliation export.
        quoted = re.findall(r"\"([^\"]+)\"", txt)
        if quoted:
            values.extend([_normalize_ws(v).strip() for v in quoted if _normalize_ws(v).strip()])
            continue
        values.extend([_normalize_ws(v).strip() for v in txt.split(";") if _normalize_ws(v).strip()])
    return list(dict.fromkeys(values))


def _openalex_domain_for_affiliation(
    affiliation: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    norm = _normalize_ws(affiliation).strip()
    if not norm:
        return None
    key = norm.lower()
    if key in cache:
        return cache[key]

    db_key = f"openalex_institution_domain::{key}"
    db_item = _get_api_cache().get(db_key)
    if db_item and _get_api_cache().is_fresh(db_item, ttl_seconds=_OPENALEX_INSTITUTION_TTL):
        payload = db_item.get("payload")
        if isinstance(payload, dict) and payload.get("_none") is True:
            cache[key] = None
            return None
        if isinstance(payload, str):
            cache[key] = payload
            return payload

    params: Dict[str, Any] = {"search": norm, "per-page": 3}
    if OPENALEX_MAILTO:
        params["mailto"] = OPENALEX_MAILTO
    try:
        r = _openalex_request(f"{OPENALEX_API.rstrip('/')}/institutions", params=params)
    except Exception as exc:
        _dbg(f"[openalex] institutions lookup error={exc}")
        cache[key] = None
        return None
    if r.status_code >= 400:
        _dbg(f"[openalex] institutions status={r.status_code} q={norm}")
        cache[key] = None
        _get_api_cache().put(db_key, {"_none": True}, source="openalex", ttl_seconds=_OPENALEX_INSTITUTION_TTL)
        return None

    data = r.json()
    results = data.get("results") or []
    domain = None
    for inst in results:
        domain = _extract_domain_from_url((inst or {}).get("homepage_url") or "")
        if domain:
            break

    cache[key] = domain
    if domain:
        _get_api_cache().put(db_key, domain, source="openalex", ttl_seconds=_OPENALEX_INSTITUTION_TTL)
    else:
        _get_api_cache().put(db_key, {"_none": True}, source="openalex", ttl_seconds=_OPENALEX_INSTITUTION_TTL)
    return domain


def _affiliation_domains_for_row(
    row: Dict[str, Any],
    domain_cache: Dict[str, Optional[str]],
) -> List[str]:
    domains: List[str] = []
    for affiliation in _affiliation_values_from_row(row):
        domain = _openalex_domain_for_affiliation(affiliation, domain_cache)
        if domain:
            domains.append(domain)
    return list(dict.fromkeys(domains))


def _email_candidates_from_row(row: Dict[str, Any]) -> List[str]:
    emails: List[str] = []
    for key, val in row.items():
        k = key.lower()
        if "email" not in k:
            continue
        if k in {"email.source", "email.similarity", "email.possible", "review_needed"}:
            continue
        emails.extend(_extract_emails_from_value(val))
    # Clean + dedupe
    cleaned: List[str] = []
    for raw in emails:
        val = _clean_email(raw)
        if val:
            cleaned.append(val)
    return list(dict.fromkeys(cleaned))


def _has_row_email(row: Dict[str, Any]) -> bool:
    val = str(row.get("email") or "").strip()
    if not val:
        return False
    if val.upper() == "NA" or val.lower() == "false":
        return False
    return True


def _clear_row_email(row: Dict[str, Any]) -> None:
    row["email"] = None
    row["email.source"] = None


def _row_has_contactable_email(
    row: Dict[str, Any],
    validation_cache: Dict[str, bool],
) -> bool:
    source = (row.get("email.source") or "").strip().lower()
    allow_blacklist = source == "orcid"
    cleaned = _clean_email((row.get("email") or "").strip(), allow_blacklist=allow_blacklist)
    if not cleaned:
        if _has_row_email(row):
            _clear_row_email(row)
        return False
    key = cleaned.lower()
    if key not in validation_cache:
        try:
            ok, _err = validate_recipient(cleaned)
            validation_cache[key] = bool(ok)
        except Exception:
            # Fail-open on transient validation failures to avoid dropping valid contacts.
            validation_cache[key] = True
    if not validation_cache[key]:
        _clear_row_email(row)
        return False
    row["email"] = cleaned
    return True


def _has_contactable_preprint_email(
    rows: List[Dict[str, Any]],
    validation_cache: Dict[str, bool],
) -> bool:
    for row in rows:
        source = (row.get("email.source") or "").strip().lower()
        if source not in {"xml", "pdf"}:
            continue
        if _row_has_contactable_email(row, validation_cache):
            return True
    return False


def _author_position(row: Dict[str, Any], fallback_index: int) -> int:
    try:
        n = int(row.get("n"))
        if n > 0:
            return n
    except Exception:
        pass
    return fallback_index + 1


def _row_email_for_selection(row: Dict[str, Any]) -> Optional[str]:
    source = (row.get("email.source") or "").strip().lower()
    allow_blacklist = source == "orcid"
    return _clean_email((row.get("email") or "").strip(), allow_blacklist=allow_blacklist)


def _full_name_from_row(row: Dict[str, Any]) -> str:
    # Prefer TEI/ORCID names over OSF display name for author_email_candidates
    given = (row.get("name.given") or "").strip()
    surname = (row.get("name.surname") or "").strip()
    if given or surname:
        return " ".join([given, surname]).strip()
    given = (row.get("name.given.orcid") or "").strip()
    surname = (row.get("name.surname.orcid") or "").strip()
    if given or surname:
        return " ".join([given, surname]).strip()
    for k in ["osf.name", "name", "full_name"]:
        val = (row.get(k) or "").strip()
        if val and val.upper() != "NA":
            return val
    return ""


def _best_email_for_author(
    given: str,
    surname: str,
    emails: List[str],
    *,
    preferred_domains: Optional[List[str]] = None,
    domain_bonus: float = AFFILIATION_DOMAIN_BONUS,
) -> Tuple[Optional[str], float]:
    if not emails:
        return None, 0.0
    best_email = None
    best_sim = 0.0
    given_n = _normalize_name(given)
    surname_n = _normalize_name(surname)
    given_no_mid = _strip_middle_initials(given)
    given_no_mid_n = _normalize_name(given_no_mid)
    full_gs = _normalize_name(f"{given} {surname}".strip())
    full_sg = _normalize_name(f"{surname} {given}".strip())
    full_gs_no_mid = _normalize_name(f"{given_no_mid} {surname}".strip())
    init_s = _normalize_name(f"{_first_name(given_no_mid)[:1]} {surname}".strip()) if given_no_mid else _normalize_name(surname)
    variants = [v for v in {full_gs, full_sg, full_gs_no_mid, init_s, given_n, given_no_mid_n, surname_n} if v]
    initials_raw = _initials_only(f"{given_no_mid} {surname}".strip())
    initials_norm = _normalize_name(initials_raw)
    initials_raw_full = _initials_only(f"{given} {surname}".strip())
    initials_norm_full = _normalize_name(initials_raw_full)
    preferred_domain_set = set()
    for dom in preferred_domains or []:
        clean = (dom or "").strip().lower()
        if clean:
            preferred_domain_set.add(clean)
    for email in emails:
        _, _, domain_raw = email.rpartition("@")
        domain = domain_raw.lower()
        local_raw = email.split("@", 1)[0].lower()
        local = _normalize_name(local_raw)
        if not local:
            continue
        sims: List[float] = []
        for v in variants:
            sims.append(_similarity(v, local))
            # A local part fully contained in a name variant (or vice versa) is a
            # strong signal even when string distance is diluted by length difference.
            # This catches patterns like "karimima" ⊂ "karimimalekabadi" or
            # "lwal" ⊂ "lwallrich" (initial + surname prefix, with digits already
            # stripped from local by _normalize_name, e.g. "lwal004" → "lwal").
            if len(local) >= 4 and local in v:
                sims.append(0.85)
            elif len(v) >= 4 and v in local:
                sims.append(0.85)
        sim = max(sims) if sims else 0.0
        # Prefer locals that contain both given + surname over single-token matches
        if given_n and surname_n:
            if given_n in local and surname_n in local:
                sim += 0.20
            elif local == surname_n:
                sim -= 0.10
        # Boost emails that are primarily initials (e.g., stz2)
        if initials_norm or initials_norm_full:
            local_alpha = _normalize_name(re.sub(r"\d+", "", local_raw))
            for init in [initials_norm_full, initials_norm]:
                if not init:
                    continue
                if local_alpha == init:
                    sim = max(sim, 0.90)
                    break
                if local_alpha.startswith(init):
                    sim = max(sim, 0.75)
                    break
                if init in local_alpha:
                    sim = max(sim, 0.65)
                    break
        if (
            preferred_domain_set
            and domain
            and domain_bonus > 0
            and any(domain == pref or domain.endswith("." + pref) for pref in preferred_domain_set)
        ):
            sim += domain_bonus
        if sim > 1.0:
            sim = 1.0
        if sim > best_sim:
            best_sim = sim
            best_email = email
    return best_email, best_sim


def _score_group_email_matches(
    group: List[Dict[str, Any]],
    threshold: float,
    *,
    repo: Optional[PreprintsRepo] = None,
    enforce_contactability: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    openalex_domain_cache: Dict[str, Optional[str]] = {}
    validation_cache: Dict[str, bool] = {}
    suppression_cache: Dict[str, bool] = {}

    def _pick(row: Dict[str, Any], keys: List[str]) -> str:
        for k in keys:
            val = row.get(k)
            if val and str(val).strip().upper() != "NA":
                return str(val).strip()
        return ""

    given_keys = [
        "given",
        "first_name",
        "first",
        "fname",
    ]
    surname_keys = [
        "surname",
        "last_name",
        "last",
        "lname",
        "family",
    ]

    def _score_with_names(
        given: str,
        surname: str,
        candidates: List[str],
        preferred_domains: List[str],
    ) -> Tuple[Optional[str], float]:
        if not given or not surname:
            return None, 0.0
        return _best_email_for_author(
            given,
            surname,
            candidates,
            preferred_domains=preferred_domains,
        )

    candidates: List[str] = []
    for row in group:
        candidates.extend(_email_candidates_from_row(row))
    candidates = list(dict.fromkeys(candidates))

    for row in group:
        scores: List[Tuple[Optional[str], float]] = []
        preferred_domains = _affiliation_domains_for_row(row, openalex_domain_cache)

        # OSF names
        osf_given = _pick(row, ["osf.name.given"])
        osf_surname = _pick(row, ["osf.name.surname"])
        scores.append(_score_with_names(osf_given, osf_surname, candidates, preferred_domains))

        # ORCID names
        orcid_given = _pick(row, ["name.given.orcid"])
        orcid_surname = _pick(row, ["name.surname.orcid"])
        scores.append(_score_with_names(orcid_given, orcid_surname, candidates, preferred_domains))

        # PDF/TEI names
        tei_given = _pick(row, ["name.given"] + given_keys)
        tei_surname = _pick(row, ["name.surname"] + surname_keys)
        scores.append(_score_with_names(tei_given, tei_surname, candidates, preferred_domains))

        # Fallback to any full name if all sources missing
        if not any(sim > 0 for _, sim in scores):
            full = _pick(row, ["osf.name", "name", "full_name"])
            if full:
                parts = full.split()
                if len(parts) >= 2:
                    fallback_given = " ".join(parts[:-1])
                    fallback_surname = parts[-1]
                    scores.append(
                        _score_with_names(fallback_given, fallback_surname, candidates, preferred_domains)
                    )

        best_email, best_sim = max(scores, key=lambda s: s[1]) if scores else (None, 0.0)
        row["email.possible"] = best_email or "NA"
        row["email.similarity"] = f"{best_sim:.3f}" if best_email else "0.000"
        existing = (row.get("email") or "").strip()
        if existing.upper() == "NA":
            existing = ""
        review = (
            (not best_email)
            or (best_sim < threshold)
            or (existing and best_email and existing.lower() != best_email.lower())
        )
        row["review_needed"] = "TRUE" if review else "FALSE"

    ranked_rows = sorted(
        list(enumerate(group)),
        key=lambda item: _author_position(item[1], item[0]),
    )
    declared_candidates: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []
    seen_declared: Set[str] = set()
    seen_fallback: Set[str] = set()

    rejected_emails: Dict[str, str] = {}  # email -> reason

    def _append_candidate(
        rank: int,
        row: Dict[str, Any],
        target: List[Dict[str, Any]],
        seen: Set[str],
    ) -> bool:
        email = _row_email_for_selection(row)
        if not email:
            return False
        key = email.lower()
        if enforce_contactability:
            if key not in validation_cache:
                try:
                    ok, err = validate_recipient(email)
                    validation_cache[key] = bool(ok)
                    if not ok:
                        rejected_emails[key] = f"validation: {err}"
                except Exception as exc:
                    # Fail-open on transient validation failures to avoid
                    # dropping valid contacts (consistent with ORCID gate).
                    validation_cache[key] = True
                    _log(f"[warn] validate_recipient({email}) raised {exc!r}; fail-open")
            if not validation_cache[key]:
                return False
            if key not in suppression_cache:
                suppression_cache[key] = is_suppressed(email)
            if suppression_cache[key]:
                rejected_emails[key] = "suppressed"
                return False
        if key in seen:
            return False
        target.append({"name": _full_name_from_row(row), "email": email, "position": rank})
        seen.add(key)
        return True

    for rank, (_idx, row) in enumerate(ranked_rows):
        source = (row.get("email.source") or "").strip().lower()
        if source in {"xml", "pdf"}:
            _append_candidate(rank, row, declared_candidates, seen_declared)
        else:
            _append_candidate(rank, row, fallback_candidates, seen_fallback)

    author_count = len(ranked_rows)

    # Prefer declared corresponding-contact emails from TEI/PDF regardless of author position.
    if declared_candidates:
        if len(declared_candidates) <= 5:
            return declared_candidates, rejected_emails
        last_author_position = author_count - 1
        last_author_candidate = next(
            (cand for cand in declared_candidates if cand.get("position") == last_author_position),
            None,
        )
        if last_author_candidate:
            return declared_candidates[:4] + [last_author_candidate], rejected_emails
        return declared_candidates[:5], rejected_emails

    # Fallback contacts (ORCID/other inferred sources) are restricted to first four + last author.
    if author_count <= 0 or not fallback_candidates:
        return [], rejected_emails

    allowed_positions: Set[int] = set(range(min(4, author_count)))
    allowed_positions.add(author_count - 1)
    selected_fallback = [cand for cand in fallback_candidates if cand.get("position") in allowed_positions]
    return selected_fallback[:5], rejected_emails


def _count_contactable_candidates(candidates: List[Dict[str, Any]]) -> int:
    count = 0
    for cand in candidates:
        email = _clean_email((cand or {}).get("email"))
        if email:
            count += 1
    return count


def _can_exclude_for_no_author_contacts(item: Dict[str, Any]) -> bool:
    """Only pre-randomization records may be excluded for current contact state.

    Suppressions and bounces can make a previously usable address disappear on
    a later extraction.  That must not rewrite the eligibility history of a
    record that has already been assigned or emailed.
    """
    return not (
        item.get("trial_assignment_status")
        or item.get("email_sent") is True
    )


def _match_emails_in_csv(path: str, threshold: float) -> None:
    in_path = Path(path)
    tmp_path = in_path.with_suffix(in_path.suffix + ".tmp")
    with open(in_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    for col in ["email.possible", "email.similarity", "review_needed"]:
        if col not in fieldnames:
            fieldnames.append(col)

    by_id: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        pid = row.get("id") or row.get("osf_id") or ""
        by_id.setdefault(pid, []).append(row)

    for pid, group in by_id.items():
        _score_group_email_matches(group, threshold)

    with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(in_path)


def _given_surname_from_row(row: Dict[str, Any]) -> Tuple[str, str]:
    """Return (given, surname) using the best available name source.

    Priority: OSF names (canonical), then ORCID, then TEI.
    """
    for given_key, surname_key in [
        ("osf.name.given", "osf.name.surname"),
        ("name.given.orcid", "name.surname.orcid"),
        ("name.given", "name.surname"),
    ]:
        given = (row.get(given_key) or "").strip()
        surname = (row.get(surname_key) or "").strip()
        if given or surname:
            return given, surname
    return "", ""


def _assign_pdf_emails(
    rows: List[Dict[str, Any]],
    pdf_emails: List[str],
    *,
    threshold: float = PDF_EMAIL_MATCH_THRESHOLD,
) -> int:
    if not rows or not pdf_emails:
        return 0

    # Only consider rows without an existing email and with a usable name.
    candidate_rows = [
        idx
        for idx, row in enumerate(rows)
        if (not _has_row_email(row)) and any(_given_surname_from_row(row))
    ]
    if not candidate_rows:
        return 0

    cleaned_emails: List[str] = []
    for raw in pdf_emails:
        clean = _clean_email(raw)
        if clean:
            cleaned_emails.append(clean)
    remaining_emails = list(dict.fromkeys(cleaned_emails))
    if not remaining_emails:
        return 0

    assigned = 0
    openalex_domain_cache: Dict[str, Optional[str]] = {}
    preferred_domains_by_row = {
        idx: _affiliation_domains_for_row(rows[idx], openalex_domain_cache)
        for idx in candidate_rows
    }
    remaining_rows: Set[int] = set(candidate_rows)
    while remaining_rows and remaining_emails:
        best_idx = None
        best_email = None
        best_sim = 0.0
        for idx in remaining_rows:
            row = rows[idx]
            given, surname = _given_surname_from_row(row)
            candidate_email, sim = _best_email_for_author(
                given,
                surname,
                remaining_emails,
                preferred_domains=preferred_domains_by_row.get(idx),
            )
            if candidate_email and sim > best_sim:
                best_idx = idx
                best_email = candidate_email
                best_sim = sim

        if best_idx is None or best_email is None or best_sim < threshold:
            break

        rows[best_idx]["email"] = best_email
        rows[best_idx]["email.source"] = "pdf"
        assigned += 1
        remaining_rows.remove(best_idx)
        remaining_emails = [e for e in remaining_emails if e.lower() != best_email.lower()]

    return assigned


def run_author_extract(
    *,
    osf_ids: Optional[List[str]] = None,
    ids_file: Optional[str] = None,
    limit: Optional[int] = None,
    out: Optional[str] = None,
    pdf_root: Optional[str] = None,
    keep_files: bool = False,
    debug: bool = False,
    debug_log: Optional[str] = None,
    match_emails_file: Optional[str] = None,
    match_emails_threshold: float = 0.90,
    include_existing: bool = False,
    write_debug_csv: bool = False,
    orcid_workers: int = 1,
) -> int:
    global _LOG_FH, DEBUG
    if debug_log:
        _LOG_FH = open(debug_log, "w", encoding="utf-8")
    delete_files = not keep_files
    DEBUG = bool(debug)
    if DEBUG:
        _log("Debug logging: enabled")
    _log("File cleanup: enabled" if delete_files else "File cleanup: disabled")

    if match_emails_file:
        _log(f"Matching emails in {match_emails_file} (threshold={match_emails_threshold})")
        _match_emails_in_csv(match_emails_file, match_emails_threshold)
        _log("Email matching complete")
        if _LOG_FH:
            _LOG_FH.close()
        return 0

    ids = list(osf_ids or [])
    if ids_file:
        with open(ids_file, "r", encoding="utf-8") as fh:
            ids.extend([line.strip() for line in fh if line.strip()])
    ids = list(dict.fromkeys(ids))

    repo = PreprintsRepo()
    items = _iter_items_by_ids(repo, ids) if ids else _iter_items_scan(repo, limit)

    pdf_root = pdf_root or os.environ.get("PDF_DEST_ROOT", "/data/preprints")
    grobid.DATA_ROOT = pdf_root

    osf_user_cache: Dict[str, dict] = {}
    orcid_cache: Dict[str, dict] = {}
    orcid_name_cache: Dict[Tuple[str, str], Optional[str]] = {}
    orcid_affil_cache: Dict[str, List[str]] = {}

    should_write_csv = bool(out or write_debug_csv)
    out_path: Optional[Path] = None
    writer: Optional[csv.DictWriter] = None
    csv_fh = None
    if should_write_csv:
        out_path = Path(out) if out else DEFAULT_DEBUG_CSV
        out_path.parent.mkdir(parents=True, exist_ok=True)
        csv_fh = open(out_path, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        _log(f"Local CSV output: enabled ({out_path})")
    else:
        _log("Local CSV output: disabled (DynamoDB-only mode)")

    count_rows = 0
    count_preprints = 0
    stats: Dict[str, int] = {}

    def _handle_result(osf_id: str, item: dict, rows: Optional[List[Dict[str, Any]]]) -> None:
        nonlocal count_rows, count_preprints
        if rows:
            candidates, rejected_emails = _score_group_email_matches(
                rows,
                match_emails_threshold,
                repo=repo,
                enforce_contactability=True,
            )
            if (
                _count_contactable_candidates(candidates) == 0
                and _can_exclude_for_no_author_contacts(item)
            ):
                # Collect emails that were present in the rows for diagnostics.
                all_row_emails = []
                for row in rows:
                    email = _row_email_for_selection(row)
                    if email:
                        all_row_emails.append(email)
                details: Dict[str, Any] = {
                    "provider_id": item.get("provider_id"),
                    "rows": len(rows),
                }
                if all_row_emails:
                    details["emails_found"] = all_row_emails
                if rejected_emails:
                    details["emails_rejected"] = rejected_emails
                log_preprint_exclusion(
                    reason="no_author_contacts_extracted",
                    osf_id=osf_id,
                    stage="author",
                    details=details,
                )
                if all_row_emails or rejected_emails:
                    _log(
                        f"[warn] {osf_id}: excluded with {len(rows)} rows, "
                        f"emails_found={all_row_emails}, rejected={rejected_emails}"
                    )
            try:
                repo.update_preprint_author_email_candidates(osf_id, candidates)
            except Exception as exc:
                _log(f"[warn] {osf_id}: author_email_candidates update failed ({exc})")
            if writer:
                for row in rows:
                    writer.writerow(_row_for_csv(row))
            count_rows += len(rows)
        elif _can_exclude_for_no_author_contacts(item):
            log_preprint_exclusion(
                reason="no_author_contacts_extracted",
                osf_id=osf_id,
                stage="author",
                details={"provider_id": item.get("provider_id"), "rows": 0},
            )
        count_preprints += 1
        if count_preprints % 50 == 0:
            _log(f"Processed {count_preprints} preprints")

    try:
        if orcid_workers > 1:
            # Process preprints in parallel; ORCID/OSF caches are shared dicts
            # (thread-safe for simple get/set under GIL).
            # Submit batches to keep memory bounded, handle results as they complete.
            BATCH_SIZE = orcid_workers * 4
            pending: Dict[Any, dict] = {}  # future -> item
            executor = ThreadPoolExecutor(max_workers=orcid_workers)
            try:
                for item in items:
                    osf_id = item.get("osf_id")
                    _inc(stats, "preprints_total")
                    if not include_existing and item.get("author_email_candidates"):
                        _dbg(f"[{osf_id}] skip: author_email_candidates already present")
                        continue
                    future = executor.submit(
                        _process_preprint,
                        item,
                        pdf_root,
                        osf_user_cache,
                        orcid_cache,
                        orcid_name_cache,
                        orcid_affil_cache,
                        delete_files=delete_files,
                        stats=stats,
                    )
                    pending[future] = item
                    # Drain completed futures when batch is full
                    if len(pending) >= BATCH_SIZE:
                        for done in as_completed(pending):
                            it = pending[done]
                            oid = it.get("osf_id")
                            try:
                                rows = done.result()
                                _handle_result(oid, it, rows)
                            except Exception as exc:
                                _log(f"[warn] {oid}: {exc}")
                        pending.clear()
                # Drain remaining
                for done in as_completed(pending):
                    it = pending[done]
                    oid = it.get("osf_id")
                    try:
                        rows = done.result()
                        _handle_result(oid, it, rows)
                    except Exception as exc:
                        _log(f"[warn] {oid}: {exc}")
            finally:
                executor.shutdown(wait=True)
        else:
            for item in items:
                osf_id = item.get("osf_id")
                _inc(stats, "preprints_total")
                try:
                    if not include_existing and item.get("author_email_candidates"):
                        _dbg(f"[{osf_id}] skip: author_email_candidates already present")
                        continue
                    rows = _process_preprint(
                        item,
                        pdf_root,
                        osf_user_cache,
                        orcid_cache,
                        orcid_name_cache,
                        orcid_affil_cache,
                        delete_files=delete_files,
                        stats=stats,
                    )
                    _handle_result(osf_id, item, rows)
                except Exception as exc:
                    _log(f"[warn] {osf_id}: {exc}")
    finally:
        if csv_fh:
            csv_fh.close()

    if out_path:
        _log(f"Wrote {count_rows} rows to {out_path}")
        _log(f"Matching emails in {out_path} (threshold={match_emails_threshold})")
        _match_emails_in_csv(str(out_path), match_emails_threshold)
        _log("Email matching complete")
    else:
        _log(f"Processed {count_rows} rows (no local CSV output)")
    _log("Stats summary:")
    _log(
        f"preprints total={stats.get('preprints_total', 0)} "
        f"pdf={stats.get('preprints_with_pdf', 0)} "
        f"tei={stats.get('preprints_with_tei', 0)} "
        f"osf={stats.get('preprints_with_osf', 0)}"
    )
    _log(
        f"rows total={stats.get('rows_total', 0)} "
        f"matched_tei={stats.get('rows_matched_tei', 0)} "
        f"unmatched_tei={stats.get('rows_unmatched_tei', 0)}"
    )
    _log(
        "emails tei found/valid/invalid="
        f"{stats.get('tei_emails_found', 0)}/"
        f"{stats.get('tei_emails_valid', 0)}/"
        f"{stats.get('tei_emails_invalid', 0)} "
        "pdf found/valid/invalid/assigned="
        f"{stats.get('pdf_emails_found', 0)}/"
        f"{stats.get('pdf_emails_valid', 0)}/"
        f"{stats.get('pdf_emails_invalid', 0)}/"
        f"{stats.get('pdf_emails_assigned', 0)} "
        "orcid added/invalid="
        f"{stats.get('orcid_emails_added', 0)}/"
        f"{stats.get('orcid_emails_invalid', 0)}"
    )
    _log(
        "orcids pdf found/valid/invalid="
        f"{stats.get('pdf_orcids_found', 0)}/"
        f"{stats.get('pdf_orcids_valid', 0)}/"
        f"{stats.get('pdf_orcids_invalid', 0)}"
    )
    _log(
        "orcid sources "
        f"osf={stats.get('orcid_source_osf', 0)} "
        f"xml={stats.get('orcid_source_xml', 0)} "
        f"pdf={stats.get('orcid_source_pdf', 0)} "
        f"name={stats.get('orcid_source_name', 0)} "
        f"osf;xml={stats.get('orcid_source_osf;xml', 0)} "
        f"none={stats.get('orcid_source_none', 0)}"
    )
    if _LOG_FH:
        _LOG_FH.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract author info from DynamoDB preprints with on-demand PDF/GROBID."
    )
    parser.add_argument("--osf-id", action="append", dest="osf_ids", default=[])
    parser.add_argument("--ids-file", default=None, help="Text file with one OSF id per line.")
    parser.add_argument("--limit", type=int, default=None, help="Limit scan count when no ids specified.")
    parser.add_argument(
        "--out",
        default=None,
        help="Optional local debug CSV path (written when --write-debug-csv or --out is set).",
    )
    parser.add_argument(
        "--write-debug-csv",
        action="store_true",
        help=f"Write a local debug CSV snapshot (default path: {DEFAULT_DEBUG_CSV}).",
    )
    parser.add_argument(
        "--pdf-root",
        default=os.environ.get("PDF_DEST_ROOT", "/data/preprints"),
        help="Base folder for PDF/TEI storage.",
    )
    parser.add_argument(
        "--keep-files",
        action="store_true",
        help="Keep PDFs/TEI after processing (default deletes).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable step-by-step debug logging.",
    )
    parser.add_argument(
        "--debug-log",
        default=None,
        help="Write logs to this file (in addition to stdout).",
    )
    parser.add_argument(
        "--match-emails-file",
        default=None,
        help="CSV file to fuzzy-match emails to authors and write back in-place.",
    )
    parser.add_argument(
        "--match-emails-threshold",
        type=float,
        default=0.90,
        help="Similarity threshold for review flag.",
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="Process preprints even if author_email_candidates already present.",
    )
    args = parser.parse_args()
    return run_author_extract(
        osf_ids=args.osf_ids,
        ids_file=args.ids_file,
        limit=args.limit,
        out=args.out,
        pdf_root=args.pdf_root,
        keep_files=args.keep_files,
        debug=args.debug,
        debug_log=args.debug_log,
        match_emails_file=args.match_emails_file,
        match_emails_threshold=args.match_emails_threshold,
        include_existing=args.include_existing,
        write_debug_csv=args.write_debug_csv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
