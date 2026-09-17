import unittest
from unittest.mock import patch

from osf_sync.augmentation.extract_to_db import (
    _extract_dois_from_raw_citation,
    _extract_unique_doi_from_raw_citation,
    write_extraction,
)


class _DummyBatchWriter:
    """Mimics DynamoDB Table.batch_writer() context manager."""

    def __init__(self, store):
        self._store = store

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def put_item(self, Item):
        self._store.append(Item)


class _DummyTable:
    def __init__(self):
        self.items = []

    def batch_writer(self):
        return _DummyBatchWriter(self.items)


class _DummyPreprintsTable:
    def __init__(self, item=None):
        self.item = item or {}

    def get_item(self, **kwargs):
        return {"Item": self.item} if self.item else {}


class _DummyRepo:
    def __init__(self) -> None:
        self.tei = []
        self.refs = []
        self.extracted = []
        self.t_refs = _DummyTable()
        self.t_preprints = _DummyPreprintsTable()

    def upsert_tei(self, osf_id, preprint):
        self.tei.append((osf_id, preprint))

    def prepare_reference_item(self, osf_id, item):
        prepared = {"osf_id": osf_id, **item}
        self.refs.append((osf_id, item))
        return prepared

    def mark_extracted(self, osf_id):
        self.extracted.append(osf_id)


class ExtractToDbDoiConsistencyTests(unittest.TestCase):
    def test_write_extraction_does_not_replace_assigned_references(self) -> None:
        repo = _DummyRepo()
        repo.t_preprints = _DummyPreprintsTable({"trial_assignment_status": "assigned"})
        with patch("osf_sync.augmentation.extract_to_db.PreprintsRepo", return_value=repo):
            out = write_extraction("protected_v1", {"title": "t"}, [{"ref_id": "b1"}])
        self.assertTrue(out["skipped_protected"])
        self.assertEqual(repo.tei, [])
        self.assertEqual(repo.t_refs.items, [])

    def test_write_extraction_can_explicitly_rebuild_protected_record(self) -> None:
        repo = _DummyRepo()
        repo.t_preprints = _DummyPreprintsTable({"email_sent": True})
        with (
            patch("osf_sync.augmentation.extract_to_db.PreprintsRepo", return_value=repo),
            patch("osf_sync.augmentation.extract_to_db.doi_resolves", return_value=True),
        ):
            out = write_extraction(
                "protected_v1",
                {"title": "t"},
                [{"ref_id": "b1", "doi": "10.1111/abcd.1234567"}],
                allow_protected_overwrite=True,
            )
        self.assertNotIn("skipped_protected", out)
        self.assertEqual(out["refs_upserted"], 1)

    def test_extract_unique_doi_from_raw_citation(self) -> None:
        raw = (
            "Condon, P., & Makransky, J. (2020). ... "
            "https://doi.org/10.3389/fpsyg.2020.02249"
        )
        self.assertEqual(
            _extract_unique_doi_from_raw_citation(raw),
            "10.3389/fpsyg.2020.02249",
        )

    def test_extract_unique_doi_returns_none_when_multiple_dois(self) -> None:
        raw = (
            "See 10.1111/abcd.12345 and also https://doi.org/10.2222/xyz.9876543 "
            "for related work."
        )
        self.assertIsNone(_extract_unique_doi_from_raw_citation(raw))
        dois = _extract_dois_from_raw_citation(raw)
        self.assertEqual(len(dois), 2)

    def test_write_extraction_prefers_raw_doi_when_tei_conflicts(self) -> None:
        repo = _DummyRepo()
        preprint = {"title": "t", "authors": []}
        refs = [
            {
                "ref_id": "b16",
                "title": "Sustainable compassion training",
                "authors": ["P Condon", "J Makransky"],
                "journal": "Frontiers in Psychology",
                "year": "2020",
                "doi": "10.1007/s10902-012-9373-z",  # incorrect TEI DOI
                "has_title": True,
                "has_authors": True,
                "has_journal": True,
                "has_year": True,
                "raw_citation": (
                    "Condon, P., & Makransky, J. (2020). "
                    "Sustainable compassion training... "
                    "https://doi.org/10.3389/fpsyg.2020.02249"
                ),
            }
        ]
        with (
            patch("osf_sync.augmentation.extract_to_db.PreprintsRepo", return_value=repo),
            patch("osf_sync.augmentation.extract_to_db.doi_resolves", return_value=True),
        ):
            out = write_extraction("bp3vh_v1", preprint, refs)
        self.assertTrue(out["tei_ok"])
        self.assertEqual(len(repo.refs), 1)
        stored = repo.refs[0][1]
        self.assertEqual(stored["doi"], "10.3389/fpsyg.2020.02249")
        self.assertEqual(stored["doi_source"], "tei_raw")

    def test_write_extraction_keeps_tei_doi_when_present_in_multi_raw_candidates(self) -> None:
        repo = _DummyRepo()
        preprint = {"title": "t", "authors": []}
        refs = [
            {
                "ref_id": "b1",
                "title": "t1",
                "authors": ["A One"],
                "journal": "J",
                "year": "2020",
                "doi": "10.1111/abcd.1234567",
                "has_title": True,
                "has_authors": True,
                "has_journal": True,
                "has_year": True,
                "raw_citation": (
                    "Contains two identifiers 10.1111/abcd.1234567 and 10.2222/xyz.9876543"
                ),
            }
        ]
        with (
            patch("osf_sync.augmentation.extract_to_db.PreprintsRepo", return_value=repo),
            patch("osf_sync.augmentation.extract_to_db.doi_resolves", return_value=True),
        ):
            out = write_extraction("osf1", preprint, refs)
        self.assertTrue(out["tei_ok"])
        stored = repo.refs[0][1]
        self.assertEqual(stored["doi"], "10.1111/abcd.1234567")
        self.assertEqual(stored["doi_source"], "tei")

    def test_write_extraction_discards_tei_doi_when_not_in_multi_raw_candidates(self) -> None:
        repo = _DummyRepo()
        preprint = {"title": "t", "authors": []}
        refs = [
            {
                "ref_id": "b2",
                "title": "t2",
                "authors": ["A Two"],
                "journal": "J",
                "year": "2020",
                "doi": "10.9999/wrong.0000001",
                "has_title": True,
                "has_authors": True,
                "has_journal": True,
                "has_year": True,
                "raw_citation": (
                    "Contains two identifiers 10.1111/abcd.1234567 and 10.2222/xyz.9876543"
                ),
            }
        ]
        with (
            patch("osf_sync.augmentation.extract_to_db.PreprintsRepo", return_value=repo),
            patch("osf_sync.augmentation.extract_to_db.doi_resolves", return_value=True),
        ):
            out = write_extraction("osf2", preprint, refs)
        self.assertTrue(out["tei_ok"])
        stored = repo.refs[0][1]
        self.assertIsNone(stored["doi"])
        self.assertEqual(stored["has_doi"], False)
        self.assertIsNone(stored["doi_source"])


if __name__ == "__main__":
    unittest.main()
