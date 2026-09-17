import unittest
from unittest.mock import patch

from osf_sync.extraction.extract_author_list import (
    _can_exclude_for_no_author_contacts,
    _assign_orcid_from_pdf,
    _assign_orcid_by_name,
    _assign_pdf_emails,
    _fill_orcid_affiliations,
    _fill_orcid_details,
    _has_contactable_preprint_email,
    _score_group_email_matches,
)


class AuthorEmailSelectionTests(unittest.TestCase):
    def test_no_contact_exclusion_is_limited_to_unassigned_records(self) -> None:
        self.assertTrue(_can_exclude_for_no_author_contacts({}))
        self.assertFalse(
            _can_exclude_for_no_author_contacts({"trial_assignment_status": "assigned"})
        )
        self.assertFalse(_can_exclude_for_no_author_contacts({"email_sent": True}))

    @patch("osf_sync.extraction.extract_author_list.is_suppressed")
    @patch("osf_sync.extraction.extract_author_list.validate_recipient")
    def test_selection_enforces_validation_and_suppression(self, mock_validate, mock_suppressed) -> None:
        def validate_side_effect(email: str):
            if email == "invalid@uni.edu":
                return False, "invalid"
            return True, None

        def suppressed_side_effect(email: str, repo=None):
            return email == "suppressed@uni.edu"

        mock_validate.side_effect = validate_side_effect
        mock_suppressed.side_effect = suppressed_side_effect

        rows = [
            {
                "name.given": "A1",
                "name.surname": "Author",
                "email": "valid@uni.edu",
                "email.source": "xml",
                "osf.name": "A1 Author",
                "n": 1,
            },
            {
                "name.given": "A2",
                "name.surname": "Author",
                "email": "invalid@uni.edu",
                "email.source": "xml",
                "osf.name": "A2 Author",
                "n": 2,
            },
            {
                "name.given": "A3",
                "name.surname": "Author",
                "email": "suppressed@uni.edu",
                "email.source": "xml",
                "osf.name": "A3 Author",
                "n": 3,
            },
        ]

        candidates, rejected = _score_group_email_matches(
            rows,
            threshold=0.90,
            repo=object(),
            enforce_contactability=True,
        )

        self.assertEqual(candidates, [{"name": "A1 Author", "email": "valid@uni.edu", "position": 0}])
        self.assertIn("invalid@uni.edu", rejected)
        self.assertIn("suppressed@uni.edu", rejected)

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(True, None))
    def test_preprint_contact_gate_detects_xml_or_pdf_contactable_email(self, _mock_validate) -> None:
        rows = [
            {"email": "known@uni.edu", "email.source": "xml"},
            {"email": None, "email.source": None},
        ]

        self.assertTrue(_has_contactable_preprint_email(rows, validation_cache={}))

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(True, None))
    def test_preprint_contact_gate_ignores_orcid_only_email(self, _mock_validate) -> None:
        rows = [
            {"email": "fallback@uni.edu", "email.source": "orcid"},
            {"email": None, "email.source": None},
        ]

        self.assertFalse(_has_contactable_preprint_email(rows, validation_cache={}))

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(False, "invalid"))
    def test_preprint_contact_gate_rejects_invalid_xml_email(self, _mock_validate) -> None:
        rows = [
            {"email": "bad@invalid.tld", "email.source": "xml"},
        ]

        self.assertFalse(_has_contactable_preprint_email(rows, validation_cache={}))

    @patch("osf_sync.extraction.extract_author_list._fetch_orcid_person")
    def test_pdf_orcid_assignment_requires_threshold(self, mock_fetch_person) -> None:
        mock_fetch_person.return_value = {
            "name": {
                "given-names": {"value": "Jane"},
                "family-name": {"value": "Roe"},
            }
        }
        rows = [
            {
                "osf.name.given": "Jane",
                "osf.name.surname": "Doe",
                "email": None,
                "email.source": None,
                "orcid.osf": None,
                "orcid.xml": None,
                "orcid.pdf": None,
            }
        ]

        assigned = _assign_orcid_from_pdf(
            rows,
            ["0000-0002-1825-0097"],
            orcid_cache={},
            threshold=1.00,
        )

        self.assertEqual(assigned, 0)
        self.assertIsNone(rows[0].get("orcid.pdf"))

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(True, None))
    @patch("osf_sync.extraction.extract_author_list._search_orcid_by_name")
    def test_orcid_name_search_skips_rows_with_existing_email(self, mock_search, _mock_validate) -> None:
        rows = [
            {
                "email": "known@uni.edu",
                "email.source": "xml",
                "osf.name.given": "Jane",
                "osf.name.surname": "Doe",
                "orcid.osf": None,
                "orcid.xml": None,
                "orcid.pdf": None,
            }
        ]

        assigned = _assign_orcid_by_name(rows, name_cache={})

        self.assertEqual(assigned, 0)
        mock_search.assert_not_called()

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(True, None))
    @patch("osf_sync.extraction.extract_author_list._fetch_orcid_employments")
    def test_orcid_affiliation_lookup_skips_rows_with_existing_email(self, mock_fetch_employments, _mock_validate) -> None:
        rows = [
            {
                "orcid": "0000-0002-1825-0097",
                "email": "known@uni.edu",
                "email.source": "xml",
            }
        ]

        filled = _fill_orcid_affiliations(rows, affil_cache={})

        self.assertEqual(filled, 0)
        mock_fetch_employments.assert_not_called()

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(True, None))
    @patch("osf_sync.extraction.extract_author_list._fetch_orcid_person")
    def test_orcid_does_not_override_existing_matched_email(self, mock_fetch_person, _mock_validate) -> None:
        rows = [
            {
                "orcid": "0000-0002-1825-0097",
                "email": "jane.doe@uni.edu",
                "email.source": "pdf",
                "name.given.orcid": None,
                "name.surname.orcid": None,
            }
        ]

        emails_added, names_added, emails_invalid = _fill_orcid_details(rows, orcid_cache={})

        self.assertEqual(emails_added, 0)
        self.assertEqual(names_added, 0)
        self.assertEqual(emails_invalid, 0)
        self.assertEqual(rows[0]["email"], "jane.doe@uni.edu")
        self.assertEqual(rows[0]["email.source"], "pdf")
        mock_fetch_person.assert_not_called()

    @patch("osf_sync.extraction.extract_author_list.validate_recipient", return_value=(False, "invalid"))
    @patch("osf_sync.extraction.extract_author_list._fetch_orcid_person")
    def test_orcid_replaces_invalid_existing_email(self, mock_fetch_person, _mock_validate) -> None:
        mock_fetch_person.return_value = {
            "name": {
                "given-names": {"value": "Jane"},
                "family-name": {"value": "Doe"},
            },
            "emails": {
                "email": [{"email": "jane.doe@orcid.edu"}],
            },
        }
        rows = [
            {
                "orcid": "0000-0002-1825-0097",
                "email": "broken@invalid.tld",
                "email.source": "pdf",
                "name.given.orcid": None,
                "name.surname.orcid": None,
            }
        ]

        emails_added, names_added, emails_invalid = _fill_orcid_details(rows, orcid_cache={})

        self.assertEqual(emails_added, 1)
        self.assertEqual(names_added, 2)
        self.assertEqual(emails_invalid, 0)
        self.assertEqual(rows[0]["email"], "jane.doe@orcid.edu")
        self.assertEqual(rows[0]["email.source"], "orcid")
        mock_fetch_person.assert_called_once()

    @patch("osf_sync.extraction.extract_author_list._fetch_orcid_person")
    def test_orcid_adds_email_only_when_missing(self, mock_fetch_person) -> None:
        mock_fetch_person.return_value = {
            "name": {
                "given-names": {"value": "Jane"},
                "family-name": {"value": "Doe"},
            },
            "emails": {
                "email": [{"email": "jane.doe@orcid.edu"}],
            },
        }
        rows = [
            {
                "orcid": "0000-0002-1825-0097",
                "email": None,
                "email.source": None,
                "name.given.orcid": None,
                "name.surname.orcid": None,
            }
        ]

        emails_added, names_added, emails_invalid = _fill_orcid_details(rows, orcid_cache={})

        self.assertEqual(emails_added, 1)
        self.assertEqual(names_added, 2)
        self.assertEqual(emails_invalid, 0)
        self.assertEqual(rows[0]["email"], "jane.doe@orcid.edu")
        self.assertEqual(rows[0]["email.source"], "orcid")
        mock_fetch_person.assert_called_once()

    @patch("osf_sync.extraction.extract_author_list._affiliation_domains_for_row", return_value=["bbk.ac.uk"])
    def test_pdf_email_assignment_accepts_affiliation_domain_bonus(self, _mock_domains) -> None:
        rows = [
            {
                "name.given": "Lukas",
                "name.surname": "Wallrich",
                "email": None,
                "email.source": None,
                "osf.name": "Lukas Wallrich",
                "affiliation": "Birkbeck, University of London",
                "n": 1,
            }
        ]

        assigned = _assign_pdf_emails(
            rows,
            ["ublwal002@bbk.ac.uk"],
            threshold=0.75,
        )

        self.assertEqual(assigned, 1)
        self.assertEqual(rows[0]["email"], "ublwal002@bbk.ac.uk")
        self.assertEqual(rows[0]["email.source"], "pdf")

    @patch("osf_sync.extraction.extract_author_list._affiliation_domains_for_row", return_value=["bbk.ac.uk"])
    def test_group_scoring_applies_affiliation_domain_bonus(self, _mock_domains) -> None:
        rows = [
            {
                "name.given": "Lukas",
                "name.surname": "Wallrich",
                "email": "ublwal002@bbk.ac.uk",
                "email.source": "pdf",
                "osf.name": "Lukas Wallrich",
                "affiliation": "Birkbeck, University of London",
                "n": 1,
            }
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.75)

        self.assertEqual(candidates, [{"name": "Lukas Wallrich", "email": "ublwal002@bbk.ac.uk", "position": 0}])
        self.assertEqual(rows[0]["email.possible"], "ublwal002@bbk.ac.uk")
        self.assertEqual(rows[0]["email.similarity"], "0.850")
        self.assertEqual(rows[0]["review_needed"], "FALSE")

    def test_declared_contacts_exclude_orcid_fallback(self) -> None:
        rows = [
            {
                "name.given": "Alice",
                "name.surname": "Smith",
                "email": "alice@uni.edu",
                "email.source": "xml",
                "osf.name": "Alice Smith",
                "n": 1,
            },
            {
                "name.given": "Bob",
                "name.surname": "Jones",
                "email": "bob@uni.edu",
                "email.source": "orcid",
                "osf.name": "Bob Jones",
                "n": 2,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        self.assertEqual(
            candidates,
            [
                {"name": "Alice Smith", "email": "alice@uni.edu", "position": 0},
            ],
        )

    def test_pdf_email_assignment_requires_plausible_name_match(self) -> None:
        rows = [
            {
                "name.given": "Jane",
                "name.surname": "Doe",
                "email": None,
                "email.source": None,
                "osf.name": "Jane Doe",
                "n": 1,
            },
            {
                "name.given": "John",
                "name.surname": "Smith",
                "email": None,
                "email.source": None,
                "osf.name": "John Smith",
                "n": 2,
            },
        ]

        assigned = _assign_pdf_emails(
            rows,
            ["jane.doe@lab.edu", "contact@journal.org"],
            threshold=0.75,
        )

        self.assertEqual(assigned, 1)
        self.assertEqual(rows[0]["email"], "jane.doe@lab.edu")
        self.assertEqual(rows[0]["email.source"], "pdf")
        self.assertFalse(rows[1].get("email"))

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)
        self.assertEqual(candidates, [{"name": "Jane Doe", "email": "jane.doe@lab.edu", "position": 0}])

    def test_declared_contacts_more_than_five_are_capped_to_first_five(self) -> None:
        rows = [
            {
                "name.given": "A1",
                "name.surname": "Author",
                "email": "a1@uni.edu",
                "email.source": "xml",
                "osf.name": "A1 Author",
                "n": 1,
            },
            {
                "name.given": "A2",
                "name.surname": "Author",
                "email": "a2@uni.edu",
                "email.source": "xml",
                "osf.name": "A2 Author",
                "n": 2,
            },
            {
                "name.given": "A3",
                "name.surname": "Author",
                "email": "a3@uni.edu",
                "email.source": "xml",
                "osf.name": "A3 Author",
                "n": 3,
            },
            {
                "name.given": "A4",
                "name.surname": "Author",
                "email": "a4@uni.edu",
                "email.source": "pdf",
                "osf.name": "A4 Author",
                "n": 4,
            },
            {
                "name.given": "A5",
                "name.surname": "Author",
                "email": "a5@uni.edu",
                "email.source": "pdf",
                "osf.name": "A5 Author",
                "n": 5,
            },
            {
                "name.given": "A6",
                "name.surname": "Author",
                "email": "a6@uni.edu",
                "email.source": "pdf",
                "osf.name": "A6 Author",
                "n": 6,
            },
            {
                "name.given": "A7",
                "name.surname": "Author",
                "email": "a7@uni.edu",
                "email.source": "orcid",
                "osf.name": "A7 Author",
                "n": 7,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        self.assertEqual(
            candidates,
            [
                {"name": "A1 Author", "email": "a1@uni.edu", "position": 0},
                {"name": "A2 Author", "email": "a2@uni.edu", "position": 1},
                {"name": "A3 Author", "email": "a3@uni.edu", "position": 2},
                {"name": "A4 Author", "email": "a4@uni.edu", "position": 3},
                {"name": "A5 Author", "email": "a5@uni.edu", "position": 4},
            ],
        )

    def test_declared_contacts_more_than_five_use_first_four_plus_last_when_last_is_declared(self) -> None:
        rows = [
            {
                "name.given": "A1",
                "name.surname": "Author",
                "email": "a1@uni.edu",
                "email.source": "xml",
                "osf.name": "A1 Author",
                "n": 1,
            },
            {
                "name.given": "A2",
                "name.surname": "Author",
                "email": "a2@uni.edu",
                "email.source": "xml",
                "osf.name": "A2 Author",
                "n": 2,
            },
            {
                "name.given": "A3",
                "name.surname": "Author",
                "email": "a3@uni.edu",
                "email.source": "xml",
                "osf.name": "A3 Author",
                "n": 3,
            },
            {
                "name.given": "A4",
                "name.surname": "Author",
                "email": "a4@uni.edu",
                "email.source": "pdf",
                "osf.name": "A4 Author",
                "n": 4,
            },
            {
                "name.given": "A5",
                "name.surname": "Author",
                "email": "a5@uni.edu",
                "email.source": "pdf",
                "osf.name": "A5 Author",
                "n": 5,
            },
            {
                "name.given": "A6",
                "name.surname": "Author",
                "email": "a6@uni.edu",
                "email.source": "pdf",
                "osf.name": "A6 Author",
                "n": 6,
            },
            {
                "name.given": "A7",
                "name.surname": "Author",
                "email": "a7@uni.edu",
                "email.source": "pdf",
                "osf.name": "A7 Author",
                "n": 7,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        self.assertEqual(
            candidates,
            [
                {"name": "A1 Author", "email": "a1@uni.edu", "position": 0},
                {"name": "A2 Author", "email": "a2@uni.edu", "position": 1},
                {"name": "A3 Author", "email": "a3@uni.edu", "position": 2},
                {"name": "A4 Author", "email": "a4@uni.edu", "position": 3},
                {"name": "A7 Author", "email": "a7@uni.edu", "position": 6},
            ],
        )

    def test_declared_contacts_take_precedence_when_present(self) -> None:
        rows = [
            {
                "name.given": "First",
                "name.surname": "Author",
                "email": "first@uni.edu",
                "email.source": "xml",
                "osf.name": "First Author",
                "n": 1,
            },
            {
                "name.given": "Second",
                "name.surname": "Author",
                "email": "second@uni.edu",
                "email.source": "pdf",
                "osf.name": "Second Author",
                "n": 2,
            },
            {
                "name.given": "Third",
                "name.surname": "Author",
                "email": "third@uni.edu",
                "email.source": "orcid",
                "osf.name": "Third Author",
                "n": 3,
            },
            {
                "name.given": "Fourth",
                "name.surname": "Author",
                "email": "fourth@uni.edu",
                "email.source": "orcid",
                "osf.name": "Fourth Author",
                "n": 4,
            },
            {
                "name.given": "Fifth",
                "name.surname": "Author",
                "email": "fifth@uni.edu",
                "email.source": "orcid",
                "osf.name": "Fifth Author",
                "n": 5,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        self.assertEqual(
            candidates,
            [
                {"name": "First Author", "email": "first@uni.edu", "position": 0},
                {"name": "Second Author", "email": "second@uni.edu", "position": 1},
            ],
        )

    def test_orcid_fallback_uses_first_four_and_last_only(self) -> None:
        rows = [
            {
                "name.given": "A1",
                "name.surname": "Author",
                "email": "a1@uni.edu",
                "email.source": "orcid",
                "osf.name": "A1 Author",
                "n": 1,
            },
            {
                "name.given": "A2",
                "name.surname": "Author",
                "email": "a2@uni.edu",
                "email.source": "orcid",
                "osf.name": "A2 Author",
                "n": 2,
            },
            {
                "name.given": "A3",
                "name.surname": "Author",
                "email": "a3@uni.edu",
                "email.source": "orcid",
                "osf.name": "A3 Author",
                "n": 3,
            },
            {
                "name.given": "A4",
                "name.surname": "Author",
                "email": "a4@uni.edu",
                "email.source": "orcid",
                "osf.name": "A4 Author",
                "n": 4,
            },
            {
                "name.given": "A5",
                "name.surname": "Author",
                "email": "a5@uni.edu",
                "email.source": "orcid",
                "osf.name": "A5 Author",
                "n": 5,
            },
            {
                "name.given": "A6",
                "name.surname": "Author",
                "email": "a6@uni.edu",
                "email.source": "orcid",
                "osf.name": "A6 Author",
                "n": 6,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        self.assertEqual(
            candidates,
            [
                {"name": "A1 Author", "email": "a1@uni.edu", "position": 0},
                {"name": "A2 Author", "email": "a2@uni.edu", "position": 1},
                {"name": "A3 Author", "email": "a3@uni.edu", "position": 2},
                {"name": "A4 Author", "email": "a4@uni.edu", "position": 3},
                {"name": "A6 Author", "email": "a6@uni.edu", "position": 5},
            ],
        )

    def test_orcid_fallback_returns_empty_if_only_middle_authors_have_email(self) -> None:
        rows = [
            {
                "name.given": "A1",
                "name.surname": "Author",
                "email": None,
                "email.source": None,
                "osf.name": "A1 Author",
                "n": 1,
            },
            {
                "name.given": "A2",
                "name.surname": "Author",
                "email": None,
                "email.source": None,
                "osf.name": "A2 Author",
                "n": 2,
            },
            {
                "name.given": "A3",
                "name.surname": "Author",
                "email": None,
                "email.source": None,
                "osf.name": "A3 Author",
                "n": 3,
            },
            {
                "name.given": "A4",
                "name.surname": "Author",
                "email": None,
                "email.source": None,
                "osf.name": "A4 Author",
                "n": 4,
            },
            {
                "name.given": "A5",
                "name.surname": "Author",
                "email": "a5@uni.edu",
                "email.source": "orcid",
                "osf.name": "A5 Author",
                "n": 5,
            },
            {
                "name.given": "A6",
                "name.surname": "Author",
                "email": None,
                "email.source": None,
                "osf.name": "A6 Author",
                "n": 6,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)
        self.assertEqual(candidates, [])


    def test_candidate_position_is_sorted_rank_not_group_index(self) -> None:
        """Position should reflect rank after sorting by author position, not the
        original group index.  Author at group index 2 with n=1 should get rank 0."""
        rows = [
            {
                "name.given": "Charlie",
                "name.surname": "Third",
                "email": "charlie@uni.edu",
                "email.source": "xml",
                "osf.name": "Charlie Third",
                "n": 3,
            },
            {
                "name.given": "Bob",
                "name.surname": "Second",
                "email": "bob@uni.edu",
                "email.source": "xml",
                "osf.name": "Bob Second",
                "n": 2,
            },
            {
                "name.given": "Alice",
                "name.surname": "First",
                "email": "alice@uni.edu",
                "email.source": "xml",
                "osf.name": "Alice First",
                "n": 1,
            },
        ]

        candidates, _ = _score_group_email_matches(rows, threshold=0.90)

        # Sorted by n: Alice(n=1) rank 0, Bob(n=2) rank 1, Charlie(n=3) rank 2
        self.assertEqual(candidates[0]["name"], "Alice First")
        self.assertEqual(candidates[0]["position"], 0)
        self.assertEqual(candidates[1]["name"], "Bob Second")
        self.assertEqual(candidates[1]["position"], 1)
        self.assertEqual(candidates[2]["name"], "Charlie Third")
        self.assertEqual(candidates[2]["position"], 2)


if __name__ == "__main__":
    unittest.main()
