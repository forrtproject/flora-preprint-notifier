import unittest
from datetime import datetime, timezone

from scripts.backfill_email_originals import (
    match_preprints_to_messages,
    parse_originals_from_plain_text,
)


class SentEmailParsingTests(unittest.TestCase):
    def test_extracts_originals_and_replication_dois(self) -> None:
        text = """
Cited: Original citation 10.1000/ABC (https://doi.org/10.1000/ABC)

> Replication 1: First replication 10.2000/one (https://doi.org/10.2000/one) — Reported as success
> Replication 2: Second replication 10.2000/two (https://doi.org/10.2000/two) — Reported as failure

Cited: Second original 10.3000/x (https://doi.org/10.3000/x)
> Replication: Third replication 10.4000/y (https://doi.org/10.4000/y)
"""
        originals = parse_originals_from_plain_text(text)

        self.assertEqual([item["doi"] for item in originals], ["10.1000/abc", "10.3000/x"])
        self.assertEqual(
            [item["doi"] for item in originals[0]["replications"]],
            ["10.2000/one", "10.2000/two"],
        )
        self.assertEqual(originals[0]["full_reference"], "Original citation")

    def test_preserves_balanced_parentheses_in_doi_suffix(self) -> None:
        text = """
Cited: Tversky and Kahneman (1973). Availability. 10.1016/0010-0285(73)90033-9 (https://doi.org/10.1016/0010-0285(73)90033-9)
> Replication: Example study 10.1000/example(2) (https://doi.org/10.1000/example(2))
"""

        originals = parse_originals_from_plain_text(text)

        self.assertEqual(originals[0]["doi"], "10.1016/0010-0285(73)90033-9")
        self.assertEqual(originals[0]["replications"][0]["doi"], "10.1000/example(2)")


class SentEmailMatchingTests(unittest.TestCase):
    def test_matches_repeated_recipient_by_title_and_time_one_to_one(self) -> None:
        preprints = [
            {
                "osf_id": "p1",
                "email_recipient": "author@example.org",
                "email_sent_at": "2026-03-01T10:00:05",
                "title": "First preprint",
            },
            {
                "osf_id": "p2",
                "email_recipient": "author@example.org",
                "email_sent_at": "2026-03-02T10:00:05",
                "title": "Second preprint",
            },
        ]
        messages = [
            {
                "uid": "11",
                "recipients": ("author@example.org",),
                "sent_at": datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc),
                "normalised_body": "thank you for sharing second preprint openly",
                "originals": [{"doi": "10.1/two"}],
            },
            {
                "uid": "10",
                "recipients": ("author@example.org",),
                "sent_at": datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc),
                "normalised_body": "thank you for sharing first preprint openly",
                "originals": [{"doi": "10.1/one"}],
            },
        ]

        matched, unmatched = match_preprints_to_messages(preprints, messages)

        self.assertEqual(unmatched, [])
        self.assertEqual(len({item["message"]["uid"] for item in matched}), 2)
        by_id = {item["preprint"]["osf_id"]: item["message"]["uid"] for item in matched}
        self.assertEqual(by_id, {"p1": "10", "p2": "11"})


if __name__ == "__main__":
    unittest.main()
