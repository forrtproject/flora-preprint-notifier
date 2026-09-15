import unittest
from collections import Counter

from scripts import generate_exclusion_audit as audit


class ExclusionAuditTests(unittest.TestCase):
    def test_current_state_uses_mutually_exclusive_priority(self) -> None:
        self.assertEqual(audit._current_state(None), "Record missing")
        self.assertEqual(
            audit._current_state({"excluded": True, "flora_eligible": False}),
            "Now excluded",
        )
        self.assertEqual(
            audit._current_state({"flora_eligible": False}),
            "No longer FLoRA-eligible",
        )
        self.assertEqual(
            audit._current_state({"flora_eligible": True, "author_email_candidates": []}),
            "No contactable author",
        )

    def test_decision_guidance_prioritizes_emailed_exclusions(self) -> None:
        priority, decision = audit._decision_guidance(
            {"state": "Now excluded", "email_archive_verified": True}
        )
        self.assertEqual(priority, "Adjudicate")
        self.assertIn("verified email", decision)

    def test_report_distinguishes_screening_and_decision_sets(self) -> None:
        excluded = [
            {
                "osf_id": f"e{i}",
                "title": "Excluded",
                "url": "https://osf.io/example/",
                "reason": "ingest_date_window",
                "reason_label": "Outside date window",
                "stage": "cleanup",
                "historically_assigned": i == 0,
                "arm": "control",
                "match_count": 1,
                "contact_count": 1,
                "email_archive_verified": False,
                "excluded_at": "2026-03-01",
            }
            for i in range(3)
        ]
        exception = {
            "osf_id": "e0",
            "title": "Excluded",
            "url": "https://osf.io/example/",
            "arm": "control",
            "state": "Now excluded",
            "reason": "ingest_date_window",
            "priority": "Adjudicate",
            "decision": "Confirm treatment.",
            "email_sent_flag": False,
            "email_archive_verified": False,
            "assigned_at": "2026-02-01",
            "excluded_at": "2026-03-01",
        }
        rendered = audit.render_html(
            {
                "generated_at": "2026-09-15 12:00 UTC",
                "matched_excluded": excluded,
                "assigned_exceptions": [exception],
                "assigned_later_excluded": [exception],
                "matched_later_excluded": [excluded[0]],
                "emailed_then_excluded": [],
                "sent_state_anomalies": [],
                "reason_counts": Counter({"ingest_date_window": 3}),
                "stage_counts": Counter({"cleanup": 3}),
                "assigned_exception_states": Counter({"Now excluded": 1}),
                "checks": [],
                "total_assigned": 2,
                "currently_assignable_assigned": 1,
                "randomisation_excluded": 0,
                "current_match_states": Counter(
                    {"assignable": 1, "excluded": 3, "validation_pending": 0, "missing_email": 0}
                ),
                "current_match_total": 4,
            }
        )

        self.assertIn("The 1 changed assigned records are the decision set", rendered)
        self.assertIn("2</strong>in the 3 only", rendered)
        self.assertIn("1</strong>in both groups", rendered)


if __name__ == "__main__":
    unittest.main()
