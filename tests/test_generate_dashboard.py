import os
import unittest
from datetime import date
from unittest.mock import patch

from scripts import generate_dashboard as dashboard


class DashboardEmailActivityTests(unittest.TestCase):
    def test_summarizes_timeline_recipients_and_providers(self) -> None:
        summary = dashboard._summarize_email_activity([
            {
                "email_sent_at": "2026-03-06T09:30:00Z",
                "email_recipient": "a@example.org, b@example.org",
                "provider_id": "psyarxiv",
            },
            {
                "email_sent_at": "2026-03-06T12:00:00+00:00",
                "email_recipient": "b@example.org",
                "provider_id": "socarxiv",
            },
            {
                "email_sent_at": "2026-04-01",
                "email_recipient": "c@example.org",
                "provider_id": "psyarxiv",
            },
        ])

        self.assertEqual(summary["total_notifications"], 3)
        self.assertEqual(summary["total_recipients"], 4)
        self.assertEqual(summary["unique_recipients"], 3)
        self.assertEqual(summary["first_day"], date(2026, 3, 6))
        self.assertEqual(summary["last_day"], date(2026, 4, 1))
        self.assertEqual(summary["active_send_days"], 2)
        self.assertEqual(summary["monthly_rows"][0]["notifications"], 2)
        self.assertEqual(summary["monthly_rows"][1]["cumulative"], 3)
        self.assertEqual(summary["provider_counts"]["PsyArXiv"], 2)

    def test_sparkline_marks_zero_weeks(self) -> None:
        self.assertEqual(dashboard._sparkline([0, 4, 0, 8]), "·▅·█")
        self.assertEqual(dashboard._sparkline([]), "—")


class DashboardTargetTests(unittest.TestCase):
    def test_counts_an_original_once_per_notification(self) -> None:
        refs = {
            "p1": [
                {
                    "doi": "10.1000/ABC",
                    "raw_citation": "Example | citation",
                    "flora_ref_pairs": [{"doi_r": "10.2000/one"}],
                },
                {
                    "doi": "https://doi.org/10.1000/abc",
                    "raw_citation": "Duplicate citation",
                    "flora_ref_pairs": [{"doi_r": "10.2000/two"}],
                },
            ],
            "p2": [
                {
                    "doi": "10.1000/abc",
                    "raw_citation": "Example citation",
                    "flora_ref_pairs": [{"doi_r": "10.2000/one"}],
                },
            ],
        }

        with patch.dict(os.environ, {"DASHBOARD_REFERENCE_WORKERS": "1"}), patch.object(
            dashboard,
            "_query_targeted_references",
            side_effect=lambda _table, osf_id: refs[osf_id],
        ):
            result = dashboard._summarize_targeted_originals(
                [
                    {"osf_id": "p1", "flora_eligible_count": 2},
                    {"osf_id": "p2", "flora_eligible_count": 1},
                ],
                object(),
            )

        self.assertEqual(result["unique_originals"], 1)
        self.assertEqual(result["original_mentions"], 2)
        self.assertEqual(result["notifications_with_targets"], 2)
        self.assertEqual(result["multi_original_notifications"], 0)
        self.assertEqual(result["originals"][0]["notifications"], 2)
        self.assertEqual(result["originals"][0]["known_replications"], 2)


class DashboardReferenceQueryTests(unittest.TestCase):
    def test_query_keeps_only_references_that_would_be_emailed(self) -> None:
        class FakeTable:
            def query(self, **_kwargs):
                return {
                    "Items": [
                        {
                            "doi": "10.1/keep",
                            "flora_replication_cited": False,
                            "flora_ref_pairs": [{"doi_r": "10.2/rep"}],
                        },
                        {
                            "doi": "10.1/cited",
                            "flora_replication_cited": True,
                            "flora_ref_pairs": [{"doi_r": "10.2/rep"}],
                        },
                        {
                            "doi": "10.1/rejected",
                            "flora_replication_cited": False,
                            "flora_ref_pairs": [{"doi_r": "10.2/rep"}],
                            "citation_validation_status": "rejected",
                        },
                    ]
                }

        result = dashboard._query_targeted_references(FakeTable(), "p1")
        self.assertEqual([item["doi"] for item in result], ["10.1/keep"])


if __name__ == "__main__":
    unittest.main()
