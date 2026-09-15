import unittest
from datetime import date

from scripts import generate_dashboard as dashboard


class DashboardEmailActivityTests(unittest.TestCase):
    def test_summarizes_timeline_recipients_and_providers(self) -> None:
        summary = dashboard._summarize_email_activity(
            [
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
            ]
        )

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
        sent_items = [
            {
                "osf_id": "p1",
                "email_originals": [
                    {
                        "doi": "10.1000/ABC",
                        "full_reference": "Example | citation",
                        "replications": [{"doi": "10.2000/one"}],
                    },
                    {
                        "doi": "https://doi.org/10.1000/abc",
                        "full_reference": "Duplicate citation",
                        "replications": [{"doi": "10.2000/two"}],
                    },
                ],
            },
            {
                "osf_id": "p2",
                "email_originals": [
                    {
                        "doi": "10.1000/abc",
                        "full_reference": "Example citation",
                        "replications": [{"doi": "10.2000/one"}],
                    },
                ],
            },
        ]

        result = dashboard._summarize_targeted_originals(sent_items)

        self.assertEqual(result["unique_originals"], 1)
        self.assertEqual(result["original_mentions"], 2)
        self.assertEqual(result["notifications_with_targets"], 2)
        self.assertEqual(result["multi_original_notifications"], 0)
        self.assertEqual(result["originals"][0]["notifications"], 2)
        self.assertEqual(result["originals"][0]["known_replications"], 2)

    def test_missing_snapshot_is_not_inferred_from_current_state(self) -> None:
        result = dashboard._summarize_targeted_originals([{"osf_id": "p1"}])
        self.assertEqual(result["notifications_with_targets"], 0)
        self.assertEqual(result["unique_originals"], 0)


if __name__ == "__main__":
    unittest.main()
