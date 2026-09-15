import unittest
from unittest.mock import ANY, Mock, call, patch

from osf_sync.email import process_email_batch


class EmailBatchBudgetTests(unittest.TestCase):
    @patch("osf_sync.email._RateLimiter")
    @patch("osf_sync.email.PreprintsRepo")
    @patch("osf_sync.email.assemble_email_context")
    @patch("osf_sync.email.is_suppressed", return_value=False)
    @patch("osf_sync.email.validate_recipient", return_value=(True, None))
    @patch("osf_sync.email.render_email", return_value=("subject", "<p>html</p>", "plain"))
    @patch("osf_sync.email.send_email", return_value={"id": "msg-1"})
    def test_budget_skips_whole_preprint_not_partial(
        self,
        _mock_send,
        _mock_render,
        _mock_validate,
        _mock_suppressed,
        mock_assemble,
        mock_repo_cls,
        mock_limiter_cls,
    ) -> None:
        repo = Mock()
        repo.select_for_email.return_value = [
            {"osf_id": "p1"},
            {"osf_id": "p2"},
            {"osf_id": "p3"},
        ]
        mock_repo_cls.return_value = repo

        limiter = Mock()
        limiter.wait_if_needed.return_value = True
        mock_limiter_cls.return_value = limiter

        contexts = {
            "p1": {
                "_recipients": [{"email": "a1@uni.edu", "first_name": "Alice"}],
                "author_greeting": "Dear Alice,",
                "originals": [{"doi": "10.1/a"}],
            },
            "p2": {
                "_recipients": [
                    {"email": "b1@uni.edu", "first_name": "Bob"},
                    {"email": "b2@uni.edu", "first_name": "Beth"},
                ],
                "author_greeting": "Dear Bob, dear Beth,",
            },
            "p3": {
                "_recipients": [{"email": "c1@uni.edu", "first_name": "Carol"}],
                "author_greeting": "Dear Carol,",
                "originals": [{"doi": "10.1/c"}],
            },
        }
        mock_assemble.side_effect = lambda pid, repo=None: contexts.get(pid)

        result = process_email_batch(limit=2, dry_run=False)

        self.assertEqual(result["sent"], 2)
        self.assertEqual(_mock_send.call_count, 2)
        sent_to = [c.kwargs["to"] for c in _mock_send.call_args_list]
        self.assertEqual(sent_to, [["a1@uni.edu"], ["c1@uni.edu"]])

        repo.mark_email_sent.assert_has_calls(
            [
                call(
                    "p1",
                    recipient="a1@uni.edu",
                    message_id="msg-1",
                    originals=[{"doi": "10.1/a"}],
                    owner=ANY,
                ),
                call(
                    "p3",
                    recipient="c1@uni.edu",
                    message_id="msg-1",
                    originals=[{"doi": "10.1/c"}],
                    owner=ANY,
                ),
            ]
        )
        sent_ids = [c.args[0] for c in repo.mark_email_sent.call_args_list]
        self.assertNotIn("p2", sent_ids)


if __name__ == "__main__":
    unittest.main()
