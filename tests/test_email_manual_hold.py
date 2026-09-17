import unittest

from osf_sync.dynamo.preprints_repo import PreprintsRepo


class _Table:
    def __init__(self):
        self.calls = []

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        return {"Items": []}

    def update_item(self, **kwargs):
        self.calls.append(("update_item", kwargs))
        return {}


class EmailManualHoldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = PreprintsRepo.__new__(PreprintsRepo)
        self.repo.t_preprints = _Table()

    def test_email_selection_excludes_manual_holds(self) -> None:
        self.repo.select_for_email(limit=5)
        _, kwargs = self.repo.t_preprints.calls[-1]
        self.assertIn("manual_review_hold", kwargs["FilterExpression"])

    def test_email_claim_rechecks_manual_hold(self) -> None:
        self.assertTrue(self.repo.claim_email_item("held_v1", owner="worker"))
        _, kwargs = self.repo.t_preprints.calls[-1]
        self.assertIn("manual_review_hold", kwargs["ConditionExpression"])

    def test_queueing_rejects_manual_holds(self) -> None:
        self.repo.set_queue_email("held_v1")
        _, kwargs = self.repo.t_preprints.calls[-1]
        self.assertIn("manual_review_hold", kwargs["ConditionExpression"])


if __name__ == "__main__":
    unittest.main()
