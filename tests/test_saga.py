import unittest

from roomready import demo
from roomready import domain as d
from roomready import saga
from roomready.providers import provider_stats
from roomready.store import ORDER_SK, order_pk
from support import create, order, runtime


class DemoScenarioTests(unittest.TestCase):
    def test_every_demo_scenario(self):
        for fn in demo.SCENARIOS:
            with self.subTest(scenario=fn.__name__):
                result = fn()
                self.assertEqual(result.actual, result.expected)
                for desc, ok in result.checks:
                    self.assertTrue(ok, desc)


class LedgerTests(unittest.TestCase):
    def test_action_keys_are_stable_and_version_free(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        for entry in rt.service.get_order("alice", oid)[1]["ledger"]:
            self.assertEqual(entry["idempotencyKey"], f"ORDER#{oid}/ACTION#{entry['action']}")

    def test_replaying_forward_steps_causes_no_duplicate_effects(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        before = provider_stats(rt.store, oid)
        # Simulate an at-least-once redelivery of every step to the current generation.
        rt.store.update(order_pk(oid), ORDER_SK, {"status": d.PREPARING})
        state = {"orderId": oid, "generation": 1, "mode": "FORWARD"}
        for step in ("reserve_supplies", "book_courier", "setup_room"):
            out = saga.dispatch({"step": step, "state": state}, rt.ctx)
            self.assertEqual(out["outcome"], saga.OK)
        after = provider_stats(rt.store, oid)
        for p in after:
            self.assertEqual(after[p]["creates"], 1)
            self.assertEqual(after[p]["calls"], before[p]["calls"])  # ledger said SUCCEEDED: no provider call

    def test_unknown_without_effect_is_reissued_with_same_key(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        # pretend book_courier was lost: ledger UNKNOWN and provider has no record
        rt.store.update(order_pk(oid), ORDER_SK, {"status": d.PREPARING})
        rt.store.update(order_pk(oid), "ACTION#book_courier", {"status": d.UNKNOWN})
        rt.store.delete("PROVIDER#courier", saga.action_key(oid, "book_courier"))
        out = saga.dispatch({"step": "book_courier", "state": {"orderId": oid, "generation": 1}}, rt.ctx)
        self.assertEqual(out["outcome"], saga.OK)
        self.assertEqual(len([r for r in rt.providers["courier"].records() if r["orderId"] == oid]), 1)

    def test_retries_exhausted_ends_prep_failed_without_effects(self):
        rt = runtime()
        oid = create(rt, "retailer_exhausted")
        rt.tick(to_next_due=True)
        o = order(rt, oid)
        self.assertEqual(o["status"], d.PREP_FAILED)
        self.assertIn("TransientError", o["failureReason"])
        entry = rt.service.get_order("alice", oid)[1]["ledger"][0]
        self.assertEqual((entry["status"], entry["attempts"]), (d.NOT_APPLIED, 4))  # 1 + 3 ASL retries
        self.assertEqual(provider_stats(rt.store, oid)["retailer"]["creates"], 0)

    def test_captured_payment_is_refunded_not_voided(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        rt.providers["retailer"].capture(saga.action_key(oid, "reserve_supplies"))
        rt.providers["courier"].deliver(saga.action_key(oid, "book_courier"))
        rt.service.cancel_order("alice", oid)
        self.assertEqual(order(rt, oid)["status"], d.CANCELLED)
        comps = {c["slot"]: c for c in rt.service.get_order("alice", oid)[1]["compensations"]}
        self.assertEqual(comps["reverse_payment"]["method"], "REFUND")
        self.assertEqual(comps["undo_delivery"]["method"], "RETURN_PICKUP")
        rec = rt.store.get("PROVIDER#retailer", saga.action_key(oid, "reserve_supplies"))
        self.assertEqual(rec["paymentStatus"], "REFUNDED")

    def test_manual_review_is_never_cancelled(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        rt.store.update(order_pk(oid), ORDER_SK, {"faults": {"hotel.cancel_setup": {"permanent": True}}})
        rt.service.cancel_order("alice", oid)
        o = order(rt, oid)
        self.assertEqual(o["status"], d.MANUAL_REVIEW)
        self.assertTrue(o["cancelRequested"])
        self.assertEqual(o["unresolved"], ["cancel_setup: compensation not confirmed"])

    def test_transient_compensation_error_is_retried(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        rt.store.update(order_pk(oid), ORDER_SK, {"faults": {"courier.cancel": {"transient": 1},
                                                             "retailer.void": {"timeout": 1}}})
        rt.service.cancel_order("alice", oid)
        self.assertEqual(order(rt, oid)["status"], d.CANCELLED)
        comps = {c["slot"]: c for c in rt.service.get_order("alice", oid)[1]["compensations"]}
        self.assertEqual(comps["undo_delivery"]["attempts"], 2)
        self.assertEqual(comps["reverse_payment"]["method"], "ALREADY_VOIDED")  # reconciled after timeout


class FencingTests(unittest.TestCase):
    def test_stale_generation_cannot_cause_side_effects(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        rt.service.cancel_order("alice", oid)  # generation 2 reverse completed
        calls_before = provider_stats(rt.store, oid)
        stale = {"orderId": oid, "generation": 1, "mode": "FORWARD"}
        for step in saga.STEP_HANDLERS:
            out = saga.dispatch({"step": step, "state": stale}, rt.ctx)
            self.assertEqual(out["outcome"], saga.FENCED, step)
        self.assertEqual(provider_stats(rt.store, oid), calls_before)
        self.assertEqual(order(rt, oid)["status"], d.CANCELLED)

    def test_duplicate_event_does_not_start_second_execution(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        event = rt.router.published[0]
        rt.router.publish([event])  # EventBridge at-least-once redelivery
        self.assertEqual(list(rt.starter.executions), [f"{oid}-g1"])
        self.assertEqual(provider_stats(rt.store, oid)["retailer"]["creates"], 1)

    def test_stale_execution_is_superseded_in_state_machine(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        result = rt._run_execution({"orderId": oid, "generation": 0, "mode": "FORWARD"})
        self.assertEqual((result["status"], result["endState"]), ("SUCCEEDED", "Superseded"))
        self.assertEqual(order(rt, oid)["status"], d.READY)

    def test_forward_and_reverse_never_overlap(self):
        rt = runtime()
        oid = create(rt, "cancel_during_prep")
        rt.tick(to_next_due=True)
        # cancel while PREPARING only records intent: no reverse generation is started
        self.assertEqual(list(rt.starter.executions), [f"{oid}-g1"])
        self.assertEqual(order(rt, oid)["generation"], 1)
        history = rt.starter.executions[f"{oid}-g1"]["history"]
        entered = [h["state"] for h in history if h["type"] == "StateEntered"]
        self.assertLess(entered.index("ReserveSupplies"), entered.index("BeginCompensation"))
        self.assertNotIn("SetupRoom", entered)

    def test_reverse_cannot_start_while_preparing(self):
        rt = runtime()
        oid = create(rt)
        rt.clock.set(d.from_iso(order(rt, oid)["dueAt"]))
        self.assertEqual(len(rt.service.poll_due_work()), 1)  # claimed (PREPARING) but not yet executed
        status, view = rt.service.cancel_order("alice", oid)
        self.assertEqual((status, view["status"], view["generation"]), (202, d.PREPARING, 1))
        rt.service.drain_outbox()
        self.assertEqual(order(rt, oid)["status"], d.CANCELLED)
        self.assertEqual(list(rt.starter.executions), [f"{oid}-g1"])
        self.assertEqual(provider_stats(rt.store, oid)["retailer"]["calls"], 0)


if __name__ == "__main__":
    unittest.main()
