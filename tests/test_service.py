import threading
import unittest

from roomready import demo
from roomready import domain as d
from roomready.service import ApiError
from support import create, order, runtime


class IdempotencyTests(unittest.TestCase):
    def test_same_key_same_payload_returns_same_order(self):
        rt = runtime()
        s1, v1 = rt.service.create_order("alice", "k1", demo.order_body())
        s2, v2 = rt.service.create_order("alice", "k1", demo.order_body())
        self.assertEqual((s1, s2), (201, 200))
        self.assertEqual(v1["orderId"], v2["orderId"])
        self.assertTrue(v2["idempotentReplay"])

    def test_same_key_different_payload_conflicts(self):
        rt = runtime()
        rt.service.create_order("alice", "k1", demo.order_body())
        with self.assertRaises(ApiError) as ctx:
            rt.service.create_order("alice", "k1", demo.order_body(hotelId="OTHER-HOTEL"))
        self.assertEqual(ctx.exception.status, 409)

    def test_keys_are_scoped_per_user(self):
        rt = runtime()
        rt.service.put_profile("bob", demo.PROFILE)
        _, a = rt.service.create_order("alice", "k1", demo.order_body())
        status, b = rt.service.create_order("bob", "k1", demo.order_body(hotelId="OTHER-HOTEL"))
        self.assertEqual(status, 201)
        self.assertNotEqual(a["orderId"], b["orderId"])

    def test_concurrent_duplicate_creates_yield_one_order(self):
        rt = runtime()
        results = []

        def worker():
            results.append(rt.service.create_order("alice", "race-key", demo.order_body()))
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len({v["orderId"] for _, v in results}), 1)
        self.assertEqual(sorted(s for s, _ in results), [200] * 7 + [201])

    def test_acceptance_writes_order_idempotency_and_due_work(self):
        rt = runtime()
        oid = create(rt)
        self.assertEqual(order(rt, oid)["GSI1PK"], "DUE")
        self.assertEqual(rt.store.get("USER#alice", rt.store.query("USER#alice", "IDEM#")[0]["SK"])["orderId"], oid)
        self.assertEqual(rt.next_due_at(), "2026-11-16T07:00:00Z")

    def test_profile_required_and_past_checkin_rejected(self):
        rt = runtime()
        with self.assertRaises(ApiError) as ctx:
            rt.service.create_order("carol", "k", demo.order_body())
        self.assertEqual(ctx.exception.status, 400)
        with self.assertRaises(d.ValidationError):
            rt.service.create_order("alice", "k2", demo.order_body(checkIn="2026-11-01T10:00:00+00:00"))


class OwnershipTests(unittest.TestCase):
    def test_other_users_get_404(self):
        rt = runtime()
        oid = create(rt)
        for call in (lambda: rt.service.get_order("bob", oid), lambda: rt.service.cancel_order("bob", oid)):
            with self.assertRaises(ApiError) as ctx:
                call()
            self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(order(rt, oid)["status"], d.ACCEPTED)  # bob's cancel had no effect
        self.assertEqual(rt.service.list_my_orders("bob")[1]["orders"], [])

    def test_admin_only_routes(self):
        rt = runtime()
        oid = create(rt)
        for call in (lambda: rt.service.list_orders_admin("alice"), lambda: rt.service.list_audit("alice", oid),
                     lambda: rt.service.provider_state("alice")):
            with self.assertRaises(ApiError) as ctx:
                call()
            self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(len(rt.service.list_orders_admin("admin")[1]["orders"]), 1)

    def test_missing_identity_is_401(self):
        rt = runtime()
        with self.assertRaises(ApiError) as ctx:
            rt.service.get_order(None, "x")
        self.assertEqual(ctx.exception.status, 401)


class CancelTests(unittest.TestCase):
    def test_cancel_flag_is_monotonic_and_idempotent(self):
        rt = runtime()
        oid = create(rt)
        rt.tick(to_next_due=True)
        rt.service.cancel_order("alice", oid)
        for _ in range(2):
            status, view = rt.service.cancel_order("alice", oid)
            self.assertEqual((status, view["status"], view["cancelRequested"]), (200, d.CANCELLED, True))
        self.assertEqual(sorted(rt.starter.executions), [f"{oid}-g1", f"{oid}-g2"])

    def test_cancel_of_final_failed_order_changes_nothing(self):
        rt = runtime()
        oid = create(rt, "hotel_permanent")
        rt.tick(to_next_due=True)
        status, view = rt.service.cancel_order("alice", oid)
        self.assertEqual(view["status"], d.PREP_FAILED)
        self.assertEqual(len(rt.starter.executions), 1)


class SchedulerTests(unittest.TestCase):
    def test_not_due_orders_are_not_claimed(self):
        rt = runtime()
        oid = create(rt)
        self.assertEqual(rt.tick()["claimed"], [])
        self.assertEqual(order(rt, oid)["status"], d.ACCEPTED)

    def test_claim_happens_once(self):
        rt = runtime()
        oid = create(rt)
        first = rt.tick(to_next_due=True)
        second = rt.tick()
        self.assertEqual(first["claimed"], [{"orderId": oid, "generation": 1}])
        self.assertEqual(second["claimed"], [])

    def test_failed_publish_stays_in_outbox_and_is_retried(self):
        rt = runtime()
        oid = create(rt)
        rt.router.fail_next = 1
        result = rt.tick(to_next_due=True)
        self.assertEqual(result["outbox"]["failed"], 1)
        self.assertEqual(order(rt, oid)["status"], d.PREPARING)
        self.assertEqual(len(rt.store.query_index("OUTBOX")), 1)
        result = rt.tick()
        self.assertEqual(result["outbox"]["published"], 1)
        self.assertEqual(order(rt, oid)["status"], d.READY)
        self.assertEqual(rt.store.query_index("OUTBOX"), [])

    def test_events_contain_no_personal_data(self):
        rt = runtime()
        create(rt)
        rt.tick(to_next_due=True)
        event = rt.router.published[0]
        self.assertEqual(set(event), {"eventId", "type", "orderId", "generation", "timestamp", "correlationId"})


if __name__ == "__main__":
    unittest.main()
