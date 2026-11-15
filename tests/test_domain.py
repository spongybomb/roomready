import unittest
from datetime import datetime, timedelta

from roomready import domain as d
from roomready.providers import SCENARIOS

NOW = datetime(2026, 11, 15, 8, 0, 0, tzinfo=d.UTC)


class CheckinAndDueAtTests(unittest.TestCase):
    def test_offset_is_required(self):
        with self.assertRaises(d.ValidationError):
            d.parse_checkin("2026-11-20T15:00:00")
        with self.assertRaises(d.ValidationError):
            d.parse_checkin("2026-11-20")
        with self.assertRaises(d.ValidationError):
            d.parse_checkin("not a date")

    def test_offsets_and_z_are_accepted(self):
        self.assertEqual(d.to_iso(d.parse_checkin("2026-11-20T15:00:00+08:00")), "2026-11-20T07:00:00Z")
        self.assertEqual(d.to_iso(d.parse_checkin("2026-11-20T07:00:00Z")), "2026-11-20T07:00:00Z")
        self.assertEqual(d.to_iso(d.parse_checkin("2026-11-20T02:00:00-05:00")), "2026-11-20T07:00:00Z")

    def test_due_at_is_checkin_minus_24h_in_utc(self):
        due = d.compute_due_at(d.parse_checkin("2026-11-17T15:00:00+08:00"), NOW)
        self.assertEqual(d.to_iso(due), "2026-11-16T07:00:00Z")

    def test_due_at_is_now_when_lead_time_already_passed(self):
        checkin = NOW + timedelta(hours=5)
        self.assertEqual(d.compute_due_at(checkin, NOW), NOW)

    def test_past_checkin_rejected(self):
        with self.assertRaises(d.ValidationError):
            d.compute_due_at(NOW - timedelta(minutes=1), NOW)
        with self.assertRaises(d.ValidationError):
            d.compute_due_at(NOW, NOW)

    def test_dst_boundary_uses_explicit_offset(self):
        # US DST ends 2026-11-01; explicit offsets make the computation unambiguous.
        due = d.compute_due_at(d.parse_checkin("2026-11-01T01:30:00-04:00"), datetime(2026, 10, 1, tzinfo=d.UTC))
        self.assertEqual(d.to_iso(due), "2026-10-31T05:30:00Z")


class ValidationTests(unittest.TestCase):
    def body(self, **kw):
        b = {"flight": "cx 880", "hotelId": "HK-1", "roomReservationRef": "RES-1",
             "checkIn": "2026-11-20T15:00:00+08:00"}
        b.update(kw)
        return b

    def test_order_normalisation(self):
        req = d.validate_order_request(self.body(), SCENARIOS)
        self.assertEqual(req["flight"], "CX880")
        self.assertEqual(req["scenario"], "none")

    def test_order_rejections(self):
        for bad in ({"flight": "???"}, {"hotelId": ""}, {"roomReservationRef": "x" * 65},
                    {"checkIn": "2026-11-20T15:00:00"}, {"scenario": "nope"}):
            with self.subTest(bad=bad), self.assertRaises(d.ValidationError):
                d.validate_order_request(self.body(**bad), SCENARIOS)
        with self.assertRaises(d.ValidationError):
            d.validate_order_request(["not", "a", "dict"], SCENARIOS)

    def test_profile_validation(self):
        p = d.validate_profile({"sizes": {"slippers": "m"}, "allergyFlags": ["Nuts", "nuts"],
                                "preferredBrands": ["Aesop"]})
        self.assertEqual(p["sizes"], {"slippers": "M"})
        self.assertEqual(p["allergyFlags"], ["nuts"])
        with self.assertRaises(d.ValidationError):
            d.validate_profile({"allergyFlags": "nuts"})
        with self.assertRaises(d.ValidationError):
            d.validate_profile({"sizes": {"slippers": "X" * 20}})

    def test_idempotency_key_validation(self):
        self.assertEqual(d.validate_idempotency_key(" abc-1 "), "abc-1")
        for bad in (None, "", "has space", "x" * 101):
            with self.subTest(bad=bad), self.assertRaises(d.ValidationError):
                d.validate_idempotency_key(bad)

    def test_payload_hash_is_canonical(self):
        self.assertEqual(d.payload_hash({"a": 1, "b": [1, 2]}), d.payload_hash({"b": [1, 2], "a": 1}))
        self.assertNotEqual(d.payload_hash({"a": 1}), d.payload_hash({"a": 2}))

    def test_transitions(self):
        self.assertTrue(d.can_transition(d.ACCEPTED, d.PREPARING))
        self.assertTrue(d.can_transition(d.READY, d.COMPENSATING))
        self.assertFalse(d.can_transition(d.READY, d.CANCELLED))  # must go through compensation
        self.assertFalse(d.can_transition(d.CANCELLED, d.PREPARING))
        with self.assertRaises(d.InvalidTransition):
            d.assert_transition(d.COMPENSATING, d.READY)


if __name__ == "__main__":
    unittest.main()
