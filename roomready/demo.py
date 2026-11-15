"""Scripted end-to-end scenarios (in-process, simulated providers).

    python -m roomready.demo

Exits non-zero if any scenario's outcome differs from the expectation.
"""
from __future__ import annotations

import sys
from datetime import datetime

from . import domain as d
from .local_orchestrator import LocalRuntime
from .providers import provider_stats
from .store import MemoryStore, ORDER_SK, order_pk

DEMO_START = datetime(2026, 11, 15, 8, 0, 0, tzinfo=d.UTC)
CHECK_IN = "2026-11-17T15:00:00+08:00"  # 07:00Z on Nov 17 -> due 07:00Z on Nov 16
PROFILE = {"displayName": "Alice Traveller", "sizes": {"slippers": "M", "bathrobe": "L"},
           "allergyFlags": ["fragrance"], "preferredBrands": ["Aesop"]}


def new_runtime():
    rt = LocalRuntime(store=MemoryStore(), clock=d.ManualClock(DEMO_START))
    rt.service.put_profile("alice", PROFILE)
    return rt


def order_body(scenario="none", **overrides):
    body = {"flight": "CX880", "hotelId": "HK-HARBOUR-01", "roomReservationRef": "RES-7781",
            "checkIn": CHECK_IN, "scenario": scenario}
    body.update(overrides)
    return body


def create(rt, scenario="none", key=None, user="alice"):
    status, view = rt.service.create_order(user, key or f"key-{d.new_id()[:12]}", order_body(scenario))
    assert status == 201, (status, view)
    return view["orderId"]


def order(rt, order_id):
    return rt.store.get(order_pk(order_id), ORDER_SK)


def detail(rt, order_id):
    return rt.service.get_order(order(rt, order_id)["ownerId"], order_id)[1]


def ledger(rt, order_id):
    return {e["action"]: e for e in detail(rt, order_id)["ledger"]}


def comps(rt, order_id):
    return {c["slot"]: c for c in detail(rt, order_id)["compensations"]}


def provider_record(rt, provider, order_id, action):
    return rt.providers[provider].store.get(f"PROVIDER#{provider}", f"ORDER#{order_id}/ACTION#{action}")


def audit_events(rt, order_id):
    return rt.service.list_audit("admin", order_id)[1]["audit"]


class Result:
    def __init__(self, title, expected):
        self.title, self.expected = title, expected
        self.actual = None
        self.checks = []

    def check(self, description, ok):
        self.checks.append((description, bool(ok)))

    @property
    def passed(self):
        return self.actual == self.expected and all(ok for _, ok in self.checks)


# --------------------------------------------------------------------------- scenarios
def s1_happy_path():
    r = Result("Happy path", d.READY)
    rt = new_runtime()
    oid = create(rt)
    tick = rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    lg = ledger(rt, oid)
    r.check("scheduler claimed the order at T-24h (2026-11-16T07:00:00Z)", tick["now"] == "2026-11-16T07:00:00Z")
    r.check("3 ledger actions SUCCEEDED", all(lg[a]["status"] == d.SUCCEEDED for a in lg) and len(lg) == 3)
    stats = provider_stats(rt.store, oid)
    r.check("exactly one effect created per provider", all(stats[p]["creates"] == 1 for p in stats))
    r.check("payment is only AUTHORIZED", provider_record(rt, "retailer", oid, "reserve_supplies")["paymentStatus"] == "AUTHORIZED")
    return r


def s2_cancel_before_preparation():
    r = Result("Cancel before preparation", d.CANCELLED)
    rt = new_runtime()
    oid = create(rt)
    rt.service.cancel_order("alice", oid)
    tick = rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    stats = provider_stats(rt.store, oid)
    r.check("zero provider calls", all(v == 0 for s in stats.values() for v in s.values()))
    r.check("scheduler did not claim it / no execution started", not tick["claimed"] and not rt.starter.executions)
    r.check("second cancel is idempotent", rt.service.cancel_order("alice", oid)[1]["status"] == d.CANCELLED)
    return r


def s3_cancel_after_ready():
    r = Result("Cancel after READY (reverse saga)", d.CANCELLED)
    rt = new_runtime()
    oid = create(rt)
    rt.tick(to_next_due=True)
    r.check("order was READY first", order(rt, oid)["status"] == d.READY)
    rt.service.cancel_order("alice", oid)
    r.actual = order(rt, oid)["status"]
    rec = provider_record(rt, "retailer", oid, "reserve_supplies")
    r.check("authorization VOIDED (not refunded)", rec["paymentStatus"] == "VOIDED")
    r.check("reservation RELEASED", rec["reservationStatus"] == "RELEASED")
    r.check("courier booking CANCELLED", provider_record(rt, "courier", oid, "book_courier")["deliveryStatus"] == "CANCELLED")
    r.check("hotel setup CANCELLED", provider_record(rt, "hotel", oid, "setup_room")["setupStatus"] == "CANCELLED")
    order_done = [e["detail"]["compensation"] for e in audit_events(rt, oid) if e["event"] == "COMPENSATION_SUCCEEDED"]
    r.check("compensations ran in reverse order",
            order_done == ["cancel_setup", "undo_delivery", "release_reservation", "reverse_payment"])
    names = sorted(rt.starter.executions)
    r.check("reverse ran as a new generation (g1 forward, g2 reverse)", names == [f"{oid}-g1", f"{oid}-g2"])
    return r


def s4_transient_retailer():
    r = Result("Transient retailer error is retried", d.READY)
    rt = new_runtime()
    oid = create(rt, "retailer_transient")
    rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    r.check("reserve_supplies took 3 attempts", ledger(rt, oid)["reserve_supplies"]["attempts"] == 3)
    r.check("only one reservation created", provider_stats(rt.store, oid)["retailer"]["creates"] == 1)
    return r


def s5_ambiguous_courier_timeout():
    r = Result("Ambiguous courier timeout is reconciled", d.READY)
    rt = new_runtime()
    oid = create(rt, "courier_timeout")
    rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    entry = ledger(rt, oid)["book_courier"]
    r.check("ledger entry was reconciled (UNKNOWN -> SUCCEEDED)", entry["reconciled"] is True)
    r.check("no double booking (1 courier effect, 1 booking record)",
            provider_stats(rt.store, oid)["courier"]["creates"] == 1
            and len([x for x in rt.providers["courier"].records() if x["orderId"] == oid]) == 1)
    r.check("audit shows ACTION_UNKNOWN then ACTION_RECONCILED",
            [e["event"] for e in audit_events(rt, oid) if e["event"] in ("ACTION_UNKNOWN", "ACTION_RECONCILED")]
            == ["ACTION_UNKNOWN", "ACTION_RECONCILED"])
    return r


def s6_permanent_hotel_failure():
    r = Result("Permanent hotel failure is compensated", d.PREP_FAILED)
    rt = new_runtime()
    oid = create(rt, "hotel_permanent")
    rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    lg, cp = ledger(rt, oid), comps(rt, oid)
    r.check("setup_room FAILED in ledger", lg["setup_room"]["status"] == d.FAILED)
    r.check("no compensation attempted for the failed step", "cancel_setup" not in cp)
    r.check("courier cancelled, reservation released, authorization voided",
            cp["undo_delivery"]["method"] == "CANCEL_BOOKING" and cp["release_reservation"]["status"] == d.SUCCEEDED
            and cp["reverse_payment"]["method"] == "VOID")
    return r


def s7_cancel_during_preparation():
    r = Result("Cancellation during preparation (race)", d.CANCELLED)
    rt = new_runtime()
    oid = create(rt, "cancel_during_prep")
    rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    lg = ledger(rt, oid)
    r.check("forward stopped before booking the courier", set(lg) == {"reserve_supplies"})
    stats = provider_stats(rt.store, oid)
    r.check("courier and hotel never called", stats["courier"]["calls"] == 0 and stats["hotel"]["calls"] == 0)
    r.check("reservation compensated (authorization voided)",
            provider_record(rt, "retailer", oid, "reserve_supplies")["paymentStatus"] == "VOIDED")
    r.check("single execution (no overlapping workflow)", list(rt.starter.executions) == [f"{oid}-g1"])
    return r


def s8_compensation_failure():
    r = Result("Compensation failure -> MANUAL_REVIEW", d.MANUAL_REVIEW)
    rt = new_runtime()
    oid = create(rt, "compensation_failure")
    rt.tick(to_next_due=True)
    r.actual = order(rt, oid)["status"]
    cp = comps(rt, oid)
    r.check("payment void FAILED and was recorded", cp["reverse_payment"]["status"] == d.FAILED)
    r.check("other compensations still ran (independent)",
            cp["undo_delivery"]["status"] == d.SUCCEEDED and cp["release_reservation"]["status"] == d.SUCCEEDED)
    r.check("unresolved effect listed on the order", order(rt, oid)["unresolved"])
    return r


def s9_idempotent_create():
    r = Result("Idempotent create + 409 conflict", "REPLAY+409")
    rt = new_runtime()
    s1, v1 = rt.service.create_order("alice", "trip-42", order_body())
    s2, v2 = rt.service.create_order("alice", "trip-42", order_body())
    from .service import ApiError
    try:
        rt.service.create_order("alice", "trip-42", order_body(flight="CX999"))
        conflict = None
    except ApiError as exc:
        conflict = exc.status
    r.actual = "REPLAY+409" if (s1, s2, v1["orderId"]) == (201, 200, v2["orderId"]) and conflict == 409 else "WRONG"
    r.check("first call 201, replay 200 with the same orderId", (s1, s2) == (201, 200) and v1["orderId"] == v2["orderId"])
    r.check("different payload with same key -> 409", conflict == 409)
    r.check("exactly one order stored", len(rt.service.list_my_orders("alice")[1]["orders"]) == 1)
    return r


SCENARIOS = [s1_happy_path, s2_cancel_before_preparation, s3_cancel_after_ready, s4_transient_retailer,
             s5_ambiguous_courier_timeout, s6_permanent_hotel_failure, s7_cancel_during_preparation,
             s8_compensation_failure, s9_idempotent_create]


def run_all(out=sys.stdout):
    results = []
    out.write("ROOMREADY scripted demo - all providers simulated, no real payments\n")
    out.write(f"Demo clock starts at {d.to_iso(DEMO_START)}; check-in {CHECK_IN}\n\n")
    for i, fn in enumerate(SCENARIOS, 1):
        try:
            res = fn()
        except Exception as exc:  # report, do not hide
            res = Result(fn.__name__, "no exception")
            res.actual = f"EXCEPTION {type(exc).__name__}: {exc}"
        results.append(res)
        out.write(f"[{'PASS' if res.passed else 'FAIL'}] {i}. {res.title}: expected {res.expected}, got {res.actual}\n")
        for desc, ok in res.checks:
            out.write(f"       {'ok ' if ok else 'XX '} {desc}\n")
    passed = sum(r.passed for r in results)
    out.write(f"\n{passed}/{len(results)} scenarios behaved as expected.\n")
    return results


def main():
    results = run_all()
    sys.exit(0 if all(r.passed for r in results) else 1)


if __name__ == "__main__":
    main()
