"""Simulated external providers: Retailer (+ payments), Courier, Hotel PMS.

Every provider:
  * is idempotent on the caller-supplied idempotency key (a stable logical action key),
  * keeps its state in the shared Store (so it survives across Lambda invocations),
  * exposes get_status(key) for reconciliation,
  * supports DETERMINISTIC fault injection from a per-order fault dict, e.g.
        {"retailer.reserve": {"transient": 2}}   # fail twice, then succeed
        {"courier.book": {"timeout": 1}}         # apply effect, then lose the response once
        {"hotel.setup": {"permanent": True}}     # always reject
        {"retailer.void": {"permanent": True}}   # compensation failure
NO real payments or bookings are made anywhere.
"""
from __future__ import annotations

import hashlib

from .domain import to_iso
from .store import ConditionFailed

PROVIDER_STATS_PK = "PROVSTATS"
FAULT_PK = "FAULT"


class ProviderError(Exception):
    pass


class TransientProviderError(ProviderError):
    """Temporary failure; no effect was applied. Safe to retry."""


class PermanentProviderError(ProviderError):
    """Business rejection; no effect was applied. Do not retry."""


class AmbiguousTimeout(ProviderError):
    """The request timed out: the effect MAY have been applied. Reconcile before reissuing."""


SCENARIOS = {
    "none": {"label": "No faults (happy path)", "faults": {}},
    "retailer_transient": {"label": "Retailer: 2 transient errors, then success",
                           "faults": {"retailer.reserve": {"transient": 2}}},
    "courier_timeout": {"label": "Courier: booking applied but response lost once (ambiguous timeout)",
                        "faults": {"courier.book": {"timeout": 1}}},
    "hotel_permanent": {"label": "Hotel PMS: room setup permanently rejected",
                        "faults": {"hotel.setup": {"permanent": True}}},
    "retailer_exhausted": {"label": "Retailer: transient errors until retries are exhausted",
                           "faults": {"retailer.reserve": {"transient": 99}}},
    "cancel_during_prep": {"label": "Traveller cancels while preparation is running (race)",
                           "faults": {}, "cancelDuringState": "BookCourier"},
    "compensation_failure": {"label": "Hotel rejects setup AND payment void fails (manual review)",
                             "faults": {"hotel.setup": {"permanent": True}, "retailer.void": {"permanent": True}}},
}


def order_id_from_key(key: str) -> str:
    head = key.split("/", 1)[0]
    return head[len("ORDER#"):] if head.startswith("ORDER#") else "unknown"


def _short_hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:10].upper()


class SimulatedProvider:
    name = "base"

    def __init__(self, store, clock):
        self.store = store
        self.clock = clock

    @property
    def pk(self):
        return f"PROVIDER#{self.name}"

    # ---- bookkeeping
    def _count(self, key, kind):
        self.store.increment(PROVIDER_STATS_PK, f"{self.name}|{order_id_from_key(key)}", kind)

    @staticmethod
    def _public(record):
        if record is None:
            return None
        out = {k: v for k, v in record.items() if k not in ("PK", "SK")}
        out["key"] = record["SK"]
        return out

    def get_status(self, key):
        self._count(key, "statusChecks")
        return self._public(self.store.get(self.pk, key))

    # ---- fault injection + idempotent primitives
    def _invoke(self, op, key, faults, apply):
        self._count(key, "calls")
        spec = (faults or {}).get(f"{self.name}.{op}") or {}
        if not spec:
            return apply()
        n = self.store.increment(FAULT_PK, f"{self.name}.{op}|{key}", "count")
        if spec.get("permanent"):
            raise PermanentProviderError(f"{self.name}.{op} rejected (simulated permanent failure)")
        if n <= int(spec.get("transient", 0)):
            raise TransientProviderError(f"{self.name}.{op} unavailable (simulated transient error #{n})")
        result = apply()
        if n <= int(spec.get("timeout", 0)):
            raise AmbiguousTimeout(f"{self.name}.{op} timed out after applying the effect (simulated #{n})")
        return result

    def _create(self, key, record):
        item = {"PK": self.pk, "SK": key, "orderId": order_id_from_key(key),
                "createdAt": to_iso(self.clock.now()), **record}
        try:
            self.store.put(item, [("not_exists", "PK")])
            self._count(key, "creates")
        except ConditionFailed:
            pass  # idempotent replay: the effect already exists
        return self._public(self.store.get(self.pk, key))

    def _transition(self, key, field, allowed_from, to):
        for _ in range(3):
            rec = self.store.get(self.pk, key)
            if rec is None:
                raise PermanentProviderError(f"{self.name}: unknown key {key}")
            if rec.get(field) == to:
                return self._public(rec)  # idempotent
            if rec.get(field) not in allowed_from:
                raise PermanentProviderError(f"{self.name}: cannot move {field} from {rec.get(field)} to {to}")
            try:
                self.store.update(self.pk, key, {field: to, "updatedAt": to_iso(self.clock.now())},
                                  conds=[("eq", field, rec[field])])
                self._count(key, "updates")
                return self._public(self.store.get(self.pk, key))
            except ConditionFailed:
                continue
        raise TransientProviderError(f"{self.name}: concurrent update on {key}")

    def records(self):
        return [self._public(r) for r in self.store.query(self.pk)]


class Retailer(SimulatedProvider):
    """Reserves personalised supplies and AUTHORIZES (never captures by itself) a payment."""
    name = "retailer"

    def reserve(self, key, request, faults=None):
        items = list(request.get("items", []))

        def apply():
            h = _short_hash(key)
            return self._create(key, {
                "reservationId": f"RSV-{h}", "reservationStatus": "RESERVED",
                "paymentId": f"PAY-{h}", "paymentStatus": "AUTHORIZED",
                "amountCents": 1500 + 450 * len(items), "currency": "USD", "items": items,
            })
        return self._invoke("reserve", key, faults, apply)

    def release(self, key, faults=None):
        return self._invoke("release", key, faults,
                            lambda: self._transition(key, "reservationStatus", {"RESERVED"}, "RELEASED"))

    def void(self, key, faults=None):
        """Cancel an AUTHORIZED payment (no money moved)."""
        return self._invoke("void", key, faults,
                            lambda: self._transition(key, "paymentStatus", {"AUTHORIZED"}, "VOIDED"))

    def capture(self, key, faults=None):
        return self._invoke("capture", key, faults,
                            lambda: self._transition(key, "paymentStatus", {"AUTHORIZED"}, "CAPTURED"))

    def refund(self, key, faults=None):
        """Return money for a CAPTURED payment (distinct from void)."""
        return self._invoke("refund", key, faults,
                            lambda: self._transition(key, "paymentStatus", {"CAPTURED"}, "REFUNDED"))


class Courier(SimulatedProvider):
    name = "courier"

    def book(self, key, request, faults=None):
        def apply():
            return self._create(key, {
                "bookingId": f"DLV-{_short_hash(key)}", "deliveryStatus": "BOOKED",
                "hotelId": request.get("hotelId"), "deliverBy": request.get("deliverBy"),
            })
        return self._invoke("book", key, faults, apply)

    def cancel(self, key, faults=None):
        return self._invoke("cancel", key, faults,
                            lambda: self._transition(key, "deliveryStatus", {"BOOKED"}, "CANCELLED"))

    def deliver(self, key, faults=None):
        return self._invoke("deliver", key, faults,
                            lambda: self._transition(key, "deliveryStatus", {"BOOKED"}, "DELIVERED"))

    def return_pickup(self, key, faults=None):
        return self._invoke("return_pickup", key, faults,
                            lambda: self._transition(key, "deliveryStatus", {"DELIVERED"}, "RETURNED"))


class HotelPMS(SimulatedProvider):
    name = "hotel"

    def setup(self, key, request, faults=None):
        def apply():
            return self._create(key, {
                "setupId": f"SET-{_short_hash(key)}", "setupStatus": "SCHEDULED",
                "hotelId": request.get("hotelId"), "roomReservationRef": request.get("roomReservationRef"),
            })
        return self._invoke("setup", key, faults, apply)

    def cancel_setup(self, key, faults=None):
        return self._invoke("cancel_setup", key, faults,
                            lambda: self._transition(key, "setupStatus", {"SCHEDULED"}, "CANCELLED"))


def build_providers(store, clock):
    return {"retailer": Retailer(store, clock), "courier": Courier(store, clock), "hotel": HotelPMS(store, clock)}


def provider_stats(store, order_id):
    """Per-provider call counters for one order: calls, creates, updates, statusChecks."""
    out = {}
    for name in ("retailer", "courier", "hotel"):
        rec = store.get(PROVIDER_STATS_PK, f"{name}|{order_id}") or {}
        out[name] = {k: rec.get(k, 0) for k in ("calls", "creates", "updates", "statusChecks")}
    return out
