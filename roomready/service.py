"""Application service shared by the local server and the AWS Lambda handlers."""
from __future__ import annotations

from . import domain as d
from .providers import SCENARIOS, build_providers, provider_stats
from .store import (INDEX_DUE, INDEX_OUTBOX, ORDER_SK, PROFILE_SK, ConditionFailed, audit_item, op_put,
                    op_update, order_pk, user_pk, write_audit)

EVENT_SOURCE = "roomready.orders"
PREPARATION_DUE = "PreparationDue"
CANCELLATION_REQUESTED = "CancellationRequested"

PUBLIC_ORDER_FIELDS = (
    "orderId", "status", "generation", "cancelRequested", "flight", "hotelId", "roomReservationRef",
    "checkIn", "checkInUtc", "dueAt", "scenario", "createdAt", "updatedAt", "readyAt", "finalizedAt",
    "compensationReason", "failureReason", "unresolved",
)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def execution_request(event: dict):
    """EventBridge domain event -> (deterministic execution name, state machine input).
    Shared by the local starter and the AWS starter Lambda."""
    mode = "REVERSE" if event["type"] == CANCELLATION_REQUESTED else "FORWARD"
    name = f"{event['orderId']}-g{event['generation']}"
    return name, {"orderId": event["orderId"], "generation": event["generation"], "mode": mode,
                  "correlationId": event.get("correlationId"), "eventId": event.get("eventId")}


class Service:
    def __init__(self, store, clock, admin_users=("admin",), publisher=None):
        self.store = store
        self.clock = clock
        self.admin_users = set(admin_users)
        self.publisher = publisher

    # ------------------------------------------------------------------ helpers
    def now_iso(self):
        return d.to_iso(self.clock.now())

    def _require_user(self, caller):
        if not caller:
            raise ApiError(401, "UNAUTHENTICATED", "caller identity is required")

    def _require_admin(self, caller):
        self._require_user(caller)
        if caller not in self.admin_users:
            raise ApiError(403, "FORBIDDEN", "admin access required")

    def _owned_order(self, caller, order_id):
        self._require_user(caller)
        order = self.store.get(order_pk(order_id), ORDER_SK) if order_id else None
        if order is None or order.get("ownerId") != caller:
            raise ApiError(404, "NOT_FOUND", "order not found")  # no existence leak to non-owners
        return order

    @staticmethod
    def order_view(order):
        return {k: order[k] for k in PUBLIC_ORDER_FIELDS if k in order}

    def order_detail(self, order):
        view = self.order_view(order)
        pk = order_pk(order["orderId"])
        view["ledger"] = [{k: v for k, v in e.items() if k not in ("PK", "SK", "entity")}
                          for e in self.store.query(pk, "ACTION#")]
        view["compensations"] = [{k: v for k, v in e.items() if k not in ("PK", "SK", "entity")}
                                 for e in self.store.query(pk, "COMP#")]
        return view

    def _outbox_op(self, order, event_type, generation):
        event_id = d.new_id()
        now = self.now_iso()
        event = {"eventId": event_id, "type": event_type, "orderId": order["orderId"],
                 "generation": generation, "timestamp": now, "correlationId": order.get("correlationId")}
        return op_put({"PK": f"OUTBOX#{event_id}", "SK": "EVENT", "entity": "OUTBOX", "event": event,
                       "status": "PENDING", "publishAttempts": 0,
                       "GSI1PK": INDEX_OUTBOX, "GSI1SK": f"{now}#{event_id}"})

    # ------------------------------------------------------------------ profile
    def put_profile(self, caller, body):
        self._require_user(caller)
        profile = d.validate_profile(body)
        self.store.put({"PK": user_pk(caller), "SK": PROFILE_SK, "entity": "PROFILE", "userId": caller,
                        "updatedAt": self.now_iso(), **profile})
        return 200, {"userId": caller, **profile}

    def get_profile(self, caller):
        self._require_user(caller)
        item = self.store.get(user_pk(caller), PROFILE_SK)
        if not item:
            raise ApiError(404, "NOT_FOUND", "profile not found")
        return 200, {k: v for k, v in item.items() if k not in ("PK", "SK", "entity")}

    # ------------------------------------------------------------------ orders
    def create_order(self, caller, idempotency_key, body):
        self._require_user(caller)
        key = d.validate_idempotency_key(idempotency_key)
        request = d.validate_order_request(body, SCENARIOS)
        request_hash = d.payload_hash(request)
        idem_sk = f"IDEM#{key}"

        existing = self.store.get(user_pk(caller), idem_sk)
        if existing:
            return self._replay(caller, existing, request_hash)

        profile = self.store.get(user_pk(caller), PROFILE_SK)
        if not profile:
            raise ApiError(400, "PROFILE_REQUIRED", "create a profile before ordering")
        checkin = d.parse_checkin(request["checkIn"])
        due = d.compute_due_at(checkin, self.clock.now())
        now = self.now_iso()
        order_id = d.new_id()
        due_iso = d.to_iso(due)
        scenario = SCENARIOS[request["scenario"]]
        order = {
            "PK": order_pk(order_id), "SK": ORDER_SK, "entity": "ORDER", "orderId": order_id,
            "ownerId": caller, "status": d.ACCEPTED, "generation": 0, "cancelRequested": False,
            **request, "checkInUtc": d.to_iso(checkin), "dueAt": due_iso,
            "faults": scenario["faults"], "cancelDuringState": scenario.get("cancelDuringState"),
            "profileSnapshot": {k: profile.get(k) for k in ("sizes", "allergyFlags", "preferredBrands")},
            "correlationId": d.new_id(), "createdAt": now, "updatedAt": now,
            "GSI1PK": INDEX_DUE, "GSI1SK": f"{due_iso}#{order_id}",  # sparse due-work index
        }
        idem = {"PK": user_pk(caller), "SK": idem_sk, "entity": "IDEMPOTENCY", "requestHash": request_hash,
                "orderId": order_id, "createdAt": now}
        try:  # durable acceptance: order + idempotency record + due work + audit, atomically
            self.store.transact([
                op_put(idem, [("not_exists", "PK")]),
                op_put(order, [("not_exists", "PK")]),
                op_put(audit_item(order_id, now, "ORDER_ACCEPTED", {"dueAt": due_iso, "scenario": request["scenario"]},
                                  actor=caller)),
            ])
        except ConditionFailed:
            existing = self.store.get(user_pk(caller), idem_sk)
            if existing:
                return self._replay(caller, existing, request_hash)
            raise
        return 201, self.order_view(order)

    def _replay(self, caller, idem, request_hash):
        if idem["requestHash"] != request_hash:
            raise ApiError(409, "IDEMPOTENCY_CONFLICT",
                           "this Idempotency-Key was already used with a different payload")
        order = self.store.get(order_pk(idem["orderId"]), ORDER_SK)
        view = self.order_view(order)
        view["idempotentReplay"] = True
        return 200, view

    def list_my_orders(self, caller):
        self._require_user(caller)
        orders = [self.order_view(o) for o in self.store.scan("ORDER#", ORDER_SK) if o.get("ownerId") == caller]
        orders.sort(key=lambda o: o.get("createdAt", ""), reverse=True)
        return 200, {"orders": orders}

    def get_order(self, caller, order_id):
        return 200, self.order_detail(self._owned_order(caller, order_id))

    def cancel_order(self, caller, order_id):
        """Idempotent. cancelRequested is monotonic (never reset)."""
        for _ in range(5):
            order = self._owned_order(caller, order_id)
            status, gen, pk, now = order["status"], order["generation"], order_pk(order_id), self.now_iso()
            try:
                if status in d.FINAL_STATUSES:
                    return 200, self.order_detail(order)
                if status == d.ACCEPTED:  # not claimed yet: cancel directly, zero provider calls
                    d.assert_transition(d.ACCEPTED, d.CANCELLED)
                    self.store.transact([
                        op_update(pk, ORDER_SK, {"status": d.CANCELLED, "cancelRequested": True,
                                                 "finalizedAt": now, "updatedAt": now},
                                  remove=["GSI1PK", "GSI1SK"],
                                  conds=[("eq", "status", d.ACCEPTED), ("eq", "generation", gen)]),
                        op_put(audit_item(order_id, now, "CANCELLED_BEFORE_PREPARATION", actor=caller)),
                    ])
                elif status == d.READY:  # start a new reverse generation
                    d.assert_transition(d.READY, d.COMPENSATING)
                    self.store.transact([
                        op_update(pk, ORDER_SK, {"status": d.COMPENSATING, "cancelRequested": True,
                                                 "generation": gen + 1, "compensationReason": "CANCEL_REQUESTED",
                                                 "updatedAt": now},
                                  conds=[("eq", "status", d.READY), ("eq", "generation", gen)]),
                        self._outbox_op(order, CANCELLATION_REQUESTED, gen + 1),
                        op_put(audit_item(order_id, now, "CANCEL_REQUESTED",
                                          {"previousStatus": status, "reverseGeneration": gen + 1}, actor=caller)),
                    ])
                    self._publish_best_effort()
                elif not order.get("cancelRequested"):  # PREPARING / COMPENSATING: record intent only
                    self.store.transact([
                        op_update(pk, ORDER_SK, {"cancelRequested": True, "updatedAt": now},
                                  conds=[("eq", "status", status), ("eq", "generation", gen)]),
                        op_put(audit_item(order_id, now, "CANCEL_REQUESTED", {"previousStatus": status},
                                          actor=caller)),
                    ])
                else:
                    return 202, self.order_detail(order)
            except ConditionFailed:
                continue  # raced with the scheduler or saga: re-read and decide again
            order = self.store.get(pk, ORDER_SK)
            return (200 if order["status"] in d.FINAL_STATUSES else 202), self.order_detail(order)
        raise ApiError(409, "CONCURRENT_UPDATE", "order is changing; retry the cancellation")

    # ------------------------------------------------------------------ admin
    def list_orders_admin(self, caller):
        self._require_admin(caller)
        orders = [{**self.order_view(o), "ownerId": o.get("ownerId")} for o in self.store.scan("ORDER#", ORDER_SK)]
        orders.sort(key=lambda o: o.get("createdAt", ""), reverse=True)
        return 200, {"orders": orders}

    def list_audit(self, caller, order_id):
        self._require_admin(caller)
        if not self.store.get(order_pk(order_id), ORDER_SK):
            raise ApiError(404, "NOT_FOUND", "order not found")
        entries = [{k: v for k, v in e.items() if k not in ("PK", "entity")}
                   for e in self.store.query(order_pk(order_id), "AUDIT#")]
        return 200, {"orderId": order_id, "audit": entries}

    def provider_state(self, caller):
        self._require_admin(caller)
        providers = build_providers(self.store, self.clock)
        return 200, {name: p.records() for name, p in providers.items()}

    def provider_stats(self, order_id):
        return provider_stats(self.store, order_id)

    @staticmethod
    def scenarios():
        return 200, {"scenarios": [{"id": k, "label": v["label"]} for k, v in SCENARIOS.items()]}

    # ------------------------------------------------------------------ scheduler + outbox
    def poll_due_work(self, limit=25):
        """Claim due ACCEPTED orders with a conditional update, bump generation, write outbox event."""
        now = self.now_iso()
        claimed = []
        for item in self.store.query_index(INDEX_DUE, max_sk=f"{now}#~", limit=limit):
            order_id, gen = item["orderId"], item["generation"]
            if item.get("status") != d.ACCEPTED or item.get("cancelRequested"):
                continue
            d.assert_transition(d.ACCEPTED, d.PREPARING)
            try:
                self.store.transact([
                    op_update(order_pk(order_id), ORDER_SK,
                              {"status": d.PREPARING, "generation": gen + 1, "claimedAt": now, "updatedAt": now},
                              remove=["GSI1PK", "GSI1SK"],
                              conds=[("eq", "status", d.ACCEPTED), ("eq", "cancelRequested", False),
                                     ("eq", "generation", gen)]),
                    self._outbox_op(item, PREPARATION_DUE, gen + 1),
                    op_put(audit_item(order_id, now, "PREPARATION_CLAIMED", {"generation": gen + 1},
                                      actor="scheduler")),
                ])
            except ConditionFailed:
                continue  # someone else claimed or cancelled it
            claimed.append({"orderId": order_id, "generation": gen + 1})
        return claimed

    def drain_outbox(self, publisher=None, limit=50):
        """Publish pending outbox events. Entries that fail stay in the outbox for the next run."""
        publisher = publisher or self.publisher
        items = self.store.query_index(INDEX_OUTBOX, limit=limit)
        if not items or publisher is None:
            return {"published": 0, "failed": 0, "pending": len(items)}
        results = publisher.publish([i["event"] for i in items])
        published = failed = 0
        for item, result in zip(items, results):
            if result.get("ok"):
                published += 1
                self.store.update(item["PK"], item["SK"], {"status": "PUBLISHED", "publishedAt": self.now_iso()},
                                  remove=["GSI1PK", "GSI1SK"])
            else:
                failed += 1
                self.store.update(item["PK"], item["SK"], {"publishAttempts": item.get("publishAttempts", 0) + 1,
                                                           "lastError": str(result.get("error"))[:300]})
        return {"published": published, "failed": failed, "pending": failed}

    def _publish_best_effort(self):
        if self.publisher is None:
            return
        try:
            self.drain_outbox()
        except Exception:  # the scheduled poller will retry; acceptance is already durable
            pass

    def audit(self, order_id, event, detail=None, actor="system"):
        write_audit(self.store, order_id, self.now_iso(), event, detail, actor)
