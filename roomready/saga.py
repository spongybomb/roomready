"""Saga step implementations shared by the local ASL interpreter and the Lambda step handler.

Every Task state in infra/statemachine.asl.json invokes ONE handler from STEP_HANDLERS with
{"step": <name>, "state": <execution state>} and receives the new execution state.

Invariants implemented here:
  * Stable logical action keys ORDER#{id}/ACTION#{name} (no order version) recorded in an
    action ledger (PENDING / SUCCEEDED / FAILED / UNKNOWN / NOT_APPLIED) and used as the
    provider idempotency key.
  * UNKNOWN/PENDING entries are reconciled via provider.get_status before any reissue.
  * cancelRequested is checked before and after every forward side effect.
  * Fencing: only the execution whose generation equals order.generation may cause side
    effects (ledger PENDING writes are conditional on the generation in one transaction).
  * Compensation runs in reverse order, only for SUCCEEDED (or reconciled) actions, each
    compensation independently; any unconfirmed compensation -> MANUAL_REVIEW.
"""
from __future__ import annotations

from dataclasses import dataclass

from . import domain as d
from .providers import AmbiguousTimeout, PermanentProviderError, TransientProviderError
from .store import ConditionFailed, ORDER_SK, op_check, op_update, order_pk, write_audit

FENCED = "FENCED"
CANCEL_REQUESTED = "CANCEL_REQUESTED"
OK = "OK"
MAX_COMPENSATION_ATTEMPTS = 3


class TransientError(Exception):
    """Retryable step failure. The class name is the Step Functions error name used in Retry."""


class PermanentStepError(Exception):
    """Non-retryable step failure; the state machine catches it and compensates."""


@dataclass
class SagaContext:
    store: object
    providers: dict
    clock: object

    def now(self) -> str:
        return d.to_iso(self.clock.now())


def action_key(order_id: str, action: str) -> str:
    return f"ORDER#{order_id}/ACTION#{action}"


# --------------------------------------------------------------------------- forward actions
def supply_list(profile: dict) -> list:
    """Personalised supply list derived from the profile snapshot (simulated catalogue)."""
    sizes = profile.get("sizes", {})
    brands = profile.get("preferredBrands", [])
    allergies = set(profile.get("allergyFlags", []))
    brand = brands[0] if brands else "house"
    items = [
        {"sku": "toiletry-kit", "brand": brand,
         "variant": "fragrance-free" if "fragrance" in allergies else "standard"},
        {"sku": "slippers", "size": sizes.get("slippers", "M")},
        {"sku": "bathrobe", "size": sizes.get("bathrobe", "M")},
        {"sku": "pillow", "variant": "hypoallergenic" if allergies else "standard"},
    ]
    if "nuts" not in allergies:
        items.append({"sku": "welcome-snack", "variant": "mixed-nuts"})
    return items


def _reserve(providers, key, order, faults):
    return providers["retailer"].reserve(key, {"items": supply_list(order.get("profileSnapshot", {}))}, faults)


def _book(providers, key, order, faults):
    return providers["courier"].book(key, {"hotelId": order["hotelId"], "deliverBy": order["checkInUtc"]}, faults)


def _setup(providers, key, order, faults):
    return providers["hotel"].setup(
        key, {"hotelId": order["hotelId"], "roomReservationRef": order["roomReservationRef"]}, faults)


FORWARD_ACTIONS = {
    "reserve_supplies": ("retailer", _reserve, ("reservationId", "paymentId", "amountCents")),
    "book_courier": ("courier", _book, ("bookingId",)),
    "setup_room": ("hotel", _setup, ("setupId",)),
}
FORWARD_ORDER = ["reserve_supplies", "book_courier", "setup_room"]


# --------------------------------------------------------------------------- compensations
# Each compensation reads provider state first (idempotent + reconciles ambiguous timeouts),
# then applies the right undo operation. Returns the method used.
def _undo_setup(providers, key, faults):
    hotel = providers["hotel"]
    if (hotel.get_status(key) or {}).get("setupStatus") == "CANCELLED":
        return "ALREADY_CANCELLED"
    hotel.cancel_setup(key, faults)
    return "CANCEL_SETUP"


def _undo_delivery(providers, key, faults):
    courier = providers["courier"]
    status = (courier.get_status(key) or {}).get("deliveryStatus")
    if status in ("CANCELLED", "RETURNED"):
        return "ALREADY_" + status
    if status == "DELIVERED":
        courier.return_pickup(key, faults)
        return "RETURN_PICKUP"
    courier.cancel(key, faults)
    return "CANCEL_BOOKING"


def _release_reservation(providers, key, faults):
    retailer = providers["retailer"]
    if (retailer.get_status(key) or {}).get("reservationStatus") == "RELEASED":
        return "ALREADY_RELEASED"
    retailer.release(key, faults)
    return "RELEASE"


def _reverse_payment(providers, key, faults):
    """Void an authorization; refund a captured payment. Never both."""
    retailer = providers["retailer"]
    status = (retailer.get_status(key) or {}).get("paymentStatus")
    if status in ("VOIDED", "REFUNDED"):
        return "ALREADY_" + status
    if status == "CAPTURED":
        retailer.refund(key, faults)
        return "REFUND"
    retailer.void(key, faults)
    return "VOID"


COMPENSATIONS = {
    "setup_room": [("cancel_setup", _undo_setup)],
    "book_courier": [("undo_delivery", _undo_delivery)],
    "reserve_supplies": [("release_reservation", _release_reservation), ("reverse_payment", _reverse_payment)],
}


# --------------------------------------------------------------------------- helpers
def _load(ctx, order_id):
    return ctx.store.get(order_pk(order_id), ORDER_SK)


def _ledger(ctx, order_id, action):
    return ctx.store.get(order_pk(order_id), f"ACTION#{action}")


def _set_ledger(ctx, order_id, action, **fields):
    fields["updatedAt"] = ctx.now()
    ctx.store.update(order_pk(order_id), f"ACTION#{action}", fields)


def _audit(ctx, order_id, event, **detail):
    write_audit(ctx.store, order_id, ctx.now(), event, detail, actor="saga")


def _out(state, outcome, step, **extra):
    new = dict(state)
    new.update(extra)
    new["outcome"] = outcome
    new["lastStep"] = step
    return new


def _forward_gate(order, generation):
    """None if this execution may proceed; otherwise FENCED or CANCEL_REQUESTED."""
    if order is None or order.get("generation") != generation or order.get("status") != d.PREPARING:
        return FENCED
    if order.get("cancelRequested"):
        return CANCEL_REQUESTED
    return None


def _gate_out(ctx, state, gate, step, order):
    if gate == FENCED and order is not None:
        _audit(ctx, state["orderId"], "STALE_EXECUTION_FENCED", step=step, executionGeneration=state["generation"],
               currentGeneration=order.get("generation"), status=order.get("status"))
    elif gate == CANCEL_REQUESTED:
        _audit(ctx, state["orderId"], "CANCEL_OBSERVED", step=step)
    return _out(state, gate, step)


def _owner_conds(generation, statuses):
    return [("eq", "generation", generation), ("in", "status", list(statuses))]


# --------------------------------------------------------------------------- steps
def begin_execution(state, ctx):
    order = _load(ctx, state["orderId"])
    expected = d.COMPENSATING if state.get("mode") == "REVERSE" else d.PREPARING
    if order is None or order.get("generation") != state["generation"] or order.get("status") != expected:
        return _gate_out(ctx, state, FENCED, "begin_execution", order)
    _audit(ctx, state["orderId"], "EXECUTION_STARTED", mode=state.get("mode", "FORWARD"),
           generation=state["generation"])
    return _out(state, OK, "begin_execution")


def _forward_step(name):
    provider_name, call, ref_fields = FORWARD_ACTIONS[name]

    def after_effect(state, ctx):
        order = _load(ctx, state["orderId"])
        gate = _forward_gate(order, state["generation"])
        return _gate_out(ctx, state, gate, name, order) if gate else _out(state, OK, name)

    def step(state, ctx):
        oid, gen = state["orderId"], state["generation"]
        order = _load(ctx, oid)
        gate = _forward_gate(order, gen)
        if gate:  # check BEFORE the side effect
            return _gate_out(ctx, state, gate, name, order)
        provider = ctx.providers[provider_name]
        key = action_key(oid, name)
        faults = order.get("faults") or {}
        entry = _ledger(ctx, oid, name)

        if entry and entry["status"] == d.SUCCEEDED:
            return after_effect(state, ctx)
        if entry and entry["status"] == d.FAILED:
            raise PermanentStepError(f"{name} already failed permanently")
        if entry and entry["status"] in (d.PENDING, d.UNKNOWN):
            found = provider.get_status(key)  # reconcile before any reissue
            if found:
                _set_ledger(ctx, oid, name, status=d.SUCCEEDED, reconciled=True,
                            providerRef={f: found.get(f) for f in ref_fields})
                _audit(ctx, oid, "ACTION_RECONCILED", action=name, result="effect found at provider; not reissued")
                return after_effect(state, ctx)
            _audit(ctx, oid, "ACTION_RECONCILED", action=name, result="no effect at provider; reissuing same key")

        attempts = (entry or {}).get("attempts", 0) + 1
        try:  # fencing: the PENDING write only succeeds for the current owner generation
            ctx.store.transact([
                op_check(order_pk(oid), ORDER_SK,
                         _owner_conds(gen, [d.PREPARING]) + [("eq", "cancelRequested", False)]),
                op_update(order_pk(oid), f"ACTION#{name}", {
                    "entity": "ACTION", "action": name, "provider": provider_name, "idempotencyKey": key,
                    "status": d.PENDING, "attempts": attempts, "generation": gen, "updatedAt": ctx.now()}),
            ])
        except ConditionFailed:
            order = _load(ctx, oid)
            return _gate_out(ctx, state, _forward_gate(order, gen) or FENCED, name, order)

        try:
            result = call(ctx.providers, key, order, faults)
        except AmbiguousTimeout as exc:
            _set_ledger(ctx, oid, name, status=d.UNKNOWN, lastError=str(exc))
            _audit(ctx, oid, "ACTION_UNKNOWN", action=name, attempt=attempts, error=str(exc))
            raise TransientError(str(exc))
        except TransientProviderError as exc:
            _set_ledger(ctx, oid, name, status=d.PENDING, lastError=str(exc))
            _audit(ctx, oid, "ACTION_RETRYABLE_ERROR", action=name, attempt=attempts, error=str(exc))
            raise TransientError(str(exc))
        except PermanentProviderError as exc:
            _set_ledger(ctx, oid, name, status=d.FAILED, lastError=str(exc))
            _audit(ctx, oid, "ACTION_FAILED", action=name, attempt=attempts, error=str(exc))
            raise PermanentStepError(str(exc))

        _set_ledger(ctx, oid, name, status=d.SUCCEEDED, reconciled=False,
                    providerRef={f: result.get(f) for f in ref_fields})
        _audit(ctx, oid, "ACTION_SUCCEEDED", action=name, attempt=attempts,
               providerRef={f: result.get(f) for f in ref_fields})
        return after_effect(state, ctx)  # check AFTER the side effect

    step.__name__ = name
    return step


def mark_ready(state, ctx):
    oid, gen = state["orderId"], state["generation"]
    order = _load(ctx, oid)
    gate = _forward_gate(order, gen)
    if gate:
        return _gate_out(ctx, state, gate, "mark_ready", order)
    for name in FORWARD_ORDER:
        entry = _ledger(ctx, oid, name)
        if not entry or entry["status"] != d.SUCCEEDED:
            raise PermanentStepError(f"cannot mark READY: {name} not confirmed")
    d.assert_transition(d.PREPARING, d.READY)
    try:
        ctx.store.update(order_pk(oid), ORDER_SK, {"status": d.READY, "readyAt": ctx.now(), "updatedAt": ctx.now()},
                         conds=_owner_conds(gen, [d.PREPARING]) + [("eq", "cancelRequested", False)])
    except ConditionFailed:
        order = _load(ctx, oid)
        return _gate_out(ctx, state, _forward_gate(order, gen) or FENCED, "mark_ready", order)
    _audit(ctx, oid, "STATUS_CHANGED", status=d.READY)
    return _out(state, OK, "mark_ready")


def begin_compensation(state, ctx):
    oid, gen = state["orderId"], state["generation"]
    error = state.get("error")
    state = {k: v for k, v in state.items() if k != "error"}
    order = _load(ctx, oid)
    if order is None or order.get("generation") != gen or order.get("status") not in (d.PREPARING, d.COMPENSATING):
        return _gate_out(ctx, state, FENCED, "begin_compensation", order)
    reason = CANCEL_REQUESTED if order.get("cancelRequested") else "STEP_FAILED"
    fields = {"status": d.COMPENSATING, "compensationReason": reason, "updatedAt": ctx.now()}
    if error:
        fields["failureReason"] = f"{error.get('Error')}: {str(error.get('Cause', ''))[:300]}"
    try:
        ctx.store.update(order_pk(oid), ORDER_SK, fields, conds=_owner_conds(gen, [d.PREPARING, d.COMPENSATING]))
    except ConditionFailed:
        return _gate_out(ctx, state, FENCED, "begin_compensation", _load(ctx, oid))
    if order["status"] != d.COMPENSATING:
        _audit(ctx, oid, "STATUS_CHANGED", status=d.COMPENSATING, reason=reason,
               failure=fields.get("failureReason"))
    _audit(ctx, oid, "COMPENSATION_STARTED", reason=reason)
    return _out(state, OK, "begin_compensation")


def _run_compensation(ctx, order, gen, action, slot, fn, key):
    oid = order["orderId"]
    sk = f"COMP#{slot}"
    rec = ctx.store.get(order_pk(oid), sk) or {}
    if rec.get("status") == d.SUCCEEDED:
        return
    attempts = rec.get("attempts", 0)
    try:  # fencing for compensation side effects
        ctx.store.transact([
            op_check(order_pk(oid), ORDER_SK, _owner_conds(gen, [d.COMPENSATING])),
            op_update(order_pk(oid), sk, {"entity": "COMPENSATION", "slot": slot, "action": action,
                                          "status": d.PENDING, "generation": gen, "updatedAt": ctx.now()}),
        ])
    except ConditionFailed:
        return
    faults = order.get("faults") or {}
    last_error = None
    for _ in range(MAX_COMPENSATION_ATTEMPTS):
        attempts += 1
        try:
            method = fn(ctx.providers, key, faults)
        except (TransientProviderError, AmbiguousTimeout) as exc:
            last_error = str(exc)
            _audit(ctx, oid, "COMPENSATION_RETRY", compensation=slot, attempt=attempts, error=last_error)
            continue
        except Exception as exc:  # permanent rejection or unexpected error: record, continue with others
            last_error = f"{type(exc).__name__}: {exc}"
            break
        ctx.store.update(order_pk(oid), sk, {"status": d.SUCCEEDED, "method": method, "attempts": attempts,
                                             "updatedAt": ctx.now()})
        _audit(ctx, oid, "COMPENSATION_SUCCEEDED", compensation=slot, action=action, method=method)
        return
    ctx.store.update(order_pk(oid), sk, {"status": d.FAILED, "attempts": attempts, "lastError": last_error,
                                         "updatedAt": ctx.now()})
    _audit(ctx, oid, "COMPENSATION_FAILED", compensation=slot, action=action, error=last_error)


def _compensate_step(name):
    provider_name = FORWARD_ACTIONS[name][0]
    ref_fields = FORWARD_ACTIONS[name][2]
    step_name = f"compensate_{name}"

    def step(state, ctx):
        oid, gen = state["orderId"], state["generation"]
        order = _load(ctx, oid)
        if order is None or order.get("generation") != gen or order.get("status") != d.COMPENSATING:
            return _gate_out(ctx, state, FENCED, step_name, order)
        entry = _ledger(ctx, oid, name)
        key = action_key(oid, name)
        if entry is None or entry["status"] in (d.FAILED, d.NOT_APPLIED):
            return _out(state, OK, step_name)  # nothing happened -> nothing to undo
        if entry["status"] in (d.PENDING, d.UNKNOWN):
            found = ctx.providers[provider_name].get_status(key)
            if not found:
                _set_ledger(ctx, oid, name, status=d.NOT_APPLIED)
                _audit(ctx, oid, "ACTION_RECONCILED", action=name, result="no effect at provider; nothing to undo")
                return _out(state, OK, step_name)
            _set_ledger(ctx, oid, name, status=d.SUCCEEDED, reconciled=True,
                        providerRef={f: found.get(f) for f in ref_fields})
            _audit(ctx, oid, "ACTION_RECONCILED", action=name, result="effect found at provider; will compensate")
        for slot, fn in COMPENSATIONS[name]:  # independent: a failure does not stop the next one
            _run_compensation(ctx, order, gen, name, slot, fn, key)
        return _out(state, OK, step_name)

    step.__name__ = step_name
    return step


def unresolved_effects(ctx, order_id):
    problems = []
    for name in FORWARD_ORDER:
        entry = _ledger(ctx, order_id, name)
        if entry is None or entry["status"] in (d.FAILED, d.NOT_APPLIED):
            continue
        if entry["status"] in (d.PENDING, d.UNKNOWN):
            problems.append(f"{name}: provider effect not reconciled")
            continue
        for slot, _ in COMPENSATIONS[name]:
            rec = ctx.store.get(order_pk(order_id), f"COMP#{slot}")
            if not rec or rec.get("status") != d.SUCCEEDED:
                problems.append(f"{slot}: compensation not confirmed")
    return problems


def finalize_compensation(state, ctx):
    oid, gen = state["orderId"], state["generation"]
    for _ in range(3):
        order = _load(ctx, oid)
        if order is None or order.get("generation") != gen or order.get("status") != d.COMPENSATING:
            return _gate_out(ctx, state, FENCED, "finalize_compensation", order)
        problems = unresolved_effects(ctx, oid)
        if problems:
            final = d.MANUAL_REVIEW  # never CANCELLED while an effect may still exist
        else:
            final = d.CANCELLED if order.get("cancelRequested") else d.PREP_FAILED
        d.assert_transition(d.COMPENSATING, final)
        try:
            ctx.store.update(order_pk(oid), ORDER_SK,
                             {"status": final, "unresolved": problems, "finalizedAt": ctx.now(), "updatedAt": ctx.now()},
                             conds=_owner_conds(gen, [d.COMPENSATING]) +
                             [("eq", "cancelRequested", bool(order.get("cancelRequested")))])
        except ConditionFailed:
            continue  # e.g. cancel arrived meanwhile: re-evaluate
        _audit(ctx, oid, "STATUS_CHANGED", status=final, unresolved=problems)
        return _out(state, OK, "finalize_compensation", finalStatus=final)
    raise TransientError("could not finalize compensation (concurrent updates)")


STEP_HANDLERS = {
    "begin_execution": begin_execution,
    "reserve_supplies": _forward_step("reserve_supplies"),
    "book_courier": _forward_step("book_courier"),
    "setup_room": _forward_step("setup_room"),
    "mark_ready": mark_ready,
    "begin_compensation": begin_compensation,
    "compensate_setup_room": _compensate_step("setup_room"),
    "compensate_book_courier": _compensate_step("book_courier"),
    "compensate_reserve_supplies": _compensate_step("reserve_supplies"),
    "finalize_compensation": finalize_compensation,
}


def dispatch(event, ctx):
    """Entry point used by both the local interpreter and the Lambda step handler."""
    name = event.get("step")
    if name not in STEP_HANDLERS:
        raise PermanentStepError(f"unknown step {name!r}")
    return STEP_HANDLERS[name](dict(event.get("state") or {}), ctx)
