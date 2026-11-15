"""HTTP routing shared by the local server and the API Gateway Lambda handler."""
from __future__ import annotations

import json
import logging
import re

from .domain import ValidationError
from .service import ApiError

log = logging.getLogger("roomready.api")

_ID = r"([A-Za-z0-9_-]{1,64})"
ROUTES = [
    ("GET", r"^/scenarios$", "scenarios"),
    ("GET", r"^/profile$", "get_profile"),
    ("POST", r"^/profile$", "put_profile"),
    ("GET", r"^/orders$", "list_orders"),
    ("POST", r"^/orders$", "create_order"),
    ("GET", rf"^/orders/{_ID}$", "get_order"),
    ("POST", rf"^/orders/{_ID}/cancel$", "cancel_order"),
    ("GET", r"^/admin/orders$", "admin_orders"),
    ("GET", rf"^/admin/orders/{_ID}/audit$", "admin_audit"),
    ("GET", r"^/admin/providers$", "admin_providers"),
    ("POST", r"^/admin/tick$", "admin_tick"),     # local only
    ("GET", r"^/admin/clock$", "admin_clock"),    # local only
]


def header(headers, name):
    for k, v in (headers or {}).items():
        if k.lower() == name.lower():
            return v
    return None


def handle_request(service, method, path, caller, headers, body_text, local=None):
    """Returns (status_code, json_payload). `local` is the LocalRuntime (tick/clock) or None on AWS."""
    method = method.upper()
    path = path.rstrip("/") or "/"
    try:
        for route_method, pattern, name in ROUTES:
            match = re.match(pattern, path)
            if match and route_method == method:
                body = _parse_body(body_text) if method == "POST" else {}
                return _dispatch(service, name, match.groups(), caller, headers, body, local)
        if any(re.match(p, path) for _, p, _ in ROUTES):
            return 405, {"error": "METHOD_NOT_ALLOWED", "message": f"{method} not allowed on {path}"}
        return 404, {"error": "NOT_FOUND", "message": f"no route for {method} {path}"}
    except ApiError as exc:
        return exc.status, {"error": exc.code, "message": exc.message}
    except ValidationError as exc:
        return 400, {"error": "VALIDATION_ERROR", "message": str(exc), "field": exc.field}
    except Exception:  # never leak internals
        log.exception("unhandled error")
        return 500, {"error": "INTERNAL_ERROR", "message": "unexpected server error"}


def _parse_body(text):
    if not text:
        return {}
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        raise ApiError(400, "INVALID_JSON", "request body must be valid JSON")


def _dispatch(svc, name, args, caller, headers, body, local):
    if name == "scenarios":
        return svc.scenarios()
    if name == "get_profile":
        return svc.get_profile(caller)
    if name == "put_profile":
        return svc.put_profile(caller, body)
    if name == "list_orders":
        return svc.list_my_orders(caller)
    if name == "create_order":
        return svc.create_order(caller, header(headers, "Idempotency-Key"), body)
    if name == "get_order":
        return svc.get_order(caller, args[0])
    if name == "cancel_order":
        return svc.cancel_order(caller, args[0])
    if name == "admin_orders":
        return svc.list_orders_admin(caller)
    if name == "admin_audit":
        return svc.list_audit(caller, args[0])
    if name == "admin_providers":
        status, payload = svc.provider_state(caller)
        if local is not None:
            payload = {**payload, "executions": local.execution_summaries()}
        return status, payload
    if name in ("admin_tick", "admin_clock"):
        svc._require_admin(caller)
        if local is None:
            raise ApiError(404, "NOT_AVAILABLE", "on AWS the EventBridge schedule runs the poller every minute")
        if name == "admin_clock":
            return 200, local.clock_info()
        return 200, local.tick(advance_hours=float(body.get("advanceHours", 0) or 0),
                               to_next_due=bool(body.get("fastForwardToDue", False)))
    raise ApiError(404, "NOT_FOUND", "unknown route")
