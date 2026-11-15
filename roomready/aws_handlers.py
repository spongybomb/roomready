"""AWS Lambda entry points (API, scheduled poller, execution starter, saga step handler).

Environment variables:
  TABLE_NAME, EVENT_BUS_NAME, STATE_MACHINE_ARN (starter), ADMIN_USERS (comma-separated
  caller ids), DEMO_ALLOW_HEADER_IDENTITY ("true" enables the X-Demo-User fallback - demo only).
boto3 is imported lazily; it is provided by the Lambda Python runtime.
"""
from __future__ import annotations

import base64
import json
import logging
import os

from . import api, saga
from .domain import SystemClock
from .providers import build_providers
from .service import EVENT_SOURCE, Service, execution_request
from .store import DynamoStore, _error_code

log = logging.getLogger()
log.setLevel(logging.INFO)


class EventBridgePublisher:
    """PutEvents in batches of 10; reports per-entry success so failures stay in the outbox."""

    def __init__(self, bus_name, client=None):
        self.bus_name = bus_name
        self._client = client

    @property
    def client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("events")
        return self._client

    def publish(self, events):
        results = []
        for i in range(0, len(events), 10):
            batch = events[i:i + 10]
            entries = [{"Source": EVENT_SOURCE, "DetailType": e["type"], "Detail": json.dumps(e),
                        "EventBusName": self.bus_name} for e in batch]
            try:
                resp = self.client.put_events(Entries=entries)
            except Exception as exc:  # whole call failed: every entry stays pending
                results.extend({"ok": False, "error": f"{type(exc).__name__}: {exc}"} for _ in batch)
                continue
            out = resp.get("Entries", [])
            if resp.get("FailedEntryCount", 0):
                log.warning(json.dumps({"putEventsFailedEntryCount": resp["FailedEntryCount"]}))
            for j in range(len(batch)):  # Entries are positional: entry j answers batch[j]
                entry = out[j] if j < len(out) else {"ErrorCode": "MissingEntry"}
                if entry.get("ErrorCode"):
                    results.append({"ok": False, "error": f"{entry['ErrorCode']}: {entry.get('ErrorMessage', '')}"})
                else:
                    results.append({"ok": True, "eventId": entry.get("EventId")})
        return results


class AwsRuntime:
    def __init__(self, store=None, clock=None, publisher=None, sfn_client=None, env=None):
        env = env if env is not None else os.environ
        self.env = env
        self.store = store or DynamoStore(env["TABLE_NAME"])
        self.clock = clock or SystemClock()
        self.providers = build_providers(self.store, self.clock)
        self.ctx = saga.SagaContext(self.store, self.providers, self.clock)
        self.publisher = publisher or EventBridgePublisher(env.get("EVENT_BUS_NAME", "default"))
        admins = [a.strip() for a in env.get("ADMIN_USERS", "").split(",") if a.strip()]
        self.service = Service(self.store, self.clock, admins, publisher=self.publisher)
        self._sfn = sfn_client

    @property
    def sfn(self):
        if self._sfn is None:
            import boto3
            self._sfn = boto3.client("stepfunctions")
        return self._sfn


_RUNTIME = None


def get_runtime():
    global _RUNTIME
    if _RUNTIME is None:
        _RUNTIME = AwsRuntime()
    return _RUNTIME


def set_runtime(runtime):
    """Test hook."""
    global _RUNTIME
    _RUNTIME = runtime


def resolve_identity(event, env=None):
    env = env if env is not None else os.environ
    ctx = event.get("requestContext") or {}
    iam = (ctx.get("authorizer") or {}).get("iam") or {}
    caller = iam.get("userArn") or iam.get("userId")
    if caller:
        return caller
    if env.get("DEMO_ALLOW_HEADER_IDENTITY", "false").lower() == "true":
        return api.header(event.get("headers"), "X-Demo-User")
    return None


def api_handler(event, context):
    rt = get_runtime()
    http = (event.get("requestContext") or {}).get("http") or {}
    method = http.get("method", "GET")
    path = event.get("rawPath") or http.get("path") or "/"
    stage = (event.get("requestContext") or {}).get("stage")
    if stage and stage != "$default" and path.startswith(f"/{stage}/"):
        path = path[len(stage) + 1:]
    body = event.get("body") or ""
    if body and event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    caller = resolve_identity(event, rt.env)
    status, payload = api.handle_request(rt.service, method, path, caller, event.get("headers") or {}, body)
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(payload)}


def poller_handler(event, context):
    """EventBridge schedule (rate 1 minute): claim due work, then publish the outbox."""
    rt = get_runtime()
    claimed = rt.service.poll_due_work()
    outbox = rt.service.drain_outbox(rt.publisher)
    log.info(json.dumps({"claimed": len(claimed), "outbox": outbox}))
    return {"claimed": claimed, "outbox": outbox}


def starter_handler(event, context):
    """EventBridge rule target: start the saga with a deterministic execution name."""
    rt = get_runtime()
    detail = event.get("detail") or {}
    if isinstance(detail, str):
        detail = json.loads(detail)
    name, sfn_input = execution_request(detail)
    try:
        resp = rt.sfn.start_execution(stateMachineArn=rt.env["STATE_MACHINE_ARN"], name=name,
                                      input=json.dumps(sfn_input))
    except Exception as exc:
        if _error_code(exc) == "ExecutionAlreadyExists" or type(exc).__name__ == "ExecutionAlreadyExists":
            log.info(json.dumps({"execution": name, "result": "already exists (treated as success)"}))
            return {"started": False, "alreadyExists": True, "name": name}
        raise
    return {"started": True, "name": name, "executionArn": resp.get("executionArn")}


def step_handler(event, context):
    """Step Functions Task target. Raised TransientError/PermanentStepError names drive Retry/Catch."""
    return saga.dispatch(event, get_runtime().ctx)
