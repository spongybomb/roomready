"""Local in-process stand-in for Step Functions + EventBridge.

AslInterpreter executes infra/statemachine.asl.json directly (subset: Task, Choice, Pass,
Succeed, Fail with Parameters, ResultPath, InputPath/OutputPath, Retry and Catch) and
dispatches Task states to the SAME saga.dispatch() used by the Lambda step handler.

LocalRuntime wires store, clock, providers, service, an EventBridge-like router and a
starter that deduplicates executions by the same deterministic name used on AWS.
"""
from __future__ import annotations

import copy
import json
import threading
from datetime import timedelta
from pathlib import Path

from . import saga
from .domain import ManualClock, from_iso, to_iso
from .providers import build_providers
from .service import CANCELLATION_REQUESTED, PREPARATION_DUE, Service, execution_request
from .store import INDEX_DUE, MemoryStore, ORDER_SK, order_pk

ASL_PATH = Path(__file__).resolve().parent.parent / "infra" / "statemachine.asl.json"
LOCAL_STEP_ARN = "arn:local:lambda:roomready-step-handler"
SUBSTITUTIONS = {"StepHandlerArn": LOCAL_STEP_ARN}


def load_definition(path=ASL_PATH, substitutions=None):
    text = Path(path).read_text(encoding="utf-8")
    for key, value in (substitutions or SUBSTITUTIONS).items():
        text = text.replace("${" + key + "}", value)
    return json.loads(text)


# --------------------------------------------------------------------------- JSONPath subset
def get_path(data, path):
    if path is None:
        return {}
    if path == "$":
        return data
    if not path.startswith("$."):
        raise ValueError(f"unsupported path {path}")
    cur = data
    for part in path[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(path)
        cur = cur[part]
    return cur


def set_path(data, path, value):
    if path is None:
        return data
    if path == "$":
        return value
    out = copy.deepcopy(data) if isinstance(data, dict) else {}
    cur = out
    parts = path[2:].split(".")
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value
    return out


def resolve_parameters(template, data):
    if isinstance(template, dict):
        out = {}
        for k, v in template.items():
            if k.endswith(".$"):
                out[k[:-2]] = copy.deepcopy(get_path(data, v))
            else:
                out[k] = resolve_parameters(v, data)
        return out
    if isinstance(template, list):
        return [resolve_parameters(v, data) for v in template]
    return template


def _rule_matches(rule, data):
    if "And" in rule:
        return all(_rule_matches(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_rule_matches(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not _rule_matches(rule["Not"], data)
    try:
        value = get_path(data, rule["Variable"])
        present = True
    except KeyError:
        value, present = None, False
    if "IsPresent" in rule:
        return present == rule["IsPresent"]
    if not present:
        return False
    for op in ("StringEquals", "BooleanEquals", "NumericEquals"):
        if op in rule:
            return value == rule[op]
    raise ValueError(f"unsupported choice rule {rule}")


class ExecutionError(Exception):
    def __init__(self, error, cause):
        super().__init__(f"{error}: {cause}")
        self.error, self.cause = error, cause


# --------------------------------------------------------------------------- interpreter
class AslInterpreter:
    def __init__(self, definition, invoke_task, on_enter=None, max_transitions=500):
        self.definition = definition
        self.invoke_task = invoke_task
        self.on_enter = on_enter
        self.max_transitions = max_transitions

    def run(self, execution_input):
        history = []
        data = copy.deepcopy(execution_input)
        name = self.definition["StartAt"]
        for _ in range(self.max_transitions):
            state = self.definition["States"][name]
            history.append({"type": "StateEntered", "state": name})
            if self.on_enter:
                self.on_enter(name, data)
            kind = state["Type"]
            try:
                if kind == "Succeed":
                    out = get_path(get_path(data, state.get("InputPath", "$")), state.get("OutputPath", "$"))
                    history.append({"type": "ExecutionSucceeded", "state": name})
                    return {"status": "SUCCEEDED", "output": out, "history": history, "endState": name}
                if kind == "Fail":
                    history.append({"type": "ExecutionFailed", "state": name, "error": state.get("Error")})
                    return {"status": "FAILED", "error": state.get("Error"), "cause": state.get("Cause"),
                            "output": data, "history": history, "endState": name}
                if kind == "Choice":
                    nxt = next((c["Next"] for c in state["Choices"] if _rule_matches(c, data)), state.get("Default"))
                    if nxt is None:
                        raise ExecutionError("States.NoChoiceMatched", name)
                    history.append({"type": "ChoiceTaken", "state": name, "next": nxt})
                    name = nxt
                    continue
                if kind == "Pass":
                    effective = get_path(data, state.get("InputPath", "$"))
                    result = state["Result"] if "Result" in state else (
                        resolve_parameters(state["Parameters"], effective) if "Parameters" in state else effective)
                    data = get_path(set_path(data, state.get("ResultPath", "$"), result), state.get("OutputPath", "$"))
                elif kind == "Task":
                    data = self._run_task(name, state, data, history)
                    if isinstance(data, _Caught):
                        name, data = data.next, data.data
                        continue
                else:
                    raise ExecutionError("States.Runtime", f"unsupported state type {kind}")
            except ExecutionError as exc:
                history.append({"type": "ExecutionFailed", "state": name, "error": exc.error})
                return {"status": "FAILED", "error": exc.error, "cause": exc.cause, "output": data,
                        "history": history, "endState": name}
            if state.get("End"):
                history.append({"type": "ExecutionSucceeded", "state": name})
                return {"status": "SUCCEEDED", "output": data, "history": history, "endState": name}
            name = state["Next"]
        raise ExecutionError("States.Runtime", "too many transitions")

    def _run_task(self, name, state, data, history):
        effective = get_path(data, state.get("InputPath", "$"))
        params = resolve_parameters(state["Parameters"], effective) if "Parameters" in state else effective
        retry_counts = [0] * len(state.get("Retry", []))
        while True:
            try:
                result = self.invoke_task(state["Resource"], params)
                history.append({"type": "TaskSucceeded", "state": name, "outcome": (result or {}).get("outcome")
                                if isinstance(result, dict) else None})
                merged = set_path(data, state.get("ResultPath", "$"), result)
                return get_path(merged, state.get("OutputPath", "$"))
            except Exception as exc:
                error = getattr(exc, "error_name", type(exc).__name__)
                history.append({"type": "TaskFailed", "state": name, "error": error, "cause": str(exc)})
                retried = False
                for i, retrier in enumerate(state.get("Retry", [])):
                    if error in retrier["ErrorEquals"] or "States.ALL" in retrier["ErrorEquals"]:
                        if retry_counts[i] < retrier.get("MaxAttempts", 3):
                            retry_counts[i] += 1
                            delay = retrier.get("IntervalSeconds", 1) * retrier.get("BackoffRate", 2.0) ** (retry_counts[i] - 1)
                            history.append({"type": "TaskRetry", "state": name, "attempt": retry_counts[i],
                                            "simulatedDelaySeconds": delay})
                            retried = True
                        break  # first matching retrier decides (as in Step Functions)
                if retried:
                    continue
                for catcher in state.get("Catch", []):
                    if error in catcher["ErrorEquals"] or "States.ALL" in catcher["ErrorEquals"]:
                        history.append({"type": "CatchTaken", "state": name, "error": error, "next": catcher["Next"]})
                        caught = set_path(data, catcher.get("ResultPath", "$"), {"Error": error, "Cause": str(exc)})
                        return _Caught(catcher["Next"], caught)
                raise ExecutionError(error, str(exc))


class _Caught:
    def __init__(self, next_state, data):
        self.next, self.data = next_state, data


# --------------------------------------------------------------------------- starter + router
class LocalStarter:
    """StartExecution with deterministic names; an existing name is treated as success."""

    def __init__(self, run_execution):
        self._run = run_execution
        self._lock = threading.Lock()
        self.executions = {}

    def start_execution(self, name, execution_input):
        with self._lock:
            if name in self.executions:
                return {"started": False, "alreadyExists": True, "name": name}
            self.executions[name] = {"name": name, "status": "RUNNING", "input": execution_input}
        result = self._run(execution_input)
        self.executions[name].update(status=result["status"], endState=result.get("endState"),
                                     history=result["history"], error=result.get("error"))
        return {"started": True, "name": name, "status": result["status"]}


class LocalEventRouter:
    """Simulates the EventBridge rule that routes domain events to the starter."""

    def __init__(self, starter, fail_next=0):
        self.starter = starter
        self.fail_next = fail_next  # deterministic PutEvents entry failures (for tests)
        self.published = []

    def publish(self, events):
        results = []
        for event in events:
            if self.fail_next > 0:
                self.fail_next -= 1
                results.append({"ok": False, "error": "SimulatedPutEventsFailure"})
                continue
            self.published.append(event)
            results.append({"ok": True})
            if event.get("type") in (PREPARATION_DUE, CANCELLATION_REQUESTED):
                name, sfn_input = execution_request(event)
                self.starter.start_execution(name, sfn_input)
        return results


# --------------------------------------------------------------------------- runtime
class LocalRuntime:
    def __init__(self, store=None, clock=None, admin_users=("admin",), asl_path=ASL_PATH):
        self.store = store if store is not None else MemoryStore()
        self.clock = clock or ManualClock()
        self.providers = build_providers(self.store, self.clock)
        self.ctx = saga.SagaContext(self.store, self.providers, self.clock)
        self.definition = load_definition(asl_path)
        self.starter = LocalStarter(self._run_execution)
        self.router = LocalEventRouter(self.starter)
        self.service = Service(self.store, self.clock, admin_users, publisher=self.router)
        self.lock = threading.RLock()

    def invoke_task(self, resource, params):
        if resource != LOCAL_STEP_ARN:
            raise ExecutionError("States.TaskFailed", f"unknown resource {resource}")
        return saga.dispatch(params, self.ctx)

    def _on_enter(self, state_name, data):
        """Scenario hook: simulate the traveller cancelling while a given state is running."""
        order_id = data.get("orderId") if isinstance(data, dict) else None
        if not order_id or data.get("mode") == "REVERSE":
            return
        order = self.store.get(order_pk(order_id), ORDER_SK) or {}
        if order.get("cancelDuringState") == state_name and not order.get("cancelRequested"):
            self.service.cancel_order(order["ownerId"], order_id)

    def _run_execution(self, execution_input):
        interpreter = AslInterpreter(self.definition, self.invoke_task, on_enter=self._on_enter)
        return interpreter.run(execution_input)

    def next_due_at(self):
        items = self.store.query_index(INDEX_DUE, limit=1)
        return items[0]["dueAt"] if items else None

    def tick(self, advance_hours=0.0, to_next_due=False):
        """One scheduler run (what the EventBridge 1-minute rule does on AWS)."""
        with self.lock:
            if advance_hours:
                self.clock.advance(timedelta(minutes=round(advance_hours * 60)))
            if to_next_due:
                due = self.next_due_at()
                if due and from_iso(due) > self.clock.now():
                    self.clock.set(from_iso(due))
            before = set(self.starter.executions)
            claimed = self.service.poll_due_work()
            outbox = self.service.drain_outbox()
            started = [n for n in self.starter.executions if n not in before]
            return {"now": to_iso(self.clock.now()), "claimed": claimed, "outbox": outbox,
                    "executions": [{"name": n, "status": self.starter.executions[n]["status"],
                                    "endState": self.starter.executions[n].get("endState")} for n in started]}

    def clock_info(self):
        return {"now": to_iso(self.clock.now()), "nextDueAt": self.next_due_at()}

    def execution_summaries(self):
        return [{"name": e["name"], "status": e["status"], "endState": e.get("endState"),
                 "states": [h["state"] for h in e.get("history", []) if h["type"] == "StateEntered"]}
                for e in self.starter.executions.values()]
