import json
import unittest

from roomready import aws_handlers, demo
from roomready import domain as d
from roomready.store import MemoryStore


class FakeEvents:
    def __init__(self, fail_indexes=()):
        self.fail_indexes = set(fail_indexes)
        self.entries = []

    def put_events(self, Entries):
        out = []
        for i, e in enumerate(Entries):
            self.entries.append(e)
            out.append({"ErrorCode": "InternalFailure", "ErrorMessage": "x"} if i in self.fail_indexes
                       else {"EventId": f"ev-{len(self.entries)}"})
        return {"FailedEntryCount": len(self.fail_indexes), "Entries": out}


class AlreadyExists(Exception):
    response = {"Error": {"Code": "ExecutionAlreadyExists"}}


class FakeSfn:
    def __init__(self):
        self.names = set()

    def start_execution(self, stateMachineArn, name, input):
        if name in self.names:
            raise AlreadyExists("exists")
        self.names.add(name)
        return {"executionArn": f"{stateMachineArn}:{name}"}


def make_runtime(events=None, env=None):
    env = {"TABLE_NAME": "t", "EVENT_BUS_NAME": "bus", "STATE_MACHINE_ARN": "arn:sm", "ADMIN_USERS": "admin",
           **(env or {})}
    rt = aws_handlers.AwsRuntime(store=MemoryStore(), clock=d.ManualClock(demo.DEMO_START),
                                 publisher=aws_handlers.EventBridgePublisher("bus", client=events or FakeEvents()),
                                 sfn_client=FakeSfn(), env=env)
    aws_handlers.set_runtime(rt)
    return rt


def http_event(method, path, body=None, iam_user=None, headers=None):
    ctx = {"http": {"method": method, "path": path}, "stage": "$default"}
    if iam_user:
        ctx["authorizer"] = {"iam": {"userArn": iam_user}}
    return {"rawPath": path, "requestContext": ctx, "headers": headers or {},
            "body": json.dumps(body) if body is not None else None, "isBase64Encoded": False}


class AwsHandlerTests(unittest.TestCase):
    def tearDown(self):
        aws_handlers.set_runtime(None)

    def test_api_uses_iam_identity(self):
        make_runtime()
        user = "arn:aws:iam::123456789012:user/alice"
        resp = aws_handlers.api_handler(http_event("POST", "/profile", demo.PROFILE, iam_user=user), None)
        self.assertEqual(resp["statusCode"], 200)
        self.assertEqual(json.loads(resp["body"])["userId"], user)

    def test_header_identity_only_when_enabled(self):
        make_runtime()
        ev = http_event("GET", "/orders", headers={"x-demo-user": "alice"})
        self.assertEqual(aws_handlers.api_handler(ev, None)["statusCode"], 401)
        make_runtime(env={"DEMO_ALLOW_HEADER_IDENTITY": "true"})
        self.assertEqual(aws_handlers.api_handler(ev, None)["statusCode"], 200)

    def test_tick_is_local_only(self):
        make_runtime(env={"DEMO_ALLOW_HEADER_IDENTITY": "true"})
        ev = http_event("POST", "/admin/tick", {}, headers={"X-Demo-User": "admin"})
        self.assertEqual(aws_handlers.api_handler(ev, None)["statusCode"], 404)

    def test_poller_publishes_and_keeps_failed_entries(self):
        events = FakeEvents(fail_indexes={0})
        rt = make_runtime(events)
        rt.service.put_profile("alice", demo.PROFILE)
        rt.service.create_order("alice", "k1", demo.order_body())
        rt.clock.set(d.from_iso("2026-11-16T07:00:00Z"))
        result = aws_handlers.poller_handler({}, None)
        self.assertEqual(len(result["claimed"]), 1)
        self.assertEqual(result["outbox"]["failed"], 1)
        self.assertEqual(len(rt.store.query_index("OUTBOX")), 1)
        events.fail_indexes = set()
        result = aws_handlers.poller_handler({}, None)
        self.assertEqual(result["outbox"]["published"], 1)
        detail = json.loads(events.entries[-1]["Detail"])
        self.assertEqual(events.entries[-1]["DetailType"], "PreparationDue")
        self.assertEqual(set(detail), {"eventId", "type", "orderId", "generation", "timestamp", "correlationId"})

    def test_starter_uses_deterministic_name_and_tolerates_duplicates(self):
        make_runtime()
        event = {"detail": {"type": "PreparationDue", "orderId": "abc", "generation": 1, "eventId": "e"}}
        first = aws_handlers.starter_handler(event, None)
        second = aws_handlers.starter_handler(event, None)
        self.assertEqual(first["name"], "abc-g1")
        self.assertTrue(first["started"])
        self.assertEqual(second, {"started": False, "alreadyExists": True, "name": "abc-g1"})

    def test_step_handler_dispatches(self):
        make_runtime()
        out = aws_handlers.step_handler({"step": "begin_execution",
                                         "state": {"orderId": "missing", "generation": 1}}, None)
        self.assertEqual(out["outcome"], "FENCED")


if __name__ == "__main__":
    unittest.main()
