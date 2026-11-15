import json
import threading
import unittest
import urllib.error
import urllib.request

from roomready import demo
from roomready.server import make_server


class HttpSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = demo.new_runtime()
        cls.server = make_server(cls.runtime, "127.0.0.1", 0)
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def request(self, method, path, body=None, user="alice", headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if user:
            req.add_header("X-Demo-User", user)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if resp.headers.get_content_type() == "application/json" else raw)
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read())

    def test_ui_is_served(self):
        for path in ("/", "/app.js", "/style.css"):
            status, body = self.request("GET", path)
            self.assertEqual(status, 200)
        self.assertIn(b"ROOMREADY", self.request("GET", "/")[1])

    def test_end_to_end_over_http(self):
        status, _ = self.request("POST", "/profile", demo.PROFILE)
        self.assertEqual(status, 200)
        status, created = self.request("POST", "/orders", demo.order_body(), headers={"Idempotency-Key": "http-1"})
        self.assertEqual(status, 201)
        oid = created["orderId"]
        self.assertEqual(self.request("POST", "/orders", demo.order_body(), headers={"Idempotency-Key": "http-1"})[0], 200)
        self.assertEqual(self.request("POST", "/orders", demo.order_body(flight="CX1"),
                                      headers={"Idempotency-Key": "http-1"})[0], 409)
        self.assertEqual(self.request("POST", "/orders", demo.order_body())[0], 400)  # missing key
        self.assertEqual(self.request("GET", f"/orders/{oid}", user="bob")[0], 404)
        self.assertEqual(self.request("GET", f"/orders/{oid}", user=None)[0], 401)
        self.assertEqual(self.request("POST", "/admin/tick", {"fastForwardToDue": True})[0], 403)

        status, tick = self.request("POST", "/admin/tick", {"fastForwardToDue": True}, user="admin")
        self.assertEqual(status, 200)
        self.assertEqual(tick["executions"][0]["endState"], "Ready")
        status, detail = self.request("GET", f"/orders/{oid}")
        self.assertEqual((status, detail["status"], len(detail["ledger"])), (200, "READY", 3))

        status, cancelled = self.request("POST", f"/orders/{oid}/cancel")
        self.assertEqual((status, cancelled["status"]), (200, "CANCELLED"))
        status, audit = self.request("GET", f"/admin/orders/{oid}/audit", user="admin")
        self.assertIn("COMPENSATION_SUCCEEDED", [a["event"] for a in audit["audit"]])
        status, providers = self.request("GET", "/admin/providers", user="admin")
        self.assertEqual(providers["retailer"][0]["paymentStatus"], "VOIDED")
        self.assertEqual(len(providers["executions"]), 2)
        self.assertEqual(self.request("GET", "/admin/orders", user="admin")[0], 200)
        self.assertEqual(self.request("GET", "/scenarios")[0], 200)
        self.assertEqual(self.request("GET", "/nope")[0], 404)
        self.assertEqual(self.request("GET", "/orders/x/cancel")[0], 405)


if __name__ == "__main__":
    unittest.main()
