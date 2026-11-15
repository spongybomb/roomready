"""Single-table storage with conditional writes and transactions.

Two implementations share identical semantics so the service and saga code is shared:
  * MemoryStore - thread-safe dict, optional JSON-file persistence (local demo/tests)
  * DynamoStore - DynamoDB via boto3 (imported lazily; available in the Lambda runtime)

Items are plain dicts with string keys "PK" and "SK". Values may be str, int, bool,
None, lists and dicts (no floats). The sparse index "GSI1" is keyed by the optional
attributes "GSI1PK"/"GSI1SK".

Conditions are tuples:
    ("exists", attr) | ("not_exists", attr) | ("eq", attr, value) | ("in", attr, [values])
Transaction operations are built with op_put / op_update / op_delete / op_check.
"""
from __future__ import annotations

import copy
import json
import os
import threading
import time

# --------------------------------------------------------------------------- keys
ORDER_SK = "META"
PROFILE_SK = "PROFILE"
INDEX_DUE = "DUE"
INDEX_OUTBOX = "OUTBOX"


def order_pk(order_id: str) -> str:
    return f"ORDER#{order_id}"


def user_pk(user_id: str) -> str:
    return f"USER#{user_id}"


class ConditionFailed(Exception):
    """A condition expression (or a transaction condition) did not hold."""


# --------------------------------------------------------------------------- transaction ops
def op_put(item, conds=()):
    return {"op": "put", "item": item, "conds": list(conds)}


def op_update(pk, sk, set_=None, remove=(), conds=()):
    return {"op": "update", "pk": pk, "sk": sk, "set": dict(set_ or {}), "remove": list(remove),
            "conds": list(conds)}


def op_delete(pk, sk, conds=()):
    return {"op": "delete", "pk": pk, "sk": sk, "conds": list(conds)}


def op_check(pk, sk, conds):
    return {"op": "check", "pk": pk, "sk": sk, "conds": list(conds)}


# --------------------------------------------------------------------------- audit helper
_seq_lock = threading.Lock()
_last_seq = [0]


def _next_seq() -> int:
    """Monotonic, wall-clock based sequence (the demo clock may be frozen)."""
    with _seq_lock:
        value = max(time.time_ns() // 1000, _last_seq[0] + 1)
        _last_seq[0] = value
        return value


def audit_item(order_id: str, now_iso: str, event: str, detail=None, actor: str = "system") -> dict:
    seq = _next_seq()
    return {
        "PK": order_pk(order_id),
        "SK": f"AUDIT#{seq:020d}",
        "entity": "AUDIT",
        "at": now_iso,
        "event": event,
        "actor": actor,
        "detail": detail or {},
    }


def write_audit(store, order_id, now_iso, event, detail=None, actor="system"):
    store.put(audit_item(order_id, now_iso, event, detail, actor))


# --------------------------------------------------------------------------- interface
class Store:
    def get(self, pk, sk):
        raise NotImplementedError

    def put(self, item, conds=()):
        raise NotImplementedError

    def update(self, pk, sk, set_=None, remove=(), conds=()):
        """Upsert-style update (like DynamoDB UpdateItem); returns the new item."""
        raise NotImplementedError

    def delete(self, pk, sk, conds=()):
        raise NotImplementedError

    def increment(self, pk, sk, attr, by=1) -> int:
        raise NotImplementedError

    def transact(self, ops):
        """All-or-nothing; raises ConditionFailed if any condition fails."""
        raise NotImplementedError

    def query(self, pk, sk_prefix=""):
        raise NotImplementedError

    def query_index(self, index_pk, max_sk=None, limit=None):
        raise NotImplementedError

    def scan(self, pk_prefix, sk):
        raise NotImplementedError


def _same(a, b) -> bool:
    return type(a) is type(b) and a == b


def check_conditions(item, conds) -> bool:
    for cond in conds:
        kind, attr = cond[0], cond[1]
        present = item is not None and attr in item
        if kind == "exists" and not present:
            return False
        if kind == "not_exists" and present:
            return False
        if kind == "eq" and (not present or not _same(item[attr], cond[2])):
            return False
        if kind == "in" and (not present or not any(_same(item[attr], v) for v in cond[2])):
            return False
        if kind not in ("exists", "not_exists", "eq", "in"):
            raise ValueError(f"unsupported condition {kind}")
    return True


# --------------------------------------------------------------------------- memory
class MemoryStore(Store):
    def __init__(self, path: str | None = None):
        self._lock = threading.RLock()
        self._items: dict[tuple[str, str], dict] = {}
        self._path = path
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                for item in json.load(fh):
                    self._items[(item["PK"], item["SK"])] = item

    def _save(self):
        if not self._path:
            return
        directory = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(directory, exist_ok=True)
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(list(self._items.values()), fh, indent=1, sort_keys=True)
        os.replace(tmp, self._path)

    def get(self, pk, sk):
        with self._lock:
            item = self._items.get((pk, sk))
            return copy.deepcopy(item) if item is not None else None

    def put(self, item, conds=()):
        self.transact([op_put(item, conds)])

    def update(self, pk, sk, set_=None, remove=(), conds=()):
        with self._lock:
            self.transact([op_update(pk, sk, set_, remove, conds)])
            return self.get(pk, sk)

    def delete(self, pk, sk, conds=()):
        self.transact([op_delete(pk, sk, conds)])

    def increment(self, pk, sk, attr, by=1):
        with self._lock:
            item = self._items.setdefault((pk, sk), {"PK": pk, "SK": sk})
            item[attr] = int(item.get(attr, 0)) + by
            self._save()
            return item[attr]

    def transact(self, ops):
        with self._lock:
            for op in ops:
                key = (op["item"]["PK"], op["item"]["SK"]) if op["op"] == "put" else (op["pk"], op["sk"])
                if not check_conditions(self._items.get(key), op["conds"]):
                    raise ConditionFailed(f"condition failed on {key}")
            for op in ops:
                if op["op"] == "put":
                    item = copy.deepcopy(op["item"])
                    self._items[(item["PK"], item["SK"])] = item
                elif op["op"] == "update":
                    key = (op["pk"], op["sk"])
                    item = self._items.get(key) or {"PK": op["pk"], "SK": op["sk"]}
                    item.update(copy.deepcopy(op["set"]))
                    for attr in op["remove"]:
                        item.pop(attr, None)
                    self._items[key] = item
                elif op["op"] == "delete":
                    self._items.pop((op["pk"], op["sk"]), None)
            self._save()

    def query(self, pk, sk_prefix=""):
        with self._lock:
            found = [copy.deepcopy(v) for (p, s), v in self._items.items() if p == pk and s.startswith(sk_prefix)]
        return sorted(found, key=lambda i: i["SK"])

    def query_index(self, index_pk, max_sk=None, limit=None):
        with self._lock:
            found = [copy.deepcopy(v) for v in self._items.values()
                     if v.get("GSI1PK") == index_pk and (max_sk is None or v.get("GSI1SK", "") <= max_sk)]
        found.sort(key=lambda i: i.get("GSI1SK", ""))
        return found[:limit] if limit else found

    def scan(self, pk_prefix, sk):
        with self._lock:
            found = [copy.deepcopy(v) for (p, s), v in self._items.items() if p.startswith(pk_prefix) and s == sk]
        return sorted(found, key=lambda i: i["PK"])


# --------------------------------------------------------------------------- dynamodb
def _ser(value):
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, int):
        return {"N": str(value)}
    if isinstance(value, str):
        return {"S": value}
    if value is None:
        return {"NULL": True}
    if isinstance(value, dict):
        return {"M": {k: _ser(v) for k, v in value.items()}}
    if isinstance(value, (list, tuple)):
        return {"L": [_ser(v) for v in value]}
    raise TypeError(f"unsupported attribute type {type(value).__name__}")


def _deser(attr):
    (kind, value), = attr.items()
    if kind == "S":
        return value
    if kind == "N":
        return int(value) if "." not in value else float(value)
    if kind == "BOOL":
        return value
    if kind == "NULL":
        return None
    if kind == "M":
        return {k: _deser(v) for k, v in value.items()}
    if kind == "L":
        return [_deser(v) for v in value]
    raise TypeError(f"unsupported DynamoDB type {kind}")


def _item_from(raw):
    return {k: _deser(v) for k, v in raw.items()} if raw else None


def _error_code(exc) -> str:
    return getattr(exc, "response", {}).get("Error", {}).get("Code", "")


class _Expr:
    """Builds expression attribute names/values with unique placeholders."""

    def __init__(self):
        self.names, self.values = {}, {}

    def name(self, attr):
        key = f"#n{len(self.names)}"
        self.names[key] = attr
        return key

    def value(self, val):
        key = f":v{len(self.values)}"
        self.values[key] = _ser(val)
        return key

    def condition(self, conds):
        parts = []
        for cond in conds:
            kind, attr = cond[0], cond[1]
            n = self.name(attr)
            if kind == "exists":
                parts.append(f"attribute_exists({n})")
            elif kind == "not_exists":
                parts.append(f"attribute_not_exists({n})")
            elif kind == "eq":
                parts.append(f"{n} = {self.value(cond[2])}")
            elif kind == "in":
                parts.append(f"{n} IN ({', '.join(self.value(v) for v in cond[2])})")
            else:
                raise ValueError(f"unsupported condition {kind}")
        return " AND ".join(parts) if parts else None

    def update(self, set_, remove):
        clauses = []
        if set_:
            clauses.append("SET " + ", ".join(f"{self.name(k)} = {self.value(v)}" for k, v in set_.items()))
        if remove:
            clauses.append("REMOVE " + ", ".join(self.name(a) for a in remove))
        return " ".join(clauses)

    def apply(self, params, condition=None):
        if condition:
            params["ConditionExpression"] = condition
        if self.names:
            params["ExpressionAttributeNames"] = self.names
        if self.values:
            params["ExpressionAttributeValues"] = self.values
        return params


class DynamoStore(Store):
    INDEX_NAME = "GSI1"

    def __init__(self, table_name: str, client=None):
        if client is None:
            import boto3  # lazy: only available/needed in the Lambda runtime
            client = boto3.client("dynamodb")
        self.table = table_name
        self.client = client

    @staticmethod
    def _key(pk, sk):
        return {"PK": {"S": pk}, "SK": {"S": sk}}

    def _call(self, fn, **params):
        try:
            return fn(**params)
        except Exception as exc:  # botocore ClientError
            if _error_code(exc) == "ConditionalCheckFailedException":
                raise ConditionFailed(str(exc)) from exc
            raise

    def get(self, pk, sk):
        resp = self.client.get_item(TableName=self.table, Key=self._key(pk, sk), ConsistentRead=True)
        return _item_from(resp.get("Item"))

    def _put_params(self, item, conds):
        e = _Expr()
        cond = e.condition(conds)
        return e.apply({"TableName": self.table, "Item": {k: _ser(v) for k, v in item.items()}}, cond)

    def _update_params(self, pk, sk, set_, remove, conds):
        e = _Expr()
        expr = e.update(set_, remove)
        cond = e.condition(conds)
        return e.apply({"TableName": self.table, "Key": self._key(pk, sk), "UpdateExpression": expr}, cond)

    def _key_params(self, pk, sk, conds):
        e = _Expr()
        cond = e.condition(conds)
        return e.apply({"TableName": self.table, "Key": self._key(pk, sk)}, cond)

    def put(self, item, conds=()):
        self._call(self.client.put_item, **self._put_params(item, conds))

    def update(self, pk, sk, set_=None, remove=(), conds=()):
        params = self._update_params(pk, sk, set_ or {}, list(remove), conds)
        params["ReturnValues"] = "ALL_NEW"
        return _item_from(self._call(self.client.update_item, **params).get("Attributes"))

    def delete(self, pk, sk, conds=()):
        self._call(self.client.delete_item, **self._key_params(pk, sk, conds))

    def increment(self, pk, sk, attr, by=1):
        resp = self.client.update_item(
            TableName=self.table, Key=self._key(pk, sk), UpdateExpression="ADD #a :by",
            ExpressionAttributeNames={"#a": attr}, ExpressionAttributeValues={":by": {"N": str(by)}},
            ReturnValues="UPDATED_NEW")
        return int(resp["Attributes"][attr]["N"])

    def transact(self, ops):
        items = []
        for op in ops:
            if op["op"] == "put":
                items.append({"Put": self._put_params(op["item"], op["conds"])})
            elif op["op"] == "update":
                items.append({"Update": self._update_params(op["pk"], op["sk"], op["set"], op["remove"], op["conds"])})
            elif op["op"] == "delete":
                items.append({"Delete": self._key_params(op["pk"], op["sk"], op["conds"])})
            elif op["op"] == "check":
                items.append({"ConditionCheck": self._key_params(op["pk"], op["sk"], op["conds"])})
        try:
            self.client.transact_write_items(TransactItems=items)
        except Exception as exc:
            if _error_code(exc) == "TransactionCanceledException":
                # ConditionalCheckFailed or TransactionConflict: caller re-reads and decides.
                raise ConditionFailed(str(exc)) from exc
            raise

    def _paginate(self, fn, params, limit=None):
        out = []
        while True:
            resp = fn(**params)
            out.extend(_item_from(i) for i in resp.get("Items", []))
            if limit and len(out) >= limit:
                return out[:limit]
            if "LastEvaluatedKey" not in resp:
                return out
            params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    def query(self, pk, sk_prefix=""):
        e = _Expr()
        expr = f"{e.name('PK')} = {e.value(pk)}"
        if sk_prefix:
            expr += f" AND begins_with({e.name('SK')}, {e.value(sk_prefix)})"
        params = e.apply({"TableName": self.table, "KeyConditionExpression": expr, "ConsistentRead": True})
        return self._paginate(self.client.query, params)

    def query_index(self, index_pk, max_sk=None, limit=None):
        e = _Expr()
        expr = f"{e.name('GSI1PK')} = {e.value(index_pk)}"
        if max_sk is not None:
            expr += f" AND {e.name('GSI1SK')} <= {e.value(max_sk)}"
        params = e.apply({"TableName": self.table, "IndexName": self.INDEX_NAME, "KeyConditionExpression": expr})
        return self._paginate(self.client.query, params, limit)

    def scan(self, pk_prefix, sk):
        e = _Expr()
        expr = f"begins_with({e.name('PK')}, {e.value(pk_prefix)}) AND {e.name('SK')} = {e.value(sk)}"
        params = e.apply({"TableName": self.table, "FilterExpression": expr, "ConsistentRead": True})
        return sorted(self._paginate(self.client.scan, params), key=lambda i: i["PK"])
