import os
import tempfile
import unittest

from roomready.store import (ConditionFailed, DynamoStore, MemoryStore, _deser, _ser, op_check, op_put,
                             op_update)


class MemoryStoreTests(unittest.TestCase):
    def test_conditional_put_and_update(self):
        s = MemoryStore()
        s.put({"PK": "A", "SK": "1", "v": 1}, [("not_exists", "PK")])
        with self.assertRaises(ConditionFailed):
            s.put({"PK": "A", "SK": "1", "v": 2}, [("not_exists", "PK")])
        s.update("A", "1", {"v": 2}, conds=[("eq", "v", 1)])
        with self.assertRaises(ConditionFailed):
            s.update("A", "1", {"v": 3}, conds=[("in", "v", [5, 6])])
        self.assertEqual(s.get("A", "1")["v"], 2)

    def test_bool_and_int_are_not_confused(self):
        s = MemoryStore()
        s.put({"PK": "A", "SK": "1", "flag": False})
        with self.assertRaises(ConditionFailed):
            s.update("A", "1", {"x": 1}, conds=[("eq", "flag", 0)])

    def test_transaction_is_all_or_nothing(self):
        s = MemoryStore()
        s.put({"PK": "A", "SK": "1", "v": 1})
        with self.assertRaises(ConditionFailed):
            s.transact([op_put({"PK": "B", "SK": "1"}), op_check("A", "1", [("eq", "v", 99)])])
        self.assertIsNone(s.get("B", "1"))
        s.transact([op_put({"PK": "B", "SK": "1"}), op_update("A", "1", {"v": 5}, remove=["nothing"])])
        self.assertEqual(s.get("A", "1")["v"], 5)

    def test_index_query_and_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "data.json")
            s = MemoryStore(path)
            s.put({"PK": "O#1", "SK": "META", "GSI1PK": "DUE", "GSI1SK": "2026-01-02"})
            s.put({"PK": "O#2", "SK": "META", "GSI1PK": "DUE", "GSI1SK": "2026-01-01"})
            s.increment("C", "1", "n")
            reloaded = MemoryStore(path)
            self.assertEqual([i["PK"] for i in reloaded.query_index("DUE")], ["O#2", "O#1"])
            self.assertEqual([i["PK"] for i in reloaded.query_index("DUE", max_sk="2026-01-01")], ["O#2"])
            self.assertEqual(reloaded.increment("C", "1", "n"), 2)
            self.assertEqual(len(reloaded.scan("O#", "META")), 2)


class FakeDynamoClient:
    def __init__(self):
        self.calls = []

    def transact_write_items(self, **kw):
        self.calls.append(("transact", kw))

    def put_item(self, **kw):
        self.calls.append(("put", kw))
        err = Exception("failed")
        err.response = {"Error": {"Code": "ConditionalCheckFailedException"}}
        raise err


class DynamoStoreTranslationTests(unittest.TestCase):
    def test_serialisation_round_trip(self):
        value = {"s": "x", "n": 3, "b": True, "z": None, "l": [1, "a"], "m": {"k": False}}
        self.assertEqual(_deser(_ser(value)), value)

    def test_condition_expressions_and_error_mapping(self):
        client = FakeDynamoClient()
        store = DynamoStore("tbl", client=client)
        store.transact([op_update("O#1", "META", {"status": "PREPARING"}, remove=["GSI1PK"],
                                  conds=[("eq", "status", "ACCEPTED"), ("in", "generation", [0, 1])]),
                        op_check("O#1", "META", [("not_exists", "lock")])])
        items = client.calls[0][1]["TransactItems"]
        update = items[0]["Update"]
        self.assertIn("SET", update["UpdateExpression"])
        self.assertIn("REMOVE", update["UpdateExpression"])
        self.assertIn(" IN (", update["ConditionExpression"])
        self.assertIn("attribute_not_exists", items[1]["ConditionCheck"]["ConditionExpression"])
        with self.assertRaises(ConditionFailed):
            store.put({"PK": "A", "SK": "B"}, [("not_exists", "PK")])


if __name__ == "__main__":
    unittest.main()
