"""Shared test helpers (not a test module)."""
from roomready import demo
from roomready.store import ORDER_SK, order_pk


def runtime():
    return demo.new_runtime()


def create(rt, scenario="none", user="alice", key=None):
    return demo.create(rt, scenario, key=key, user=user)


def order(rt, order_id):
    return rt.store.get(order_pk(order_id), ORDER_SK)
