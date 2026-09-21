import time

from kademlia.storage import DataStore


def test_put_and_get():
    store = DataStore()
    store.put(b"key", b"value", ttl=10)
    assert store.get(b"key") == b"value"
    assert b"key" in store


def test_get_missing_key_returns_none():
    store = DataStore()
    assert store.get(b"missing") is None


def test_expiry():
    store = DataStore()
    store.put(b"key", b"value", ttl=0.05)
    assert store.get(b"key") == b"value"
    time.sleep(0.1)
    assert store.get(b"key") is None
    assert b"key" not in store


def test_purge_expired_removes_and_reports_expired_keys():
    store = DataStore()
    store.put(b"a", b"1", ttl=0.05)
    store.put(b"b", b"2", ttl=10)
    time.sleep(0.1)
    expired = store.purge_expired()
    assert expired == [b"a"]
    assert store.get(b"a") is None
    assert store.get(b"b") == b"2"


def test_items_due_for_republish():
    store = DataStore()
    store.put(b"a", b"1", ttl=10)
    assert store.items_due_for_republish(interval=0.05) == []
    time.sleep(0.1)
    due = store.items_due_for_republish(interval=0.05)
    assert len(due) == 1
    key, value, remaining_ttl = due[0]
    assert (key, value) == (b"a", b"1")
    assert 9.0 < remaining_ttl < 10.0  # ~10s ttl minus the ~0.1s we slept


def test_mark_republished_resets_timer():
    store = DataStore()
    store.put(b"a", b"1", ttl=10)
    time.sleep(0.1)
    store.mark_republished(b"a")
    assert store.items_due_for_republish(interval=0.05) == []


def test_snapshot_excludes_expired():
    store = DataStore()
    store.put(b"a", b"1", ttl=0.05)
    store.put(b"b", b"2", ttl=10)
    time.sleep(0.1)
    assert store.snapshot() == {b"b": b"2"}
