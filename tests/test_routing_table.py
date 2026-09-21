from kademlia.constants import K
from kademlia.identifier import NodeID
from kademlia.routing_table import Contact, KBucket, RoutingTable


def make_contact(node_id=None, port=1000):
    return Contact(node_id=node_id or NodeID.random(), ip="127.0.0.1", port=port)


def test_kbucket_add_and_get():
    bucket = KBucket(capacity=3)
    c1, c2 = make_contact(), make_contact()
    assert bucket.add_contact(c1) is None
    assert bucket.add_contact(c2) is None
    assert set(bucket.get_contacts()) == {c1, c2}


def test_kbucket_full_returns_lrs_contact():
    bucket = KBucket(capacity=2)
    c1, c2, c3 = make_contact(), make_contact(), make_contact()
    bucket.add_contact(c1)
    bucket.add_contact(c2)
    lrs = bucket.add_contact(c3)
    assert lrs == c1
    # new contact not actually inserted yet
    assert c3.node_id not in bucket.contacts


def test_kbucket_reinsert_moves_to_most_recently_seen():
    bucket = KBucket(capacity=2)
    c1, c2 = make_contact(), make_contact()
    bucket.add_contact(c1)
    bucket.add_contact(c2)
    bucket.add_contact(c1)  # touch c1
    assert next(iter(bucket.contacts.values())) == c2  # c2 is now LRS


def test_kbucket_remove_promotes_replacement():
    bucket = KBucket(capacity=1)
    c1, c2 = make_contact(), make_contact()
    bucket.add_contact(c1)
    bucket.add_contact(c2)  # goes to replacement cache, bucket full
    bucket.remove_contact(c1.node_id)
    assert c2.node_id in bucket.contacts


def test_routing_table_bucket_for_is_symmetric_with_distance():
    owner = NodeID.random()
    table = RoutingTable(owner)
    other = NodeID.random()
    expected = owner.bucket_index(other)
    assert table.bucket_for(other) == expected


def test_routing_table_find_closest_orders_by_xor_distance():
    owner = NodeID.random()
    table = RoutingTable(owner)
    contacts = [make_contact(port=i) for i in range(50)]
    for c in contacts:
        table.add_contact(c)

    target = NodeID.random()
    closest = table.find_closest(target, count=5)
    all_contacts = table.all_contacts()
    brute_force = sorted(all_contacts, key=lambda c: target.distance(c.node_id))[:5]
    assert [c.node_id for c in closest] == [c.node_id for c in brute_force]


def test_routing_table_ignores_self():
    owner = NodeID.random()
    table = RoutingTable(owner)
    assert table.add_contact(Contact(node_id=owner, ip="127.0.0.1", port=1)) is None
    assert table.all_contacts() == []


def test_routing_table_never_exceeds_k_per_bucket():
    owner = NodeID.random()
    table = RoutingTable(owner)
    # Force many contacts into the same bucket by fixing the same distance bit.
    for i in range(K + 10):
        node_id = NodeID.from_int(owner.as_int ^ (1 << 5) ^ i)
        table.add_contact(Contact(node_id=node_id, ip="127.0.0.1", port=i))
    bucket = table.buckets[5]
    assert len(bucket.contacts) <= K
