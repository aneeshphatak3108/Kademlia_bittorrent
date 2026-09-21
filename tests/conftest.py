from typing import List

import pytest_asyncio

from kademlia.node import Node


@pytest_asyncio.fixture
async def make_node():
    created: List[Node] = []

    async def _make(node_id=None, **kwargs):
        node = Node("127.0.0.1", 0, node_id=node_id, **kwargs)
        await node.start()
        created.append(node)
        return node

    yield _make

    for node in created:
        await node.stop()
