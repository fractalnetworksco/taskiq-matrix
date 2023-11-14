import asyncio
import json
import os
from typing import Generator
from uuid import uuid4

import pytest
from nio import AsyncClient, RoomCreateError
from taskiq.message import BrokerMessage
from taskiq_matrix.matrix_broker import BroadcastQueue, MatrixBroker, MatrixQueue

try:
    TEST_HOMESERVER_URL = os.environ["MATRIX_HOMESERVER_URL"]
    TEST_USER_ACCESS_TOKEN = os.environ["MATRIX_ACCESS_TOKEN"]
    TEST_ROOM_ID = os.environ["MATRIX_ROOM_ID"]
except KeyError:
    raise Exception(
        "Please run prepare-test.py first, then source the generated environment file"
    )


@pytest.fixture(scope="function")
def matrix_client() -> Generator[AsyncClient, None, None]:
    client = AsyncClient(homeserver=TEST_HOMESERVER_URL)
    client.access_token = TEST_USER_ACCESS_TOKEN
    yield client
    asyncio.run(client.close())


@pytest.fixture(scope="function")
def test_matrix_broker(matrix_client: AsyncClient):
    async def create():
        """
        Creates a MatrixBroker instance whose queues are configured to
        use a new room each time the fixture is called.
        """
        broker = MatrixBroker()

        # create a new room
        res = await matrix_client.room_create(name="test_room")
        if isinstance(res, RoomCreateError):
            raise Exception("Failed to create test room")

        room_id = res.room_id

        # recreate the broker's queues using the new room id
        broker.mutex_queue = MatrixQueue(broker.mutex_queue.name, room_id=room_id)
        broker.device_queue = MatrixQueue(broker.device_queue.name, room_id=room_id)
        broker.broadcast_queue = BroadcastQueue(broker.broadcast_queue.name, room_id=room_id)

        return broker

    return create


@pytest.fixture
def test_broker_message():
    """
    Create a BrokerMessage Fixture
    """
    task_id = str(uuid4())
    message = {
        "task_id": task_id,
        "foo": "bar",
    }

    # convert the message into json
    message_string = json.dumps(message)

    # encode the message into message bytes
    message_bytes = message_string.encode("utf-8")

    # create the BrokerMessage object
    return BrokerMessage(task_id=task_id, task_name="test_name", message=message_bytes, labels={})


# FIXME: Add a Matrix result backend fixture. The fixture should look very similar
#        to the matrix_client fixture above. The only difference is that the
#        result backend fixture should return a MatrixResultBackend instance.
#        Make sure to call the shutdown method on the result backend after yielding!


@pytest.fixture
def test_room_id() -> str:
    return TEST_ROOM_ID
