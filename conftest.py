import asyncio
import json
import os
from typing import Any, Awaitable, Callable, Generator
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from nio import AsyncClient, RoomCreateError, RoomGetStateEventResponse, UnknownEvent
from taskiq.message import BrokerMessage

from taskiq_matrix.matrix_broker import (
    BroadcastQueue,
    MatrixBroker,
    MatrixQueue,
    ReplicatedQueue,
)
from taskiq_matrix.matrix_queue import Checkpoint

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
def new_matrix_room(matrix_client: AsyncClient):
    """
    Creates a new room and returns its room id.
    """

    async def create():
        res = await matrix_client.room_create(name="test_room")
        if isinstance(res, RoomCreateError):
            await matrix_client.close()
            raise Exception("Failed to create test room")
        await matrix_client.close()
        return res.room_id

    return create


@pytest.fixture(scope="function")
def test_matrix_broker(new_matrix_room: Callable[[], Awaitable[str]]):
    async def create():
        """
        Creates a MatrixBroker instance whose queues are configured to
        use a new room each time the fixture is called.
        """
        room_id = await new_matrix_room()

        broker = MatrixBroker()

        # set the broker's room id
        broker.room_id = room_id

        # use room_id for the queues
        broker._init_queues()

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


@pytest.fixture(scope="function")
def test_checkpoint(test_room_id) -> Checkpoint:
    mock_client_parameter = MagicMock(spec=AsyncClient)
    mock_client_parameter.room_get_state_event.return_value = RoomGetStateEventResponse(
        content={"checkpoint": "abc"}, event_type="abc", state_key="", room_id=test_room_id
    )
    return Checkpoint(type="abc", room_id=test_room_id, client=mock_client_parameter)


# FIXME: Add a Matrix result backend fixture. The fixture should look very similar
#        to the matrix_client fixture above. The only difference is that the
#        result backend fixture should return a MatrixResultBackend instance.
#        Make sure to call the shutdown method on the result backend after yielding!


@pytest.fixture
def test_room_id() -> str:
    return TEST_ROOM_ID


@pytest.fixture
def unknown_event_factory() -> Callable[[str, str], UnknownEvent]:
    """
    Returns a mock Matrix event class.
    """

    def create_test_event(body: str, sender: str) -> UnknownEvent:
        return UnknownEvent(
            source={
                "event_id": "test_event_id",
                "sender": sender,
                "origin_server_ts": 0,
                "content": {"type": "test.event", "body": body},
            },
            type="test_event",
        )

    return create_test_event
