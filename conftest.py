import asyncio
import json
import os
from typing import Any, Awaitable, Callable, Generator
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fractal.matrix import FractalAsyncClient
from nio import RoomCreateError, RoomGetStateEventResponse, UnknownEvent
from taskiq.message import BrokerMessage
from taskiq_matrix.matrix_broker import MatrixBroker
from taskiq_matrix.matrix_queue import Checkpoint, Task
from taskiq_matrix.matrix_result_backend import MatrixResultBackend

try:
    TEST_HOMESERVER_URL = os.environ["MATRIX_HOMESERVER_URL"]
    TEST_USER_ACCESS_TOKEN = os.environ["MATRIX_ACCESS_TOKEN"]
    TEST_ROOM_ID = os.environ["MATRIX_ROOM_ID"]
except KeyError:
    raise Exception(
        "Please run prepare-test.py first, then source the generated environment file"
    )


@pytest.fixture(scope="function")
def matrix_client() -> Generator[FractalAsyncClient, None, None]:
    client = FractalAsyncClient(access_token=TEST_USER_ACCESS_TOKEN)
    yield client
    asyncio.run(client.close())


@pytest.fixture(scope="function")
def new_matrix_room(matrix_client: FractalAsyncClient):
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
def test_matrix_result_backend(new_matrix_room):
    """
    Creates a MatrixResultBackend object
    """

    async def create():
        room_id = await new_matrix_room()
        return MatrixResultBackend(
            homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
            access_token=os.environ["MATRIX_ACCESS_TOKEN"],
            room_id=room_id,
        )

    return create


class MockAsyncIterable:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)


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
        broker.with_matrix_config(
            room_id, os.environ["MATRIX_HOMESERVER_URL"], os.environ["MATRIX_ACCESS_TOKEN"]
        )

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

@pytest.fixture
def test_multiple_broker_message():
    """
    Create a BrokerMessage Fixture
    """
    async def create(num_messages: int):
        messages = []
        for i in range(num_messages):
            task_id = str(uuid4())
            message = {
                "task_id": task_id,
                "foo": "bar",
            }

            # convert the message into json
            message_string = json.dumps(message)

            # encode the message into message bytes
            message_bytes = message_string.encode("utf-8")

            messages.append(BrokerMessage(task_id=task_id, task_name="test_name", message=message_bytes, labels={}))

        # create the BrokerMessage object
        return messages
    return create

@pytest.fixture(scope="function")
def test_checkpoint(test_room_id):
    mock_client_parameter = MagicMock(spec=FractalAsyncClient)
    mock_client_parameter.room_get_state_event.return_value = RoomGetStateEventResponse(
        content={"checkpoint": "abc"}, event_type="abc", state_key="", room_id=test_room_id
    )
    return Checkpoint(type="abc", room_id=test_room_id, client=mock_client_parameter)


@pytest.fixture
def test_room_id() -> str:
    return TEST_ROOM_ID


@pytest.fixture
def unknown_event_factory() -> Callable[[str, str], UnknownEvent]:
    """
    Returns a mock Matrix event class.
    """

    def create_test_event(body: str, sender: str, msgtype: str = "test.event") -> UnknownEvent:
        return UnknownEvent(
            source={
                "event_id": "test_event_id",
                "sender": sender,
                "origin_server_ts": 0,
                "content": {"msgtype": msgtype, "body": body, "sender": sender},
            },
            type=msgtype,
        )

    return create_test_event


@pytest.fixture
def test_iterable_tasks(unknown_event_factory):
    """
    get_tasks() interable
    """

    def factory(num_tasks: int):
        tasks = []
        for i in range(num_tasks):
            event = unknown_event_factory(
                {
                    "queue": "mutex",
                    "task_id": str(uuid4()),
                    "msgtype": "taskiq.mutex.task",
                    "task": json.dumps(
                        {
                            "name": "task_fixture",
                            "cron": "* * * * *",
                            "labels": {"task_id": "mutex_checkpoint", "queue": "mutex"},
                            "args": ["mutex"],
                            "kwargs": {},
                        }
                    ),
                },
                "test_sender",
            )

            tasks.append(Task(**event.source['content']))

        return MockAsyncIterable([tasks])
    return factory
