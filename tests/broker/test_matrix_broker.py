import asyncio
import json
import os
from typing import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import uuid4

import pytest
from fractal.matrix.async_client import FractalAsyncClient
from nio import (
    AsyncClient,
    RoomGetStateEventError,
    RoomGetStateEventResponse,
    RoomPutStateError,
    RoomPutStateResponse,
)
from taskiq.message import BrokerMessage
from taskiq_matrix.exceptions import (
    DeviceQueueRequiresDeviceLabel,
    ScheduledTaskRequiresTaskIdLabel,
)
from taskiq_matrix.matrix_broker import (
    AsyncBroker,
    AsyncResultBackend,
    LockAcquireError,
    MatrixBroker,
    MatrixResultBackend,
)
from taskiq_matrix.matrix_queue import MatrixQueue


async def test_matrix_broker_with_matrix_config():
    """
    Tests that with_matrix_config properly sets the attributes of the MatrixBroker object
    with the values passed to it.
    """

    # create matrix config variables
    test_homeserver_url = "test homeserver url"
    test_access_token = "test access token"

    # create a MatrixBroker object
    test_broker = MatrixBroker()

    # verify that the broker has a device name as a control
    assert hasattr(test_broker, "device_name")

    # verify that it doesn not have any matrix config attributes
    assert not hasattr(test_broker, "homeserver_url")
    assert not hasattr(test_broker, "access_token")

    # call with_matrix_config
    test_broker.with_matrix_config(test_homeserver_url, test_access_token)

    # verify that the attributes were set
    assert test_broker.homeserver_url == test_homeserver_url
    assert test_broker.access_token == test_access_token


async def test_matrix_broker_with_result_backend_exception(test_matrix_broker):
    """
    Tests that an exception is raised when the result_backend variable is not
    a MatrixResultBackend
    """

    # create a matrix broker object
    matrix_broker = await test_matrix_broker()

    # create a mock result_backend object
    result_backend = MagicMock(spec=AsyncResultBackend)

    # set the expected error message
    expected_error_message = "result_backend must be an instance of MatrixResultBackend"

    # call with_result_backend and raise the exception
    with pytest.raises(Exception) as e:
        matrix_broker.with_result_backend(result_backend)

        # verify that the exception that was raised matches what was expected
        assert expected_error_message == str(e.value)


async def test_matrix_broker_with_result_backend_no_exception(test_matrix_broker):
    """
    Tests that the MatrixBroker object's parent class is modified to have a result_backend
    attribute that matches the mock MatrixResultBackend object created locally.
    """

    # create a matrix broker object
    test_broker = await test_matrix_broker()

    # create a mock result_backend object
    test_result_backend = MagicMock(spec=MatrixResultBackend)

    broker_with_backend = test_broker.with_result_backend(test_result_backend)

    assert broker_with_backend.result_backend == test_result_backend


async def test_matrix_broker_init_queues_no_matrix_variables_side_effect():
    """
    Tests that an exception is raised if there are no matrix config variables
    """

    test_broker = MatrixBroker()

    test_broker.room_id = ""
    test_broker.homeserver_url = ""
    test_broker.access_token = ""

    with pytest.raises(Exception, match="Matrix config must be set with with_matrix_config."):
        test_broker._init_queues()


async def test_matrix_broker_init_queues_no_existing_queues():
    """ """

    # create matrix config variables
    test_homeserver_url = "test homeserver url"
    test_access_token = "test access token"

    test_broker = MatrixBroker()

    # call with_matrix_config
    test_broker.with_matrix_config(test_homeserver_url, test_access_token)

    # verify that the broker does not have existing queues
    assert not hasattr(test_broker, "mutex_queue")
    assert not hasattr(test_broker, "device_queue")
    assert not hasattr(test_broker, "broadcast_queue")
    assert not hasattr(test_broker, "replication_queue")

    # call _init_queues to create the broker's queues
    test_broker._init_queues()

    # verify that queues were created for the broker
    assert hasattr(test_broker, "mutex_queue")
    assert hasattr(test_broker, "device_queue")
    assert hasattr(test_broker, "broadcast_queue")
    assert hasattr(test_broker, "replication_queue")


async def test_matrix_broker_init_queues_existing_queues(test_matrix_broker):
    """
    Tests that constructors are not called for the broker's queues if there are
    already existing queues for that broker.
    """

    # create a broker fixture with existing queues
    test_broker = await test_matrix_broker()

    # verify that the broker already has queues
    assert hasattr(test_broker, "mutex_queue")
    assert hasattr(test_broker, "device_queue")
    assert hasattr(test_broker, "broadcast_queue")
    assert hasattr(test_broker, "replication_queue")

    # patch the MatrixQueue class to check for constructor calls
    with patch("taskiq_matrix.matrix_queue.MatrixQueue") as mock_queue:
        # call _init_queues
        test_broker._init_queues()

        # verify that the constructors weren't called
        mock_queue.assert_not_called()


async def test_matrix_broker_with_result_backend_not_instance(test_matrix_broker):
    """
    Tests that an exception is raised if an object is passed to with_result_backend
    that is not a MatrixResultBackend object
    """

    # create a MatrixBroker object from a fixture
    test_broker = await test_matrix_broker()
    # create a generic mock object that is not a MatrixResultBackend
    mock_backend = MagicMock()

    # call with_result_backend to raise an exception
    with pytest.raises(
        Exception, match="result_backend must be an instance of MatrixResultBackend"
    ):
        test_broker.with_result_backend(mock_backend)


async def test_matrix_broker_with_result_backend_is_instance(test_matrix_broker):
    """
    Tests that if a MatrixResultBackend object is passed to with_result_backend, the
    superclass' with_result_backend is called with the same object
    """

    # create a MatrixBroker object from a fixture
    test_broker = await test_matrix_broker()

    # mock a MatrixResultBackend object
    mock_backend = MagicMock(spec=MatrixResultBackend)

    # call with_result_backend and verify that the object returned has a
    # result_backend attribute matching the mocked MatrixResultBackend object
    result = test_broker.with_result_backend(mock_backend)
    assert result.result_backend == mock_backend


@pytest.mark.skip(
    reason="update_checkpoints is an infinite loop. need to figure out how to test infinite loop"
)
async def test_matrix_broker_update_checkpoints(test_matrix_broker):
    """
    Stuck in a while True statement
    """
    mock_interval = 5

    matrix_broker: MatrixBroker = await test_matrix_broker()

    with patch(
        "taskiq_matrix.tasks.update_checkpoint", new=AsyncMock()
    ) as mock_update_checkpoint:
        await matrix_broker.update_checkpoints(mock_interval)

        mock_update_checkpoint.assert_has_calls([call("device"), call("broadcast")])


async def test_matrix_broker_shutdown_proper_shutdown(test_matrix_broker):
    """
    Test that the MatrixBroker object properly shuts down
    """

    # create MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    await matrix_broker.startup()

    # close the broker's REAL queue clients
    await matrix_broker.device_queue.client.close()
    await matrix_broker.mutex_queue.client.close()
    await matrix_broker.broadcast_queue.client.close()
    await matrix_broker.replication_queue.client.close()

    # create mock queue clients for the broker
    mock_device_queue_client = AsyncMock()
    mock_broadcast_queue_client = AsyncMock()
    mock_mutex_queue_client = AsyncMock()
    mock_replication_queue_client = AsyncMock()
    matrix_broker.device_queue.client = mock_device_queue_client
    matrix_broker.broadcast_queue.client = mock_broadcast_queue_client
    matrix_broker.mutex_queue.client = mock_mutex_queue_client
    matrix_broker.replication_queue.client = mock_replication_queue_client

    # shut down the MatrixBroker object
    await matrix_broker.shutdown()

    # verify that the close functions were called for each queue client
    matrix_broker.device_queue.client.close.assert_called_once()
    matrix_broker.broadcast_queue.client.close.assert_called_once()
    matrix_broker.mutex_queue.client.close.assert_called_once()
    matrix_broker.replication_queue.client.close.assert_called_once()


async def test_matrix_broker_use_task_id(test_matrix_broker):
    """
    Tests that broker message task_id can be replaced with a chosen task_id
    """

    # generate a uuid
    task_id = str(uuid4())

    # create a message
    message = {
        "task_id": "abc",
        "wassup": "dood",
    }

    # convert the message into json
    message_string = json.dumps(message)

    # encode the message into message bytes
    message_bytes = message_string.encode("utf-8")

    # create the BrokerMessage object
    message = BrokerMessage(
        task_id="abc", task_name="chicken tender", message=message_bytes, labels={}
    )

    # create a MatrixBroker
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # call _use_task_id
    msg = matrix_broker._use_task_id(task_id=task_id, message=message)

    # check that information matches what was passed to the function
    assert msg.task_id == task_id
    message_string = msg.message.decode("utf-8")
    json_dictionary = json.loads(message_string)
    assert json_dictionary["task_id"] == task_id


async def test_matrix_broker_startup(test_matrix_broker):
    """
    Tests that all functions are called on startup
    """

    # create a MatrixBroker
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # assert that checkpoint paths exist
    assert not os.path.exists(matrix_broker.device_queue.checkpoint.checkpoint_path)
    assert not os.path.exists(matrix_broker.broadcast_queue.checkpoint.checkpoint_path)
    assert not os.path.exists(matrix_broker.mutex_queue.checkpoint.checkpoint_path)
    assert not os.path.exists(matrix_broker.replication_queue.checkpoint.checkpoint_path)

    await matrix_broker.startup()

    # assert that checkpoint paths exist
    assert os.path.exists(matrix_broker.device_queue.checkpoint.checkpoint_path)
    assert os.path.exists(matrix_broker.broadcast_queue.checkpoint.checkpoint_path)
    assert os.path.exists(matrix_broker.mutex_queue.checkpoint.checkpoint_path)
    assert os.path.exists(matrix_broker.replication_queue.checkpoint.checkpoint_path)


async def test_matrix_broker_kick_no_next_batch(test_matrix_broker, test_broker_message):
    """
    Tests that if the broker's backend client doesn't have a next batch, one will be
    provided for it.
    """

    # create a matrix broker object from a fixture
    test_broker = await test_matrix_broker()

    # set by test_matrix_broker fixture
    room_id = test_broker._test_room_id
    test_broker_message.labels["room_id"] = room_id

    # mock its result_backend and its client
    mock_backend = MagicMock(spec=MatrixResultBackend)
    mock_backend.matrix_client = MagicMock()
    test_broker.result_backend = mock_backend
    # set its next_batch to None
    test_broker.result_backend.matrix_client.next_batch = None

    # create a custom sync token for comparison
    async def get_latest_sync_token():
        return "test_sync_token"

    # patch the get_latest_sync_token function
    with patch.object(
        mock_backend.matrix_client, "get_latest_sync_token", return_value=get_latest_sync_token()
    ):
        await test_broker.kick(test_broker_message)

    # verify that the broker's backend's client's next batch matches what was created locally
    assert test_broker.result_backend.matrix_client.next_batch == "test_sync_token"


@pytest.mark.integtest
async def test_matrix_broker_kick_functional_test(
    test_matrix_broker, test_broker_message: BrokerMessage
):
    """
    Tests that kick calls send_message with the appropriate information
    """
    matrix_broker: MatrixBroker = await test_matrix_broker()
    room_id = matrix_broker._test_room_id
    test_broker_message.labels["room_id"] = room_id

    await matrix_broker.kick(test_broker_message)

    # verify that message was sent into the room
    tasks, _ = await matrix_broker.mutex_queue.get_tasks_from_room(room_id)

    assert len(tasks) == 1

    assert test_broker_message.task_id == tasks[0].id


async def test_matrix_broker_kick_scheduled_task_no_task_id(
    test_matrix_broker, test_broker_message
):
    """
    Test that a ValueError is raised when there is no task_id present in
    message.labels
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task
    test_broker_message.labels = {"scheduled_task": "abc", "room_id": matrix_broker._test_room_id}

    # patch the MatrixLock.clock function
    with pytest.raises(ScheduledTaskRequiresTaskIdLabel):
        await matrix_broker.kick(test_broker_message)


@pytest.mark.integtest
async def test_matrix_broker_kick_scheduled_task_success(test_matrix_broker, test_broker_message):
    """
    Tests that the proper message is sent if the lock is successful.
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()
    room_id = matrix_broker._test_room_id

    # modify the labels property to add a scheduled_task and a task_id
    test_broker_message.labels = {
        "scheduled_task": "abc",
        "task_id": "task_id_to_lock_on",
        "room_id": room_id,
    }

    await matrix_broker.kick(test_broker_message)

    # verify that message was sent into the room
    tasks, _ = await matrix_broker.mutex_queue.get_tasks_from_room(room_id)

    assert len(tasks) == 1

    # task id should be different since a new id is generated for scheduled tasks
    assert test_broker_message.task_id != tasks[0].id


async def test_matrix_broker_kick_lock_fail(test_matrix_broker, test_broker_message):
    """
    Tests that a LockAcquireError is raised if the lock fails
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task and a task_id
    test_broker_message.labels = {
        "scheduled_task": "abc",
        "task_id": "abcd",
        "room_id": matrix_broker._test_room_id,
    }

    # mock send_message function
    mock_send_message = AsyncMock()

    # patch the send_message function and the MatrixLock.lock function
    with patch("taskiq_matrix.matrix_broker.send_message", mock_send_message):
        with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
            with patch("taskiq_matrix.matrix_broker.logger", new=MagicMock()) as mock_logger:
                # force the lock method to raise a LockAcquireError
                lock_instance = mock_lock.return_value
                lock_instance.lock.side_effect = LockAcquireError("Test Error")
                # with pytest.raises(LockAcquireError):
                async_gen = await matrix_broker.kick(test_broker_message)
                assert async_gen is None
                mock_lock.assert_called_once()
                mock_logger.info.assert_called_with("Failed to acquire lock for schedule abcd")


@pytest.mark.integtest
async def test_matrix_broker_kick_uses_task_id(test_matrix_broker, test_broker_message):
    """
    Ensure that task id is updated when task_id is passed in the message.labels.
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()
    room_id = matrix_broker._test_room_id

    # modify the labels property to add a scheduled_task and a task_id
    test_broker_message.labels = {
        "task_id": "should_use_this_task_id",
        "room_id": room_id,
    }

    await matrix_broker.kick(test_broker_message)

    # verify that message was sent into the room
    tasks, _ = await matrix_broker.mutex_queue.get_tasks_from_room(room_id)

    assert len(tasks) == 1

    # task id should be the task id that was passed in the message labels
    assert test_broker_message.task_id != tasks[0].id
    assert tasks[0].id == "should_use_this_task_id"


async def test_matrix_broker_kick_device_queue_raises_exception_if_no_device_label(
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
    test_broker_message: BrokerMessage,
):
    """
    Tasks kicked to the Device queue should raise an exception if the task
    does not have a device label.
    """
    broker = await test_matrix_broker()

    test_broker_message.labels = {"queue": "device", "room_id": broker._test_room_id}

    # kick task to replication queue
    with pytest.raises(DeviceQueueRequiresDeviceLabel):
        await broker.kick(test_broker_message)


async def test_matrix_broker_kick_device_queue_valid_device_label(
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
    test_broker_message: BrokerMessage,
):
    """
    Tests that the msgtype and queue_name reflect what is in the broker message labels if
    if it is for a device.
    """
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    test_broker_message.labels = {"room_id": room_id, "queue": "device", "device": "test device"}
    msgtype = broker.device_queue.task_types.device_task(test_broker_message.labels["device"])

    test_client = FractalAsyncClient(
        homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
        access_token=os.environ["MATRIX_ACCESS_TOKEN"],
    )

    # kick task
    with patch("taskiq_matrix.matrix_broker.send_message", new=AsyncMock()) as mock_send_message:
        with patch("taskiq_matrix.matrix_broker.FractalAsyncClient", return_value=test_client):
            await broker.kick(test_broker_message)

    # verify the message sent
    mock_send_message.assert_called_once_with(
        test_client,
        room_id,
        test_broker_message.message,
        msgtype=msgtype,
        task_id=test_broker_message.task_id,
        queue="device",
    )


async def test_matrix_broker_listen_tasks_present(test_iterable_tasks, test_matrix_broker):
    """
    Tests that listen() yields the same number of tasks that is returned by get_tasks()
    """

    # create a matrix broker object
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    # create an iterable of tasks
    num_tasks = 5
    tasks_iterator = test_iterable_tasks(num_tasks, room_id)
    tasks = []

    # patch get_tasks to return the iterable that was created locally
    with patch("taskiq_matrix.matrix_broker.MatrixBroker.get_tasks", return_value=tasks_iterator):
        # call the function and store the yielded tasks
        async def listen_wrapper():
            async for task in broker.listen():
                tasks.append(task)

        await listen_wrapper()

    # verify that the number of tasks received is the same as the number of tasks that
    # were in the broker
    assert len(tasks) == num_tasks


async def test_matrix_broker_listen_lock_error(test_iterable_tasks, test_matrix_broker):
    """
    Tests that an LockAcquireError is caught and logged when there is an error acquiring
    a lock.
    """

    # create a matrix broker object
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    # create an iterable of tasks
    num_tasks = 5
    tasks_iterator = test_iterable_tasks(num_tasks, room_id)
    tasks = []

    # set yield_task to raise a LockAcquireError
    broker.mutex_queue.yield_task = AsyncMock(side_effect=LockAcquireError())

    # patch get_tasks to return the iterable that was created locally
    with patch("taskiq_matrix.matrix_broker.MatrixBroker.get_tasks", return_value=tasks_iterator):
        with patch("taskiq_matrix.matrix_broker.logger") as mock_logger:

            async def listen_wrapper():
                async for task in broker.listen():
                    tasks.append(task)

            await listen_wrapper()

    # verify that logger.error was called and the number of tasks are the same
    call_count = mock_logger.error.call_count
    assert call_count == num_tasks


async def test_matrix_broker_listen_yield_error(test_iterable_tasks, test_matrix_broker):
    """
    Tests that an exception caught and logged when there is an error yielding a task.
    """

    # create a matrix broker object
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    # create an iterable of tasks
    num_tasks = 5
    tasks_iterator = test_iterable_tasks(num_tasks, room_id)
    tasks = []

    # set yield_task to raise an Exception
    broker.mutex_queue.yield_task = AsyncMock(side_effect=Exception())

    # patch get_tasks to return the iterable that was created locally
    with patch("taskiq_matrix.matrix_broker.MatrixBroker.get_tasks", return_value=tasks_iterator):
        with patch("taskiq_matrix.matrix_broker.logger") as mock_logger:

            async def listen_wrapper():
                async for task in broker.listen():
                    tasks.append(task)

            await listen_wrapper()

    # verify that logger.error was called and the number of tasks are the same
    call_count = mock_logger.error.call_count
    assert call_count == num_tasks
