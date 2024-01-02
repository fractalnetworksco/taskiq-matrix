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
    test_room_id = "test room id"
    test_homeserver_url = "test homeserver url"
    test_access_token = "test access token"

    # create a MatrixBroker object
    test_broker = MatrixBroker()

    # verify that the broker has a device name as a control 
    assert hasattr(test_broker, "device_name")
    # verify that it doesn not have any matrix config attributes
    assert not hasattr(test_broker, "room_id")
    assert not hasattr(test_broker, "homeserver_url")
    assert not hasattr(test_broker, "access_token")

    # call with_matrix_config
    test_broker.with_matrix_config(test_room_id, test_homeserver_url, test_access_token)

    # verify that the attributes were set
    assert test_broker.room_id == test_room_id
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
    """
    """

    # create matrix config variables
    test_room_id = "test room id"
    test_homeserver_url = "test homeserver url"
    test_access_token = "test access token"

    test_broker = MatrixBroker()

    # call with_matrix_config
    test_broker.with_matrix_config(test_room_id, test_homeserver_url, test_access_token)

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

@pytest.mark.integtest
async def test_matrix_broker_add_mutex_checkpoint_task_unknown_error(test_matrix_broker):
    """
    Tests exception raised if room get state event in the mutex queue
    is an error and that error status code is not "M_NOT_FOUND"
    """

    # create matrix broker object
    broker = await test_matrix_broker()

    # mock the matrix broker's mutex queue and client
    mock_mutex_queue = MagicMock()
    mock_mutex_queue.client = AsyncMock()
    broker.mutex_queue = mock_mutex_queue

    # set room_get_state_event to return a RoomGetStateEventError
    mock_mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="abc", message="Test Error Message"
    )

    # call add_mutex_checkpoint_task to raise the exception
    with pytest.raises(Exception):
        await broker.add_mutex_checkpoint_task()

async def test_matrix_broker_add_mutex_checkpoint_task_state_not_found(test_matrix_broker):
    """
    Tests exception raised if room get state event in the mutex queue
    is an error and that error status code is not "M_NOT_FOUND"
    """

    # create matrix broker object
    broker = await test_matrix_broker()

    # mock the matrix broker's mutex queue and client
    mock_mutex_queue = MagicMock()
    mock_mutex_queue.client = AsyncMock()
    broker.mutex_queue = mock_mutex_queue

    # set room_get_state_event to return a RoomGetStateEventError
    mock_mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="M_NOT_FOUND", message="Test Error Message"
    )

    with patch("taskiq_matrix.matrix_broker.logger", new=MagicMock()) as mock_logger:
        await broker.add_mutex_checkpoint_task()
        mock_logger.info.assert_called_once()

async def test_matrix_broker_add_mutex_checkpoint_task_content_errcode(test_matrix_broker):
    """
    Test that an exception is raised when room_get_state_event returns a
    RoomGetStateResponse object with an error code
    """

    # create matrix broker object
    broker: MatrixBroker = await test_matrix_broker()

    # mock the matrix broker's mutex queue and client
    mock_mutex_queue = MagicMock()
    mock_mutex_queue.client = AsyncMock()
    broker.mutex_queue = mock_mutex_queue

    # create a content dictionary to be passed to the RoomGetStateEventResponse
    content_error = {"errcode": "Test Error"}

    # set room_get_state_event to return a RoomGetStateEventResponse
    mock_mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventResponse(
        content=content_error, event_type="abc", state_key="abc", room_id="abc"
    )

    # call add_mutex_checkpoint_task to raise an exception caused by the "errcode" key
    with pytest.raises(Exception):
        await broker.add_mutex_checkpoint_task()

@pytest.mark.integtest
async def test_matrix_broker_add_mutex_checkpoint_task_checkpoint_exists(test_matrix_broker):
    """
    Test that function returns True if the chekpoint already exists and
    room_put_state is never called
    """

    # create matrix broker object
    broker: MatrixBroker = await test_matrix_broker()

    # mock the matrix broker's mutex queue and client
    mock_mutex_queue = MagicMock()
    mock_mutex_queue.client = AsyncMock()
    broker.mutex_queue = mock_mutex_queue

    # create a task dictionary matching the one that is created in add_mutex_checkpoint
    task = {
        "name": "taskiq.update_checkpoint",
        "cron": "* * * * *",
        "labels": {"task_id": "mutex_checkpoint", "queue": "mutex"},
        "args": ["mutex"],
        "kwargs": {},
    }

    # create an event dictionary containing the task dictionary to pass as a parameter to RoomGetStateEventResponse
    event_content = {"tasks": [task]}

    # set room_get_state_event to return a RoomGetStateEventResponse
    mock_mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventResponse(
        content=event_content, event_type="abc", state_key="abc", room_id="abc"
    )

    # patch the room_put_state function call
    with patch.object(broker.mutex_queue.client, "room_put_state") as mock_room_put_state:
        # call add_mutex_checkpoint_task
        result = await broker.add_mutex_checkpoint_task()
        assert result

        mock_room_put_state.assert_not_called()

@pytest.mark.integtest
async def test_matrix_broker_add_mutex_checkpoint_task_update_schedule(test_matrix_broker):
    """
    Tests that updating the schedule returns True when room_put_state
    returns a RoomPutStateEvent
    """

    # create matrix broker object
    broker: MatrixBroker = await test_matrix_broker()

    result = await broker.add_mutex_checkpoint_task()

    assert result
    res = await broker.mutex_queue.client.room_get_state_event(
        broker.mutex_queue.room_id,
        "taskiq.schedules",
    )
    assert isinstance(res, RoomGetStateEventResponse)
    assert len(res.content["tasks"]) == 1 
    assert res.content["tasks"][0]["name"] == "taskiq.update_checkpoint"
    assert res.content["tasks"][0]["cron"] == "* * * * *"
    assert res.content["tasks"][0]["labels"]["task_id"] == "mutex_checkpoint"
    assert res.content["tasks"][0]["labels"]["queue"] == "mutex"
    assert res.content["tasks"][0]["args"] == ["mutex"]
    assert res.content["tasks"][0]["kwargs"] == {}

@pytest.mark.integtest
async def test_matrix_broker_add_mutex_checkpoint_task_put_state_error(test_matrix_broker):
    """
    Tests that function returns False if room_put_state returns
    a RoomPutStateError
    """

    # create matrix broker object
    broker: MatrixBroker = await test_matrix_broker()

    # create a dictionary with an empty "tasks" list
    event_content = {"tasks": []}

    await broker.mutex_queue.client.room_put_state(
        room_id=broker.mutex_queue.room_id,
        event_type="taskiq.schedules",
        content=event_content,
    )

    # patch the room_put_state function call
    with patch.object(broker.mutex_queue.client, "room_put_state") as mock_room_put_state:
        # force room_put_state to return a RoomPutStateError
        mock_room_put_state.return_value = RoomPutStateError(message="test error")
        result = await broker.add_mutex_checkpoint_task()

        assert not result
        mock_room_put_state.assert_called_once()

async def test_matrix_broker_add_mutex_checkpoint_task_lock_fail(test_matrix_broker):
    """
    Tests that the function returns False if the MatrixLock().lock(SCHEDULE_STATE_TYPE)
    raises a LockAcquireError exception
    """

    # create matrix broker object
    broker: MatrixBroker = await test_matrix_broker()

    # mock the matrix broker's mutex queue and client
    mock_mutex_queue = MagicMock()
    mock_mutex_queue.client = AsyncMock()
    broker.mutex_queue = mock_mutex_queue

    # create a dictionary with an empty "tasks" list
    event_content = {"tasks": []}

    # set room_get_state_event to return a RoomGetStateEventResponse
    mock_mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventResponse(
        content=event_content, event_type="abc", state_key="abc", room_id="abc"
    )

    # set room_put_state to return a RoomPutStateResponse
    mock_mutex_queue.client.room_put_state.return_value = RoomPutStateResponse(
        event_id="abc", room_id="abc"
    )

    # patch the MatrixLock client to rais a LockAcquireError
    with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.lock.side_effect = LockAcquireError("Test Error")
        result = await broker.add_mutex_checkpoint_task()

        mock_mutex_queue.client.room_put_state.assert_not_called()

        assert not result


@pytest.mark.integtest
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


@pytest.mark.integtest
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


@pytest.mark.integtest
async def test_matrix_broker_startup(test_matrix_broker):
    """
    Tests that all functions are called on startup
    """

    # create a MatrixBroker
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # mock matrix broker function
    matrix_broker.add_mutex_checkpoint_task = AsyncMock()
    matrix_broker.update_checkpoints = AsyncMock()

    res = await matrix_broker.startup()

    # verify that the applicable functions were only called once
    matrix_broker.add_mutex_checkpoint_task.assert_called_once()
    matrix_broker.update_checkpoints.assert_called_once()


async def test_matrix_broker_kick_no_next_batch(test_matrix_broker, test_broker_message):
    """
    Tests that if the broker's backend client doesn't have a next batch, one will be
    provided for it.
    """

    # create a matrix broker object from a fixture
    test_broker = await test_matrix_broker()

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


async def test_matrix_broker_kick_functional_test(test_matrix_broker, test_broker_message):
    """
    Tests that kick calls send_message with the appropriate information
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    mock_client = AsyncMock(spec=FractalAsyncClient)
    mock_client.close = AsyncMock()

    # mock the send_message function
    mock_send_message = AsyncMock()

    # patch the send_message function and the MatrixLock.lock function
    with patch("taskiq_matrix.matrix_broker.FractalAsyncClient", return_value=mock_client):
        with patch("taskiq_matrix.matrix_broker.send_message", mock_send_message):
            with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
                # call kick
                await matrix_broker.kick(test_broker_message)

                # verify that mock lock was not called and that
                # send_message was called with the appropriate information
                mock_lock.assert_not_called()
                mock_send_message.assert_called_with(
                    mock_client, #type:ignore
                    matrix_broker.mutex_queue.room_id,
                    test_broker_message.message,
                    msgtype=matrix_broker.mutex_queue.task_types.task,
                    task_id=test_broker_message.task_id,
                    queue=matrix_broker.mutex_queue.name,
                )

@pytest.mark.skip(reason="run_sync_filter no longer called")
async def test_matrix_broker_kick_sync_filter(test_matrix_broker, test_broker_message):
    """
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()
 
    # copy the empty filter dictionary from the matrix_broker.py file
    test_empty_filter = {
        "presence": {"limit": 0, "types": []},
        "account_data": {"limit": 0, "types": []},
        "room": {
            "rooms": [],
            "state": {"types": [], "limit": 0},
            "timeline": {"types": [], "limit": 0},
            "account_data": {"limit": 0, "types": []},
            "ephemeral": {"limit": 0, "types": []},
        },
    }

    mock_backend_result = AsyncMock(spec=MatrixResultBackend)
    mock_backend_result.matrix_client = AsyncMock()
    mock_backend_result.matrix_client.next_batch = False
    matrix_broker.result_backend = mock_backend_result


    with patch("taskiq_matrix.matrix_broker.run_sync_filter", callable=AsyncMock()) as mock_sync_filter:
        with patch("taskiq_matrix.matrix_broker.send_message", callable=AsyncMock()) as mock_message:

            await matrix_broker.kick(test_broker_message)

            mock_sync_filter.assert_called_once_with(
                mock_backend_result.matrix_client,
                test_empty_filter,
                timeout=0,
            )

            mock_message.assert_called_once()


@pytest.mark.integtest
async def test_matrix_broker_kick_no_task_id(test_matrix_broker, test_broker_message):
    """
    Test that a ValueError is raised when there is no task_id present in
    message.labels
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task
    test_broker_message.labels = {"scheduled_task": "abc"}

    # patch the MatrixLock.clock function
    with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
        # raise ValueError by not having a task_id in message.labels
        with pytest.raises(ScheduledTaskRequiresTaskIdLabel):
            async_gen = await matrix_broker.kick(test_broker_message)
            mock_lock.assert_not_called()

async def test_matrix_broker_kick_lock_success(test_matrix_broker, test_broker_message):
    """
    Tests that the proper message is sent if the lock is successful.
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task and a task_id
    original_task_id = "original task id"
    test_broker_message.labels = {"scheduled_task": "abc", "task_id": original_task_id}

    # mock send_message function
    mock_send_message = AsyncMock()

    # make a second broker message to compare
    comparison_message = test_broker_message
    matrix_broker.id_generator = lambda: "test_task_id"
    test_task_id = matrix_broker.id_generator()
    comparison_message = matrix_broker._use_task_id(test_task_id, comparison_message)
    comparison_task_id = comparison_message.task_id
    comparison_message = comparison_message.message

    # create a client to patch in to ensure we have the same client
    test_client = FractalAsyncClient(
        homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
        access_token=os.environ["MATRIX_ACCESS_TOKEN"],
    )

    # patch the send_message function and the MatrixLock.lock function
    with patch("taskiq_matrix.matrix_broker.send_message", mock_send_message):
        with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
            with patch(
                "taskiq_matrix.matrix_broker.FractalAsyncClient", return_value=test_client
            ):
                async_gen = await matrix_broker.kick(test_broker_message)

                # verify that the lock function was only called once
                mock_lock.assert_called_once()

                # verify that send_message was called with the appropriate information
                mock_send_message.assert_called_with(
                    test_client,
                    matrix_broker.mutex_queue.room_id,
                    comparison_message,
                    msgtype=matrix_broker.mutex_queue.task_types.task,
                    task_id=comparison_task_id,
                    queue=matrix_broker.mutex_queue.name,
                )

async def test_matrix_broker_kick_lock_fail(test_matrix_broker, test_broker_message):
    """
    Tests that a LockAcquireError is raised if the lock fails
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task and a task_id
    test_broker_message.labels = {"scheduled_task": "abc", "task_id": "abcd"}

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


async def test_matrix_broker_kick_no_scheduled_task(test_matrix_broker, test_broker_message):
    """
    Ensure that task id is updated when task_id is passed in the message.labels.
    """

    # create a MatrixBroker object
    matrix_broker: MatrixBroker = await test_matrix_broker()

    # modify the labels property to add a scheduled_task and a task_id
    test_broker_message.labels = {"task_id": "abcd"}

    # mock send_message function
    mock_send_message = AsyncMock()

    # make a second broker message to compare
    comparison_message = test_broker_message
    comparison_message = matrix_broker._use_task_id(
        comparison_message.labels["task_id"], comparison_message
    )
    comparison_message = comparison_message.message.decode("utf-8")

    # create a client to patch in to ensure we have the same client
    test_client = FractalAsyncClient(
        homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
        access_token=os.environ["MATRIX_ACCESS_TOKEN"],
    )

    # patch the send_message function and the MatrixLock.lock function
    with patch("taskiq_matrix.matrix_broker.send_message", mock_send_message):
        with patch("taskiq_matrix.matrix_broker.MatrixLock", autospec=True) as mock_lock:
            with patch(
                "taskiq_matrix.matrix_broker.FractalAsyncClient", return_value=test_client
            ):
                async_gen = await matrix_broker.kick(test_broker_message)

                # verify that the lock function was not called
                mock_lock.assert_not_called()

                # verify that send_message was called with the appropriate information
                mock_send_message.assert_called_with(
                    test_client,
                    matrix_broker.mutex_queue.room_id,
                    comparison_message,
                    msgtype=matrix_broker.mutex_queue.task_types.task,
                    task_id="abcd",
                    queue=matrix_broker.mutex_queue.name,
                )


@pytest.mark.integtest
async def test_matrix_broker_kick_device_queue_raises_exception_if_no_device_label(
    matrix_client: AsyncClient,
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
    test_broker_message: BrokerMessage,
):
    """
    Tasks kicked to the Device queue should raise an exception if the task
    does not have a device label.
    """
    broker = await test_matrix_broker()

    laptop_queue = MatrixQueue(
        "device.laptop",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,  # type:ignore
        device_name="laptop",
    )
    desktop_queue = MatrixQueue(
        "device.desktop",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,  # type:ignore
        device_name="desktop",
    )

    # ensure the replication queue label is set
    test_broker_message.labels = {"queue": "device"}

    # kick task to replication queue
    with pytest.raises(DeviceQueueRequiresDeviceLabel):
        await broker.kick(test_broker_message)

    _, laptop_tasks = await laptop_queue.get_unacked_tasks(timeout=0)
    _, desktop_tasks = await desktop_queue.get_unacked_tasks(timeout=0)

    # neither queue should have the task since it was never kicked
    assert len(laptop_tasks) == 0
    assert len(desktop_tasks) == 0

    # cleanup
    await laptop_queue.shutdown()
    await desktop_queue.shutdown()

async def test_matrix_broker_kick_device_queue_valid_device_label(
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
    test_broker_message: BrokerMessage,
):
    """
    Tests that the msgtype and queue_name reflect what is in the broker message labels if
    if it is for a device.
    """
    broker = await test_matrix_broker()

    test_broker_message.labels = {
        "queue": "device",
        "device": "test device"
    }
    msgtype = broker.device_queue.task_types.device_task(test_broker_message.labels["device"])

    test_client = FractalAsyncClient(
        homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
        access_token=os.environ["MATRIX_ACCESS_TOKEN"],
    )

    # kick task 
    with patch('taskiq_matrix.matrix_broker.send_message', new=AsyncMock()) as mock_send_message:
        with patch(
            "taskiq_matrix.matrix_broker.FractalAsyncClient", return_value=test_client
        ):
            await broker.kick(test_broker_message)

    # verify the message sent
    mock_send_message.assert_called_once_with(
        test_client,
        broker.room_id,
        test_broker_message.message,
        msgtype=msgtype,
        task_id=test_broker_message.task_id,
        queue='device'
    )

async def test_matrix_broker_listen_no_tasks(test_matrix_broker):
    """
    """
    broker = await test_matrix_broker()

    test_device_since = broker.device_queue.client.next_batch
    test_broadcast_since = broker.broadcast_queue.client.next_batch
    test_mutex_since = broker.mutex_queue.client.next_batch
    test_replication_since = broker.replication_queue.client.next_batch

    tasks = []

    with patch('taskiq_matrix.matrix_broker.MatrixBroker.get_tasks', return_value=[]):
        with patch('taskiq_matrix.matrix_broker.logger') as mock_logger:
            async def listen_wrapper():
                async for task in broker.listen():
                    tasks.append(task)
                
            await listen_wrapper()
            
    assert tasks == []
    mock_logger.debug.assert_called_once()

    assert broker.device_queue.checkpoint.since_token == test_device_since
    assert broker.broadcast_queue.checkpoint.since_token == test_broadcast_since
    assert broker.mutex_queue.checkpoint.since_token == test_mutex_since
    assert broker.replication_queue.checkpoint.since_token == test_replication_since


        


