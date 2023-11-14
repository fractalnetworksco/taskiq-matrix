from unittest.mock import AsyncMock, MagicMock, patch
from nio import RoomGetStateEventError, RoomGetStateEventResponse
from homeserver.matrix_taskiq_broker.matrix_queue import (
    AckableMessage,
    LockAcquireError,
    MatrixQueue,
    Task,
    TaskTypes
)
from functools import partial

import json
import pytest
import asyncio

async def test_matrix_queue_verify_room_exists_error():
    """
    Tests that an exception is raised if room_get_state_evetnt() returns a
    RoomGetStateEventError
    """

    # create a MatrixQueue object and mock its room state
    matrix_queue = MatrixQueue(name="test_matrix_queue")
    mock_client = AsyncMock()
    mock_response = RoomGetStateEventError(message="test error message")
    mock_client.room_get_state_event.return_value = mock_response
    matrix_queue.client = mock_client

    # Raise an exception caused by a RoomGetStateEventError
    with pytest.raises(Exception) as e:
        await matrix_queue.verify_room_exists()

    await matrix_queue.client.close()

async def test_matrix_queue_verify_room_exists_exists():
    """
    Tests that an exception is not raised if a RoomGetStateEventResponse
    is returned
    """

    # create a MatrixQueue object and mock its room state
    matrix_queue = MatrixQueue(name="test_matrix_queue")
    mock_client = AsyncMock()
    mock_response = MagicMock(spec=RoomGetStateEventResponse)
        
    # set room_get_state_event() to return the RoomGetStateEventResopnse
    mock_client.room_get_state_event.return_value = mock_response
    matrix_queue.client = mock_client

    # call the function
    await matrix_queue.verify_room_exists()

    # verify that room_get_state_event() was only called once
    matrix_queue.client.room_get_state_event.assert_called_once()
    await matrix_queue.client.close()

async def test_matrix_queue_get_tasks_return_tasks():
    """
    Tests that all tasks are returned by get_tasks()
    """

    # create a MatrixQueue object
    matrix_queue = MatrixQueue(name="test_matrix_queue")

    # create a list of Task dictionaries to use a parameters
    test_task_list = [
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1"}),
                "queue": "mutex",
            },
            "msgtype": "task",
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "task",
        },
    ]

    # create a list of Task objects using the dictionaries from above
    expected_tasks = [Task(**test_task_list[0]), Task(**test_task_list[1])]

    # patch the create_filter and run_sync_filter functions and call get_tasks() 
    with patch(
        "homeserver.matrix_taskiq_broker.matrix_queue.create_filter"
    ) as mock_create_filter:
        mock_create_filter.return_value = {}
        with patch(
            "homeserver.matrix_taskiq_broker.matrix_queue.run_sync_filter", new=AsyncMock()
        ) as mock_sync_filter:
            mock_sync_filter.return_value = {matrix_queue.room_id: test_task_list}
            result = await matrix_queue.get_tasks()

            # verify that the function returned the same tasks that were created locally
            for i in range(len(result)):
                assert result[i].id == expected_tasks[i].id
                assert result[i].type == expected_tasks[i].type
                assert result[i].data == expected_tasks[i].data
                assert result[i].queue == expected_tasks[i].queue
    

async def test_matrix_queue_filter_acked_tasks_proper_filter():
    """
    Test that filter_acked_tasks returns only the Tasks that have
    the correct message type
    """

    # create a list of task dictionaries to use as parameters
    test_task_list = [
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue",
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue.2",
        },
    ]

    # create task objects using the dictionaries and put them into a list
    task1 = Task(**test_task_list[0])
    task2 = Task(**test_task_list[1])
    test_task_objects = [ task1, task2 ]

    # crate a MatrixQueue object and set its task_type properties
    matrix_queue = MatrixQueue(name="test_matrix_queue")
    matrix_queue.task_types.task = "test_matrix_queue"
    matrix_queue.task_types.ack = "test_matrix_queue"

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects)

    # verify that a list of length 1 is returned and that the only item is matching 
    # the Task that was created locally
    assert len(unacked_tasks) == 1
    assert unacked_tasks[0] == test_task_objects[0]

async def test_matrix_queue_get_unacked_tasks():
    """
    NOT DONE
    """

    matrix_queue = MatrixQueue(name="test_matrix_queue")

    event1 = {
        "body": {
            "task_id": "josdfj09b48907w3",
            "task": "{}",
            "queue": "test_matrix_queue",
        },
        "msgtype": "taskiq.test_matrix_queue.task"
    }

    event2 = {
        "body": {
            "task_id": "josdfj09b48907w3",
            "task": "{}",
            "queue": "test_matrix_queue",
        },
        "msgtype": "taskiq.test_matrix_queue.task.ack.josdfj09b48907w3"
    }

    event3 = {
        "body": {
            "task_id": "kdjfosdf-4j239034",
            "task": "{}",
            "queue": "test_matrix_queue",
        },
        "msgtype": "taskiq.test_matrix_queue.task"
    }

    test_tasks = [
        Task(**event1),
        Task(**event2),
        Task(**event3)
    ]
    
    result = await matrix_queue.get_unacked_tasks()
    print(type(result[1]))
    assert isinstance(result[1], list)
    assert len(result[1]) == 1
    assert result[1][0] == test_tasks[2]

async def test_matrix_queue_ack_msg_uses_given_id():
    """
    Tests that ack_msg uses the task_id that is provided
    """

    # create a MatrixQueue object and task_id
    matrix_queue = MatrixQueue(name="test_matrix_queue")
    test_task_id = "abc"

    # create a test message json
    test_message = json.dumps(
        {
            "task_id": test_task_id,
            "task": "{}",
        }
    )

    # patch the send_message function with an AsyncMock
    with patch(
        "homeserver.matrix_taskiq_broker.matrix_queue.send_message", new=AsyncMock()
    ) as mock_message:
        # call ack_msg, passing the task_id that was created locally
        await matrix_queue.ack_msg(test_task_id)

        # verify that send_message was called using the given task_id
        mock_message.assert_called_with(
            matrix_queue.client,
            matrix_queue.room_id,
            message=test_message,
            msgtype=f"{matrix_queue.task_types.ack}.{test_task_id}",
            task_id=test_task_id,
            queue=matrix_queue.name,
        )


async def test_matrix_queue_yield_task_lock_fail():
    """
    Tests that if the MatrixLock.lock() fails in the try block, an 
    exception is raised
    """

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    matrix_queue = MatrixQueue(name="test_matrix_queue")

    # mock the matrix queue's task_is_acked() function
    matrix_queue.task_is_acked = AsyncMock()

    # patch the MatrixLock.lock() function to fail and raise an exception
    with patch(
        "homeserver.matrix_taskiq_broker.matrix_queue.MatrixLock", autospec=True
    ) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.lock.side_effect = LockAcquireError("Test Error")
        with pytest.raises(Exception):
            await matrix_queue.yield_task(test_task)
            matrix_queue.task_is_acked.assert_not_caled()


async def test_matrix_queue_yield_task_already_acked():
    """
    Tests that a yielded task tha has been acked will raise
    an exception
    """

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    matrix_queue = MatrixQueue(name="test_matrix_queue")

    # mock the matrix queue's task_is_acked() function to have it
    # return true
    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = True

    # call yield_task and verify that it raises an exception
    with pytest.raises(Exception) as e:
        await matrix_queue.yield_task(test_task)
        assert e == "Task 1 has already been acked"


async def test_matrix_queue_yield_task_not_acked():
    """ 
    Tests that a yielded tast that has not been acknowledged will 
    return an Ackablemessage object
    """

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    matrix_queue = MatrixQueue(name="test_matrix_queue")

    # mock the matrix queue's task_is_acked() function to have it
    # return true
    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = False

    # call yield_task
    acked_message = await matrix_queue.yield_task(test_task)

    data = test_task_info["body"]["task"].encode("utf-8")
    ack = partial(matrix_queue.ack_msg, "1")

    # verify that the acked message that was returned matches the data that was
    # passed to it
    assert acked_message.data == data
    assert acked_message.ack.func == ack.func
    assert acked_message.ack.args == ack.args
