import json
from functools import partial
from unittest.mock import AsyncMock, patch

import pytest
from nio import RoomGetStateEventError
from taskiq_matrix.matrix_queue import LockAcquireError, MatrixQueue, Task


async def test_matrix_queue_verify_room_error():
    """
    Tests
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


async def test_matrix_queue_get_tasks_return_tasks():
    """
    Tests that all tasks are returned by get_tasks()
    ~~~add to spreadsheet~~~
    """

    matrix_queue = MatrixQueue(name="test_matrix_queue")

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

    expected_tasks = [Task(**test_task_list[0]), Task(**test_task_list[1])]

    with patch("taskiq_matrix.matrix_queue.create_filter") as mock_create_filter:
        mock_create_filter.return_value = {}
        with patch(
            "taskiq_matrix.matrix_queue.run_sync_filter", new=AsyncMock()
        ) as mock_sync_filter:
            mock_sync_filter.return_value = {matrix_queue.room_id: test_task_list}
            result = await matrix_queue.get_tasks()

            for i in range(len(result)):
                assert result[i].id == expected_tasks[i].id
                assert result[i].type == expected_tasks[i].type
                assert result[i].data == expected_tasks[i].data
                assert result[i].queue == expected_tasks[i].queue


@pytest.mark.skip(reason="Tasks need to use the correct queue types for this to work")
async def test_matrix_queue_filter_acked_tasks_proper_filter():
    """
    why is this returning a list of length 0
    """
    matrix_queue = MatrixQueue(name="test_matrix_queue")

    test_task_list = [
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1"}),
                "queue": "mutex",
            },
            "msgtype": "task",  # should be matrix_queue.task_types.task
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "task",  # should be matrix_queue.task_types.ack for the task id above
        },
    ]

    test_task_objects = [
        Task(acknowledged=False, **test_task_list[0]),
        Task(acknowledged=True, **test_task_list[1]),
    ]

    unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects)

    assert len(unacked_tasks) == 1
    assert unacked_tasks[0] == test_task_objects[0]


async def test_matrix_queue_ack_msg_uses_given_id():
    """
    Tests that ack_msg uses the task_id that is provided
    ~~~add to spreadsheet~~~
    """

    matrix_queue = MatrixQueue(name="test_matrix_queue")
    test_task_id = "abc"

    test_message = json.dumps(
        {
            "task_id": test_task_id,
            "task": "{}",
        }
    )

    with patch("taskiq_matrix.matrix_queue.send_message", new=AsyncMock()) as mock_message:
        await matrix_queue.ack_msg(test_task_id)

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
    ~~~add to spreadsheet~~~
    """

    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    test_task = Task(**test_task_info)

    matrix_queue = MatrixQueue(name="test_matrix_queue")

    matrix_queue.task_is_acked = AsyncMock()

    with patch("taskiq_matrix.matrix_queue.MatrixLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.lock.side_effect = LockAcquireError("Test Error")
        with pytest.raises(Exception):
            await matrix_queue.yield_task(test_task)
            matrix_queue.task_is_acked.assert_not_caled()


async def test_matrix_queue_yield_task_already_acked():
    """
    Tests that a yielded task tha has been acked will raise
    an exception
    ~~~add to spreadsheet~~~
    """

    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    test_task = Task(**test_task_info)

    matrix_queue = MatrixQueue(name="test_matrix_queue")

    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = True

    with pytest.raises(Exception) as e:
        await matrix_queue.yield_task(test_task)
        assert e == "Task 1 has already been acked"


async def test_matrix_queue_yield_task_not_acked():
    """
    Tests that a yielded tast that has not been acknowledged will
    return an Ackablemessage object
    ~~~add to spreadsheet~~~
    """

    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
    }

    test_task = Task(**test_task_info)

    matrix_queue = MatrixQueue(name="test_matrix_queue")

    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = False

    acked_message = await matrix_queue.yield_task(test_task)

    data = test_task_info["body"]["task"].encode("utf-8")
    ack = partial(matrix_queue.ack_msg, "1")

    assert acked_message.data == data
    assert acked_message.ack.func == ack.func
    assert acked_message.ack.args == ack.args
