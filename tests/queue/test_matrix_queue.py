import json
import os
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nio import RoomGetStateEventError, RoomGetStateEventResponse
from taskiq_matrix.matrix_queue import (
    AckableMessage,
    LockAcquireError,
    MatrixQueue,
    Task,
    TaskAlreadyAcked,
    WhoamiError,
    create_room_message_filter,
    create_sync_filter,
)
from taskiq_matrix.utils import send_message


#! ===============================================
@pytest.mark.skip(reason="filter isn't returning acks, synapse issue")
async def test_matrix_queue_get_tasks_(test_matrix_broker):
    """
    #? this returns tasks (acked and unacked)
    #! come back when filter works
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    # await test_broker.startup()
    test_broker._init_queues()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of unacked and acked
    # task events to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex",
        "msgtype": matrix_queue.task_types.task,
    }
    event2 = {
        "task_id": "josdfj09b48900w3",
        "queue": "mutex",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48900w3",
    }
    event3 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex",
        "msgtype": matrix_queue.task_types.task,
    }

    # send messages to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event3),
        queue="mutex",
        msgtype=event3["msgtype"],
        task_id=event3["task_id"],
    )

    matrix_queue.checkpoint.since_token = None

    result = await matrix_queue.get_tasks(timeout=0)
    print(result)

    matrix_queue.checkpoint.since_token = None
    result = await matrix_queue.get_tasks(timeout=0)
    print(result)
    await matrix_queue.shutdown()


async def test_matrix_queue_filter_acked_tasks_exclude_self(
    test_matrix_broker,
):
    """
    Test that filter_acked_tasks returns no tasks if they are all send by the queue's
    client matrix account.
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
            "sender": "sender",
            "room_id": "xyz",
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue.2",
            "sender": "sender",
            "room_id": "xyz",
        },
    ]

    # create task objects using the dictionaries and put them into a list
    task1 = Task(**test_task_list[0])
    task2 = Task(**test_task_list[1])
    test_task_objects = [task1, task2]

    # crate a MatrixQueue object and set its task_type properties
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue
    matrix_queue.task_types.task = "test_matrix_queue"
    matrix_queue.task_types.ack = "test_matrix_queue"
    matrix_queue.client.user_id = "sender"

    # verify that the sender of the tasks are not the same as the queue's client's user id
    assert test_task_list[0]["sender"] is matrix_queue.client.user_id
    assert test_task_list[1]["sender"] is matrix_queue.client.user_id

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects, exclude_self=True)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 0

    await matrix_queue.shutdown()


async def test_matrix_queue_filter_acked_tasks_mixed_tasks_exclude_self_not_sent_by_self(
    test_matrix_broker,
):
    """
    Test that filter_acked_tasks returns acknowledged tasks that were not sent by
    the queue's client matrix account.
    """
    # crate a MatrixQueue object and set its task_type properties
    broker = await test_matrix_broker()

    # ensure user id is set on the client
    await broker.mutex_queue.client.whoami()

    # create a list of task dictionaries to use as parameters
    test_task_list = [
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1"}),
                "queue": "mutex",
            },
            "msgtype": broker.mutex_queue.task_types.task,
            "sender": "someone-else",
            "room_id": "xyz",
        },
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1 ack"}),
                "queue": "mutex",
            },
            "msgtype": f"{broker.mutex_queue.task_types.ack}.1",
            "sender": "someone-else",
            "room_id": "xyz",
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": broker.mutex_queue.task_types.task,
            "sender": "someone-else",
            "room_id": "xyz",
        },
        {
            "body": {
                "task_id": "3",
                "task": json.dumps({"data": "This task should be excluded"}),
                "queue": "mutex",
            },
            "msgtype": broker.mutex_queue.task_types.task,
            "sender": broker.mutex_queue.client.user_id,
            "room_id": "xyz",
        },
    ]

    # create task objects using the dictionaries and put them into a list
    task1 = Task(**test_task_list[0])
    task1_ack = Task(**test_task_list[1])
    task2 = Task(**test_task_list[2])
    task3 = Task(**test_task_list[3])
    test_task_objects = [task1, task1_ack, task2, task3]

    mutex_queue = broker.mutex_queue

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = mutex_queue.filter_acked_tasks(test_task_objects, exclude_self=True)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 1
    assert unacked_tasks[0] == test_task_objects[2]

    await mutex_queue.shutdown()


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_filter_acked_tasks_mixed_tasks_include_self(test_matrix_broker):
    """
    Test that filter_acked_tasks returns all acked tasks, even if sent by the queue's
    client matrix account.
    """
    broker = await test_matrix_broker()
    mutex_queue = broker.mutex_queue

    # ensure user id is set on the client
    await broker.mutex_queue.client.whoami()

    # create a list of task dictionaries to use as parameters
    test_task_list = [
        {
            "body": {
                "task_id": "1",
                "task": json.dumps({"data": "Test Task 1"}),
                "queue": "mutex",
            },
            "msgtype": mutex_queue.task_types.task,
            "sender": broker.mutex_queue.client.user_id,
            "room_id": "xyz",
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": mutex_queue.task_types.task,
            "sender": "not-current-user",
            "room_id": "xyz",
        },
    ]

    # create task objects using the dictionaries and put them into a list
    task1 = Task(**test_task_list[0])
    task2 = Task(**test_task_list[1])
    test_task_objects = [task1, task2]

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = mutex_queue.filter_acked_tasks(test_task_objects)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 2
    assert unacked_tasks[0] == test_task_objects[0]
    assert unacked_tasks[1] == test_task_objects[1]

    await mutex_queue.shutdown()


async def test_matrix_queue_get_unacked_tasks_whoamierror(test_matrix_broker):
    """
    Tests that when the client doesn't have a user id, whoami is called and if it
    returns an error, an exception is raised
    """

    # create a matrix queue
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue

    # mock the client, set the user id to none and set whoami to return an error
    matrix_queue.client = AsyncMock()
    matrix_queue.client.user_id = None
    mock_error = MagicMock(spec=WhoamiError)
    mock_error.message = "test message"
    matrix_queue.client.whoami.return_value = mock_error

    # call get_unacked tasks to raise exception
    with pytest.raises(Exception, match="test message"):
        result = await matrix_queue.get_unacked_tasks(timeout=0)


@pytest.mark.integtest
async def test_matrix_queue_get_unacked_tasks_mixed_tasks(test_matrix_broker):
    """
    Tests that the dictionary returned by get_unacked_tasks() contains only the
    tasks that are not acknowledged by the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id

    await test_broker.startup()
    mutex_queue = test_broker.mutex_queue

    # get latest sync token to avoid seeing other test messages
    mutex_queue.client.next_batch = await mutex_queue.client.get_latest_sync_token(room_id)
    mutex_queue.checkpoint.since_token = mutex_queue.client.next_batch

    # create dictionaries of unacked and acked
    # task events to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex",
        "msgtype": mutex_queue.task_types.task,
    }
    event1_ack = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex",
        "msgtype": f"{mutex_queue.task_types.ack}.josdfj09b48907w3",
    }
    event2 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex",
        "msgtype": mutex_queue.task_types.task,
    }

    # send messages to the queue
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1_ack),
        queue="mutex_queue",
        msgtype=event1_ack["msgtype"],
        task_id=event1_ack["task_id"],
    )
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that a list of size 1 is returned containing the only unacked
    # task that was sent to the queue
    queue_name, result = await mutex_queue.get_unacked_tasks(timeout=0)

    assert queue_name == "mutex"

    assert len(result) == 1
    assert result[0].type == event2["msgtype"]
    assert result[0].id == event2["task_id"]

    await test_broker.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_get_unacked_tasks_only_acked_tasks(test_matrix_broker):
    """
    Tests that get_unacked_tasks() returns a list of size 0 if there are no unacknowledged
    in the queue
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id
    await test_broker.startup()
    mutex_queue: MatrixQueue = test_broker.mutex_queue

    # get latest sync token to avoid seeing other test messages
    mutex_queue.client.next_batch = await mutex_queue.client.get_latest_sync_token(room_id)
    mutex_queue.checkpoint.since_token = mutex_queue.client.next_batch

    # create dictionaries of acked tasks to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": mutex_queue.task_types.task,
    }
    event1_ack = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{mutex_queue.task_types.ack}.josdfj09b48907w3",
    }

    prev_batch = mutex_queue.client.next_batch

    # send acked tasks to the queue
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1_ack),
        queue="mutex_queue",
        msgtype=event1_ack["msgtype"],
        task_id=event1_ack["task_id"],
    )

    # verify that a list of size 0 is returned
    queue, result = await mutex_queue.get_unacked_tasks(timeout=0)
    assert queue == "mutex"
    assert isinstance(result, list)
    assert len(result) == 0

    # checkpoint and client's next_batch should have changed
    assert mutex_queue.client.next_batch != prev_batch
    assert mutex_queue.checkpoint.since_token == mutex_queue.client.next_batch

    await test_broker.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_get_unacked_tasks_no_tasks_in_queue(test_matrix_broker):
    """
    Tests that get_unacked tasks returns an empty list if there are no tasks in
    the queue.
    """

    # create a matrix broker object
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id

    await test_broker.startup()
    mutex_queue = test_broker.mutex_queue

    mutex_queue.client.next_batch = await mutex_queue.client.get_latest_sync_token(room_id)
    mutex_queue.checkpoint.since_token = mutex_queue.client.next_batch

    # call the function without kicking any tasks to the queue
    queue_name, result = await mutex_queue.get_unacked_tasks(timeout=0)

    assert queue_name == "mutex"

    # verify that the queue has no unacked tasks in it
    assert len(result) == 0


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_all_tasks_acked_unacked_tasks_only(test_matrix_broker):
    """
    Tests that att_tasks_acked() returns false if there are only
    unacked tasks in the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id

    await test_broker.startup()
    mutex_queue = test_broker.mutex_queue

    mutex_queue.client.next_batch = await mutex_queue.client.get_latest_sync_token(room_id)
    mutex_queue.checkpoint.since_token = mutex_queue.client.next_batch

    # create dictionaries of unacked tasks to send to the queue
    event1 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex_queue",
        "msgtype": mutex_queue.task_types.task,
    }
    event2 = {
        "task_id": "kaljsdkf030345knvbr",
        "queue": "mutex_queue",
        "msgtype": mutex_queue.task_types.task,
    }

    # send unacked tasks to the queue
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1),
        queue="mutex",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event2),
        queue="mutex",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    assert await mutex_queue.all_tasks_acked() is False

    await test_broker.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_all_tasks_acked_acked_tasks_only(test_matrix_broker):
    """
    Tests that all_tasks_acked() returns True if there are only acked tasks in
    the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id
    await test_broker.startup()
    mutex_queue = test_broker.mutex_queue

    mutex_queue.client.next_batch = await mutex_queue.client.get_latest_sync_token(room_id)
    mutex_queue.checkpoint.since_token = mutex_queue.client.next_batch

    # create dictionaries of acknowledged tasks to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": mutex_queue.task_types.task,
    }
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{mutex_queue.task_types.ack}.josdfj09b48907w3",
    }

    # send the acknowledged tasks to the queue
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event1),
        queue="mutex",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event2),
        queue="mutex",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    assert await mutex_queue.all_tasks_acked() is True

    await test_broker.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_task_is_acked_unacked_task(test_matrix_broker):
    """
    Tests that task_is_acked() returns False if it is given an unacked task.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id

    await test_broker.startup()
    mutex_queue = test_broker.mutex_queue

    # verify that the task is not acknowledged
    task_id = "josdfj09b48907w3"
    assert await mutex_queue.task_is_acked(task_id, room_id) is False

    # create an acked task
    event = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex",
        "msgtype": f"{mutex_queue.task_types.ack}.josdfj09b48907w3",
    }

    # send the acked message to the queue
    await send_message(
        mutex_queue.client,
        room_id,
        message=json.dumps(event),
        queue="mutex",
        msgtype=event["msgtype"],
        task_id=event["task_id"],
    )

    # verify that the task is still unacked
    assert await mutex_queue.task_is_acked(task_id, room_id) is True

    await test_broker.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_ack_msg_uses_given_id(test_matrix_broker):
    """
    Tests that ack_msg uses the task_id that is provided
    """

    # create a MatrixQueue object and task_id
    broker = await test_matrix_broker()
    room_id = broker._test_room_id
    matrix_queue = broker.mutex_queue
    test_task_id = "abc"

    # task shouldn't have an ack yet
    assert await matrix_queue.task_is_acked(test_task_id, room_id) is False

    # ack task
    await matrix_queue.ack_msg(test_task_id, room_id)

    # task should now have an ack
    assert await matrix_queue.task_is_acked(test_task_id, room_id) is True

    await matrix_queue.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_yield_task_lock_fail(test_matrix_broker):
    """
    Tests that if the MatrixLock.lock() fails in the try block, an
    exception is raised
    """
    # create a MatrixQueue object
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
        "sender": "sender",
        "room_id": room_id,
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    matrix_queue = broker.mutex_queue

    # mock the matrix queue's task_is_acked() function
    matrix_queue.task_is_acked = AsyncMock()

    # patch the MatrixLock.lock() function to fail and raise an exception
    with patch("taskiq_matrix.matrix_queue.MatrixLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.lock.side_effect = LockAcquireError("Test Error")
        with pytest.raises(Exception):
            await matrix_queue.yield_task(test_task)
        matrix_queue.task_is_acked.assert_not_called()

    await matrix_queue.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_yield_task_already_acked(test_matrix_broker):
    """
    Tests that a yielded task tha has been acked will raise
    an exception
    """
    broker = await test_matrix_broker()
    room_id = broker._test_room_id

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
        "sender": "sender",
        "room_id": room_id,
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    matrix_queue = broker.mutex_queue

    await matrix_queue.ack_msg(test_task.id, room_id)

    # call yield_task and verify that it raises an exception
    with pytest.raises(TaskAlreadyAcked):
        await matrix_queue.yield_task(test_task)

    await matrix_queue.shutdown()


@pytest.mark.integtest
async def test_matrix_queue_yield_task_not_acked(test_matrix_broker):
    """
    Tests that a yielded tast that has not been acknowledged will
    return an Ackablemessage object
    """
    broker = await test_matrix_broker()
    room_id = broker._test_room_id
    matrix_queue = broker.mutex_queue

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
        "sender": "sender",
        "room_id": room_id,
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # call yield_task
    acked_message = await matrix_queue.yield_task(test_task)

    # establish what should be returned by yield_task
    data = test_task_info["body"]["task"].encode("utf-8")
    ack = partial(matrix_queue.ack_msg, "1", room_id)

    # verify that the acked message that was returned matches the data that was
    # passed to it
    assert isinstance(acked_message, AckableMessage)
    assert acked_message.data == data  # type:ignore
    assert acked_message.ack.func == ack.func  # type:ignore
    assert acked_message.ack.args == ack.args  # type:ignore

    await matrix_queue.shutdown()


async def test_matrix_queue_yield_task_no_lock(test_matrix_broker):
    """
    Tests that the task type is not locked when the lock arg is set to False.
    """

    # create a broker object
    test_broker = await test_matrix_broker()
    room_id = test_broker._test_room_id

    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create a task dictionary to use as a parameter
    test_task_info = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "task",
        "sender": "sender",
        "room_id": room_id,
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # establish what the function should return
    expected_result = json.dumps(test_task.data).encode("utf-8")

    with patch("taskiq_matrix.matrix_queue.MatrixLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        result = await matrix_queue.yield_task(test_task, ignore_acks=True, lock=False)
        lock_instance.assert_not_called()

    # verify that yield_task returned bytes
    assert isinstance(result, bytes)
    assert result == expected_result
