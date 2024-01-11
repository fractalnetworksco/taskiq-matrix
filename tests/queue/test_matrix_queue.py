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


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_verify_room_exists_error(test_matrix_broker):
    """
    Tests that an exception is raised if room_get_state_evetnt() returns a
    RoomGetStateEventError
    """

    # create a MatrixQueue object and mock its room state
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue
    mock_client = AsyncMock()
    mock_response = RoomGetStateEventError(message="test error message")
    mock_client.room_get_state_event.return_value = mock_response
    matrix_queue.client = mock_client

    # Raise an exception caused by a RoomGetStateEventError
    with pytest.raises(Exception) as e:
        await matrix_queue.verify_room_exists()

    await matrix_queue.shutdown()


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_verify_room_exists_exists(test_matrix_broker):
    """
    Tests that an exception is not raised if a RoomGetStateEventResponse
    is returned
    """

    # create a MatrixQueue object and mock its room state
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue
    mock_client = AsyncMock()
    mock_response = MagicMock(spec=RoomGetStateEventResponse)

    # set room_get_state_event() to return the RoomGetStateEventResopnse
    mock_client.room_get_state_event.return_value = mock_response
    matrix_queue.client = mock_client

    # call the function
    await matrix_queue.verify_room_exists()

    # verify that room_get_state_event() was only called once
    matrix_queue.client.room_get_state_event.assert_called_once()
    await matrix_queue.shutdown()

#! ===============================================
@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
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
    print('msgtype=======', event2['msgtype'])

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
    print('roomid=====', matrix_queue.room_id)

    matrix_queue.checkpoint.since_token = None

    result = await matrix_queue.get_tasks(timeout=0)
    print(result)
    print('lenth of list======', len(result))

    matrix_queue.checkpoint.since_token = None
    result = await matrix_queue.get_tasks(timeout=0)
    print(result)
    print('lenth of list======', len(result))
    await matrix_queue.shutdown()

async def test_matrix_queue_get_tasks_no_filter_caught_up(test_matrix_broker, test_broker_message):
    """
    Tests that create_room_message_filter and run_room_message_filter are not called if
    the queue is caught up.
    """

    # create a broker object
    broker = await test_matrix_broker()

    # create a broker message and kick it
    message = test_broker_message
    await broker.kick(message)

    # set the broker's mutex_queue caught_up attribute to True
    broker.mutex_queue.caught_up = True

    # patch create_room_message_filter to verify it wasn't called
    with patch('taskiq_matrix.matrix_queue.create_room_message_filter') as mock_create_room_filter:
        # patch run_room_message_filter to verify it wasn't called
        with patch('taskiq_matrix.matrix_queue.run_room_message_filter') as mock_run_room_message_filter:
            tasks = await broker.mutex_queue.get_tasks(timeout=0)

    # verify that the room filter function calls were not made
    mock_create_room_filter.assert_not_called()
    mock_run_room_message_filter.assert_not_called()

    # verify that the task that was kicked to the broker is the same as the one
        # that was returned
    assert tasks[0].id == message.task_id

async def test_matrix_queue_get_tasks_existing_filter_not_caught_up(test_matrix_broker, test_broker_message):
    """
    Tests that filters are not created again if a filter is passed as a parameter to 
    get_tasks.
    """

    # create a broker object
    broker = await test_matrix_broker()

    # create a broker message and kick it
    message = test_broker_message
    await broker.kick(message)

    # set the broker's mutex_queue caught_up attribute to False
    broker.mutex_queue.caught_up = False

    # create a task filter
    task_filter = create_room_message_filter(
        broker.room_id,
        types=[broker.mutex_queue.task_types.task, f"{broker.mutex_queue.task_types.ack}.*"]
    )

    # patch create_room_message_filter to verify it was not called
    with patch('taskiq_matrix.matrix_queue.create_room_message_filter') as mock_create_room_message_filter:
        # patch create_sync_message_filter to verify it was not called
        with patch('taskiq_matrix.matrix_queue.create_sync_filter') as mock_create_sync_filter:
            # patch run_sync_filter to verify it was not called
            with patch('taskiq_matrix.matrix_queue.run_sync_filter') as mock_run_sync_filter:
                tasks = await broker.mutex_queue.get_tasks(
                    timeout=0,
                    task_filter=task_filter
                )

    # verify that filter functions were not called
    mock_create_room_message_filter.assert_not_called()
    mock_run_sync_filter.assert_not_called()
    mock_create_sync_filter.assert_not_called()

    # verify that the task that was kicked to the broker is the same as the one
        # that was returned
    assert tasks[0].id == message.task_id

async def test_matrix_queue_get_tasks_existing_filter_caught_up(test_matrix_broker, test_broker_message):
    """
    #? not related to this test but how do you hit "not end:" ?
    """

    # create a broker object
    broker = await test_matrix_broker()

    # create a broker message and kick it
    message = test_broker_message
    await broker.kick(message)

    # set the broker's mutex_queue caught_up attribute to True
    broker.mutex_queue.caught_up = True

    # create a filter to pass as a parameter
    task_filter = create_sync_filter(
        broker.room_id,
        types=[broker.mutex_queue.task_types.task, f"{broker.mutex_queue.task_types.ack}.*"]
    )

    # patch filter funcions to verify that they are not called
    with patch('taskiq_matrix.matrix_queue.run_room_message_filter') as mock_run_room_message_filter:
        with patch('taskiq_matrix.matrix_queue.create_sync_filter') as mock_create_sync_filter:
            with patch('taskiq_matrix.matrix_queue.create_room_message_filter') as mock_create_room_message_filter:
                tasks = await broker.mutex_queue.get_tasks(
                    timeout=0,
                    task_filter=task_filter,
                )
    
    # verify that the filter functions are not called
    mock_run_room_message_filter.assert_not_called()
    mock_create_sync_filter.assert_not_called()
    mock_create_room_message_filter.assert_not_called()

    # verify that the task returned has a task_id matching what was created locally
    assert tasks[0].id == message.task_id
    
async def test_matrix_queue_get_tasks_not_end(test_matrix_broker, test_broker_message):
    """
    ! fix this test
    """

    # create a broker object
    broker = await test_matrix_broker()

    # create a broker message and kick it
    message = test_broker_message
    await broker.kick(message)

    await broker.mutex_queue.checkpoint.get_or_init_checkpoint(full_sync=True)

    print('full sync')

    tasks = await broker.mutex_queue.get_tasks(timeout=0)


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_filter_acked_tasks_mixed_tasks_exclude_self_sent_by_self(test_matrix_broker):
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
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue.2",
            "sender": "sender",
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
    assert test_task_list[0]['sender'] is matrix_queue.client.user_id
    assert test_task_list[1]['sender'] is matrix_queue.client.user_id

    with patch('taskiq_matrix.matrix_queue.logger', new=MagicMock()) as mock_logger:
        # call filter_acked_tasks and store the result in unacked_tasks
        unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects, exclude_self=True)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 0
    mock_logger.warning.assert_called_once()

    await matrix_queue.shutdown()

@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_filter_acked_tasks_mixed_tasks_exclude_self_not_sent_by_self(test_matrix_broker):
    """
    Test that filter_acked_tasks returns acknowledged tasks that were not sent by 
    the queue's client matrix account.
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
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue.2",
            "sender": "sender",
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


    # verify that the sender of the tasks are not the same as the queue's client's user id
    assert test_task_list[0]['sender'] is not matrix_queue.client.user_id
    assert test_task_list[1]['sender'] is not matrix_queue.client.user_id

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects, exclude_self=True)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 1
    assert unacked_tasks[0] == test_task_objects[0]

    await matrix_queue.shutdown()


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_filter_acked_tasks_mixed_tasks_include_self(test_matrix_broker):
    """
    Test that filter_acked_tasks returns all acked tasks, even if sent by the queue's
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
        },
        {
            "body": {
                "task_id": "2",
                "task": json.dumps({"data": "Test Task 2"}),
                "queue": "mutex",
            },
            "msgtype": "test_matrix_queue.2",
            "sender": "sender",
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
    matrix_queue.client.user_id = 'sender'

    assert test_task_list[0]['sender'] is matrix_queue.client.user_id
    assert test_task_list[1]['sender'] is matrix_queue.client.user_id

    # call filter_acked_tasks and store the result in unacked_tasks
    unacked_tasks = matrix_queue.filter_acked_tasks(test_task_objects)

    # verify that a list of length 1 is returned and that the only item is matching
    # the Task that was created locally
    assert len(unacked_tasks) == 1
    assert unacked_tasks[0] == test_task_objects[0]

    await matrix_queue.shutdown()

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


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_get_unacked_tasks_mixed_tasks(test_matrix_broker):
    """
    Tests that the dictionary returned by get_unacked_tasks() contains only the
    tasks that are not acknowledged by the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of unacked and acked
    # task events to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }
    event2 = {
        "task_id": "josdfj09b48900w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48900w3",
    }
    event3 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }

    # send messages to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event3),
        queue="mutex_queue",
        msgtype=event3["msgtype"],
        task_id=event3["task_id"],
    )

    # verify that a list of size 2 is returned containing the only unacked
    # task that was sent to the queue
    result = await matrix_queue.get_unacked_tasks(timeout=0)
    assert isinstance(result[1], list)
    assert len(result[1]) == 2
    assert result[1][0].type == event1["msgtype"]
    assert result[1][0].id == event1["task_id"]
    assert result[1][1].type == event3["msgtype"]
    assert result[1][1].id == event3["task_id"]

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_get_unacked_tasks_only_acked_tasks(test_matrix_broker):
    """
    Tests that get_unacked_tasks() returns a list of size 0 if there are no unacknowledged
    in the queue
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of acked tasks to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }

    # send acked tasks to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that a list of size 0 is returned
    result = await matrix_queue.get_unacked_tasks(timeout=0)
    assert isinstance(result[1], list)
    assert len(result[1]) == 0

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_get_unacked_tasks_only_unacked_tasks(test_matrix_broker):
    """
    Tests that a list of multiple unacked tasks are returned if there are
    are more than one unacked tasks in the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of unacked tasks to send to the queue
    event1 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }
    event2 = {
        "task_id": "kdjfosdf-4j2334735r",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }

    # send the unacked tasks to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that a list of size 2 is returned containing both of the unacked
    # tasks sent to the queue
    result = await matrix_queue.get_unacked_tasks(timeout=0)
    assert isinstance(result[1], list)
    assert len(result[1]) == 2

    await test_broker.shutdown()

async def test_matrix_queue_get_unacked_tasks_no_tasks_in_queue(test_matrix_broker):
    """
    Tests that get_unacked tasks returns an empty list if there are no tasks in 
    the queue.
    """

    # create a matrix broker object
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # call the function without kicking any tasks to the queue
    result = await matrix_queue.get_unacked_tasks(timeout=0)
    
    # verify that the queue has no unacked tasks in it
    assert len(result[1]) == 0


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_all_tasks_acked_unacked_tasks_only(test_matrix_broker):
    """
    Tests that att_tasks_acked() returns false if there are only
    unacked tasks in the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of unacked tasks to send to the queue
    event1 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }
    event2 = {
        "task_id": "kdjfosdf-4j2334735r",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }

    # send unacked tasks to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that all_tasks_acked() returns False
    assert await matrix_queue.all_tasks_acked() == False

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_all_tasks_acked_acked_tasks_only(test_matrix_broker):
    """
    Tests that all_tasks_acked() returns True if there are only acked tasks in
    the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of acknowledged tasks to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }

    # send the acknowledged tasks to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that all_tasks_acked() returns True
    assert await matrix_queue.all_tasks_acked()

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_all_tasks_acked_mixed_tasks(test_matrix_broker):
    """
    Tests that all_tasks_acked() returns False if there are both acked and unacked
    tasks in the queue.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create dictionaries of unacked and acked tasks to be sent to the queue
    event1 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }
    event3 = {
        "task_id": "kdjfosdf-4j239034",
        "queue": "mutex_queue",
        "msgtype": matrix_queue.task_types.task,
    }

    # send the tasks to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event1),
        queue="mutex_queue",
        msgtype=event1["msgtype"],
        task_id=event1["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event3),
        queue="mutex_queue",
        msgtype=event3["msgtype"],
        task_id=event3["task_id"],
    )

    # verify that not all tasks are acknowledged
    assert await matrix_queue.all_tasks_acked() == False

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_task_is_acked_unacked_task(test_matrix_broker):
    """
    Tests that task_is_acked() returns False if it is given an unacked task.
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create an unacked task
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}",
    }

    # verify that the task is not acknowledged
    assert not await matrix_queue.task_is_acked(event2["task_id"])

    # send the unacked message to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that the task is still unacked
    assert await matrix_queue.task_is_acked(event2["task_id"]) == False

    await test_broker.shutdown()


@pytest.mark.integtest  # depends on MatrixBroker clients and send_message
async def test_matrix_queue_task_is_acked_acked_task(test_matrix_broker):
    """
    Tests that task_is_acked returns True of it is given an acknowledged task
    """

    # create a broker object using fixture
    test_broker = await test_matrix_broker()
    await test_broker.startup()
    matrix_queue = test_broker.mutex_queue

    # create an acknowledged task
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }

    # verify that that the acknowledged task is not in the queue yet
    assert not await matrix_queue.task_is_acked(event2["task_id"])

    # send the task to the queue
    await send_message(
        matrix_queue.client,
        matrix_queue.room_id,
        message=json.dumps(event2),
        queue="mutex_queue",
        msgtype=event2["msgtype"],
        task_id=event2["task_id"],
    )

    # verify that the task is acknowledged and sent to the queue
    assert await matrix_queue.task_is_acked(event2["task_id"])

    await test_broker.shutdown()


@pytest.mark.integtest  # depends an AsyncClient, Checkpoint, and TaskTypes in the class constructor
async def test_matrix_queue_ack_msg_uses_given_id(test_matrix_broker):
    """
    Tests that ack_msg uses the task_id that is provided
    """

    # create a MatrixQueue object and task_id
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue
    test_task_id = "abc"

    # create a test message json
    test_message = json.dumps(
        {
            "task_id": test_task_id,
            "task": "{}",
        }
    )

    # patch the send_message function with an AsyncMock
    with patch("taskiq_matrix.matrix_queue.send_message", new=AsyncMock()) as mock_message:
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

    await matrix_queue.shutdown()

@pytest.mark.integtest  # depends on Task
async def test_matrix_queue_yield_task_lock_fail(test_matrix_broker):
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
        "sender": "sender",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    broker = await test_matrix_broker()
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


@pytest.mark.integtest  # depends on Task
async def test_matrix_queue_yield_task_already_acked(test_matrix_broker):
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
        "sender": "sender",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue

    # mock the matrix queue's task_is_acked() function to have it
    # return true
    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = True

    # call yield_task and verify that it raises an exception
    with pytest.raises(TaskAlreadyAcked):
        await matrix_queue.yield_task(test_task)

    await matrix_queue.shutdown()


@pytest.mark.integtest  # depends on Task
async def test_matrix_queue_yield_task_not_acked(test_matrix_broker):
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
        "sender": "sender",
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # create a MatrixQueue object
    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue

    # mock the matrix queue's task_is_acked() function to have it
    # return true
    matrix_queue.task_is_acked = AsyncMock()
    matrix_queue.task_is_acked.return_value = False

    # establish what should be returned by yield_task
    data = test_task_info["body"]["task"].encode("utf-8")
    ack = partial(matrix_queue.ack_msg, "1")

    # call yield_task
    acked_message = await matrix_queue.yield_task(test_task)

    # establish what should be returned by yield_task
    data = test_task_info["body"]["task"].encode("utf-8")
    ack = partial(matrix_queue.ack_msg, "1")

    # verify that the acked message that was returned matches the data that was
    # passed to it
    assert isinstance(acked_message, AckableMessage)
    assert acked_message.data == data #type:ignore
    assert acked_message.ack.func == ack.func #type:ignore
    assert acked_message.ack.args == ack.args #type:ignore

    await matrix_queue.shutdown()

async def test_matrix_queue_yield_task_no_lock(test_matrix_broker):
    """
    Tests that the task type is not locked when the lock arg is set to False.
    """

    # create a broker object
    test_broker = await test_matrix_broker()
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
    }

    # create a Task object using the dictionary from above
    test_task = Task(**test_task_info)

    # establish what the function should return
    expected_result = json.dumps(test_task.data).encode('utf-8') 

    # patch the logger in lock.py 
    with patch('taskiq_matrix.lock.logger', new=MagicMock()) as mock_lock_logger:
        result = await matrix_queue.yield_task(test_task, ignore_acks=True, lock=False)

    # verify that the lock's logger is never used
    mock_lock_logger.debug.assert_not_called()

    # verify that yield_task returned bytes 
    assert isinstance(result, bytes)
    assert result == expected_result