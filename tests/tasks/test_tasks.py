import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nio import MessageDirection, RoomMessagesResponse
from taskiq_matrix.matrix_broker import MatrixBroker
from taskiq_matrix.tasks import (
    QueueDoesNotExist,
    RoomContextError,
    get_first_unacked_task,
    update_checkpoint,
)
from taskiq_matrix.utils import send_message


async def test_tasks_update_checkpoint_queue_does_not_exist():
    """
    Tests that calling update_checkpoint passing a queue name that does not match an
    existing queue raises an exception.
    """

    # create a fake queue name
    queue_name = "fake queue to raise exception"

    # call update_checkpoint with a non-existent queue name to raise an exception
    with pytest.raises(QueueDoesNotExist, match=f"Queue {queue_name} does not exist"):
        result = await update_checkpoint(queue_name=queue_name)


async def test_tasks_update_checkpoint_is_replicated_queue(test_matrix_broker):
    """
    Tests that get_or_init_checkpoint is called passing full_sync as True if the queue
    is a ReplicatedQueue object.
    """

    # create a broker fixture
    test_broker = await test_matrix_broker()

    # mock the get_or_init_checkpoint function from the broker
    mock_get_or_init = AsyncMock()
    test_broker.replication_queue.checkpoint.get_or_init_checkpoint = mock_get_or_init

    # patch the task.py broker with the broker fixture
    with patch("taskiq_matrix.tasks.broker", test_broker):
        # NOTE an exception is raised in run_sync_filter but is not relavent to
        # this test
        with pytest.raises(Exception):
            # call update_checkpoint passing "replication" as the queue name
            result = await update_checkpoint("replication")

    # verify that get_or_init_checkpoint was called
    mock_get_or_init.assert_called_with(full_sync=True)


async def test_tasks_update_checkpoint_no_tasks(test_matrix_broker):
    """
    Tests that the "if tasks:" block of code does not run if there are no tasks in the
    queue.
    """

    # create a MatrixBroker object from fixture
    broker = await test_matrix_broker()

    # patch the get_first_unacked_task function to verify it was not called
    with patch("taskiq_matrix.tasks.get_first_unacked_task") as mock_get_first_unacked:
        # patch the logger to verify its function calls
        with patch("taskiq_matrix.tasks.logger") as mock_logger:
            # call update_checkpoint
            result = await update_checkpoint("mutex")

        # verify that get_first_unacked_task was not called
        mock_get_first_unacked.assert_not_awaited()

        # verify that logger.info was called twice and that logger.debug was called
        # with the expected string
        assert mock_logger.info.call_count == 2
        mock_logger.debug.assert_called_once_with("Task update_checkpoint: No tasks found")

    assert result


@pytest.mark.skip(reason="not returning acks, might be related to the synapse issue")
async def test_tasks_update_checkpoint_mixed_tasks(test_matrix_broker):
    """
    #? This might be an issue with the same Synapse-glob issue not returning acks
    """

    broker = await test_matrix_broker()
    matrix_queue = broker.mutex_queue

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

    print("test tasks==========", await matrix_queue.get_tasks())
    print("test queue type", matrix_queue)

    with patch("taskiq_matrix.tasks.broker", broker):
        result = await update_checkpoint("mutex")


async def test_tasks_update_checkpoint_unable_to_update(test_matrix_broker):
    """
    Tests that an exception is raised if the
    """

    # create a broker fixture
    test_broker = await test_matrix_broker()

    # mock the queue's put_checkpoint_state function to return False to raise an exception
    mock_checkpoint_state = AsyncMock()
    mock_checkpoint_state.return_value = False
    test_broker.mutex_queue.checkpoint.put_checkpoint_state = mock_checkpoint_state

    # patch the task.py broker with the broker created locally
    with patch("taskiq_matrix.tasks.broker", test_broker):
        # call update_checkpoint to raise an exception
        with pytest.raises(Exception) as e:
            await update_checkpoint("mutex")

        # verify that the exception that was raised matches what was expected
        assert str(e.value) == "Failed to update checkpoint mutex."


async def test_tasks_update_checkpoint_unacked_tasks_only(test_matrix_broker):
    """
    ? not recognizing unacked tasks
    """

    test_broker = await test_matrix_broker()

    await test_broker.startup()

    matrix_queue = test_broker.mutex_queue

    # create unacked task events
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}",
    }

    event3 = {
        "task_id": "josdfj09b48907w4",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}",
    }

    assert not await matrix_queue.task_is_acked(event2["task_id"])
    assert not await matrix_queue.task_is_acked(event3["task_id"])

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

    with patch("taskiq_matrix.tasks.broker", test_broker):
        await update_checkpoint("mutex")


async def test_tasks_update_checkpoint_unacked_tasks_room_context_error(test_matrix_broker):
    """ """
    test_broker = await test_matrix_broker()

    await test_broker.startup()

    matrix_queue = test_broker.mutex_queue

    #!============
    # ? Get the above test to
    # ? work and paste here
    #!============
    matrix_queue.client.room_context = AsyncMock(return_value=MagicMock(spec=RoomContextError))

    with patch("taskiq_matrix.tasks.broker", test_broker):
        with pytest.raises(Exception):
            await update_checkpoint("mutex")


async def test_tasks_update_checkpoint_acked_tasks_only(test_matrix_broker, test_broker_message):
    """
    ? not recognizing the tasks, instead skipping the "no tasks found" logger
    ? debug function call
    
    ! kick a task to the broker and see if that works
    """

    test_broker = await test_matrix_broker()

    await test_broker.startup()

    matrix_queue = test_broker.mutex_queue
    test_broker_message.labels = {"test": "labels"}
    await test_broker.kick(test_broker_message)

    # create unacked task events
    event2 = {
        "task_id": "josdfj09b48907w3",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
    }

    event3 = {
        "task_id": "josdfj09b48907w4",
        "queue": "mutex_queue",
        "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w4",
    }

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

    assert await matrix_queue.task_is_acked(event2["task_id"])
    assert await matrix_queue.task_is_acked(event3["task_id"])

    with patch("taskiq_matrix.tasks.broker", test_broker):
        await update_checkpoint("mutex")
