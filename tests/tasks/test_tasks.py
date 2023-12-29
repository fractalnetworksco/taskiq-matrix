import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from taskiq_matrix.matrix_broker import MatrixBroker
from taskiq_matrix.tasks import (
    QueueDoesNotExist,
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

    #? Fix this test, figure out how to make a real replicated queue while still testing
    #? get_or_init_checkpoint

    #! make another test testing the case where it isn't a replicated queue
    """

    test_broker = MatrixBroker()
    test_broker.with_matrix_config(
        os.environ["MATRIX_ROOM_ID"],
        os.environ["MATRIX_HOMESERVER_URL"],
        os.environ["MATRIX_ACCESS_TOKEN"],
    )
    test_broker._init_queues()
    mock_get_or_init = AsyncMock()
    test_broker.replication_queue.checkpoint = MagicMock()
    test_broker.replication_queue.checkpoint.get_or_init_checkpoint = mock_get_or_init

    with patch("taskiq_matrix.tasks.broker", test_broker):
        result = await update_checkpoint("replication")
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

    print('test tasks==========', await matrix_queue.get_tasks())
    print('test queue type', matrix_queue)

    with patch("taskiq_matrix.tasks.broker", broker):
        result = await update_checkpoint("mutex")
