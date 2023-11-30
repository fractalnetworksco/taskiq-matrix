import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nio import RoomContextResponse

from taskiq_matrix.filters import run_sync_filter
from taskiq_matrix.matrix_queue import MatrixQueue
from taskiq_matrix.tasks import QueueDoesNotExist, broker, update_checkpoint
from taskiq_matrix.utils import send_message


@pytest.mark.integtest  # depends on QueueDoesNotExist and AttributeError
async def test_update_checkpoint_attribute_error():
    """
    Tests that a QeueuDoesNotExist error is raised when a queue name is passed as a parameter
    that doesn't match an existing queue.
    """

    queue_name = "test"
    expected_error = f"Queue {queue_name} does not exist"

    with patch("taskiq_matrix.tasks.getattr", MagicMock(side_effect=AttributeError())):
        with pytest.raises(QueueDoesNotExist, match=expected_error):
            await update_checkpoint(queue_name)


async def test_update_checkpoint_no_tasks():
    """
    Tests that the function proceeds to the "else" block if there are no tasks. Verified by
    asserting that get_first_unacked_task() is not called.
    """

    with patch(
        "taskiq_matrix.tasks.get_first_unacked_task", new_callable=AsyncMock
    ) as mock_unacked_task:
        await update_checkpoint("mutex")
        mock_unacked_task.assert_not_called()


async def test_update_checkpoint_unacked_task(test_matrix_broker):
    """ 
    Test that context.start is used as the new checkpoint if there is an unacked task
    ! find a way to check context.start to verify
    """
    broker = await test_matrix_broker()
    await broker.startup()

    with patch("taskiq_matrix.tasks.broker", broker) as test_broker:
        matrix_queue: MatrixQueue = test_broker.mutex_queue

        await matrix_queue.checkpoint.get_or_init_checkpoint()

        event2 = {
            "task_id": "kdjfosdf-4j239034",
            "queue": "mutex",
            "msgtype": matrix_queue.task_types.task,
        }

        await send_message(
            matrix_queue.client,
            matrix_queue.room_id,
            message=json.dumps(event2),
            queue="mutex",
            msgtype=event2["msgtype"],
            task_id=event2["task_id"],
        )


        await update_checkpoint("mutex")

    await broker.shutdown()


async def test_update_checkpoint_acked_task(test_matrix_broker):
    """ 
    """
    broker = await test_matrix_broker()
    await broker.startup()

    with patch("taskiq_matrix.tasks.broker", broker) as test_broker:
        matrix_queue: MatrixQueue = test_broker.mutex_queue

        checkpoint = await matrix_queue.checkpoint.get_or_init_checkpoint()

        event2 = {
            "task_id": "josdfj09b48907w3",
            "queue": "mutex",
            "msgtype": f"{matrix_queue.task_types.ack}.josdfj09b48907w3",
        }

        await send_message(
            matrix_queue.client,
            matrix_queue.room_id,
            message=json.dumps(event2),
            queue="mutex",
            msgtype=event2["msgtype"],
            task_id=event2["task_id"],
        )

        

        await update_checkpoint("mutex")

    await broker.shutdown()