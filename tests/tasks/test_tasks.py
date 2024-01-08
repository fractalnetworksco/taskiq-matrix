import json
import os
import time
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from nio import MessageDirection, RoomContextResponse, RoomMessagesResponse
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


@pytest.mark.integtest
async def test_tasks_update_checkpoint_mixed_tasks(
    test_matrix_broker, test_multiple_broker_message
):
    """
    Tests that if there are unacked tasks in the queue, the queue's checkpoint is
    updated to before the first unacked task.
    """

    test_broker = await test_matrix_broker()
    messages = await test_multiple_broker_message(3)
    starting_checkpoint = await test_broker.mutex_queue.checkpoint.get_or_init_checkpoint(
        full_sync=False
    )

    # assign labels to the broker message
    message_1 = messages[0]
    message_2 = messages[1]
    message_3 = messages[2]

    # kick the broker message to the broker
    await test_broker.kick(message_1)
    await test_broker.kick(message_2)
    await test_broker.kick(message_3)

    await test_broker.mutex_queue.ack_msg(message_1.task_id)
    await test_broker.mutex_queue.ack_msg(message_2.task_id)

    with patch("taskiq_matrix.tasks.broker", test_broker):
        with patch("taskiq_matrix.tasks.logger") as mock_logger:
            result = await update_checkpoint("mutex")

    assert test_broker.mutex_queue.checkpoint.since_token != starting_checkpoint
    mock_logger.debug.assert_not_called()


async def test_tasks_update_checkpoint_unable_to_update(test_matrix_broker):
    """
    Tests that an exception is raised if there is an error in setting the queue's
    checkpoint.
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


@pytest.mark.integtest
async def test_tasks_update_checkpoint_acked_tasks(test_matrix_broker, test_broker_message):
    """
    Tests that the queue's checkpoint is updated when there are no unacked tasks.
    """

    # create a MatrixBroker object
    test_broker = await test_matrix_broker()
    starting_checkpoint = await test_broker.mutex_queue.checkpoint.get_or_init_checkpoint(
        full_sync=False
    )

    # assign labels to the broker message
    test_broker_message.labels = {"test": "labels"}

    # kick the broker message to the broker
    await test_broker.kick(test_broker_message)

    await test_broker.mutex_queue.ack_msg(test_broker_message.task_id)

    with patch("taskiq_matrix.tasks.broker", test_broker):
        with patch("taskiq_matrix.tasks.logger") as mock_logger:
            await update_checkpoint("mutex")

        mock_logger.debug.assert_called_with("Task update_checkpoint: No unacked tasks found")
        assert test_broker.mutex_queue.checkpoint.since_token != starting_checkpoint


@pytest.mark.integtest
async def test_tasks_update_checkpoint_unacked_tasks_only(
    test_matrix_broker, test_broker_message
):
    """
    Tests that if an unacked task is present in the room, the queue's sync token is
    set to before the unacked task.
    """

    # create a MatrixBroker object
    test_broker = await test_matrix_broker()

    await test_broker.mutex_queue.checkpoint.get_or_init_checkpoint(full_sync=False)

    # assign labels to the broker message
    test_broker_message.labels = {"test": "labels"}

    # kick the broker message to the broker
    await test_broker.kick(test_broker_message)

    # patch the task.py broker with the broker created locally
    with patch("taskiq_matrix.tasks.broker", test_broker):
        # patch the logger to verify function calls
        with patch("taskiq_matrix.tasks.logger") as mock_logger:
            await update_checkpoint("mutex")

        # verify that the logger never called in the block of code that is executed
        # when there are no unacked tasks found
        mock_logger.debug.assert_not_called()


@pytest.mark.integtest
async def test_tasks_update_checkpoint_unacked_tasks_room_context_error(
    test_matrix_broker, test_broker_message
):
    """
    Tests that an exception is raised and the function is exited if room_context
    returns a RoomContextError.
    """
    # create a MatrixBroker object
    test_broker = await test_matrix_broker()

    await test_broker.mutex_queue.checkpoint.get_or_init_checkpoint(full_sync=False)

    # assign labels to the broker message
    test_broker_message.labels = {"test": "labels"}

    # kick the broker message to the broker
    await test_broker.kick(test_broker_message)

    # set room_context to return a RoomContextError
    matrix_queue = test_broker.mutex_queue
    mock_context = AsyncMock(return_value=MagicMock(spec=RoomContextError))
    mock_context.message = "test error message"
    matrix_queue.client.room_context = mock_context

    # patch the task.py broker with the broker created locally
    with patch("taskiq_matrix.tasks.broker", test_broker):
        # call update_checkpoint to raise an exception
        with pytest.raises(Exception):
            await update_checkpoint("mutex")
