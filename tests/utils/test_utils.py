import json
import logging
from base64 import decode
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nio import RoomSendError
from taskiq_matrix.utils import RoomSendResponse, send_message, setup_console_logging


def test_utils_setup_console_logging_not_setup():
    """
    Tests that a logger is set up if there isn't one already
    """

    # create a logger object
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)

    # loop through the logger and remove its handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # verify that it has no handlers
    assert not logger.hasHandlers()

    # patch the utils logger with the logger with no handlers
    with patch("taskiq_matrix.utils.logger", logger):
        # call setup_console_logging
        setup_console_logging()

    # verify that the logger now has handlers
    assert logger.hasHandlers()


def test_utils_setup_console_logging_already_setup():
    """
    Tests that the logger does not go through the setup process if it is already
    set up
    """

    # create a logger object
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)

    # verify that the logger has handlers
    assert logger.hasHandlers()

    # patch the StreamHandler function to verify that it was never called
    with patch("taskiq_matrix.utils.logging.StreamHandler") as mock_stream_handler:
        # call setup_console_logging
        setup_console_logging()

    # verify that StreamHandler() was not called, signifying that the block of code was
    # never entered
    mock_stream_handler.assert_not_called()


async def test_utils_send_message_bytes(test_matrix_broker):
    """
    Tests that the message is decoded if bytes are the data type of the message sent.
    """

    # create a broker from a fixture
    broker = await test_matrix_broker()
    client = broker.mutex_queue.client
    room_id = broker._test_room_id

    # create a byte message and cosntruct what the content dictionary should look like
    message = b"test message"
    content = {"msgtype": "taskiq.task", "body": {"task": message.decode("utf-8")}}

    # patch the room_send function to verify what was called
    with patch(
        "taskiq_matrix.utils.FractalAsyncClient.room_send",
        new=AsyncMock(return_value=RoomSendResponse),
    ) as mock_room_send:
        await send_message(
            client,
            room_id,
            message,
        )

    # verify that room_send was called with what was expected
    mock_room_send.assert_called_once_with(room_id, "taskiq.task", content)


async def test_utils_send_message_string_or_dict(test_matrix_broker):
    """
    Tests that the proper messages are constructed in send_message that reflect the
    data types of the messages passed to the function.
    """

    # create a broker from a fixture
    broker = await test_matrix_broker()
    client = broker.mutex_queue.client
    room_id = broker._test_room_id

    # create string and dictionary messages
    string_message = "test string message"
    dictionary_message = {"test": "dictionary message"}

    # create the expected message content dictionaries to be passed to room_send
    string_content = {"msgtype": "taskiq.task", "body": {"task": string_message}}
    dictionary_content = {"msgtype": "taskiq.task", "body": {"task": dictionary_message}}

    # patch room_send to verify what it was called with
    with patch(
        "taskiq_matrix.utils.FractalAsyncClient.room_send",
        new=AsyncMock(return_value=RoomSendResponse),
    ) as mock_send_room:
        # call send_message and verify that room_send was called using a dictionary
        # matching what was expected
        # NOTE the dictionary will containg the original string created in this test
        # function
        await send_message(
            client,
            room_id,
            string_message,
        )
        mock_send_room.assert_called_with(room_id, "taskiq.task", string_content)

        # call send_message and verify that room_send was called using a dictionary
        # matching what was expected
        # NOTE the dictionary will contain the original dictionary created in this test
        # function
        await send_message(
            client,
            room_id,
            dictionary_message,
        )
        mock_send_room.assert_called_with(room_id, "taskiq.task", dictionary_content)


async def test_utils_send_message_with_kwargs(test_matrix_broker):
    """
    Tests that if there are kwargs passed to send_message, that they show up in the
    msg_content being passed to room_send
    """

    # create a broker object from a fixture
    broker = await test_matrix_broker()
    client = broker.mutex_queue.client
    room_id = broker._test_room_id

    # create a message and a kwarg
    message = "test message"
    kwargs = "chicken"

    # create a dictionary reflecting what should be made in the function for comparison
    content = {"msgtype": "taskiq.task", "body": {"task": message, "test_kwargs": "chicken"}}

    # patch room_send to verify what it was called with
    with patch(
        "taskiq_matrix.utils.FractalAsyncClient.room_send",
        new=AsyncMock(return_value=RoomSendResponse),
    ) as mock_send_room:
        # call send_message with a key word argument
        await send_message(client, room_id, message, test_kwargs=kwargs)

    # verify that the dictionary that room_send was called with matches what was
    # expected
    mock_send_room.assert_called_with(room_id, "taskiq.task", content)


async def test_utils_send_message_error_sending_message(test_matrix_broker):
    """
    Tests that an exception is rasied if there is an error sending a message to a
    room.
    """

    # create a broker object from a fixture
    broker = await test_matrix_broker()
    client = broker.mutex_queue.client
    room_id = broker._test_room_id

    # create a byte message
    message = b"test message"

    # patch room_send to raise an exception
    with patch("taskiq_matrix.utils.FractalAsyncClient.room_send", side_effect=Exception):
        with pytest.raises(Exception):
            await send_message(
                client,
                room_id,
                message,
            )
