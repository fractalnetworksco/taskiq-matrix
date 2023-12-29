import json
import logging
from base64 import decode
from unittest.mock import AsyncMock, patch

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
    tests that the bytes are decoded
    """

    broker = await test_matrix_broker()
    client = broker.mutex_queue.client
    room_id = broker.room_id
    message = b"test message"
    content = {
        "msgtype": "taskiq.task",
        "body":{
            "task":message.decode("utf-8") 
        } 
    }

    with patch(
        "taskiq_matrix.utils.FractalAsyncClient.room_send",
        new=AsyncMock(return_value=RoomSendResponse),
    ) as mock_room_send:
        await send_message(
            client,
            room_id,
            message,
        )

    mock_room_send.assert_called_once_with(
        room_id,
        "taskiq.task",
        content
    )
