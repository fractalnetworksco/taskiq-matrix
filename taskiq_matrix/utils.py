import logging
from typing import Any, Dict, Union

from fractal.matrix.async_client import FractalAsyncClient
from nio import RoomSendResponse

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def setup_console_logging():
    """
    Setup a logger for console output if no handlers exist
    """
    if not logging.getLogger().hasHandlers():
        # Create a console handler
        console_handler = logging.StreamHandler()

        # Set the level of this handler to DEBUG
        console_handler.setLevel(logging.DEBUG)

        # Create a formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Add the formatter to the handler
        console_handler.setFormatter(formatter)

        # Get the 'root' logger and add our handler to it
        logging.getLogger().addHandler(console_handler)


async def send_message(
    matrix_client: FractalAsyncClient,
    room: str,
    message: Union[bytes, str, Dict[Any, Any]],
    msgtype: str = "taskiq.task",
    **kwargs,
) -> None:
    """
    Send a message to a room.

    Note: Encrypted rooms are not supported for now.

    Args:
        matrix_client (AsyncClient): The Matrix client to use to send the message.
        room (str): The room id to send the message to.
        message (bytes | str): The message to send.
        msgtype (str): The message type to send. Defaults to "m.taskiq.task".
    """
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    # if not isinstance(message, str):
    #     message = json.dumps(message, default=str)

    msg_body: Dict[str, Any] = {"task": message, **kwargs}
    msg_content = {"msgtype": msgtype, "body": msg_body}
    logger.debug("Sending message: %s to room %s", msg_content, room)
    try:
        response = await matrix_client.room_send(room, msgtype, msg_content)
        if not isinstance(response, RoomSendResponse):
            logger.error("Error sending message: %s", response)
        logger.debug("Response from room_send: %s", response)
    except Exception as err:
        logger.error("Error sending message: %s", err)
        raise
