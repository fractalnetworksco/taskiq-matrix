from unittest.mock import AsyncMock, MagicMock

import pytest
from nio import AsyncClient, RoomGetStateEventError
from taskiq_matrix.matrix_queue import Checkpoint


@pytest.mark.skip(reason="Get state event should return a type not a dictionary")
async def test_checkpoint_get_or_init_checkpoint_verify_since():
    """ """

    # create a Checkpoint object
    mock_client_parameter = MagicMock(spec=AsyncClient)
    checkpoint = Checkpoint(
        type="abc",
        room_id="abc",
        client=mock_client_parameter,
    )

    room_get_state_response = {"content": {"checkpoint": "test value"}}

    mock_client_parameter.room_get_state_event.return_value = room_get_state_response

    returned_since = await checkpoint.get_or_init_checkpoint()
    assert returned_since == "test value"


async def test_checkpoint_get_or_init_checkpoint_unknown_error():
    """
    Event loop already running
    """

    # create a Checkpoint object
    mock_client_parameter = AsyncMock(spec=AsyncClient)
    checkpoint = Checkpoint(type="abc", room_id="abc", client=mock_client_parameter)
    mock_client = AsyncMock()
    checkpoint.client = mock_client

    # mock checkopint.client.room_get_state return value
    checkpoint.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="abc", message="Test Error Message"
    )

    with pytest.raises(Exception):
        await checkpoint.get_or_init_checkpoint()
