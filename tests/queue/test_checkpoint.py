from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nio import (
    AsyncClient,
    RoomGetStateEventError,
    RoomGetStateEventResponse,
    RoomMessagesError,
    RoomMessagesResponse,
    RoomPutStateError,
    RoomPutStateResponse,
    SyncError,
)

from taskiq_matrix.matrix_queue import (
    Checkpoint,
    CheckpointGetOrInitError,
    LockAcquireError,
)


async def test_checkpoint_get_or_init_checkpoint_unknown_error(test_checkpoint):
    """
    Tests that an erro is raised when "M_NOT_FOUND" is not the status code when
    room_get_state_event() returns a RoomGetStateEventError
    """

    # mock checkopint.client.room_get_state return value
    test_checkpoint.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="abc", message="Test Error Message"
    )

    # call get_or_init_checkpoint and raise Exception
    with pytest.raises(Exception):
        await test_checkpoint.get_or_init_checkpoint()

        test_checkpoint.client.room_get_state_event.assert_called_once()


async def test_checkpoint_get_or_init_checkpoint_room_messages_fail(test_checkpoint: Checkpoint):
    """
    Tests that a MatrixSyncError is raised when sync() does not return a SyncResponse
    """

    # mock checkopint.client.room_get_state return value
    test_checkpoint.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="M_NOT_FOUND", message="Test Error Message"
    )

    # set sync to return a RoomGetStateEventError
    mock_response = MagicMock(spec=SyncError)
    mock_response.message = "Test Response Message"
    test_checkpoint.client.room_messages.return_value = mock_response

    # raise an exception caused by the RoomGetStateEventError
    with pytest.raises(CheckpointGetOrInitError):
        await test_checkpoint.get_or_init_checkpoint()

    test_checkpoint.client.room_get_state_event.assert_called_once()
    test_checkpoint.client.room_messages.assert_called_once()


async def test_checkpoint_get_or_init_checkpoint_verify_next_batch(test_checkpoint: Checkpoint):
    """
    Tests that the since token that if a RoomGetStateEventError is returned by room_get_state_event(),
    the next_batch value from that response is stored as the checkpoint's since_token attribute.
    """

    # mock checkopint.client.room_get_state return value
    test_checkpoint.client.room_get_state_event.return_value = RoomGetStateEventError(
        status_code="M_NOT_FOUND", message="Test Error Message"
    )

    # set sync to return a SyncResponse
    mock_response = MagicMock(spec=RoomMessagesResponse)
    mock_response.start = "test batch"
    test_checkpoint.client.room_messages.return_value = mock_response

    # set the SyncResponse's next_batch
    mock_checkpoint_state = AsyncMock()
    test_checkpoint.put_checkpoint_state = mock_checkpoint_state

    since_token = await test_checkpoint.get_or_init_checkpoint()

    # compare the since_token that was returned to the checkpoint since token
    # and the since token created in the test method
    assert since_token == test_checkpoint.since_token == "test batch"
    test_checkpoint.put_checkpoint_state.assert_called_with(since_token)


async def test_checkpoint_get_or_init_checkpoint_verify_since(test_checkpoint: Checkpoint):
    """
    Tests that if no exceptions are raised, and room_get_state_event() returns a
    RoomGetStateResponse, the checkpoint from the response is stored as the since_token
    attribute for the checkpoint object.
    """

    # create a mock response object
    mock_response = MagicMock(spec=RoomGetStateEventResponse)
    mock_response.content = {"checkpoint": "abc"}

    # set room_get_state_event to return the mock response object
    test_checkpoint.client.room_get_state_event.return_value = mock_response

    # call get_or_init_checkpoint and verify the since token is consistent
    since_token = await test_checkpoint.get_or_init_checkpoint()
    assert since_token == test_checkpoint.since_token == "abc"


async def test_checkpoint_put_checkpoint_state_lock_error(test_checkpoint: Checkpoint):
    """
    Tests that the function returns False if the MatrixLock().lock() function
    fails
    """

    # set room_put_state to return a RoomGetStateEventError
    test_checkpoint.client.room_put_state.return_value = RoomGetStateEventError(
        status_code="M_NOT_FOUND", message="Test Error Message"
    )
    since_token = str(test_checkpoint.since_token)

    # patch the MatrixLock().lock() function and have it raise a LockAcquireError
    with patch("taskiq_matrix.matrix_queue.MatrixLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.lock.side_effect = LockAcquireError("Test Error")

        # call put_checkpoint_state
        res = await test_checkpoint.put_checkpoint_state(since_token)

        # verify that the function returns False and that room_put_state() was not called
        assert res == False
        test_checkpoint.client.room_put_state.assert_not_called()


async def test_checkpoint_put_checkpoint_state_state_error(test_checkpoint: Checkpoint):
    """
    Tests that put_checkpoint_state() raises an error if it encounters a
    RoomPutStateError
    """

    # set room_put_state to return a RoomPutStateError with status code "M_NOT_FOUND"
    test_checkpoint.client.room_put_state.return_value = RoomPutStateError(
        status_code="M_NOT_FOUND", message="Test Error Message"
    )

    # store the since_token
    since_token = str(test_checkpoint.since_token)

    # call put_checkpoint_state and store the result
    res = await test_checkpoint.put_checkpoint_state(since_token)

    # verify that the function returned false and that
    # room_put_state() was called once
    assert res == False
    test_checkpoint.client.room_put_state.assert_called_once()


async def test_checkpoint_put_checkpoint_state_checkpoint_set(test_checkpoint: Checkpoint):
    """
    Tests that put_checkpoint_state() returns True and sets the checkpoint's since_token
    value equal to the since token that is passed.
    """

    # set room_put_state to return a RoomPutStateEventResponse
    test_checkpoint.client.room_put_state.return_value = RoomPutStateResponse(
        event_id="abc", room_id="abc"
    )

    # store the since token
    since_token = str(test_checkpoint.since_token)

    # call put_checkpoint_state and store the result
    res = await test_checkpoint.put_checkpoint_state(since_token)

    # verify that the function retruned True, since_token is equal to the
    # checkpoint object's since_token property, and that room_put_state() was
    # only called once
    assert res == True
    assert since_token == str(test_checkpoint.since_token)
    test_checkpoint.client.room_put_state.assert_called_once()


async def test_checkpoint_create_checkpoint():
    """
    Tests the creation of a checkpoint object using the create() class method.
    """

    # create a mock client
    mock_client_parameter = MagicMock(spec=AsyncClient)

    # set room_get_state_event to return a RoomGetStateEventResponse
    mock_client_parameter.room_get_state_event.return_value = RoomGetStateEventResponse(
        content={"checkpoint": "abc"}, event_type="abc", state_key="", room_id="test room"
    )

    # create a checkpoint using the create() class method
    checkpoint = await Checkpoint.create("test_type", mock_client_parameter, "test_room_id")

    # verify that the checkpoint object created uses the same
    # attributes that were passed to it.
    assert checkpoint.type == "test_type.checkpoint"
    assert checkpoint.room_id == "test_room_id"
