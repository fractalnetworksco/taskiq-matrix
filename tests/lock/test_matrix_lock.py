import json
from base64 import b64encode
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch
from fractal.matrix.async_client import FractalAsyncClient


import pytest
from nio import MatrixRoom, RoomMessagesResponse, SyncResponse
from taskiq_matrix.lock import (
    LockAcquireError,
    MatrixLock,
    RoomMessagesError,
    RoomSendResponse,
    SyncError,
)


async def test_matrix_lock_constructor_missing_homeserver():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no homeserver_url
    """

    # establish the expected error message
    expected_error = "MATRIX_HOMESERVER_URL is required if not passed explicitly"

    # create a MatrixLock object to raise the exception
    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(homeserver_url="")

    # verify that the exception raised matches expectations
    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_missing_access_token():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no access_token
    """

    # establish the expected error message
    expected_error = "MATRIX_ACCESS_TOKEN is required if not passed explicitly"

    # create a MatrixLock object to raise the exception
    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(access_token="")

    # verify that the exception raised matches expectations
    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_missing_room_id():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no room_id
    """

    # establish the expected error message
    expected_error = "MATRIX_ROOM_ID is required if not passed explicitly"

    # create a MatrixLock object to raise the exception
    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(room_id="")

    # verify that the exception raised matches expectations
    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_no_next_batch():
    """
    Tests that if there is no pre-set next_batch for the MatrixLock class, None is used
    """

    # patch the setup_console_logging() function to verify it was called
    with patch(
        "taskiq_matrix.lock.setup_console_logging", new_awaitable=AsyncMock
    ) as mock_console_log:
        # ensure that the MatrixLock class' next_batch attribute is None
        with patch("taskiq_matrix.lock.MatrixLock.next_batch", None):
            lock = MatrixLock()

            # verify that the patched function was called
            mock_console_log.assert_called_once()

            # verify that the lock object's next_batch is None
            assert lock.next_batch == None


async def test_matrix_lock_constructor_existing_next_batch():
    """
    Tests that if the MatrixLock has a next_batch, the new MatrixLock object is
    given the existing next_batch
    """

    # make a test next_batch 
    test_next_batch = "test_next_batch"

    # patch the MatrixLock class' next_batch to be the created nest_batch
    with patch("taskiq_matrix.lock.MatrixLock.next_batch", test_next_batch):
        # patch the setup_console_logging() function to verify it was called
        with patch(
            "taskiq_matrix.lock.setup_console_logging", new_awaitable=AsyncMock
        ) as mock_console_log:
            lock = MatrixLock()

            # verify that the mocked function was called and that the lock object's 
            # next_batch reflects what was created locally
            mock_console_log.assert_called_once()
            assert lock.next_batch == test_next_batch


async def test_matrix_lock_create_filter_no_room_id():
    """
    Tests that if a room id is not passed a parameter, the lock object's room id
    attribute is used to create a filter
    """

    # create a MatrixLock object and store the room_id
    lock = MatrixLock()
    room_id = lock.room_id

    # patch the create_filter function with a mock
    with patch("taskiq_matrix.lock.create_filter", new_callable=MagicMock) as mock_create_filter:
        # call create_filter without passing a room_id
        lock.create_filter(room_id=None)

        # verify that create_filter was called using the lock's room_id attribute
        mock_create_filter.assert_called_once_with(room_id, types=[], limit=None)


async def test_matrix_lock_create_filter_given_room_id():
    """
    Tests that if a room id is passed as a parameter, it is used to create a filter
    instead of the lock's room id attribute
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # patch the create_filter function with a mock
    with patch("taskiq_matrix.lock.create_filter", new_callable=MagicMock) as mock_create_filter:
        # call create_filter and pass a room id
        lock.create_filter(room_id="test room id")

        # verify that the function was called using the given room id instead of 
        # the lock object's room id
        mock_create_filter.assert_called_once_with("test room id", types=[], limit=None)


async def test_matrix_lock_send_message_is_bytes():
    """
    Tests that if the message parameter is given in bytes, it is encoded into base64 bytes
    and then decoded into a string.
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # create a message in bytes
    test_message = b"test byte message"
    encoded_test_message = b64encode(test_message).decode("utf-8")

    # mock the room_send function 
    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    # call send_message
    await lock.send_message(test_message)

    # establish what the expected content dictionary should be
    expected_content = {"msgtype": "m.room.message", "body": encoded_test_message, "bytes": True}

    # verify that room_send was called with the expected dictionary
    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)


async def test_matrix_lock_send_message_is_dictionary():
    """
    Tests that if the message parameter is given as a dictionary, it is converted into
    JSON.
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # create a message dictionary
    test_message = {"test": "test message"}
    expected_message = json.dumps(test_message)

    # mock the room_send function 
    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    # call send_message
    await lock.send_message(test_message)

    # establish what the expected content dictionary should be
    expected_content = {
        "msgtype": "m.room.message",
        "body": expected_message,
    }

    # verify that room_send was called with the expected dictionary
    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)


async def test_matrix_lock_send_message_is_string():
    """
    Tests that if a string is passed as a message, it is left unchanged.
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # create a message string
    test_message = "test message"

    # mock the room_send function 
    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    # call send_message
    await lock.send_message(test_message)

    # establish what the expected content dictionary should be
    expected_content = {
        "msgtype": "m.room.message",
        "body": test_message,
    }

    # verify that room_send was called with the expected dictionary
    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)


async def test_matrix_lock_send_message_MatrixRoom_is_passed():
    """
    Tests that if a MatrixRoom object is passed to the function, that it's room id is
    used instead of the lock's room id
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # create a messsage string
    test_message = "test message"

    # create a MatrixRoom object
    test_room = MatrixRoom(room_id="test room id", own_user_id="test user")

    # mock the room_send function 
    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    # call send message, passing the MatrixRoom as a parameter
    await lock.send_message(message=test_message, room=test_room)

    # establish what the expected content dictionary should be
    expected_content = {
        "msgtype": "m.room.message",
        "body": test_message,
    }

    # verify that room_send was called using the MatrixRoom's room_id and the expected dictionary
    mock_room_send.assert_called_once_with(test_room.room_id, "m.room.message", expected_content)


async def test_matrix_lock_send_message_error_sending_message():
    """
    Tests that an exception is raised when there is an error sending a message
    """

    # create a MatrixLock object
    lock = MatrixLock()

    # create a message string
    test_message = "test message"

    # mock the room_send function and have it raise an exception
    mock_room_send = AsyncMock()
    mock_room_send.side_effect = Exception()
    lock.client.room_send = mock_room_send

    # call send_message to raise the exception
    with pytest.raises(Exception) as e:
        await lock.send_message(test_message)

    # verify that the exception message matches what is expectec
    assert "Error sending message type" in (str(e.value))


async def test_matrix_lock_send_message_error_response():
    """
    Tests that an exception is raised when there is an error sending a message
    """
    
    # create a MatrixLock object
    lock = MatrixLock()

    # create a message string
    test_message = "test message"

    # mock the room_send function and have it return a RoomMessagesError
    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomMessagesError(
        message="test error message", status_code="abc"
    )
    lock.client.room_send = mock_room_send

    # call send_message to raise the exception
    with pytest.raises(Exception) as e:
        await lock.send_message(test_message)

    # verify that the exception message matches what is expectec
    assert "Got error response when sending message:" in (str(e.value))


async def test_matrix_lock_lock_LockAcquireError(new_matrix_room):
    """
    Tests that a LockAcquireError is raised if the lock could not be acquired
    """

    # load in a room_id fixture
    room_id = await new_matrix_room()

    # create a MatrixLock object and mock the _acquire_lock function
    lock = MatrixLock()
    mock_acquire_lock = AsyncMock()
    # set _acquire_lock to return False to raise an exception
    mock_acquire_lock.return_value = False
    lock._acquire_lock = mock_acquire_lock

    # call the function to raise the exception
    with pytest.raises(LockAcquireError) as e:
        async with lock.lock(room_id):
            print("locking")

    # verify that the exception message matches what is expectec
    assert "Could not acquire lock" in str(e.value)


async def test_matrix_lock_lock_functional_test(new_matrix_room):
    """
    ? how do i check the yield
    """

    # load in a room_id fixture
    room_id = await new_matrix_room()

    # create a MatrixLock object and mock the _acquire_lock function
    lock = MatrixLock()
    mock_acquire_lock = AsyncMock()
    # set _acquire_lock to return True
    mock_acquire_lock.return_value = True
    lock._acquire_lock = mock_acquire_lock

    # patch the send_message function to verify it was called
    with patch(
        "taskiq_matrix.lock.MatrixLock.send_message", new_awaitable=AsyncMock
    ) as mock_send_message:
        async with lock.lock(room_id):
            print("locking")

        # verify that send_message was called
        mock_send_message.assert_awaited_once()


async def test_matrix_lock_acquire_lock_existing_next_batch():
    """
    Tests that get_latest_sync_token is not called if the lock already had a next batch
    """
    lock = MatrixLock()
    lock.next_batch = await lock.get_latest_sync_token()
    with patch(
        "taskiq_matrix.lock.MatrixLock.get_latest_sync_token", new_awaitable=AsyncMock
    ) as mock_sync_token:
        await lock._acquire_lock()
        mock_sync_token.assert_not_awaited()


async def test_matrix_lock_acquire_lock_no_next_batch():
    """
    Tests that if next_batch is set to None, get_latest_sync_token is called to get a
    sync token for the lock
    """
    lock = MatrixLock()
    lock.next_batch = None
    with patch(
        "taskiq_matrix.lock.MatrixLock.get_latest_sync_token", new_awaitable=AsyncMock
    ) as mock_sync_token:
        # to test that get_latest_sync_token was called it must be mocked, this will raise
        # an exception as it is not producing a real sync token
        with pytest.raises(Exception):
            await lock._acquire_lock()
            mock_sync_token.assert_awaited_once()


async def test_matrix_lock_acquire_lock_room_id_not_in_res():
    """
    Tests that if the lock's room id doesn't match what is in the filter, the function
    proceeds to the else block and returns false.
    """
    lock = MatrixLock()

    mock_filter = AsyncMock()
    mock_filter.return_value = {
        lock.room_id: [{"type": "fn.lock.acquire.None", "lock_id": lock.lock_id}]
    }
    lock.filter = mock_filter

    mock_send_message = AsyncMock()
    lock.send_message = mock_send_message

    result = await lock._acquire_lock()

    mock_send_message.assert_not_called()
    mock_filter.assert_called_once()
    assert not result


async def test_matrix_lock_acquire_lock_room_id_in_res():
    """
    Tests that the function returns True if the second filter acquires
    acquires the lock.
    """
    lock = MatrixLock()
    mock_filter = AsyncMock()
    second_call = {lock.room_id: [{"type": "fn.lock.acquire.None", "lock_id": lock.lock_id}]}
    mock_filter.side_effect = [{}, second_call]
    lock.filter = mock_filter

    mock_send_message = AsyncMock()
    lock.send_message = mock_send_message

    result = await lock._acquire_lock()
    assert result
    mock_send_message.assert_called_once()


async def test_matrix_lock_acquire_lock_not_acquired():
    """ """
    lock = MatrixLock()
    mock_filter = AsyncMock()
    second_call = {
        lock.room_id: [
            {
                "type": "fn.lock.acquire.None",
                "lock_id": "not_the_same_lock_id",
            }
        ]
    }
    mock_filter.side_effect = [{}, second_call]
    lock.filter = mock_filter

    with patch("taskiq_matrix.lock.logger", new=MagicMock) as mock_logger:
        mock_logger.debug = MagicMock()
        mock_logger.info = MagicMock()
        result = await lock._acquire_lock()
        assert not result
        mock_logger.info.assert_called_once()

async def test_matrix_lock_filter_works(new_matrix_room):
    """ """
    room_id = await new_matrix_room()

    lock = MatrixLock(room_id=room_id)
    lock_types = [f"fn.lock.acquire.test", f"fn.lock.release.test"]

    next = await lock.get_latest_sync_token()

    lock.next_batch = next
    await lock.send_message(
        {"test": "chicken"},
        msgtype=lock_types[0],
    )

    res = await lock.filter(
        lock.create_filter(types=lock_types), timeout=0
    )
    assert lock.room_id in res

async def test_matrix_lock_filter_syncerror():
    """ """
    lock = MatrixLock()
    mock_sync = AsyncMock()
    mock_sync.return_value = SyncError(message="test error message")
    lock.client.sync = mock_sync

    with pytest.raises(Exception) as e:
        await lock.filter(filter={}, timeout=0)

    assert "test error message" == str(e.value)


async def test_matrix_lock_get_latest_sync_token_error():
    """ """
    lock = MatrixLock()

    lock.client.room_messages = AsyncMock()
    lock.client.room_messages.return_value = RoomMessagesError(message="test_message")

    with pytest.raises(Exception) as e:
        await lock.get_latest_sync_token()
    assert lock.room_id in str(e.value)


async def test_matrix_lock_get_latest_sync_token_good_response():
    """ """

    lock = MatrixLock()

    lock.client.room_messages = AsyncMock()
    mock_response = AsyncMock(spec=RoomMessagesResponse)
    mock_response.start = "test token"
    lock.client.room_messages.return_value = mock_response

    result = await lock.get_latest_sync_token()
    assert result == "test token"
