from unittest.mock import AsyncMock, patch, MagicMock
from base64 import b64encode
from nio import MatrixRoom
import json

import pytest

from taskiq_matrix.lock import MatrixLock, RoomSendResponse, RoomMessagesError, LockAcquireError


async def test_matrix_lock_constructor_missing_homeserver():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no homeserver_url
    """
    expected_error = "MATRIX_HOMESERVER_URL is required if not passed explicitly"

    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(homeserver_url="")

    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_missing_access_token():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no access_token
    """
    expected_error = "MATRIX_ACCESS_TOKEN is required if not passed explicitly"

    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(access_token="")

    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_missing_room_id():
    """
    Tests that an Exception is raised and a MatrixLock object is not created if there
    is no room_id
    """
    expected_error = "MATRIX_ROOM_ID is required if not passed explicitly"

    with pytest.raises(Exception) as e:
        test_lock = MatrixLock(room_id="")

    assert expected_error == str(e.value)


async def test_matrix_lock_constructor_no_next_batch():
    """
    Tests that if there is no pre-set next_batch for the MatrixLock class, None is used
    """
    with patch(
        "taskiq_matrix.lock.setup_console_logging", new_awaitable=AsyncMock
    ) as mock_console_log:
        lock = MatrixLock()

        mock_console_log.assert_called_once()
        assert lock.next_batch == None


async def test_matrix_lock_constructor_existing_next_batch():
    """
    Tests that if the MatrixLock has a next_batch, the new MatrixLock object is
    given the existing next_batch
    """
    test_next_batch = "test_next_batch"
    with patch("taskiq_matrix.lock.MatrixLock.next_batch", test_next_batch):
        with patch(
            "taskiq_matrix.lock.setup_console_logging", new_awaitable=AsyncMock
        ) as mock_console_log:
            lock = MatrixLock()
            mock_console_log.assert_called_once()
            assert lock.next_batch == test_next_batch


async def test_matrix_lock_create_filter_no_room_id():
    """ 
    Tests that if a room id is not passed a parameter, the lock object's room id
    attribute is used to create a filter
    """
    lock = MatrixLock()
    room_id = lock.room_id

    with patch("taskiq_matrix.lock.create_filter", new_callable=MagicMock) as mock_create_filter:
        lock.create_filter(room_id=None)
        mock_create_filter.assert_called_once_with(room_id, types=[], limit=None)

async def test_matrix_lock_create_filter_given_room_id():
    """
    Tests that if a room id is passed as a parameter, it is used to create a filter
    instead of the lock's room id attribute
    """
    lock = MatrixLock()

    with patch("taskiq_matrix.lock.create_filter", new_callable=MagicMock) as mock_create_filter:
        lock.create_filter(room_id="test room id")
        mock_create_filter.assert_called_once_with("test room id", types=[], limit=None)

async def test_matrix_lock_send_message_is_bytes():
    """
    Tests that if the message parameter is given in bytes, it is encoded into base64 bytes
    and then decoded into a string.
    """
    lock = MatrixLock()

    test_message = b"test byte message"
    encoded_test_message = b64encode(test_message).decode("utf-8")

    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    await lock.send_message(test_message)
    
    expected_content = {
        "msgtype": "m.room.message",
        "body": encoded_test_message,
        "bytes": True
    }

    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)
    
async def test_matrix_lock_send_message_is_dictionary():
    """
    Tests that if the message parameter is given as a dictionary, it is converted into
    JSON.
    """
    lock = MatrixLock()

    test_message = {
        "test": "test message"
    }

    expected_message = json.dumps(test_message)

    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    await lock.send_message(test_message)
    
    expected_content = {
        "msgtype": "m.room.message",
        "body": expected_message,
    }

    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)

async def test_matrix_lock_send_message_is_string():
    """
    Tests that if a string is passed as a message, it is left unchanged.
    """
    lock = MatrixLock()

    test_message = "test message"

    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    await lock.send_message(test_message)
    
    expected_content = {
        "msgtype": "m.room.message",
        "body": test_message,
    }

    mock_room_send.assert_called_once_with(lock.room_id, "m.room.message", expected_content)

async def test_matrix_lock_send_message_MatrixRoom_is_passed():
    """
    Tests that if a MatrixRoom object is passed to the function, that it's room id is
    used instead of the lock's room id
    """
    lock = MatrixLock()

    test_message = "test message"

    test_room = MatrixRoom(room_id="test room id", own_user_id="test user")

    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomSendResponse(event_id="abc", room_id=lock.room_id)
    lock.client.room_send = mock_room_send

    await lock.send_message(message=test_message, room=test_room)
    
    expected_content = {
        "msgtype": "m.room.message",
        "body": test_message,
    }

    mock_room_send.assert_called_once_with(test_room.room_id, "m.room.message", expected_content)

async def test_matrix_lock_send_message_error_sending_message():
    """
    Tests that an exception is raised when there is an error sending a message
    """
    lock = MatrixLock()

    test_message = "test message"

    mock_room_send = AsyncMock()
    mock_room_send.side_effect = Exception()
    lock.client.room_send = mock_room_send

    with pytest.raises(Exception) as e:        
        await lock.send_message(test_message)
    
    assert "Error sending message type" in (str(e.value))

async def test_matrix_lock_send_message_error_response():
    """
    Tests that an exception is raised when there is an error sending a message
    """
    lock = MatrixLock()

    test_message = "test message"

    mock_room_send = AsyncMock()
    mock_room_send.return_value = RoomMessagesError(message="test error message", status_code="abc")
    lock.client.room_send = mock_room_send

    with pytest.raises(Exception) as e:        
        await lock.send_message(test_message)
    
    assert "Got error response when sending message:" in (str(e.value))

async def test_matrix_lock_lock_LockAcquireError(new_matrix_room):
    """
    Tests that a LockAcquireError is raised if the lock could not be acquired
    """
    room_id = await new_matrix_room()
    lock = MatrixLock()
    mock_acquire_lock = AsyncMock()
    mock_acquire_lock.return_value = False
    lock._acquire_lock = mock_acquire_lock

    with pytest.raises(LockAcquireError) as e:
        async with lock.lock(room_id):
            print('locking')
    
    assert "Could not acquire lock" in str(e.value)

async def test_matrix_lock_lock(new_matrix_room):
    """
    ? how do i check the yield
    """
    room_id = await new_matrix_room()
    lock = MatrixLock()
    mock_acquire_lock = AsyncMock()
    mock_acquire_lock.return_value = True
    lock._acquire_lock = mock_acquire_lock

    with patch('taskiq_matrix.lock.MatrixLock.send_message', new_awaitable=AsyncMock) as mock_send_message:
        async with lock.lock(room_id):
            print('locking')
        
        mock_send_message.assert_awaited_once()

async def test_matrix_lock_acquire_lock_existing_next_batch():
    """
    Tests that get_latest_sync_token is not called if the lock already had a next batch
    """
    lock = MatrixLock()
    lock.next_batch = await lock.get_latest_sync_token()
    with patch('taskiq_matrix.lock.MatrixLock.get_latest_sync_token', new_awaitable=AsyncMock) as mock_sync_token:
        await lock._acquire_lock()
        mock_sync_token.assert_not_awaited()

async def test_matrix_lock_acquire_lock_no_next_batch():
    """
    Tests that if next_batch is set to None, get_latest_sync_token is called to get a
    sync token for the lock
    """
    lock = MatrixLock()
    lock.next_batch = None
    with patch('taskiq_matrix.lock.MatrixLock.get_latest_sync_token', new_awaitable=AsyncMock) as mock_sync_token:
        # to test that get_latest_sync_token was called it must be mocked, this will raise
        # an exception as it is not producing a real sync token
        with pytest.raises(Exception):
            await lock._acquire_lock()
            mock_sync_token.assert_awaited_once()
    
async def test_matrix_lock_acquire_lock_room_not_in_res():
    """
    ! WIP
    """
    lock = MatrixLock()
    mock_filter = AsyncMock()
    mock_filter.return_value = {
        "room_id": lock.room_id
    }
    lock.filter = mock_filter
    await lock._acquire_lock()