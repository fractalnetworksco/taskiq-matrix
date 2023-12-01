from unittest.mock import AsyncMock, patch

import pytest

from taskiq_matrix.lock import MatrixLock


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
    test_next_batch = 'test_next_batch'
    with patch("taskiq_matrix.lock.MatrixLock.next_batch", test_next_batch):
        with patch(
            "taskiq_matrix.lock.setup_console_logging", new_awaitable=AsyncMock
        ) as mock_console_log:
            lock = MatrixLock()
            mock_console_log.assert_called_once()
            assert lock.next_batch == test_next_batch

async def test_matrix_lock_constructor():
    lock = MatrixLock()
    print('LAST TEST NEXT BATCH',lock.next_batch)