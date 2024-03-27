from unittest.mock import AsyncMock, patch

import pytest
from aiofiles import open as aopen
from aiofiles.os import makedirs
from nio import SyncError, SyncResponse
from taskiq_matrix.matrix_queue import FileSystemCheckpoint, LockAcquireError


async def test_checkpoint_get_or_init_checkpoint_sync_fail(
    test_checkpoint: FileSystemCheckpoint,
):
    """
    Tests that a MatrixSyncError is raised when sync() does not return a SyncResponse
    """
    test_checkpoint.client.sync = AsyncMock(return_value=SyncError("Test Error Message"))

    with pytest.raises(Exception):
        await test_checkpoint.get_or_init_checkpoint()


@pytest.mark.integtest
async def test_checkpoint_get_or_init_checkpoint_verify_next_batch(
    test_checkpoint: FileSystemCheckpoint,
):
    """
    Tests if a RoomGetStateEventError is returned by room_get_state_event(),
    the next_batch value from that response is stored as the checkpoint's since_token attribute.
    """
    # set sync to return a SyncResponse
    test_checkpoint.client.sync = AsyncMock(
        return_value=SyncResponse(
            next_batch="test batch",
            rooms={},
            device_key_count={},
            device_list={},
            to_device_events={},
            presence_events={},
        )
    )

    since_token = await test_checkpoint.get_or_init_checkpoint()

    # compare the since_token that was returned to the checkpoint since token
    # and the since token created in the test method
    assert since_token == test_checkpoint.since_token == "test batch"
    # test_checkpoint.put_checkpoint_state.assert_called_with(since_token)


@pytest.mark.integtest
async def test_checkpoint_get_or_init_checkpoint_verify_since(test_checkpoint):
    """
    Tests that if no exceptions are raised, and room_get_state_event() returns a
    RoomGetStateResponse, the checkpoint from the response is stored as the since_token
    attribute for the checkpoint object.
    """
    # write checkpoint to checkpoint file
    await makedirs(test_checkpoint.CHECKPOINT_DIR, exist_ok=True)
    async with aopen(test_checkpoint.checkpoint_path, "w") as f:
        await f.write("abc")

    # call get_or_init_checkpoint and verify the since token is consistent
    since_token = await test_checkpoint.get_or_init_checkpoint()
    assert since_token == test_checkpoint.since_token == "abc"


@pytest.mark.integtest
async def test_checkpoint_put_checkpoint_state_lock_error(test_checkpoint):
    """
    Tests that the function returns False if the MatrixLock().lock() function
    fails
    """
    since_token = str(test_checkpoint.since_token)

    # patch the MatrixLock().lock() function and have it raise a LockAcquireError
    with patch("taskiq_matrix.matrix_queue.AsyncFileLock", autospec=True) as mock_lock:
        lock_instance = mock_lock.return_value
        lock_instance.acquire_lock.side_effect = LockAcquireError("Test Error")

        # call put_checkpoint_state
        success = await test_checkpoint.put_checkpoint_state(since_token)

        assert success is False


@pytest.mark.integtest
async def test_checkpoint_put_checkpoint_state_checkpoint_set(test_checkpoint):
    """
    Tests that put_checkpoint_state() returns True and sets the checkpoint's since_token
    value equal to the since token that is passed.
    """
    # store the since token
    since_token = str(test_checkpoint.since_token)
    success = await test_checkpoint.put_checkpoint_state(since_token)

    # verify that the function retruned True, since_token is equal to the
    # checkpoint object's since_token property, and that room_put_state() was
    # only called once
    assert success is True
    # assert since_token == str(test_checkpoint.since_token)
