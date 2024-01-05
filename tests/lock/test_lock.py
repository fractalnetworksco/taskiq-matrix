import json
from typing import Awaitable, Callable
from uuid import uuid4

import pytest
from nio import AsyncClient
from taskiq_matrix.exceptions import LockAcquireError
from taskiq_matrix.filters import create_filter, run_sync_filter
from taskiq_matrix.lock import MatrixLock


async def test_matrix_lock_acquired(
    matrix_client: AsyncClient, new_matrix_room: Callable[[], Awaitable[str]]
):
    """
    Ensure that a lock can be acquired, and if it is acquired,
    it can't be acquired again.
    """
    # generate a unique key to lock on
    key = str(uuid4())
    test_room_id = await new_matrix_room()
    async with MatrixLock(room_id=test_room_id).lock(key) as lock_id:
        # verify that lock is acquired
        res = await run_sync_filter(
            matrix_client,
            create_filter(test_room_id, types=[f"fn.lock.acquire.{key}"]),
            timeout=0,
        )
        assert res[test_room_id][0]["msgtype"] == f"fn.lock.acquire.{key}"
        assert json.loads(res[test_room_id][0]["body"])["lock_id"] == lock_id

        # attempting to acquire the lock again should fail since it's already acquired
        with pytest.raises(LockAcquireError):
            async with MatrixLock(room_id=test_room_id).lock(key):
                assert False  # should never get here
