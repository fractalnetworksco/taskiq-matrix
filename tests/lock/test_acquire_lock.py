import asyncio
from typing import Awaitable, Callable
from uuid import uuid4

from taskiq_matrix.exceptions import LockAcquireError
from taskiq_matrix.lock import MatrixLock


async def test_acquire_lock_success(new_matrix_room: Callable[[], Awaitable[str]]):
    """
    Ensure that only one instance of a lock can be acquired for a certain key.
    """
    room_id = await new_matrix_room()
    key = str(uuid4())

    async def get_lock(key: str) -> bool:
        async with MatrixLock(room_id=room_id).lock(key) as lock_id:
            await asyncio.sleep(1)
            assert lock_id is not None
        return True

    tasks = await asyncio.gather(
        get_lock(key), get_lock(key), get_lock(key), return_exceptions=True
    )

    exceptions = sum(1 for item in tasks if isinstance(item, LockAcquireError))
    assert exceptions == 2
