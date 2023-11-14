import asyncio
from uuid import uuid4

from taskiq_matrix.exceptions import LockAcquireError
from taskiq_matrix.lock import MatrixLock


async def test_acquire_lock_success():
    """
    Ensure that only one instance of a lock can be acquired for a certain key.
    """
    key = str(uuid4())

    async def get_lock(key: str) -> bool:
        async with MatrixLock().lock(key) as lock_id:
            assert lock_id is not None
        return True

    tasks = await asyncio.gather(
        get_lock(key), get_lock(key), get_lock(key), return_exceptions=True
    )

    exceptions = sum(1 for item in tasks if isinstance(item, LockAcquireError))
    assert exceptions == 2
