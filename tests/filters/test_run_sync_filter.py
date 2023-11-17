import asyncio
from typing import Awaitable, Callable

import pytest
from taskiq_matrix.filters import EMPTY_FILTER, create_filter, run_sync_filter
from taskiq_matrix.matrix_broker import MatrixBroker


@pytest.mark.skip(reason="This test is flakey. Seems to cause synapse to sync loop.")
async def test_run_sync_filter_respects_timeout(
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
):
    """
    Run sync filter should block if no new events are received and timeout is
    set.
    """
    broker = await test_matrix_broker()

    mutex_client = broker.mutex_queue.client
    room_id = broker.room_id

    # run a sync filter using an empty sync filter and a timeout of 0 seconds in order to get latest since token on your client
    # (client.next_batch should have that after running the filter)
    await run_sync_filter(mutex_client, EMPTY_FILTER, timeout=0)

    latest_since_token = mutex_client.next_batch
    broker.mutex_queue.checkpoint.since_token = latest_since_token

    # create a sync filter that filters for taskiq.mutex.task and their acks (taskiq.mutex.task.ack.*)
    task_filter = create_filter(
        room_id,
        types=[broker.mutex_queue.task_types.task, f"{broker.mutex_queue.task_types.ack}.*"],
    )

    # run a sleep async task and run_sync_filter task in parallel
    filter_task = asyncio.create_task(
        run_sync_filter(mutex_client, task_filter, timeout=30000, since=latest_since_token)
    )
    sleep_task = asyncio.create_task(asyncio.sleep(1))

    # wait for the first task to finish
    done, pending = await asyncio.wait(
        [filter_task, sleep_task], return_when=asyncio.FIRST_COMPLETED
    )

    assert sleep_task in done and filter_task in pending

    # cancel the other task
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await mutex_client.close()
