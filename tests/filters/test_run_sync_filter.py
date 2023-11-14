from nio import AsyncClient
from taskiq_matrix.filters import EMPTY_FILTER, create_filter, run_sync_filter


async def test_run_sync_filter_success():
    pass


async def test_sync_filter_timeout(matrix_client: AsyncClient):
    """
    Make a test that verifies that run_sync_filter blocks if there are no new events
    to return.

    You can do this by running two tasks in parallel:
        1. asyncio.sleep(5) # task that simply sleeps for 5 seconds
        2. run_sync_filter(client, some_sync_filter, timeout=10000)

    Refer to taskiq_matrix.matrix_broker's get_tasks
    function for more info on how to run tasks in parallel.

    You should verify that the first task that returns is the sleep task since
    run_sync_filter should block for 10 seconds if there haven't been any new events.
    """
    # using the matrix_client fixture:

    # run a sync filter using an empty sync filter and a timeout of 0 seconds in order to get latest since token on your client
    # (client.next_batch should have that after running the filter)

    # then, create a sync filter that filters for taskiq.mutex.task and their acks (taskiq.mutex.task.ack.*)

    # run a sleep async task and run_sync_filter task in parallel

    # assert that the done task is the sleep task and that the run_sync_filter task is still running (pending)
