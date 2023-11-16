from typing import Awaitable, Callable

import pytest
from nio import AsyncClient
from taskiq import BrokerMessage
from taskiq_matrix.matrix_broker import MatrixBroker
from taskiq_matrix.matrix_queue import ReplicatedQueue


async def test_both_run(
    matrix_client: AsyncClient,
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
    test_broker_message: BrokerMessage,
):
    broker = await test_matrix_broker()

    # create two replication queues
    laptop_queue = ReplicatedQueue(
        "replication",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,
        device_name="laptop",
    )
    desktop_queue = ReplicatedQueue(
        "replication",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,
        device_name="desktop",
    )

    # ensure the replication queue label is set
    test_broker_message.labels = {"queue": "replication"}

    # kick task to replication queue
    await broker.kick(test_broker_message)

    _, laptop_tasks = await laptop_queue.get_unacked_tasks(timeout=0)
    _, desktop_tasks = await desktop_queue.get_unacked_tasks(timeout=0)

    # both queues should have the task
    assert len(laptop_tasks) == 1
    assert len(desktop_tasks) == 1

    # cleanup
    await laptop_queue.shutdown()
    await desktop_queue.shutdown()


@pytest.mark.skip(reason="TODO: finish this test")
async def test_replication_queue_checkpoints_are_device_specific(
    matrix_client: AsyncClient,
    test_matrix_broker: Callable[[], Awaitable[MatrixBroker]],
):
    """
    Checkpoints for Replication queues should be device-specific.
    """
    broker = await test_matrix_broker()

    # create two replication queues
    laptop_queue = ReplicatedQueue(
        "replication",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,
        device_name="laptop",
    )
    desktop_queue = ReplicatedQueue(
        "replication",
        homeserver_url=matrix_client.homeserver,
        access_token=matrix_client.access_token,
        room_id=broker.room_id,
        device_name="desktop",
    )

    # call get_or_init_checkpoint on each of the queues' checkpoints and save each returned checkpoint

    # use the matrix_client to get each of their respective checkpoints from their rooms
    # e.g. matrix_client.room_get_state_event(room_id, "device.{device_name}.checkpoint")

    # compare each of the since tokens returned in the responses above
    # to the checkpoints returned by the queues' get_or_init_checkpoint methods
