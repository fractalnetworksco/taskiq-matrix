import random
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fractal.matrix.async_client import FractalAsyncClient
from nio import AsyncClient, RoomMessagesResponse, SyncError, SyncResponse
from taskiq_matrix.filters import (
    RoomMessagesError,
    create_filter,
    create_room_message_filter,
    create_sync_filter,
    get_first_unacked_task,
    run_room_message_filter,
    run_sync_filter,
)
from taskiq_matrix.matrix_queue import TaskTypes


async def test_filters_run_sync_filter_sync_error():
    """
    Tests that the function raises an exception of a SyncError is returned by
    client.sync()
    """

    # create an AsyncClient object
    test_client = FractalAsyncClient(user="test_user", homeserver_url="test_homeserver")

    # set the sync method to return a SyncError and set it's error message
    test_client.sync = AsyncMock()
    mock_response = AsyncMock(spec=SyncError)
    mock_response.message = "test error message"
    test_client.sync.return_value = mock_response

    # call run_sync_filter and raise the exception
    with pytest.raises(Exception) as e:
        await run_sync_filter(test_client, {})

    # verify that sync was called and that the error message raised is the
    # same as the one set locally
    test_client.sync.assert_called_once()
    assert str(e.value) == mock_response.message

    await test_client.close()


async def test_filters_run_sync_filter_false_content_only():
    """
    Test that setting content_only to False returns a dictionary of rooms
    with a list of events
    """

    # create a mock FractalAsyncClient object and mock its sync function
    mock_client = MagicMock(spec=FractalAsyncClient)
    mock_sync = AsyncMock()
    mock_client.sync = mock_sync

    # set content_only to false
    false_content_only = False

    # create a dictionary of rooms
    mock_client.sync.return_value.rooms.join = {
        "room1": MagicMock(),
        "room2": MagicMock(),
    }

    # create a dictionary of mock event objects and assign them a room
    mock_client.sync.return_value.rooms.join["room1"].timeline.events = [
        AsyncMock(source={"content": "event1"}),
        AsyncMock(source={"content": "event2"}),
    ]
    mock_client.sync.return_value.rooms.join["room2"].timeline.events = [
        AsyncMock(source={"content": "event3"}),
    ]

    # Call the run_sync_filter function
    result = await run_sync_filter(
        client=mock_client,
        filter={},
        timeout=30000,
        since=None,
        content_only=false_content_only,
    )

    # assert the structure of the result
    assert result == {
        "room1": [{"content": "event1"}, {"content": "event2"}],
        "room2": [{"content": "event3"}],
    }


async def test_filters_run_sync_filter_true_content_only(unknown_event_factory):
    """
    Test that setting content_only to True returns a dictionary of rooms
    with a list of what was the value associated with the 'content' key of the
    events and the sender of the event
    """

    # create a mock FractalAsyncClient object and mock its sync function
    mock_client = MagicMock(spec=FractalAsyncClient)
    mock_sync = AsyncMock()
    mock_client.sync = mock_sync

    # set content_only to True
    true_content_only = True

    # create a dictionary of rooms
    mock_client.sync.return_value.rooms.join = {
        "room1": MagicMock(),
        "room2": MagicMock(),
    }

    event1 = unknown_event_factory("event1", "sender1")
    event2 = unknown_event_factory("event2", "sender2")
    event3 = unknown_event_factory("event3", "sender3")

    # create a dictionary of event objects and assign them a room
    mock_client.sync.return_value.rooms.join["room1"].timeline.events = [event1, event2]
    mock_client.sync.return_value.rooms.join["room2"].timeline.events = [event3]

    # Call the run_sync_filter function
    result = await run_sync_filter(
        client=mock_client,
        filter={},
        timeout=30000,
        since=None,
        content_only=true_content_only,
    )

    # assert the structure of the result
    assert result["room1"][0] == event1.source["content"]
    assert "origin_server_ts" not in result["room1"][0]

    assert result["room1"][1] == event2.source["content"]
    assert "origin_server_ts" not in result["room1"][1]

    assert result["room2"][0] == event3.source["content"]
    assert "origin_server_ts" not in result["room2"][0]


async def test_filters_get_first_unacked_task_mixed_tasks():
    """
    Tests that the first unacked task in a list is returned. Duplicate tasks are
    inserted into the list to test conditional statements.
    """

    # create a TaskType object
    t_types = TaskTypes("test")

    # create a list of unacknowledged task dictionaries
    unacknowledged_tasks = [
        {"content": {"body": {"task_id": 1}, "msgtype": t_types.task}},
        {"content": {"body": {"task_id": 2}, "msgtype": t_types.task}},
        {"content": {"body": {"task_id": 2}, "msgtype": t_types.task}},
    ]

    # create a list of acknowledged task dictionaries
    acknowledged_tasks = [
        {"content": {"body": {"task_id": 2}, "msgtype": t_types.ack}},
        {"content": {"body": {"task_id": 5}, "msgtype": t_types.ack}},
    ]

    # combine the two lists into a list of tasks
    tasks = unacknowledged_tasks + acknowledged_tasks

    # call the get_first_unacked_task function
    result = await get_first_unacked_task(tasks, t_types)

    # assert that the task that is returned is the first task in the list
    assert (
        result["content"]["body"]["task_id"]
        == unacknowledged_tasks[0]["content"]["body"]["task_id"]
    )


async def test_filters_get_first_unacked_task_only_acked_tasks():
    """
    Tests that no tasks are returned if no unacked tasks are passed to it
    """

    # create a TaskType object
    t_types = TaskTypes("test")

    # create a dictionary of acknowledged tasks
    acknowledged_tasks = [
        {"content": {"body": {"task_id": 1}, "msgtype": t_types.ack}},
        {"content": {"body": {"task_id": 2}, "msgtype": t_types.ack}},
    ]

    tasks = acknowledged_tasks

    # call the get_first_unacked_task function
    result = await get_first_unacked_task(tasks, t_types)

    # verify that an empty list is returned
    assert result == {}


async def test_filters_create_filter_with_limit():
    """
    Tests that create_filter returns a dictionary with the same room_id and limit that
    were created locally.
    """

    # create random room id and test limit variables
    test_room_id = str(uuid4())
    test_limit = random.randint(1, 9999)

    # call create_filter and store the dictionary
    filter = create_filter(room_id=test_room_id, limit=test_limit)

    # verify that the dictionary matches what is created locally
    assert filter["room"]["rooms"][0] == test_room_id
    assert filter["room"]["timeline"]["limit"] == test_limit


async def test_filters_create_filter_no_limit():
    """
    Tests that a dictionary with the correct room_id and missing the limit key is
    returned when limit is set to None
    """

    # create random room id variable
    test_room_id = str(uuid4())

    # call create_filter and store the dictionary passing None for the limit
    filter = create_filter(room_id=test_room_id, limit=None)

    # verify that the dictionary matches what is created locally
    # and that limit is not in the dictionary
    assert "limit" not in filter["room"]["timeline"]
    assert filter["room"]["rooms"][0] == test_room_id


async def test_filters_create_filter_true_room_event_filter():
    """
    Tests that only the filter room timeline dictionary is returned along with the request
    id if room_event_filter is passed as True
    """

    # create random room id variable
    test_room_id = str(uuid4())

    # call create_filter and store the dictionary passing True for the room_event_filter param
    filter = create_filter(room_id=test_room_id, room_event_filter=True)

    # verify what is found in the filter
    assert "types" in filter
    assert "not_types" in filter
    assert "not_senders" in filter
    assert "request_id" in filter

    # verify what is not found in the filter
    assert "presence" not in filter
    assert "account_data" not in filter
    assert "room" not in filter


async def test_filters_create_room_message_filter_returns_expected_filter():
    """
    Tests that the expected format is returned from create_filter when
    create_room_message_filter is called.
    """

    # create random room id variable
    test_room_id = str(uuid4())

    # call create_filter and store the dictionary
    filter = create_room_message_filter(
        test_room_id,
        types=["test_types"],
        not_types=["not_types_test"],
        not_senders=["not_senders_test"],
    )

    # verify that the the expected structure is returned
    assert "types" in filter
    assert "not_types" in filter
    assert "not_senders" in filter
    assert "request_id" in filter

    # verify that the values passed to create_room_message_filter
    # are sent to create_filter and are returned in the dictionary
    assert filter["types"][0] == "test_types"
    assert filter["not_types"][0] == "not_types_test"
    assert filter["not_senders"][0] == "not_senders_test"


async def test_filters_create_sync_filter_returns_expected_filter():
    """
    Tests that the expected format is returned from create_filter when
    create_room_message_filter is called.
    """

    # create random room id variable
    test_room_id = str(uuid4())

    # call create_filter and store the dictionary
    filter = create_sync_filter(
        test_room_id,
        types=["test_types"],
        not_types=["not_types_test"],
        not_senders=["not_senders_test"],
    )

    # verify that the the expected structure is returned
    assert "presence" in filter
    assert "account_data" in filter
    assert "room" in filter
    assert "request_id" in filter

    # verify that the values passed to create_room_message_filter
    #     are sent to create_filter and are returned in the dictionary
    assert filter["room"]["timeline"]["types"][0] == "test_types"
    assert filter["room"]["timeline"]["not_types"][0] == "not_types_test"
    assert filter["room"]["timeline"]["not_senders"][0] == "not_senders_test"


async def test_filters_run_room_message_filter_room_message_error():
    """
    Tests that an exception is raised if room_messages returns a RoomMessagesError
    """

    # create a FractalAsyncClient object
    client = FractalAsyncClient()

    # mock the room_messages function to return a RoomMessagesError
    client.room_messages = AsyncMock()
    client.room_messages.return_value = RoomMessagesError(message="test error message")

    # call run_room_message_filter to raise an exception
    with pytest.raises(Exception) as e:
        await run_room_message_filter(
            client,
            "test_room_id",
            {},
        )

    # verify that the exception message raised matches what was created locally
    assert str(e.value) == client.room_messages.return_value.message


async def test_filters_run_room_message_filter_content_only(
    test_multiple_broker_message,
    test_matrix_broker,
):
    """ 
    ! got this test semi-working but check what you're testing again
    """

    broker = await test_matrix_broker()
    queue = broker.mutex_queue
    client = queue.client
    room_id: str = client.room_id  # type:ignore

    num_messages = 3
    messages = await test_multiple_broker_message(num_messages)

    task_ids = []
    for message in messages:
        await broker.kick(message)
        task_ids.append(message.task_id)

    task_filter = create_room_message_filter(
        broker.room_id,
        types=[queue.task_types.task, f"{queue.task_types.ack}.*"],
    )

    # Call the run_sync_filter function
    result, _ = await run_room_message_filter(
        client, room_id, task_filter, content_only=True
    )
    
    for i in range(num_messages):
        assert result[room_id][i]['body']['task_id'] == task_ids[i]



async def test_filters_run_room_message_filter_():
    """ """
