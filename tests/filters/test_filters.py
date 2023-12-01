import random
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fractal import FractalAsyncClient
from nio import SyncError, SyncResponse
from taskiq_matrix.filters import (
    create_filter,
    get_first_unacked_task,
    get_sync_token,
    run_sync_filter,
)


async def test_filters_get_sync_token_sync_error():
    """
    Tests that an exception is raised if client.sync() returns a SyncError
    """

    # create an FractalAsyncClient object
    test_client = FractalAsyncClient()

    # set the sync method to return a SyncError and set it's error message
    test_client.sync = AsyncMock()
    mock_response = AsyncMock(spec=SyncError)
    mock_response.message = "test error message"
    test_client.sync.return_value = mock_response

    # call get_sync_token to raise the exception
    with pytest.raises(Exception) as e:
        await get_sync_token(test_client)

    # verify that sync was called and that the error message raised is the
    # same as the one set locally
    test_client.sync.assert_called_once()
    assert str(e.value) == mock_response.message


async def test_filters_get_sync_token_verify_next_batch():
    """
    Tests that the function returns the same next_batch as the one created locally
    """

    # create an FractalAsyncClient object
    test_client = FractalAsyncClient()

    # set the sync method to return a SyncResponse and set it's next_batch
    test_client.sync = AsyncMock()
    mock_response = AsyncMock(spec=SyncResponse)
    mock_response.next_batch = "abc"
    test_client.sync.return_value = mock_response

    # call get_sync_token and store the result
    result = await get_sync_token(test_client)

    # verify that sync was called once and that the next_batch token that is
    # returned matches what was set locally
    test_client.sync.assert_called_once()
    assert result == "abc"


async def test_filters_run_sync_filter_sync_error():
    """
    Tests that the function raises an exception of a SyncError is returned by
    client.sync()
    """

    # create an FractalAsyncClient object
    test_client = FractalAsyncClient()

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


async def test_filters_run_sync_filter_true_content_only():
    """
    Test that setting content_only to True returns a dictionary of rooms
    with a list of what was the value associated with the 'content' key of the
    events
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

    # create a dictionary of mock event objects and assign them a room
    mock_client.sync.return_value.rooms.join["room1"].timeline.events = [
        AsyncMock(source={"content": "event1", "sender": "test_sender1"}),
        AsyncMock(source={"content": "event2", "sender": "test_sender2"}),
    ]
    mock_client.sync.return_value.rooms.join["room2"].timeline.events = [
        AsyncMock(source={"content": "event3", "sender": "test_sender3"}),
    ]

    # Call the run_sync_filter function
    result = await run_sync_filter(
        client=mock_client,
        filter={},
        timeout=30000,
        since=None,
        content_only=true_content_only,
    )

    # assert the structure of the result
    assert result == {
        "room1": ["event1", "event2"],
        "room2": ["event3"],
    }


async def test_filters_run_sync_filter_with_kwargs():
    """
    Test that run_sync_filters properly filters events using kwargs
    """

    # create a mock FractalAsyncClient object and mock its sync function
    mock_client = MagicMock(spec=FractalAsyncClient)
    mock_sync = AsyncMock()
    mock_client.sync = mock_sync
    content_only = False

    # create a dictionary of rooms
    mock_client.sync.return_value.rooms.join = {
        "room1": MagicMock(),
        "room2": MagicMock(),
    }

    # create a dictionary of mock event objects and assign them a room, making
    # sure to include events with key:value pairs that don't align with the kwargs that
    # will be passed
    mock_client.sync.return_value.rooms.join["room1"].timeline.events = [
        AsyncMock(source={"content": "event1", "key1": "value1", "key2": "value2"}),
        AsyncMock(source={"content": "event2", "key1": "value1", "key2": "value2"}),
        AsyncMock(source={"content": "event3", "key1": "value3", "key2": "value3"}),
    ]
    mock_client.sync.return_value.rooms.join["room2"].timeline.events = [
        AsyncMock(source={"content": "event4", "key1": "value1", "key2": "value2"}),
    ]

    # set the kwargs to further filter the events
    test_kwargs = {
        "key1": "value1",
        "key2": "value2",
    }

    # Call the run_sync_filter function
    result = await run_sync_filter(
        client=mock_client,
        filter={},
        timeout=30000,
        since=None,
        content_only=content_only,
        **test_kwargs,
    )

    # set the expectation for the structure of the dictionary that is returned
    expected_result = {
        "room1": [
            {"content": "event1", "key1": "value1", "key2": "value2"},
            {"content": "event2", "key1": "value1", "key2": "value2"},
        ],
        "room2": [
            {"content": "event4", "key1": "value1", "key2": "value2"},
        ],
    }

    # verify that the dictionary that is returned matches what was expected
    assert result == expected_result


@pytest.mark.integtest # ? "not" and "append" appear to be external functions
async def test_filters_get_first_unacked_task_mixed_tasks():
    """
    Tests that the first unacked task in a list is returned. Duplicate tasks are
    inserted into the list to test conditional statements.
    """

    # create a list of unacknowledged task dictionaries
    unacknowledged_tasks = [
        {"content": {"body": {"task_id": 1}, "msgtype": "taskiq.task"}},
        {"content": {"body": {"task_id": 2}, "msgtype": "taskiq.task"}},
        {"content": {"body": {"task_id": 2}, "msgtype": "taskiq.task"}},
    ]

    # create a list of acknowledged task dictionaries
    acknowledged_tasks = [
        {"content": {"body": {"task_id": 2}, "msgtype": "taskiq.ack"}},
        {"content": {"body": {"task_id": 5}, "msgtype": "taskiq.ack"}},
    ]

    # combine the two lists into a list of tasks
    tasks = unacknowledged_tasks + acknowledged_tasks

    # call the get_first_unacked_task function
    result = await get_first_unacked_task(tasks)

    # assert that the task that is returned is the first task in the list
    assert (
        result["content"]["body"]["task_id"]
        == unacknowledged_tasks[0]["content"]["body"]["task_id"]
    )


@pytest.mark.integtest # depends on "append" and "not"
async def test_filters_get_first_unacked_task_only_acked_tasks():
    """
    Tests that no tasks are returned if no unacked tasks are passed to it
    """

    # create a dictionary of acknowledged tasks
    acknowledged_tasks = [
        {"content": {"body": {"task_id": 1}, "msgtype": "taskiq.ack"}},
        {"content": {"body": {"task_id": 2}, "msgtype": "taskiq.ack"}},
    ]

    tasks = acknowledged_tasks

    # call the get_first_unacked_task function
    result = await get_first_unacked_task(tasks)

    # verify that an empty list is returned
    assert result == {}


@pytest.mark.integtest # depends on uuid
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


@pytest.mark.integtest # depends on uuid
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
