from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fractal.matrix import FractalAsyncClient
from nio import SyncError
from taskiq_matrix.schedulesource import MatrixRoomScheduleSource


async def test_matrix_room_schedule_constructor_error():
    """
    Tests that an exception is rasied if the MatrixRoomScheduleSource constrtuctor
    is called without passing a MatrixBroker
    """

    # create a MatrixRoomScheduleSource without passing a
    # valid broker to raise an exception
    with pytest.raises(TypeError):
        broker = "test broker not a real broker should raise error"
        MatrixRoomScheduleSource(broker)


async def test_matrix_room_schedule_startup(test_matrix_broker):
    """
    Tests that the startup() function sets the schedule's '_initial' property
    to True
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # verify that the schedule source's initial attribute is set to False before startup
    assert not hasattr(test_schedule, "_initial")

    # call startup() and verify that initial is changed to True
    await test_schedule.startup()
    assert test_schedule._initial is True


async def test_matrix_room_schedule_get_schedules_true_initial(test_matrix_broker):
    """
    Test that get_schedules() returns an empty list if the schedule's 'initial' property
    is set to True
    """
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # call startup() and verify that initial is set to True
    await test_schedule.startup()
    assert test_schedule._initial is True

    # mock the get_schedules_from_room function
    test_schedule.get_schedules_from_rooms = MagicMock()

    # call the get_schedules function and verify that it returned an
    # empty list
    schedules = await test_schedule.get_schedules()
    assert schedules == []

    # verify that get_schedules_from_room was not called
    test_schedule.get_schedules_from_rooms.assert_not_called()


@pytest.mark.integtest
async def test_matrix_room_schedule_get_schedules_no_tasks(test_matrix_broker):
    """
    Test that an epty list is returned if there are no tasks in the schedule
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule._initial = False

    # call get_schedules
    schedules = await test_schedule.get_schedules()

    # verify that an empty list was returned
    assert schedules == []


@pytest.mark.integtest
async def test_matrix_room_schedule_get_schedules_broker_task(test_matrix_broker_with_cleanup):
    """
    Tests that a task is returned if a task is registered with the broker
    """
    async with test_matrix_broker_with_cleanup() as test_broker:
        room_id = test_broker._test_room_id
        test_schedule = MatrixRoomScheduleSource(test_broker)
        test_schedule._initial = False

        # verify that there are no scheduled tasks
        result = await test_schedule.get_schedules()
        assert result == []

        task_json = {
            "name": "some_task",
            "cron": "* * * * *",
            "labels": {"task_id": "some_task", "queue": "mutex"},
            "args": ["an_arg"],
            "kwargs": {},
        }

        # register task with broker
        test_broker.register_task(
            lambda x: print("A", x),
            task_name="some_task",
        )
        test_broker._init_queues()

        # put a schedule into a room for the scheduler to find
        await test_broker.mutex_queue.client.room_put_state(
            room_id, "taskiq.schedules", {"tasks": [task_json]}
        )

        # call get_schedules and verify that a ScheduledTask was returned
        result = await test_schedule.get_schedules()
        assert len(result) == 1

        assert result[0].task_name == task_json["name"]
        assert "scheduled_task" in result[0].labels
        assert result[0].labels["room_id"] == room_id

        # FIXME: This should go in the test_matrix_broker filter
        # leave the room so that the any other test does not pick up schedules from this room
        await test_broker.mutex_queue.client.room_leave(room_id)


async def test_matrix_room_schedule_get_schedules_non_existant_task(test_matrix_broker):
    """
    Tests that the function raises an exception if the task name is not in broker_tasks
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule._initial = False

    #  patch get_schedules_from_room
    with patch.object(
        test_schedule, "get_schedules_from_rooms", new_callable=AsyncMock
    ) as mock_get_schedules_from_room:
        mock_get_schedules_from_room.return_value = {
            "!abc:localhost": [{"name": "non_existant_task", "labels": {}}]
        }

        # call get_schedules to raise an exception, verify that the error message matches
        # what is expected
        result = await test_schedule.get_schedules()
        assert len(result) == 0

        # verify that the mocked function was called once
        mock_get_schedules_from_room.assert_called_once()


async def test_matrix_room_schedule_get_schedules_broker_check(test_matrix_broker):
    """
    Test that if the task broker does not match the schedule source broker,
    the loop is exited and the list of schedules is returned without appending
    any tasks.
    """
    broker = await test_matrix_broker()

    with patch(
        "taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_rooms",
        new_callable=AsyncMock,
    ) as mock_get_schedules_from_room:
        task_name = "task_with_different_broker"
        task_with_different_broker = {
            "name": task_name,
            "labels": {},
        }
        mock_get_schedules_from_room.return_value = {
            "!abc:localhost": [task_with_different_broker]
        }

        with patch.object(
            broker,
            "get_all_tasks",
            return_value={task_name: MagicMock(broker=MagicMock())},
        ):
            test_schedule = MatrixRoomScheduleSource(broker)
            test_schedule._initial = False
            result = await test_schedule.get_schedules()

        # verify that get_schedules returned an empty list
        assert result == []


async def test_matrix_room_schedule_get_schedules_no_cron_or_time(
    test_matrix_broker_with_cleanup,
):
    """
    Tests that an error is raised if the task does not have "cron" or "time" key-value
    pairs
    """
    async with test_matrix_broker_with_cleanup() as test_broker:
        room_id = test_broker._test_room_id
        test_schedule = MatrixRoomScheduleSource(test_broker)
        test_schedule._initial = False

        # verify that there are no scheduled tasks
        result = await test_schedule.get_schedules()
        assert result == []

        task_json = {
            "name": "some_task",
            "labels": {"task_id": "some_task", "queue": "mutex"},
            "args": ["an_arg"],
            "kwargs": {},
        }

        # register task with broker
        test_broker.register_task(
            lambda x: print("A", x),
            task_name="some_task",
        )
        test_broker._init_queues()

        # put a schedule into a room for the scheduler to find
        await test_broker.mutex_queue.client.room_put_state(
            room_id, "taskiq.schedules", {"tasks": [task_json]}
        )

        result = await test_schedule.get_schedules()

        # task shouldn't be returned since it lacks a cron or time key-value pair
        assert len(result) == 0


async def test_matrix_room_schedule_get_schedules_from_room_unknown_error(
    test_matrix_broker_with_cleanup,
):
    """
    Tests that an empty dictionary is returned when there is an unknown error in the
    room state. Verifies that status code is not "M_NOT_FOUND" by checking the logger.
    """
    async with test_matrix_broker_with_cleanup() as broker:
        test_schedule = MatrixRoomScheduleSource(broker)

        test_schedule.client = MagicMock(spec=FractalAsyncClient)

        test_schedule.client.sync.return_value = SyncError(
            message="test message", status_code="test status code"
        )

        resp = await test_schedule.get_schedules_from_rooms()
        assert resp == {}


async def test_matrix_room_schedule_get_schedules_from_room_content_returned(
    test_matrix_broker_with_cleanup,
):
    """
    Tests that if room_get_state_event returns a RoomGetStateEventResponse, the function
    returns a list of tasks present in the room
    """

    async with test_matrix_broker_with_cleanup() as test_broker:
        room_id = test_broker._test_room_id
        test_schedule = MatrixRoomScheduleSource(test_broker)
        test_schedule._initial = False

        task_json = {
            "name": "some_task",
            "cron": "* * * * *",
            "labels": {"task_id": "some_task", "queue": "mutex"},
            "args": ["an_arg"],
            "kwargs": {},
        }

        # register task with broker
        test_broker.register_task(
            lambda x: print("A", x),
            task_name="some_task",
        )
        test_broker._init_queues()

        # put a schedule into a room for the scheduler to find
        await test_broker.mutex_queue.client.room_put_state(
            room_id, "taskiq.schedules", {"tasks": [task_json]}
        )
        # call get_schedules_from_room
        resp = await test_schedule.get_schedules_from_rooms()

        # should get back the task that was put into the room
        assert len(resp) == 1
        assert resp[room_id] == [task_json]
