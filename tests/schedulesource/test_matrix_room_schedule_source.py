from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from fractal.matrix import FractalAsyncClient
from nio import RoomGetStateEventError, RoomGetStateEventResponse
from taskiq import ScheduledTask
from taskiq_matrix.schedulesource import MatrixRoomScheduleSource


async def test_matrix_room_schedule_constructor_error():
    """
    Tests that an exception is rasied if the MatrixRoomScheduleSource constrtuctor
    is called without passing a MatrixBroker
    """

    with pytest.raises(TypeError):
        broker = "test broker not a real broker should raise error"
        test_schedule = MatrixRoomScheduleSource(broker)


@pytest.mark.integtest  # depends on parent class' startup function
async def test_matrix_room_schedule_startup(test_matrix_broker):
    """
    Tests that the startup() function sets the schedule's 'initial' property
    to True
    """

    broker = await test_matrix_broker()

    test_schedule = MatrixRoomScheduleSource(broker)

    try:
        assert test_schedule.initial == False
    except Exception as e:
        assert str(e) == "'MatrixRoomScheduleSource' object has no attribute 'initial'"

    await test_schedule.startup()
    assert test_schedule.initial == True


@pytest.mark.integtest  # depends on parent class' startup function
async def test_matrix_room_schedule_get_schedules_true_initial(test_matrix_broker):
    """
    Test that get_schedules() returns an empty list if the schedule's 'initial' property
    is set to True
    """

    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    await test_schedule.startup()
    assert test_schedule.initial == True

    test_schedule.get_schedules_from_room = MagicMock()

    schedules = await test_schedule.get_schedules()
    assert schedules == []
    test_schedule.get_schedules_from_room.assert_not_called()


@pytest.mark.integtest  # depends on parent class' startup function
async def test_matrix_room_schedule_get_schedules_no_tasks(test_matrix_broker):
    """
    Test that an epty list is returned if there are no tasks in the schedule
    ! passing but there is a warning for coroutines not being awaited
    """

    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule.initial = False

    with patch.object(
        test_schedule, "get_schedules_from_room", new_callable=AsyncMock
    ) as mock_get_schedules_from_room, patch.object(
        test_schedule.broker, "get_all_tasks", new_callable=AsyncMock
    ) as mock_get_all_tasks:
        mock_get_schedules_from_room.return_value = []
        mock_get_all_tasks.return_value = {}

        schedules = await test_schedule.get_schedules()

        assert schedules == []
        test_schedule.get_schedules_from_room.assert_called_once()
        test_schedule.broker.get_all_tasks.assert_called_once()


@pytest.mark.integtest  # depends on Exception class
async def test_matrix_room_schedule_get_schedules_non_existant_task(test_matrix_broker):
    """
    Tests that the function raises an exception if the task name is not in broker_tasks
    """
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule.initial = False

    with patch.object(
        test_schedule, "get_schedules_from_room", new_callable=AsyncMock
    ) as mock_get_schedules_from_room:
        mock_get_schedules_from_room.return_value = [{"name": "non_existent_task", "labels": {}}]

        with pytest.raises(
            Exception, match="Got schedule for non-existant task: non_existent_task"
        ):
            await test_schedule.get_schedules()

            mock_get_schedules_from_room.assert_called_once()


@pytest.mark.skip(reason="not implemented")
async def test_matrix_room_schedule_get_schedules_broker_check(test_matrix_broker):
    """
    Test the behavior of the line:
    if broker_tasks[task["name"]].broker != self.broker: continue
    ! this test went horribly, come back to this one
    ! test below triggers the continue, but not sure how to verify
    """


@pytest.mark.integtest  # depends on Exception and get_schedules_from_room
# @pytest.mark.skip(reason="Not working as intended")
async def test_matrix_room_schedule_get_schedules_no_cron_or_time(test_matrix_broker):
    """
    Tests that an error is raised if the task does not have "cron" or "time" key-value
    pairs

    ! NOT WOKRING
    ! keeps continuing
    """
    broker = await test_matrix_broker()
    with patch(
        "taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_room",
        new_callable=AsyncMock,
    ) as mock_get_schedules_from_room:
        task_name = "task_with_missing_schedule_no_cron_time"
        task_with_missing_schedule_no_cron_time = {
            "name": task_name,
            "labels": {},
        }
        mock_get_schedules_from_room.return_value = [task_with_missing_schedule_no_cron_time]

        with patch.object(broker, "get_all_tasks", return_value={task_name: MagicMock()}):
            test_schedule = MatrixRoomScheduleSource(broker)
            test_schedule.initial = False

            with pytest.raises(Exception) as e:
                await test_schedule.get_schedules()
                print("this is the error: ", str(e.value))
                assert str(e.value) == f"Schedule for task {task_name} has no cron or time"


@pytest.mark.integtest  # uses a broker and its clients
async def test_matrix_room_schedule_get_schedules_from_room_unknown_error(test_matrix_broker):
    """
    Tests that an empty dictionary is returned when there is an unknown error in the
    room state. Verifies that status code is not "M_NOT_FOUND" by checking the logger.
    """
    broker = await test_matrix_broker()

    test_schedule = MatrixRoomScheduleSource(broker)

    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        message="test message", status_code="test status code"
    )

    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_log:
        mock_log.warn = MagicMock()
        resp = await test_schedule.get_schedules_from_room()
        assert resp == []

    mock_log.warn.assert_called()


@pytest.mark.integtest  # uses a broker and its clients
async def test_matrix_room_schedule_get_schedules_from_room_not_found(test_matrix_broker):
    """
    Tests that an empty dictionary is returned when the room is not found.
    Verifies that status code is "M_NOT_FOUND" by checking the logger.
    """
    broker = await test_matrix_broker()

    test_schedule = MatrixRoomScheduleSource(broker)

    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        message="test message", status_code="M_NOT_FOUND"
    )

    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_log:
        mock_log.info = MagicMock()
        resp = await test_schedule.get_schedules_from_room()
        assert resp == []

    mock_log.info.assert_called()


@pytest.mark.integtest  # uses a broker and its clients
async def test_matrix_room_schedule_get_schedules_from_room_content_returned(test_matrix_broker):
    """ """
    broker = await test_matrix_broker()

    test_schedule = MatrixRoomScheduleSource(broker)

    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    content = {"tasks": ["test task"]}
    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventResponse(
        content=content, event_type="test type", state_key="test key", room_id="test id"
    )

    resp = await test_schedule.get_schedules_from_room()

    assert len(resp) == 1
    assert resp[0] == "test task"
