from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from taskiq_matrix.schedulesource import MatrixRoomScheduleSource


async def test_matrix_room_schedule_constructor_error():
    """
    Tests that an exception is rasied if the MatrixRoomScheduleSource constrtuctor
    is called without passing a MatrixBroker
    """

    with pytest.raises(TypeError):
        broker = "test broker not a real broker should raise error"
        test_schedule = MatrixRoomScheduleSource(broker)


async def test_matrix_room_schedule_startup(test_matrix_broker):
    """
    Tests that the startup() function sets the schedule's 'initial' property
    to True
    ~~~add to spreadsheet~~~
    """

    broker = await test_matrix_broker()

    test_schedule = MatrixRoomScheduleSource(broker)

    try:
        assert test_schedule.initial == False
    except Exception as e:
        assert str(e) == "'MatrixRoomScheduleSource' object has no attribute 'initial'"

    await test_schedule.startup()
    assert test_schedule.initial == True


async def test_matrix_room_schedule_get_schedules_true_initial(test_matrix_broker):
    """
    Test that get_schedules() returns an empty list if the schedule's 'initial' property
    is set to True
    ~~~add to spreadsheet~~~
    """

    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    await test_schedule.startup()
    assert test_schedule.initial == True

    test_schedule.get_schedules_from_room = MagicMock()

    schedules = await test_schedule.get_schedules()
    assert schedules == []
    test_schedule.get_schedules_from_room.assert_not_called()


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

async def test_matrix_room_schedule_get_schedules_non_existant_task(test_matrix_broker):
    """
    Tests that the function raises an exception if the task name is not in broker_tasks
    """
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule.initial = False

    with patch.object(test_schedule, "get_schedules_from_room", new_callable=AsyncMock) as mock_get_schedules_from_room:
        mock_get_schedules_from_room.return_value = [{"name": "non_existent_task", "labels": {}}]

        with pytest.raises(Exception, match="Got schedule for non-existant task: non_existent_task"):
            await test_schedule.get_schedules()

            mock_get_schedules_from_room.assert_called_once()


async def test_matrix_room_schedule_get_schedules_broker_check(test_matrix_broker):
    """
    Test the behavior of the line: 
    if broker_tasks[task["name"]].broker != self.broker: continue
    ! this test went horribly, come back to this one
    """

async def test_matrix_room_schedule_get_schedules_no_cron_or_time(test_matrix_broker):
    """
    """
    with patch("taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_room", new_callable=AsyncMock) as mock_get_schedules_from_room:
        task_with_missing_schedule_no_cron_time = {"name": "task_with_missing_schedule_no_cron_time", "labels": {}}
        mock_get_schedules_from_room.return_value = [task_with_missing_schedule_no_cron_time]

        broker = await test_matrix_broker()  
        test_schedule = MatrixRoomScheduleSource(broker)
        test_schedule.initial = False

        with pytest.raises(Exception) as e:
            await test_schedule.get_schedules()

        assert str(e.value) == "Schedule for task task_with_missing_schedule_no_cron_time has no cron or time"
