from unittest.mock import MagicMock

from taskiq_matrix.schedulesource import MatrixRoomScheduleSource


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


async def test_matrix_room_schedule_get_schedules_(test_matrix_broker):
    """ """

    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
