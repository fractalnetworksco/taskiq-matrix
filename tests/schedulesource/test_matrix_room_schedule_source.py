from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from fractal.matrix import FractalAsyncClient
from nio import RoomGetStateEventError, RoomGetStateEventResponse
from taskiq import ScheduledTask
from taskiq_matrix.schedulesource import MatrixRoomScheduleSource
from taskiq_matrix.tasks import update_checkpoint


async def test_matrix_room_schedule_constructor_error():
    """
    Tests that an exception is rasied if the MatrixRoomScheduleSource constrtuctor
    is called without passing a MatrixBroker
    """

    # create a MatrixRoomScheduleSource without passing a
    # valid broker to raise an exception
    with pytest.raises(TypeError):
        broker = "test broker not a real broker should raise error"
        test_schedule = MatrixRoomScheduleSource(broker)


async def test_matrix_room_schedule_startup(test_matrix_broker):
    """
    Tests that the startup() function sets the schedule's 'initial' property
    to True
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # verify that the schedule source's initial attribute is set to False before startup
    try:
        assert test_schedule.initial == False
    except Exception as e:
        assert str(e) == "'MatrixRoomScheduleSource' object has no attribute 'initial'"

    # call startup() and verify that initial is changed to True
    await test_schedule.startup()
    assert test_schedule.initial == True


async def test_matrix_room_schedule_get_schedules_true_initial(test_matrix_broker):
    """
    Test that get_schedules() returns an empty list if the schedule's 'initial' property
    is set to True
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # call startup() and verify that initial is set to True
    await test_schedule.startup()
    assert test_schedule.initial == True

    # mock the get_schedules_from_room function
    test_schedule.get_schedules_from_room = MagicMock()

    # call the get_schedules function and verify that it returned an
    # empty list
    schedules = await test_schedule.get_schedules()
    assert schedules == []

    # verify that get_schedules_from_room was not called
    test_schedule.get_schedules_from_room.assert_not_called()


@pytest.mark.integtest  
async def test_matrix_room_schedule_get_schedules_no_tasks(test_matrix_broker):
    """
    Test that an epty list is returned if there are no tasks in the schedule
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule.initial = False

    # call get_schedules
    schedules = await test_schedule.get_schedules()

    # verify that an empty list was returned
    assert schedules == []


@pytest.mark.integtest  
async def test_matrix_room_schedule_get_schedules_broker_task(test_matrix_broker):
    """ 
    Tests that a task is returned if a task is registered with the broker
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    test_broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(test_broker)
    test_schedule.initial = False

    # verify that there are no scheduled tasks
    result = await test_schedule.get_schedules()
    assert result == []

    # kick a task up to the broker
    task = test_broker.register_task(
        lambda x: print("A", x),
        task_name="taskiq.update_checkpoint",
    )

    test_broker._init_queues()
    await test_broker.add_mutex_checkpoint_task()

    # call get_schedules and verify that a ScheduledTask was returned
    result = await test_schedule.get_schedules()
    assert len(result) == 1

async def test_matrix_room_schedule_get_schedules_non_existant_task(test_matrix_broker):
    """
    Tests that the function raises an exception if the task name is not in broker_tasks
    """

    # create a broker fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)
    test_schedule.initial = False

    #  patch get_schedules_from_room
    with patch.object(
        test_schedule, "get_schedules_from_room", new_callable=AsyncMock
    ) as mock_get_schedules_from_room:
        mock_get_schedules_from_room.return_value = [{"name": "non_existent_task", "labels": {}}]

        # call get_schedules to raise an exception, verify that the error message matches
        # what is expected
        with pytest.raises(
            Exception, match="Got schedule for non-existant task: non_existent_task"
        ):
            await test_schedule.get_schedules()

            # verify that the mocked function was called once
            mock_get_schedules_from_room.assert_called_once()

async def test_matrix_room_schedule_get_schedules_broker_check(test_matrix_broker):
    """
    Test that if the task broker does not match the schedule source broker,
    the loop is exited and the list of schedules is returned without appending
    any tasks.
    """

    # create the broker from fixture
    broker = await test_matrix_broker()

    # patch the logger in the schedulesource.py file
    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_logger:
        # patch get_schedules_from_room
        with patch(
            "taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_room",
            new_callable=AsyncMock,
        ) as mock_get_schedules_from_room:
            # create a task dictionary and set get_schedules_from_room to return it
            task_name = "task_with_different_broker"
            task_with_different_broker = {
                "name": task_name,
                "labels": {},
            }
            mock_get_schedules_from_room.return_value = [task_with_different_broker]

            # patch get_all_tasks and set its return value to a dictionary with a
            # MagicMock whose broker does not match the schedule source's broker
            with patch.object(
                broker, "get_all_tasks", return_value={task_name: MagicMock(broker=MagicMock())}
            ):
                test_schedule = MatrixRoomScheduleSource(broker)
                test_schedule.initial = False
                result = await test_schedule.get_schedules()

        # verify that the logger.debug() function was called
        mock_logger.debug.assert_called_once()
        # verify that get_schedules returned an empty list
        assert result == []

async def test_matrix_room_schedule_get_schedules_no_cron_or_time(test_matrix_broker):
    """
    Tests that an error is raised if the task does not have "cron" or "time" key-value
    pairs
    """

    # create the broker from fixture
    broker = await test_matrix_broker()

    # patch the logger in the schedulesource.py file
    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_logger:
        # patch get_schedules_from_room
        with patch(
            "taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_room",
            new_callable=AsyncMock,
        ) as mock_get_schedules_from_room:
            # create a task dictionary with no "cron" or "time" and set
            # get_schedules_from_room to return it
            task_name = "task_with_missing_schedule_no_cron_time"
            task_with_missing_schedule_no_cron_time = {
                "name": task_name,
                "labels": {},
            }
            mock_get_schedules_from_room.return_value = [task_with_missing_schedule_no_cron_time]

            # patch get_all_tasks and set its return value to a dictionary with a
            # MagicMock whose broker matches the schedule source's broker
            with patch.object(
                broker, "get_all_tasks", return_value={task_name: MagicMock(broker=broker)}
            ):
                test_schedule = MatrixRoomScheduleSource(broker)
                test_schedule.initial = False

                # call get_schedules() to raise an exception
                with pytest.raises(
                    Exception, match=f"Schedule for task {task_name} has no cron or time"
                ):
                    await test_schedule.get_schedules()

        # verify that logger.debug() was not called
        mock_logger.debug.assert_not_called()

async def test_matrix_room_schedule_get_schedules_valid_task(test_matrix_broker):
    """
    Tests that a list containing a ScheduledTask object is returned if the task
    in the schedulesource contains all of the necessary information
    """

    # create the broker from fixture
    broker = await test_matrix_broker()

    # patch the logger in the schedulesource.py file
    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_logger:
        # patch get_schedules_from_room
        with patch(
            "taskiq_matrix.schedulesource.MatrixRoomScheduleSource.get_schedules_from_room",
            new_callable=AsyncMock,
        ) as mock_get_schedules_from_room:
            # create a valid task dictionary with all necessary information and set
            # get_schedules_from_room to return it
            task_name = "valid_task"
            valid_task = {
                "name": task_name,
                "labels": {},
                "args": ["test arg"],
                "kwargs": {"test": "kwarg"},
                "cron": "test cron",
                "time": datetime(2023, 12, 1, 14, 30, 0),
            }
            mock_get_schedules_from_room.return_value = [valid_task]

            # patch get_all_tasks and set its return value to a dictionary with a
            # MagicMock whose broker matches the schedule source's broker
            with patch.object(
                broker, "get_all_tasks", return_value={task_name: MagicMock(broker=broker)}
            ):
                test_schedule = MatrixRoomScheduleSource(broker)
                test_schedule.initial = False

                result = await test_schedule.get_schedules()

            # verify that logger.debug() was called once
            mock_logger.debug.assert_called_once()

            # verify that the ScheduledTask object properties match what was created
            # in the dictionary above
            assert result[0].task_name == valid_task["name"]
            assert result[0].labels == valid_task["labels"]
            assert result[0].args == valid_task["args"]
            assert result[0].kwargs == valid_task["kwargs"]
            assert result[0].cron == valid_task["cron"]
            assert result[0].time == valid_task["time"]

async def test_matrix_room_schedule_get_schedules_from_room_unknown_error(test_matrix_broker):
    """
    Tests that an empty dictionary is returned when there is an unknown error in the
    room state. Verifies that status code is not "M_NOT_FOUND" by checking the logger.
    """

    # create a broker from fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # mock the broker's mutex_queue.client object
    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    # mock room_get_state_event to return a RoomGetStateEventError with an invalid
    # status code
    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        message="test message", status_code="test status code"
    )

    # patch the logger object and call the function
    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_log:
        mock_log.warn = MagicMock()
        resp = await test_schedule.get_schedules_from_room()
        assert resp == []

    # verify that logger.warn() was called
    mock_log.warn.assert_called()

async def test_matrix_room_schedule_get_schedules_from_room_not_found(test_matrix_broker):
    """
    Tests that an empty dictionary is returned when the room is not found.
    Verifies that status code is "M_NOT_FOUND" by checking the logger.
    """

    # create a broker from fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # mock the broker's mutex_queue.client object
    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    # mock room_get_state_event to return a RoomGetStateEventError with a valid
    # status code of "M_NOT_FOUND"
    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventError(
        message="test message", status_code="M_NOT_FOUND"
    )

    # patch the logger object and call the function
    with patch("taskiq_matrix.schedulesource.logger", new=MagicMock()) as mock_log:
        mock_log.info = MagicMock()
        resp = await test_schedule.get_schedules_from_room()
        assert resp == []

    # verify that logger.info() was called
    mock_log.info.assert_called()

async def test_matrix_room_schedule_get_schedules_from_room_content_returned(test_matrix_broker):
    """
    Tests that if room_get_state_event returns a RoomGetStateEventResponse, the function
    returns a list of tasks present in the room
    """

    # create a broker from fixture and use it to create a MatrixRoomScheduleSource object
    broker = await test_matrix_broker()
    test_schedule = MatrixRoomScheduleSource(broker)

    # mock the broker's mutex_queue.client object
    broker.mutex_queue.client = MagicMock(spec=FractalAsyncClient)

    # create a dictionary of tasks and put it in a RoomGetStateEventResponse object
    content = {"tasks": ["test task"]}
    broker.mutex_queue.client.room_get_state_event.return_value = RoomGetStateEventResponse(
        content=content, event_type="test type", state_key="test key", room_id="test id"
    )

    # call get_schedules_from_room
    resp = await test_schedule.get_schedules_from_room()

    # verify that the function returned a list of size 1 containing the task from the
    # dictionary created above
    assert len(resp) == 1
    assert resp[0] == "test task"
