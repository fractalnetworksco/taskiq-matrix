import os
import pickle
from unittest.mock import patch, AsyncMock, MagicMock
from uuid import uuid4
from base64 import b64decode, b64encode

import pytest
from taskiq.result import TaskiqResult
from taskiq_matrix.exceptions import DuplicateExpireTimeSelectedError
from taskiq_matrix.matrix_result_backend import (
    ExpireTimeMustBeMoreThanZeroError,
    MatrixResultBackend,
    RoomMessagesError,
)

async def test_matrix_result_backend_constructor_expire_time_error():
    """ 
    Tests that an exception is raised if the expire time that is passed to the 
    constructor is less than zero

    """
    with pytest.raises(
        ExpireTimeMustBeMoreThanZeroError,
        match="You must select one expire time param and it must be more than zero.",
    ):
        test_backend = MatrixResultBackend(
            homeserver_url=os.environ['MATRIX_HOMESERVER_URL'],
            access_token=os.environ['MATRIX_ACCESS_TOKEN'],
            room_id=os.environ['MATRIX_ROOM_ID'],
            result_ex_time=None,
            result_px_time=-1
        )

    with pytest.raises(
        ExpireTimeMustBeMoreThanZeroError,
        match="You must select one expire time param and it must be more than zero.",
    ):
        test_backend = MatrixResultBackend(
            homeserver_url=os.environ['MATRIX_HOMESERVER_URL'],
            access_token=os.environ['MATRIX_ACCESS_TOKEN'],
            room_id=os.environ['MATRIX_ROOM_ID'],
            result_ex_time=-1,
            result_px_time=None
        )

    with pytest.raises(
        ExpireTimeMustBeMoreThanZeroError,
        match="You must select one expire time param and it must be more than zero.",
    ):
        test_backend = MatrixResultBackend(
            homeserver_url=os.environ['MATRIX_HOMESERVER_URL'],
            access_token=os.environ['MATRIX_ACCESS_TOKEN'],
            room_id=os.environ['MATRIX_ROOM_ID'],
            result_ex_time=-1,
            result_px_time=-1
        )

async def test_matrix_result_backend_constructor_duplicate_expire_time_error():
    """ 
    Tests that an exception is raised if two expire times are passed to the constructor.
    """
    with pytest.raises(
        DuplicateExpireTimeSelectedError,
        match="Choose either result_ex_time or result_px_time.",
    ):
        test_backend = MatrixResultBackend(
            homeserver_url=os.environ['MATRIX_HOMESERVER_URL'],
            access_token=os.environ['MATRIX_ACCESS_TOKEN'],
            room_id=os.environ['MATRIX_ROOM_ID'],
            result_ex_time=1,
            result_px_time=1
        )

async def test_matrix_result_backend_shutdown(test_matrix_result_backend):
    """
    Tests that calling shutdown closes the result backend's client
    """
    test_backend = await test_matrix_result_backend()
    mock_client = AsyncMock()
    test_backend.matrix_client = mock_client
    await test_backend.shutdown()

    mock_client.close.assert_called_once()
    
async def test_matrix_result_backend_set_result_ex_time_case(test_matrix_result_backend):
    """
    """
    test_backend = await test_matrix_result_backend()
    # set result_ex_time to 1 for test purposes
    test_backend.result_ex_time = 1

    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    assert result.labels == {}

    with patch('taskiq_matrix.matrix_result_backend.send_message', new=AsyncMock()) as mock_send_message:
        await test_backend.set_result(test_task_id, result)

    comparison_message = {
        "name": test_task_id,
        "value": b64encode(pickle.dumps(result)).decode(),
        "ex": test_backend.result_ex_time
    }

    mock_send_message.assert_called_with(
        test_backend.matrix_client,
        test_backend.room,
        comparison_message,
        msgtype=f"taskiq.result.{test_task_id}"
    )

    assert result.labels == {'device': test_backend.device_name}
    
async def test_matrix_result_backend_set_result_px_time_case(test_matrix_result_backend):
    """
    """
    test_backend = await test_matrix_result_backend()
    # set result_ex_time to 1 for test purposes
    test_backend.result_px_time = 1

    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    assert result.labels == {}

    with patch('taskiq_matrix.matrix_result_backend.send_message', new=AsyncMock()) as mock_send_message:
        await test_backend.set_result(test_task_id, result)

    comparison_message = {
        "name": test_task_id,
        "value": b64encode(pickle.dumps(result)).decode(),
        "px": test_backend.result_px_time
    }

    mock_send_message.assert_called_with(
        test_backend.matrix_client,
        test_backend.room,
        comparison_message,
        msgtype=f"taskiq.result.{test_task_id}"
    )

    assert result.labels == {'device': test_backend.device_name}

async def test_matrix_result_backend_is_result_ready_room_message_error(test_matrix_result_backend):
    """
    """

    test_backend = await test_matrix_result_backend()
    test_task_id = str(uuid4())
    test_backend.next_batch = None
    test_backend.matrix_client.next_batch = None

    with patch('taskiq_matrix.matrix_result_backend.FractalAsyncClient.room_messages', return_value=AsyncMock(spec=RoomMessagesError, start="test start")):
        with patch('taskiq_matrix.matrix_result_backend.run_sync_filter', new=AsyncMock(return_value={})) as mock_run_sync_filter:
            await test_backend.is_result_ready(test_task_id)
    
    assert test_backend.next_batch == None
    assert test_backend.matrix_client.next_batch == None

async def test_matrix_result_backend_is_result_ready_room_message_error_next_batch_updates(test_matrix_result_backend):
    """
    NOTE The client is the only object that get's its next batch updated in this case.
    """

    test_backend = await test_matrix_result_backend()
    test_task_id = str(uuid4())
    test_backend.next_batch = None
    test_backend.matrix_client.next_batch = None

    with patch('taskiq_matrix.matrix_result_backend.FractalAsyncClient.room_messages', return_value=AsyncMock(spec=RoomMessagesError, start="test start")):
        await test_backend.is_result_ready(test_task_id)
    
    assert test_backend.next_batch == None
    assert test_backend.matrix_client.next_batch != None


async def test_matrix_result_backend_is_result_ready_result_is_ready(test_matrix_result_backend):
    """
    #! find out how to return true
    """
    test_backend = await test_matrix_result_backend()
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await test_backend.set_result(test_task_id, result)
    result = await test_backend.is_result_ready(test_task_id)
    print('result====', result)


async def test_matrix_result_backend_is_result_ready_result_not_ready(test_matrix_result_backend):
    """
    """

async def test_matrix_result_backend_get_result_decode_error_loading_result_from_task(test_matrix_result_backend):
    """
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    # assign the task to the MatrixResultBackend object without logs to raise an exception
    with patch("taskiq_matrix.matrix_result_backend.b64decode", side_effect=Exception):
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception) as e:
                await result_backend.get_result(task_id=task_id)

            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        assert "Error loading result from returned task" in logged_string

    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_decode_error_fetching_result_from_matrix(test_matrix_result_backend):
    """
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    with patch('taskiq_matrix.matrix_result_backend.run_sync_filter', new=AsyncMock(return_value=None)):
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception) as e:
                await result_backend.get_result(task_id=task_id)

            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        assert "Error getting task result from Matrix" in logged_string

async def test_matrix_result_backend_get_result_decode_error_loading_as_taskiq_result(test_matrix_result_backend):
    """
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    with patch("pickle.loads", side_effect=Exception):
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception):
                await result_backend.get_result(task_id=task_id)

            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        assert "Error loading result as taskiq result:" in logged_string

async def test_matrix_result_backend_get_result_not_with_logs(test_matrix_result_backend):
    """
    clears the log in the taskiqresult object
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    result.log = "test log"

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=False)

    assert taskiq_result_from_get_result.log is None

async def test_matrix_result_backend_get_result_with_logs(test_matrix_result_backend):
    """
    preserves the log in the taskiqresult object
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    result.log = "test log"

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)

    assert taskiq_result_from_get_result.log == "test log"

async def test_matrix_result_backend_get_result_result_available(test_matrix_result_backend):
    """
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    await result_backend.set_result(task_id=task_id, result=result)

    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)
    assert taskiq_result_from_get_result == result

async def test_matrix_result_backend_get_result_no_result_set(test_matrix_result_backend):
    """
    """
    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # create a task_id
    task_id = str(uuid4())

    with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
        with pytest.raises(Exception):
            taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)

        call_args = mock_logger.error.mock_calls[0][1]  
        logged_string = call_args[0]

    assert "Error getting task result from Matrix" in logged_string