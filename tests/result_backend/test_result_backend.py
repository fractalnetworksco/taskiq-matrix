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

    # call the constructor with ex=None and px=-1 to raise an exception
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

    # call the constructor with ex=-1 and px=None to raise an exception
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

    # call the constructor with ex=-1 and px=-1 to raise an exception
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

    # call the constructor with two expire times to raise an exception
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

    # create a MatrixResultBackend object from fixture
    test_backend = await test_matrix_result_backend()

    # set the backend result's client
    mock_client = AsyncMock()
    test_backend.matrix_client = mock_client

    #call shutdown and verify that the client was closed
    await test_backend.shutdown()
    mock_client.close.assert_called_once()
    
async def test_matrix_result_backend_set_result_ex_time_case(test_matrix_result_backend):
    """
    Tests that the message dictionary created in set_result contains an "ex" key-value
    pair if the MatrixBackendResult object has an existing ex time
    """

    test_backend = await test_matrix_result_backend()
    # set result_ex_time to 1 for test purposes
    test_backend.result_ex_time = 1

    # create a TaskiqResult object
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # verify that the TaskiqResult labels attribute is an empty dictionary
    assert result.labels == {}

    # call set result, patching the send_message function
    with patch('taskiq_matrix.matrix_result_backend.send_message', new=AsyncMock()) as mock_send_message:
        await test_backend.set_result(test_task_id, result)

    # establish what the function should return for a comparison.
        # NOTE this case should include an "ex" in the dictionary
    comparison_message = {
        "name": test_task_id,
        "value": b64encode(pickle.dumps(result)).decode(),
        "ex": test_backend.result_ex_time
    }

    # verify that the send_message arguments match what was expected and that the
    # result's labels were updated
    mock_send_message.assert_called_with(
        test_backend.matrix_client,
        test_backend.room,
        comparison_message,
        msgtype=f"taskiq.result.{test_task_id}"
    )
    assert result.labels == {'device': test_backend.device_name}
    await test_backend.shutdown()
    
async def test_matrix_result_backend_set_result_px_time_case(test_matrix_result_backend):
    """
    Tests that the message dictionary created in set_result contains an "px" key-value
    pair if the MatrixBackendResult object has an existing px time
    """
    test_backend = await test_matrix_result_backend()
    # set result_px_time to 1 for test purposes
    test_backend.result_px_time = 1

    # create a TaskiqResult object
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # verify that the TaskiqResult labels attribute is an empty dictionary
    assert result.labels == {}

    # call set result, patching the send_message function
    with patch('taskiq_matrix.matrix_result_backend.send_message', new=AsyncMock()) as mock_send_message:
        await test_backend.set_result(test_task_id, result)

    # establish what the function should return for a comparison.
        # NOTE this case should include an "px" in the dictionary
    comparison_message = {
        "name": test_task_id,
        "value": b64encode(pickle.dumps(result)).decode(),
        "px": test_backend.result_px_time
    }

    # verify that the send_message arguments match what was expected and that the
    # result's labels were updated
    mock_send_message.assert_called_with(
        test_backend.matrix_client,
        test_backend.room,
        comparison_message,
        msgtype=f"taskiq.result.{test_task_id}"
    )
    assert result.labels == {'device': test_backend.device_name}
    await test_backend.shutdown()

async def test_matrix_result_backend_set_result_no_time_case(test_matrix_result_backend):
    """
    Tests that the message dictionary created in set_result contains no "ex" or "px"
    key-value pairs if the MatrixBackendResult object doesn't have either of them set.
    """
    test_backend = await test_matrix_result_backend()

    # create a TaskiqResult object
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # verify that the TaskiqResult labels attribute is an empty dictionary
    assert result.labels == {}

    # call set result, patching the send_message function
    with patch('taskiq_matrix.matrix_result_backend.send_message', new=AsyncMock()) as mock_send_message:
        await test_backend.set_result(test_task_id, result)

    # establish what the function should return for a comparison.
        # NOTE this case should should not include an "ex" or "px" key-value pair
    comparison_message = {
        "name": test_task_id,
        "value": b64encode(pickle.dumps(result)).decode(),
    }

    # verify that the send_message arguments match what was expected and that the
    # result's labels were updated
    mock_send_message.assert_called_with(
        test_backend.matrix_client,
        test_backend.room,
        comparison_message,
        msgtype=f"taskiq.result.{test_task_id}"
    )
    assert result.labels == {'device': test_backend.device_name}
    await test_backend.shutdown()

async def test_matrix_result_backend_is_result_ready_room_message_error(test_matrix_result_backend):
    """
    Tests that the MatrixBackendResult's as well as its client's next_batch are not
    updated from within the is_result_ready function if room_messages returns a RoomMessagesError

    NOTE The client's next_batch attribute will be altered outside of this function in 
    run_sync_filter, as shown in a later test
    """

    # create a MatrixResultBackend object
    test_backend = await test_matrix_result_backend()

    # create a task id 
    test_task_id = str(uuid4())

    # set the next_batch attributes to None
    test_backend.next_batch = None
    test_backend.matrix_client.next_batch = None

    # patch room_messages to return a RoomMessagesError
    with patch('taskiq_matrix.matrix_result_backend.FractalAsyncClient.room_messages', return_value=AsyncMock(spec=RoomMessagesError, start="test start")):
        # patch run_sync_filter to properly test that next_batch is not updated in _is_result_ready 
        with patch('taskiq_matrix.matrix_result_backend.run_sync_filter', new=AsyncMock(return_value={})) as mock_run_sync_filter:
            await test_backend.is_result_ready(test_task_id)
    
    # verify that the next_batch attributes were not changed
    assert test_backend.next_batch == None
    assert test_backend.matrix_client.next_batch == None
    await test_backend.shutdown()

async def test_matrix_result_backend_is_result_ready_room_message_error_next_batch_updates(test_matrix_result_backend):
    """
    Tests that the MatrixBackendResult's client's next_batch attribute is changed outside of 
    the function when is_result_ready is called

    NOTE The client is the only object that get's its next batch updated in this case.
    """

    # create a MatrixResultBackend object
    test_backend = await test_matrix_result_backend()

    # create a task id 
    test_task_id = str(uuid4())

    # set the next_batch attributes to None
    test_backend.next_batch = None
    test_backend.matrix_client.next_batch = None

    # patch room_messages to return a RoomMessagesError
    with patch('taskiq_matrix.matrix_result_backend.FractalAsyncClient.room_messages', return_value=AsyncMock(spec=RoomMessagesError, start="test start")):
        await test_backend.is_result_ready(test_task_id)
    
    # verify that the backend result's next_batch remained unchanged while the client's
        # next_batch was updated
    assert test_backend.next_batch == None
    assert test_backend.matrix_client.next_batch != None
    await test_backend.shutdown()


async def test_matrix_result_backend_is_result_ready_result_is_ready(test_matrix_result_backend):
    """
    Tests that is_result_ready returns True if there is a result already set
    """

    # create a MatrixBackendResult object from a fixture
    test_backend = await test_matrix_result_backend()
    
    # create a TaskiqResult object and set it
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await test_backend.set_result(test_task_id, result)

    # call is_result_ready and verify that it returned true
    result = await test_backend.is_result_ready(test_task_id)
    assert result
    await test_backend.shutdown()


async def test_matrix_result_backend_is_result_ready_result_not_ready_no_result(test_matrix_result_backend):
    """
    Tests that is_result_ready returns False if a result has not been set
    """

    # create a MatrixBackendResult object from a fixture
    test_backend = await test_matrix_result_backend()
    
    # create a TaskiqResult object but don't set it
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # call is_result_ready and verify that it returned false
    result = await test_backend.is_result_ready(test_task_id)
    assert not result
    await test_backend.shutdown()

async def test_matrix_result_backend_is_result_ready_result_not_ready_wrong_task_id(test_matrix_result_backend):
    """
    Tests that is_result_ready returns False if there is a result that is set but it is
    given a different task_id
    """

    # create a MatrixBackendResult object from a fixture
    test_backend = await test_matrix_result_backend()
    
    # create a TaskiqResult object and set it
    test_task_id = str(uuid4())
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await test_backend.set_result(test_task_id, result)

    # call is_result_ready with a different task_id and verify that it returned false
    result = await test_backend.is_result_ready("wrong_task_id")
    assert not result
    await test_backend.shutdown()


async def test_matrix_result_backend_get_result_decode_error_loading_result_from_task(test_matrix_result_backend):
    """
    Tests that an exception is raised if there is an error loading the result from the 
    returned task.
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)

    # patch the b64decode function call to raise an exception
    with patch("taskiq_matrix.matrix_result_backend.b64decode", side_effect=Exception):
        # patch the file's logger to make sure the correct exception is raised
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception):
                await result_backend.get_result(task_id=task_id)

            # store the logger's call arguments
            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        # verify that the logger argument was the correct one
        assert "Error loading result from returned task" in logged_string
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_decode_error_fetching_result_from_matrix(test_matrix_result_backend):
    """
    Tests that an exception is raised if there is an error getting the task from Matrix
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()


    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)

    # patch run_sync_filter to return None, raising an exception when its indeces are inaccessable
    with patch('taskiq_matrix.matrix_result_backend.run_sync_filter', new=AsyncMock(return_value=None)):
        # patch the file's logger to make sure the correct exception is raised
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception):
                await result_backend.get_result(task_id=task_id)

            # store the logger's call arguments
            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        # verify that the logger argument was the correct one
        assert "Error getting task result from Matrix" in logged_string
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_decode_error_loading_as_taskiq_result(test_matrix_result_backend):
    """
    Tests that an exception is raised if there is an error loading the result as
    a TaskiqResult object
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)

    # patch pickle.loads to raise an exception
    with patch("pickle.loads", side_effect=Exception):
        # patch the file's logger to make sure the correct exception is raised
        with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
            with pytest.raises(Exception):
                await result_backend.get_result(task_id=task_id)

            # store the logger's call arguments
            call_args = mock_logger.error.mock_calls[0][1]  
            logged_string = call_args[0]

        # verify that the logger argument was the correct one
        assert "Error loading result as taskiq result:" in logged_string
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_not_with_logs(test_matrix_result_backend):
    """
    Tests that passing with_logs as False clears the log in the taskiqresult object
    """

    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    result.log = "test log"
    await result_backend.set_result(task_id=task_id, result=result)

    # call get_result passing with_logs=False
    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=False)

    # verify that the returned object's log attribute is set to None
    assert taskiq_result_from_get_result.log is None
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_with_logs(test_matrix_result_backend):
    """
    Tests that passing with_logs as True preserves the log in the taskiqresult object
    """

    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    result.log = "test log"
    await result_backend.set_result(task_id=task_id, result=result)

    # call get_result passing with_logs=True
    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)

    # verify that the returned object's log attribute is the same as the created log in the 
    # TaskiqResult object that was set
    assert taskiq_result_from_get_result.log == "test log"
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_result_available(test_matrix_result_backend):
    """
    Tests that get_result returns the same TaskiqResult object that was set using set_result
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # set a result for the backend object
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)

    # call get_result and verify that it matches the object that was set
    taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)
    assert taskiq_result_from_get_result == result
    await result_backend.shutdown()

async def test_matrix_result_backend_get_result_no_result_set(test_matrix_result_backend):
    """
    Tests that an exception is raised if you call get_result when no result has been set
    """
    # create a MatrixResultBackend object
    result_backend = await test_matrix_result_backend()

    # create a task_id
    task_id = str(uuid4())

    # patch the file's logger to make sure the correct exception is raised
    with patch('taskiq_matrix.matrix_result_backend.logger', new=MagicMock()) as mock_logger:
        with pytest.raises(Exception):
            taskiq_result_from_get_result = await result_backend.get_result(task_id=task_id, with_logs=True)

        # store the logger's call arguments
        call_args = mock_logger.error.mock_calls[0][1]  
        logged_string = call_args[0]

    # verify that the logger argument was the correct one
    assert "Error getting task result from Matrix" in logged_string
    await result_backend.shutdown()