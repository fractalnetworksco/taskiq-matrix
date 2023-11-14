from unittest.mock import patch
from uuid import uuid4

import pytest
from taskiq.result import TaskiqResult
from taskiq_matrix.matrix_result_backend import MatrixResultBackend

# TODO: move result_backend instantiation into a fixture so that it can be reused
#       See `conftest.py` for an example. The implementation of the fixture should
#       be the same as the matrix_client fixture that is in that file.


async def test_result_backend_works():
    """
    Ensure that only one instance of a lock can be acquired for a certain key.
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = MatrixResultBackend()

    # set a task
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # assign the task to the MatrixResultBackend object
    await result_backend.set_result(task_id, result)

    # check if the result is ready and available to the user
    assert await result_backend.is_result_ready(task_id)

    result = await result_backend.get_result(task_id)
    assert isinstance(result, TaskiqResult)
    assert result.return_value == "chicken"

    await result_backend.shutdown()


async def test_is_result_ready():
    """
    Test if the result from MaxtrixResultBacken is available to the user
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = MatrixResultBackend()

    # check if the result is ready before giving a task
    assert await result_backend.is_result_ready(task_id=task_id) == False

    # sets a task and returns True
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)
    assert await result_backend.is_result_ready(task_id=task_id)


async def test_get_result_no_result():
    """
    Test error
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = MatrixResultBackend()

    with pytest.raises(Exception):
        assert await result_backend.get_result(task_id=task_id, with_logs=False)


async def test_get_result_decode_error():
    """
    Test decode error exception by setting with_logs to false
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = MatrixResultBackend()

    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)

    # set a result for the backend object
    await result_backend.set_result(task_id=task_id, result=result)

    # assign the task to the MatrixResultBackend object without logs to raise an exception
    with patch("taskiq_matrix.matrix_result_backend.b64decode", side_effect=Exception):
        with pytest.raises(Exception):
            await result_backend.get_result(task_id=task_id, with_logs=False)

    with patch("pickle.loads", side_effect=Exception):
        with pytest.raises(Exception):
            await result_backend.get_result(task_id=task_id, with_logs=False)


async def test_set_result_ex_px():
    """
    Test manually setting px_time and ex_time
    """
    # create a task_id
    task_id = str(uuid4())

    # create a MatrixResultBackend object
    result_backend = MatrixResultBackend(result_ex_time=1)
    result = TaskiqResult(is_err=False, return_value="chicken", execution_time=1.0)
    await result_backend.set_result(task_id=task_id, result=result)

    # set a px time of 1
    result_backend = MatrixResultBackend(result_px_time=1)
    await result_backend.set_result(task_id=task_id, result=result)


async def test_constructor_duplicate_time():
    """
    Test sending both px_time and ex_time to the MatrixResultBackend
    constructor to raise an exception
    """
    # create a task_id
    task_id = str(uuid4())

    # set two different px and ex times to raise an exception
    with pytest.raises(Exception):
        result_backend = MatrixResultBackend(result_ex_time=10, result_px_time=100)


async def test_constructor_ex_px_gt_zero():
    """
    Test raising an exception by passing a negative
    ex_time in the MatrixResultBackend constructor
    """
    # create a task_id
    task_id = str(uuid4())

    # set ex_time to a negative number to raise an exception
    with pytest.raises(Exception):
        result_backend = MatrixResultBackend(result_ex_time=-10, result_px_time=100)
