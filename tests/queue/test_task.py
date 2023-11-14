from taskiq_matrix.matrix_queue import Task
import json

async def test_task_verify_constructor():
    """
    Test that the data passed to the Task constructor matches what is in the task
    object.
    """

    # create a task event dictionary to use as a parameter for a Task object
    task_event = {
        "body": {
            "task_id": "1",
            "task": json.dumps({"data": "Test Task 1"}),
            "queue": "mutex",
        },
        "msgtype": "matrix_queue.task_types.ack"
    }

    # create a Task object
    test_task = Task(**task_event)
    
    # verify that the information created locally matches what is 
    # returned in the task object
    assert test_task.id == task_event["body"]["task_id"]
    assert test_task.type == task_event["msgtype"]
    assert test_task.data == json.loads(task_event["body"]["task"])
    assert test_task.queue == task_event["body"]["queue"]