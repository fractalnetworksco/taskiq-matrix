import logging

from nio import RoomContextError

from .exceptions import QueueDoesNotExist
from .filters import create_filter, get_first_unacked_task, run_sync_filter
from .instance import broker
from .matrix_queue import MatrixQueue

logger = logging.getLogger(__name__)


@broker.task(task_name="taskiq.update_checkpoint", retry_on_error=True, labels={"queue": "mutex"})
async def update_checkpoint(queue_name: str) -> bool:
    """
    Updates the checkpoint for the current broker

    FIXME: There is an edge case where a task has been acked but a result hasn't
           come in, but because it has an ack, we could potentially update the
           checkpoint past the task (causing the task to be lost) without the task
           ever actually having a result.

    Args:
        queue: The name of the queue on the broker to update the checkpoint for.

    Returns:
        True if checkpoint was successfully updated, False otherwise.
    """
    try:
        queue: MatrixQueue = getattr(broker, f"{queue_name}_queue")
    except AttributeError:
        raise QueueDoesNotExist(f"Queue {queue_name} does not exist")

    current_since_token = await queue.checkpoint.get_or_init_checkpoint()

    logger.info("Fetching all tasks")

    # gets all tasks and acks since the checkpoint (including their event ids)
    # TODO: handle using results. Once we use them, we can use task_types.all()
    room_filter = create_filter(
        queue.room_id, types=[queue.task_types.task, queue.task_types.ack]
    )
    tasks = await run_sync_filter(
        queue.client,
        room_filter,
        since=current_since_token,
        content_only=False,
        timeout=0,
    )

    # pull tasks for the broker's room
    tasks = tasks.get(queue.room_id)
    if tasks:
        unacked_task = await get_first_unacked_task(tasks)
        if unacked_task:
            event_id = unacked_task["event_id"]

            # limit on 2 so that the "start" sync token is the event before the first unacked task
            context = await queue.client.room_context(queue.room_id, event_id, limit=2)
            if isinstance(context, RoomContextError):
                raise Exception("Error fetching context: ", context.message)

            new_checkpoint = context.start
        else:
            logger.info("No unacked tasks found")
            # no unacked tasks, so just use the queue's current matrix client sync token
            new_checkpoint = queue.client.next_batch

    else:
        logger.info("No tasks found")
        # no tasks found, so just use the queue's matrix client sync token
        # TODO: ensure that this is the correct sync token to use
        new_checkpoint = queue.client.next_batch

    logger.info(f"Updating {queue.name}'s checkpoint to {new_checkpoint}")
    success = await queue.checkpoint.put_checkpoint_state(new_checkpoint)
    if success:
        logger.info(f"Updated {queue.name}'s checkpoint to {new_checkpoint}")

    return True
