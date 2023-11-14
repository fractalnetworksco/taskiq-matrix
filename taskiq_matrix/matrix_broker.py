import asyncio
import itertools
import json
import logging
import os
import socket
from typing import Any, AsyncGenerator, List
from uuid import uuid4

from nio import RoomGetStateEventError, RoomPutStateError
from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from .exceptions import LockAcquireError, ScheduledTaskRequiresTaskIdLabel
from .lock import MatrixLock
from .log import Logger
from .matrix_queue import BroadcastQueue, MatrixQueue, Task
from .utils import send_message

logging.getLogger("nio").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEDULE_STATE_TYPE = "taskiq.schedules"


class MatrixBroker(AsyncBroker):
    device_queue: MatrixQueue
    broadcast_queue: BroadcastQueue
    mutex_queue: MatrixQueue

    def __init__(
        self,
        result_backend: Any = None,
        task_id_generator: Any = None,
    ) -> None:
        """
        A taskiq broker backed by the Matrix protocol.

        A MatrixBroker is comprised of three independent task queues:

        - device_queue: a device's independent queue of tasks that are
        to be executed by only that device
        - broadcast_queue: a queue of tasks that should be executed
        by all devices in a room (group)
        - mutex_queue: a queue of tasks that should be run by any (only one)
        device in a group

        Requires the following environment variables to be set:
        - MATRIX_HOMESERVER_URL
        - MATRIX_ACCESS_TOKEN
        - MATRIX_ROOM_ID

        NOTE: Rate limiting for the configured user should be disabled:
        `insert into ratelimit_override values ("@mjolnir:my-homeserver.chat", 0, 0);`
        https://github.com/matrix-org/synapse/issues/6286#issuecomment-646944920
        """
        super().__init__(result_backend=result_backend, task_id_generator=task_id_generator)

        try:
            os.environ["MATRIX_HOMESERVER_URL"]
            os.environ["MATRIX_ACCESS_TOKEN"]
            self.room_id = os.environ["MATRIX_ROOM_ID"]
        except KeyError as e:
            raise KeyError(f"Missing required environment variable: {e}")

        self.device_name = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname())
        self.mutex_queue = MatrixQueue("mutex", room_id=self.room_id)
        self.device_queue = MatrixQueue(f"device.{self.device_name}", room_id=self.room_id)
        self.broadcast_queue = BroadcastQueue("broadcast", room_id=self.room_id)
        self.worker_id = uuid4().hex
        self.logger = Logger()

    async def add_mutex_checkpoint_task(self) -> bool:
        """
        Adds the mutex checkpoint task to the room's taskiq.schedules
        if it isn't already there.

        Returns:
            True if the checkpoint task was added (or already exists), else False.
        """
        task = {
            "name": "taskiq.update_checkpoint",
            "cron": "* * * * *",
            "labels": {"task_id": "mutex_checkpoint", "queue": "mutex"},
            "args": ["mutex"],
            "kwargs": {},
        }

        schedules = await self.mutex_queue.client.room_get_state_event(
            self.mutex_queue.room_id,
            SCHEDULE_STATE_TYPE,
        )

        if isinstance(schedules, RoomGetStateEventError):
            if schedules.status_code != "M_NOT_FOUND":
                raise Exception(schedules.message)

            self.logger.log(
                f"No schedules found for room {self.mutex_queue.room_id}, will attempt to add checkpoint task",
                "info",
            )
            content = {"tasks": [task]}

        # nio for some reason does not return a RoomGetStateEventError when
        # the user is not in the room (M_FORBIDDEN). Instead, the content
        # will be a dict with an "errcode" key.
        elif "errcode" in schedules.content:
            raise Exception(schedules.content["error"])

        elif task not in schedules.content["tasks"]:
            # there were already scheduled tasks in the room but the checkpoint
            # task was not found in the list of tasks, so add it
            self.logger.log(
                f"Checkpoint task not found in {self.mutex_queue.room_id} schedules, adding it",
                "info",
            )
            schedules.content["tasks"].append(task)
            content = schedules.content

        else:
            self.logger.log(
                f"Checkpoint task already exists in {self.mutex_queue.room_id} schedules", "info"
            )
            return True

        # update schedule state to include checkpoint task
        try:
            async with MatrixLock().lock(SCHEDULE_STATE_TYPE):
                self.logger.log(
                    f"Adding checkpoint task to {self.mutex_queue.room_id} schedules", "info"
                )
                res = await self.mutex_queue.client.room_put_state(
                    self.mutex_queue.room_id,
                    SCHEDULE_STATE_TYPE,
                    content,
                )
                if isinstance(res, RoomPutStateError):
                    self.logger.log(f"Failed to add checkpoint task: {res}", "error")
                    return False
                else:
                    return True
        except LockAcquireError as e:
            self.logger.log(f"{e}\n\n")
            return False

    async def update_device_checkpoints(self, interval: int = 60):
        """
        Background task that periodically updates the device and broadcast
        queue checkpoints.

        Note: Figure out how to test block of code in infinite loop.
            The infinite loop makes this currently untestable

        Args:
            interval (int): The interval in seconds to update the checkpoints.
                            Defaults to 60 seconds.
        """
        from .tasks import update_checkpoint

        while True:
            try:
                # run both updates in parallel
                await asyncio.gather(update_checkpoint("device"), update_checkpoint("broadcast"))
            except Exception as err:
                self.logger.log(f"Encountered error in update_device_checkpoint: {err}", "error")

            await asyncio.sleep(interval)

    async def startup(self) -> None:
        """
        Starts up the broker by connecting to the matrix server and
        performing an initial sync.

        Will exit if the initial sync fails or the provided room is not found.
        """
        await super().startup()

        # create and initialize queues
        await self.device_queue.checkpoint.get_or_init_checkpoint()
        await self.broadcast_queue.checkpoint.get_or_init_checkpoint()
        await self.mutex_queue.checkpoint.get_or_init_checkpoint()

        # ensure that checkpoint schedule task is added to schedules
        await self.add_mutex_checkpoint_task()

        # launch brackground task that updates device checkpoints
        asyncio.create_task(self.update_device_checkpoints())

        return None

    async def shutdown(self) -> None:
        """
        Shuts down the broker.
        """
        await self.device_queue.shutdown()
        await self.broadcast_queue.shutdown()
        await self.mutex_queue.shutdown()
        return await super().shutdown()

    def _use_task_id(self, task_id: str, message: BrokerMessage) -> BrokerMessage:
        """
        Updates the given message with the provided task id.

        Args:
            task_id (str): The task id to use.
            message (BrokerMessage): The message to update.

        Returns:
            The updated message.
        """
        message = message.model_copy()
        message.task_id = task_id

        # decode message body back into string and load as json
        message_body = message.message.decode("utf-8")
        message_body = json.loads(message_body)

        # update task id for the message then encode back into bytes
        message_body["task_id"] = task_id
        message_body = json.dumps(message_body)
        message_body = message_body.encode("utf-8")

        message.message = message_body
        return message

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicks a task into the broker.
        """
        queue_name = message.labels.get("queue", "mutex")
        queue: MatrixQueue = getattr(self, f"{queue_name}_queue")

        message_body = message.message
        if message.labels.get("scheduled_task"):
            # task is a scheduled task, so need to lock on the task id that
            # is provided in the schedule. This ensures that the task
            # is only kicked once.
            task_id = message.labels.get("task_id")
            if not task_id:
                raise ScheduledTaskRequiresTaskIdLabel(message.task_id)

            try:
                async with MatrixLock().lock(f"{queue.task_types.task}.{task_id}"):
                    # generate a new unique task id for the message
                    task_id = self.id_generator()
                    message = self._use_task_id(task_id, message)
                    message_body = json.loads(message.message.decode("utf-8"))
                    await send_message(
                        queue.client,
                        queue.room_id,
                        message_body,
                        msgtype=f"{queue.task_types.task}",
                        task_id=message.task_id,
                        queue=queue_name,
                    )
                return None
            except LockAcquireError:
                self.logger.log(f"Failed to acquire lock for schedule {task_id}", "info")
                return None

        if message.labels.get("task_id"):
            # task id was provided in labels, so use it
            message = self._use_task_id(message.labels["task_id"], message)
            message_body = json.loads(message.message.decode("utf-8"))

        # regular task was kicked, simply send message into room
        return await send_message(
            queue.client,
            queue.room_id,
            message_body,
            msgtype=f"{queue.task_types.task}",
            task_id=message.task_id,
            queue=queue_name,
        )

    async def get_tasks(self) -> AsyncGenerator[List[Task], Any]:
        while True:
            tasks = {
                self.device_queue.name: asyncio.create_task(
                    self.device_queue.get_unacked_tasks(), name=self.device_queue.name
                ),
                self.broadcast_queue.name: asyncio.create_task(
                    self.broadcast_queue.get_unacked_tasks(), name=self.broadcast_queue.name
                ),
                self.mutex_queue.name: asyncio.create_task(
                    self.mutex_queue.get_unacked_tasks(), name=self.mutex_queue.name
                ),
            }
            sync_tasks = [
                tasks[self.device_queue.name],
                tasks[self.broadcast_queue.name],
                tasks[self.mutex_queue.name],
            ]

            done, pending = await asyncio.wait(sync_tasks, return_when=asyncio.FIRST_COMPLETED)

            sync_task_results: List[List[Task]] = []
            for completed_task in done:
                try:
                    queue, pending_tasks = completed_task.result()
                    if pending_tasks:
                        sync_task_results.append(pending_tasks)
                        self.logger.log(f"Got {len(pending_tasks)} tasks from {queue}", "debug")
                except Exception as e:
                    self.logger.log(f"Sync failed: {e}", "error")
                    raise e
            for pending_task in pending:
                if pending_task.done():
                    queue, pending_tasks = pending_task.result()
                    if pending_tasks:
                        self.logger.log(f"Got {len(pending_tasks)} tasks from {queue}", "debug")
                        sync_task_results.append(pending_tasks)
                else:
                    pending_task.cancel()
            yield list(itertools.chain.from_iterable(sync_task_results))

    async def listen(self) -> AsyncGenerator[AckableMessage, Any]:
        """
        Listen Matrix for new messages.

        This function sends sync requests to the Matrix server
        and yields all "taskiq.task" events.

        :yields: broker messages.
        """
        async for tasks in self.get_tasks():
            if not tasks:
                self.logger.log(f"No tasks found for room: {self.room_id}")

                # Using the next batch from the client since the current checkpoint
                # is likely returning tasks that all have acks. Since the checkpoint
                # is returning task events that proceed its sync token, the
                # timeout is not respected. This can result in rapid fire
                # requests to the Matrix server. To avoid this, the next batch
                # token on the matrix client will allow catching new messages
                # that come in while still respecting the matrix client's timeout.
                # Doing so helps avoid rapid fire requests.
                self.device_queue.checkpoint.since_token = self.device_queue.client.next_batch
                self.broadcast_queue.checkpoint.since_token = (
                    self.broadcast_queue.client.next_batch
                )
                self.mutex_queue.checkpoint.since_token = self.mutex_queue.client.next_batch
                continue

            for task in tasks:
                queue_name = task.queue
                queue: MatrixQueue = getattr(self, f"{queue_name}_queue")

                # TODO: check if task has an assigned worker id.

                try:
                    yield await queue.yield_task(task)
                except LockAcquireError as lock_err:
                    # if lock cannot be acquired, then another worker is already processing the task
                    self.logger.log(str(lock_err))
                    continue
                except Exception as e:
                    self.logger.log(f"Error occurred while yielding task: {e}")
                    continue
