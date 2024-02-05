import asyncio
import itertools
import json
import logging
import os
import socket
from typing import Any, AsyncGenerator, List, Optional, Self, TypeVar, Union
from uuid import uuid4

from fractal.matrix.async_client import FractalAsyncClient
from nio import RoomGetStateEventError, RoomPutStateError
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from .exceptions import (
    DeviceQueueRequiresDeviceLabel,
    LockAcquireError,
    ScheduledTaskRequiresTaskIdLabel,
)
from .lock import MatrixLock
from .matrix_queue import BroadcastQueue, MatrixQueue, ReplicatedQueue, Task
from .matrix_result_backend import MatrixResultBackend
from .schedulesource import SCHEDULE_STATE_TYPE
from .utils import send_message

logging.getLogger("nio").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class MatrixBroker(AsyncBroker):
    device_queue: MatrixQueue
    broadcast_queue: BroadcastQueue
    mutex_queue: MatrixQueue
    replication_queue: ReplicatedQueue
    result_backend: MatrixResultBackend
    room_id: str
    homeserver_url: str
    access_token: str

    def __init__(
        self,
        result_backend: Optional[AsyncResultBackend] = None,
        task_id_generator: Any = None,
    ) -> None:
        """
        A taskiq broker backed by the Matrix protocol.

        A MatrixBroker is comprised of four independent task queues:

        - device_queue: a device's independent queue of tasks that are
        to be executed by only that device
        - broadcast_queue: a queue of tasks that should be executed
        by all devices in a room (group)
        - mutex_queue: a queue of tasks that should be run by any (only one)
        device in a group
        - replication_queue: a queue of tasks that should be run by each device

        NOTE: Rate limiting for the configured user should be disabled:
        `insert into ratelimit_override values ("@mjolnir:my-homeserver.chat", 0, 0);`
        https://github.com/matrix-org/synapse/issues/6286#issuecomment-646944920

        TODO: Figure out how to dynamically register queues.
        """
        super().__init__(result_backend=result_backend, task_id_generator=task_id_generator)

        self.device_name = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname())
        self.worker_id = uuid4().hex

    def with_matrix_config(self, room_id: str, homeserver_url: str, access_token: str) -> Self:
        self.room_id = room_id
        self.homeserver_url = homeserver_url
        self.access_token = access_token
        return self

    def _init_queues(self):
        try:
            if not all([self.room_id, self.homeserver_url, self.access_token]):
                raise Exception("Matrix config must be set with with_matrix_config.")
        except:
            raise Exception("Matrix config must be set with with_matrix_config.")

        if not hasattr(self, "mutex_queue"):
            self.mutex_queue = MatrixQueue(
                "mutex",
                homeserver_url=self.homeserver_url,
                access_token=self.access_token,
                room_id=self.room_id,
            )
            self.device_queue = MatrixQueue(
                f"device.{self.device_name}",
                homeserver_url=self.homeserver_url,
                access_token=self.access_token,
                room_id=self.room_id,
            )
            self.broadcast_queue = BroadcastQueue(
                "broadcast",
                homeserver_url=self.homeserver_url,
                access_token=self.access_token,
                room_id=self.room_id,
            )
            self.replication_queue = ReplicatedQueue(
                "replication",
                homeserver_url=self.homeserver_url,
                access_token=self.access_token,
                room_id=self.room_id,
            )

    def with_result_backend(self, result_backend: AsyncResultBackend[_T]) -> Self:
        if not isinstance(result_backend, MatrixResultBackend):
            raise Exception("result_backend must be an instance of MatrixResultBackend")

        return super().with_result_backend(result_backend)

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

            logger.info(
                "No schedules found for room %s, will attempt to add checkpoint task",
                self.mutex_queue.room_id,
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
            logger.debug(
                f"Checkpoint task not found in {self.mutex_queue.room_id} schedules, adding it",
            )
            schedules.content["tasks"].append(task)
            content = schedules.content

        else:
            logger.debug(
                f"Checkpoint task already exists in {self.mutex_queue.room_id} schedules"
            )
            return True

        # update schedule state to include checkpoint task
        try:
            async with MatrixLock(room_id=self.mutex_queue.room_id).lock(SCHEDULE_STATE_TYPE):
                logger.info(f"Adding checkpoint task to {self.mutex_queue.room_id} schedules")
                res = await self.mutex_queue.client.room_put_state(
                    self.mutex_queue.room_id,
                    SCHEDULE_STATE_TYPE,
                    content,
                )
                if isinstance(res, RoomPutStateError):
                    logger.error(f"Failed to add checkpoint task: {res}")
                    return False
                else:
                    return True
        except LockAcquireError as e:
            logger.error(f"{e}\n\n")
            return False

    async def update_checkpoints(self, interval: int = 60):
        """
        Background task that periodically updates the device, broadcast,
        and replication queue checkpoints.

        Args:
            interval (int): The interval in seconds to update the checkpoints.
                            Defaults to 60 seconds.
        """
        from .tasks import update_checkpoint

        try:
            while True:
                try:
                    # run both updates in parallel
                    await asyncio.gather(
                        update_checkpoint("device"),
                        update_checkpoint("broadcast"),
                        update_checkpoint("replication"),
                    )
                except Exception as err:
                    logger.error("Encountered error in update_device_checkpoint: %s", err)

                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return None

    async def startup(self) -> None:
        """
        Starts up the broker by connecting to the matrix server and
        performing an initial sync.

        Will exit if the initial sync fails or the provided room is not found.
        """
        logger.info("Starting Taskiq Matrix Broker")
        await super().startup()

        self._init_queues()

        # create and initialize queues
        await self.device_queue.checkpoint.get_or_init_checkpoint()
        await self.broadcast_queue.checkpoint.get_or_init_checkpoint()
        await self.mutex_queue.checkpoint.get_or_init_checkpoint()
        # full sync is required for replication queue because it needs to
        # sync any tasks that were sent before the checkpoint was created for
        # this device
        await self.replication_queue.checkpoint.get_or_init_checkpoint(full_sync=True)

        # ensure that checkpoint schedule task is added to schedules
        await self.add_mutex_checkpoint_task()

        # launch background task that updates device checkpoints
        self.checkpoint_updater = asyncio.create_task(self.update_checkpoints())

        return None

    async def shutdown(self) -> None:
        """
        Shuts down the broker.
        """
        logger.info("Shutting down the broker")
        if hasattr(self, "checkpoint_updater"):
            self.checkpoint_updater.cancel()
            await self.checkpoint_updater

        await self.device_queue.shutdown()
        await self.broadcast_queue.shutdown()
        await self.mutex_queue.shutdown()
        await self.replication_queue.shutdown()
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

        room_id = message.labels.get("room_id")

        self._init_queues()

        queue_name = message.labels.get("queue", "mutex")
        device_name = message.labels.get("device")
        queue: MatrixQueue = (
            self.device_queue if device_name else getattr(self, f"{queue_name}_queue")
        )
        if not room_id:
            room_id = queue.room_id

        # populate next batch on the result backend client to avoid result delay
        if (
            isinstance(self.result_backend, MatrixResultBackend)
            and not self.result_backend.matrix_client.next_batch
        ):
            since_token = await self.result_backend.matrix_client.get_latest_sync_token(
                room_id=room_id
            )
            self.result_backend.matrix_client.next_batch = since_token

        # use a fresh new client here because kicking a task can sometimes be from
        # an ephemeral event loop
        client = FractalAsyncClient(
            homeserver_url=self.homeserver_url,
            access_token=self.access_token,
        )

        if queue == self.device_queue:
            if not device_name:
                raise DeviceQueueRequiresDeviceLabel(message.task_id)
            msgtype = queue.task_types.device_task(device_name)
            queue_name = "device"
        else:
            msgtype = queue.task_types.task

        message_body = message.message
        if message.labels.get("scheduled_task"):
            # task is a scheduled task, so need to lock on the task id that
            # is provided in the schedule. This ensures that the task
            # is only kicked once.
            task_id = message.labels.get("task_id")
            if not task_id:
                raise ScheduledTaskRequiresTaskIdLabel(message.task_id)

            try:
                async with MatrixLock(room_id=room_id).lock(f"{queue.task_types.task}.{task_id}"):
                    # generate a new unique task id for the message
                    task_id = self.id_generator()
                    message = self._use_task_id(task_id, message)
                    message_body = message.message
                    await send_message(
                        client,
                        room_id,
                        message_body,
                        msgtype=msgtype,
                        task_id=message.task_id,
                        queue=queue_name,
                    )
                return None
            except LockAcquireError:
                logger.info(f"Failed to acquire lock for schedule {task_id}")
                return None

        if message.labels.get("task_id"):
            # task id was provided in labels, so use it
            message = self._use_task_id(message.labels["task_id"], message)
            message_body = message.message.decode("utf-8")

        # regular task was kicked, simply send message into room
        await send_message(
            client,
            room_id,
            message_body,
            msgtype=msgtype,
            task_id=message.task_id,
            queue=queue_name,
        )
        return await client.close()

    async def get_tasks(self) -> AsyncGenerator[List[Task], Any]:  # pragma: no cover
        while True:
            tasks = {
                "device_queue": asyncio.create_task(
                    self.device_queue.get_unacked_tasks(), name="device_queue"
                ),
                "broadcast_queue": asyncio.create_task(
                    self.broadcast_queue.get_unacked_tasks(), name="broadcast_queue"
                ),
                "mutex_queue": asyncio.create_task(
                    self.mutex_queue.get_unacked_tasks(), name="mutex_queue"
                ),
                "replication_queue": asyncio.create_task(
                    self.replication_queue.get_unacked_tasks(exclude_self=True),
                    name="replication_queue",
                ),
            }
            sync_task_results: List[List[Task]] = []

            while tasks:
                done, _ = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                for completed_task in done:
                    queue_name = completed_task.get_name()
                    try:
                        queue, pending_tasks = completed_task.result()
                        if pending_tasks:
                            sync_task_results.append(pending_tasks)
                            logger.debug(f"Got {len(pending_tasks)} tasks from {queue}")
                    except Exception as e:
                        logger.error(f"Sync failed: {e}")

                    # Reschedule a new task for the completed queue
                    tasks[queue_name] = asyncio.create_task(
                        getattr(self, queue_name).get_unacked_tasks(), name=queue_name
                    )

                if sync_task_results:
                    yield list(itertools.chain.from_iterable(sync_task_results))
                    sync_task_results = []  # Reset for the next iteration

                # Optionally, add a short delay before starting the next round
                await asyncio.sleep(0)

    async def listen(self) -> AsyncGenerator[Union[AckableMessage, bytes], Any]:
        """
        Listen Matrix for new messages.

        This function sends sync requests to the Matrix server
        and yields all "taskiq.task" events.

        :yields: broker messages.
        """
        async for tasks in self.get_tasks():
            if not tasks:
                logger.debug(f"No tasks found for room: {self.room_id}")

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
                self.replication_queue.checkpoint.since_token = (
                    self.replication_queue.client.next_batch
                )
                continue

            for task in tasks:
                queue_name = task.queue
                queue: MatrixQueue = getattr(self, f"{queue_name}_queue")

                # TODO: check if task has an assigned worker id.

                try:
                    yield await queue.yield_task(task)
                except LockAcquireError as lock_err:
                    # if lock cannot be acquired, then another worker is already processing the task
                    logger.error(str(lock_err))
                    continue
                except Exception as e:
                    logger.error(f"Error occurred while yielding task: {e}")
                    continue
