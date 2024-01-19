import json
import logging
import os
import socket
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, Union

from fractal.matrix.async_client import FractalAsyncClient
from nio import (
    MessageDirection,
    RoomGetStateEventError,
    RoomMessagesResponse,
    RoomPutStateError,
    WhoamiError,
)
from taskiq import AckableMessage

from .exceptions import CheckpointGetOrInitError, LockAcquireError, TaskAlreadyAcked
from .filters import (
    create_room_message_filter,
    create_sync_filter,
    run_room_message_filter,
    run_sync_filter,
)
from .lock import MatrixLock
from .utils import send_message

logger = logging.getLogger(__name__)


class TaskTypes:
    """
    A TaskTypes dict represents the task types for a MatrixQueue.
    """

    def __init__(self, queue_name: str):
        self.task = f"taskiq.{queue_name}.task"
        self.ack = f"{self.task}.ack"
        self.result = "taskiq.result"
        self.lock = f"{self.task}.lock"

    def all(self) -> List[str]: # pragma: no cover
        """
        Returns the task types for the queue.
        """
        return [f"{self.task}", f"{self.ack}", f"{self.result}"]

    def device_task(self, device_name: str) -> str:
        """
        Returns the task type for the given device name.
        """
        return f"taskiq.device.{device_name}.task"


class Task:
    acknowledged: bool
    type: str
    data: str
    queue: str
    sender: str

    def __init__(self, *args, **event):
        # FIXME: if event is malformed this will fail resulting in
        # a task that can never be acked which breaks the queue
        self.id = event["body"]["task_id"]
        self.type = event["msgtype"]
        self.data = json.loads(event["body"]["task"])
        self.queue = event["body"]["queue"]
        self.sender = event["sender"]

    def __repr__(
        self,
    ):
        return f"Task(id={self.id}, type={self.type}, queue={self.queue})"

    async def yield_task(self, *args, **kwargs) -> AckableMessage:
        raise NotImplementedError


class Checkpoint:
    """
    A Checkpoint represents a point in time (Matrix sync token) at which
    all tasks prior to that point have been acked. Each Matrix backed task queue
    is backed by its own Checkpoint.
    """

    type: str
    room_id: str
    client: FractalAsyncClient
    since_token: Optional[str] = None

    def __init__(
        self,
        type: str,
        room_id: str,
        client: FractalAsyncClient,
        since_token: Optional[str] = None,
    ):
        self.type = type
        self.room_id = room_id
        self.client = client
        self.since_token = since_token

    async def get_or_init_checkpoint(self, full_sync: bool = False) -> Optional[str]:
        """
        Gets the current checkpoint from the Matrix server. If it doesn't exist,
        it will be initialized.

        Returns:
            The current checkpoint or None if not found in room state.
        """
        resp = await self.client.room_get_state_event(self.room_id, self.type)
        if isinstance(resp, RoomGetStateEventError):
            if resp.status_code != "M_NOT_FOUND":
                raise Exception(resp.message)

            # checkpoint state wasn't found, so initialize it
            logger.debug(f"No checkpoint found for type: {self.type}")

            # fetch latest sync token
            # if full_sync is false, then we fetch the latest sync token
            # if full_sync is true, then we get a sync token from the beginning of the room's timeline
            res = await self.client.room_messages(
                self.room_id,
                start="",
                limit=1,
                direction=MessageDirection.back if not full_sync else MessageDirection.front,
            )
            if not isinstance(res, RoomMessagesResponse):
                raise CheckpointGetOrInitError(self.type)
            # update the checkpoint state in the Matrix room
            await self.put_checkpoint_state(res.start)

            self.since_token = res.start
            return self.since_token
        else:
            # got back some checkpoint state, use the checkpoint value
            self.since_token = resp.content.get("checkpoint")
            return self.since_token

    async def put_checkpoint_state(self, since_token: str) -> bool:
        """
        Sets the current checkpoint on the Matrix server.

        Returns:
            True if the checkpoint was set, else False.
        """
        # acquire lock on checkpoint
        try:
            async with MatrixLock(room_id=self.room_id).lock(key=self.type):
                logger.debug(f"Setting checkpoint for type {self.type}")
                # set checkpoint
                resp = await self.client.room_put_state(
                    self.room_id,
                    self.type,
                    {"checkpoint": since_token},
                )
                if isinstance(resp, RoomPutStateError):
                    logger.error(f"Failed to set checkpoint for type {self.type}: {resp}")
                    return False
                else:
                    self.since_token = since_token
                    return True

        except LockAcquireError as e:
            logger.info(f"Failed to set checkpoint: {e}\n")
            return False

    @classmethod
    async def create(cls, type: str, client: FractalAsyncClient, room_id: str) -> "Checkpoint":
        """
        Create a Checkpoint instance for the given type.
        """
        checkpoint = cls(type=f"{type}.checkpoint", room_id=room_id, client=client)
        await checkpoint.get_or_init_checkpoint()
        return checkpoint


class MatrixQueue:
    """
    A MatrixQueue represents a Matrix backed task queue. Each Matrix backed task
    queue is backed by a Matrix room.
    """

    name: str
    room_id: str
    client: FractalAsyncClient
    checkpoint: Checkpoint
    task_types: TaskTypes

    def __init__(
        self,
        name: str,
        homeserver_url: str,
        access_token: str,
        room_id: str,
        device_name: str = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname()),
    ):
        self.client = FractalAsyncClient(
            homeserver_url=homeserver_url, access_token=access_token, room_id=room_id
        )
        self.name = name
        self.checkpoint = Checkpoint(type=name, client=self.client, room_id=room_id)
        self.task_types = TaskTypes(name)
        self.device_name = device_name
        self.room_id = room_id
        # Boolean that determines whether the queue is caught up with the room's timeline
        # This is used to determine whether to use the room_messages API or the sync API
        self.caught_up = False

    async def verify_room_exists(self) -> None:
        """
        Verifies that the configured room exists
        """
        # verify room exists by fetching a piece of its room state
        res = await self.client.room_get_state_event(self.room_id, "m.room.create")
        if isinstance(res, RoomGetStateEventError):
            raise Exception(f"Matrix room {self.room_id} not found: {res.message}")

    async def get_tasks(
        self,
        timeout: int = 30000,
        since_token: Optional[str] = None,
        task_filter: Optional[Dict[str, Any]] = None,
    ) -> list[Task]:
        """
        Returns a list of tasks and acks.

        Args:
            timeout (int): The timeout to use when fetching tasks.
            since_token (str): The next batch token to use when fetching tasks.
                               Defaults to the current checkpoint of the broker.
            task_fitler (dict): A filter to use when fetching tasks. Defaults to
                                a filter that fetches all tasks and acks for the
                                queue.

        Returns:
            A list of tasks and acks.
        """
        next_batch = since_token or self.checkpoint.since_token

        if not task_filter:
            if self.caught_up:
                task_filter = create_sync_filter(
                    self.room_id,
                    types=[self.task_types.task, f"{self.task_types.ack}.*"],
                )
            else:
                task_filter = create_room_message_filter(
                    self.room_id,
                    types=[self.task_types.task, f"{self.task_types.ack}.*"],
                )

        if self.caught_up:
            task_events = await run_sync_filter(
                self.client, task_filter, timeout=timeout, since=next_batch
            )
        else:
            task_events, end = await run_room_message_filter(
                self.client, self.room_id, task_filter, since=next_batch
            )
            if not end:
                self.caught_up = True
                # finished processing all events in the room, so use the sync API
                # to get new events from now on
                self.client.next_batch = await self.client.get_latest_sync_token(self.room_id)
                logger.info(
                    f"Caught up - Updating checkpoint in room to: {self.client.next_batch}"
                )
                await self.checkpoint.put_checkpoint_state(self.client.next_batch)
                return await self.get_tasks(timeout=timeout)
            else:
                logger.info(f"Still catching up - Updating checkpoint to {end}")
                self.checkpoint.since_token = end

        # acks should be a separate type
        # tasks should only be tasks
        tasks = [Task(**task) for task in task_events.get(self.room_id, [])]
        return tasks

    def filter_acked_tasks(self, tasks: list["Task"], exclude_self: bool = False) -> list["Task"]:
        """
        Filter out all events that have been acked

        Args:
            tasks (list[Task]): The tasks to filter.
            exclude_self (bool): Whether to exclude tasks that were sent by us.

        Returns:
            A list of unacked tasks.
        """
        task_dict: Dict[str, Task] = {}
        for task in tasks:
            if task.type == self.task_types.task:
                task.acknowledged = False
                task_dict[task.id] = task

            elif task.type == f"{self.task_types.ack}.{task.id}":
                task.acknowledged = True
                task_dict[task.id] = task

        # filter tasks that were not acknowledged and return them
        unacked = []
        for task in task_dict.values():
            if not task.acknowledged:
                # if exclude_self is False, then append any unacked task
                if not exclude_self:
                    unacked.append(task)
                # if exclude_self is True, then filter out tasks that were sent by us
                # since we don't want to run tasks that we sent
                elif task.sender != self.client.user_id:
                    unacked.append(task)
                else:
                    logger.warning(f"Filtering out task {task.id} sent by {task.sender}")

        logger.debug(f"{self.name} Unacked tasks: {unacked}")
        return unacked

    async def get_unacked_tasks(
        self, timeout: int = 30000, exclude_self: bool = False
    ) -> Tuple[str, List[Task]]:
        """
        Args:
            timeout (int): The timeout to use when fetching tasks.

        Returns:
            A list of unacked tasks.

        TODO: Instead of filtering acked tasks here, get a list of all task ids
        for tasks from the ``since_token``, then filter out all acks for those
        task ids. This should result in only tasks that dont have acks, and
        should be more efficient since the filtering happens serverside.
        """
        # ensure that the client has a user id. This is necessary when exclude_self
        # is True since we need to know the client's user id to filter out tasks
        # that were sent by us.
        if not self.client.user_id:
            whoami = await self.client.whoami()
            if isinstance(whoami, WhoamiError):
                raise Exception(whoami.message)

        tasks = await self.get_tasks(timeout=timeout, since_token=self.checkpoint.since_token)
        unacked = self.filter_acked_tasks(tasks, exclude_self=exclude_self)
        if not unacked:
            logger.debug(f"No unacked tasks found for queue: {self.name}")

        return self.name, unacked

    async def all_tasks_acked(self) -> bool:
        """
        Returns a boolean for all tasks being acked or not.
        """
        unacked_tasks = await self.get_unacked_tasks(timeout=0)
        return len(unacked_tasks[1]) == 0

    async def task_is_acked(self, task_id: str, since: Optional[str] = None) -> bool:
        """
        Returns a boolean for a task being acked or not.
        """
        next_batch = since or self.checkpoint.since_token

        # FIXME: Maybe this should be a method on a task type?
        queue_ack_type = self.task_types.ack
        expected_ack = f"{queue_ack_type}.{task_id}"

        task_filter = create_room_message_filter(self.room_id, types=[expected_ack])
        tasks, _ = await run_room_message_filter(
            self.client, self.room_id, task_filter, since=next_batch
        )

        # if anything for the room was returned, then the task was acked
        return tasks.get(self.room_id) is not None

    async def ack_msg(self, task_id: str) -> None:
        """
        Acks a given task id.
        """
        message = json.dumps(
            {
                "task_id": task_id,
                "task": "{}",
            }
        )
        logger.debug(
            f"Sending ack for task {task_id} to room: {self.room_id}\nAck type: {self.task_types.ack}.{task_id}",
        )
        await send_message(
            self.client,
            self.room_id,
            message=message,
            msgtype=f"{self.task_types.ack}.{task_id}",
            task_id=task_id,
            queue=self.name,
        )

    async def yield_task(
        self,
        task: Task,
        ignore_acks: bool = False,
        lock: bool = True,
    ) -> Union[AckableMessage, bytes]:
        """
        Attempts to lock a task and yield it to a worker. If the lock is
        acquired and the task has been acked since the lock was acquired, then
        a TaskAlreadyAcked exception is raised.

        Args:
            task (Task): The task to yield.
            ignore_acks (bool): If true, returns bytes.
            lock (bool): If true, acquire a lock on the task id.

        Returns:
            An AckableMessage or bytes containing the task data.

        Raises:
            LockAcquireError: If the lock could not be acquired.
            TaskAlreadyAcked: If the task has been acked since the lock was acquired.
        """

        async def _yield_task():
            # ensure that task has not been acked since lock was acquired
            if not ignore_acks:
                acked = await self.task_is_acked(task.id)
                if acked:
                    raise TaskAlreadyAcked(task.id)

            # encode task data
            task_data = json.dumps(task.data).encode("utf-8")

            logger.info(
                f"Yielding message {task.data} for task {task.id} to worker {self.device_name}",
            )
            if ignore_acks:
                return task_data

            return AckableMessage(
                data=task_data,
                ack=partial(self.ack_msg, task.id),
            )

        if not lock:
            return await _yield_task()

        async with MatrixLock(room_id=self.room_id).lock(f"{self.task_types.lock}.{task.id}"):
            return await _yield_task()

    async def shutdown(self) -> None:
        """
        Closes the Queue's Matrix client session.
        """
        await self.client.close()


class BroadcastQueue(MatrixQueue):
    # FIXME: instead of locking in the matrix room, lock locally since
    # there are multiple workers are run on the same machine. This will
    # speed up the lock acquisition process.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_types.ack = f"{self.task_types.ack}.{self.device_name}"
        self.task_types.lock = f"{self.task_types.lock}.{self.device_name}"


class ReplicatedQueue(BroadcastQueue):
    """
    Replicated queues are broadcast queues whose checkpoints are device specific.
    """

    def __init__(
        self,
        name: str,
        homeserver_url: str,
        access_token: str,
        room_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(name, homeserver_url, access_token, room_id, *args, **kwargs)
        self.checkpoint.type = f"{self.checkpoint.type}.{self.device_name}"
