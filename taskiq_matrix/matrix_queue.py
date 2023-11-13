import asyncio
import json
import os
import socket
from functools import partial
from typing import Dict, List, Optional, Tuple

import nest_asyncio
from nio import AsyncClient, RoomGetStateEventError, RoomPutStateError, SyncResponse
from taskiq import AckableMessage

from .exceptions import LockAcquireError, MatrixRoomNotFound, MatrixSyncError
from .filters import EMPTY_FILTER, create_filter, run_sync_filter
from .lock import MatrixLock
from .log import Logger
from .utils import send_message


class TaskTypes:
    """
    A TaskTypes dict represents the task types for a MatrixQueue.
    """

    def __init__(self, queue_name: str):
        self.task = f"taskiq.{queue_name}.task"
        self.ack = f"{self.task}.ack"
        self.result = "taskiq.result"
        self.lock = f"{self.task}.lock"

    def all(self) -> List[str]:
        """
        Returns the task types for the queue.
        """
        return [f"{self.task}", f"{self.ack}", f"{self.result}"]


class Task:
    acknowledged: bool
    type: str
    data: str
    queue: str

    def __init__(self, *args, **event):
        self.id = event["body"]["task_id"]
        self.type = event["msgtype"]
        self.data = json.loads(event["body"]["task"])
        self.queue = event["body"]["queue"]

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
    client: AsyncClient
    since_token: Optional[str] = None
    logger: Logger = Logger()

    def __init__(
        self,
        type: str,
        room_id: str,
        client: AsyncClient,
        since_token: Optional[str] = None,
        logger: Logger = Logger(),
    ):
        self.type = type
        self.room_id = room_id
        self.client = client
        self.since_token = since_token
        self.logger = logger

        # initialize checkpoint
        loop = asyncio.new_event_loop()
        # https://stackoverflow.com/questions/46827007/runtimeerror-this-event-loop-is-already-running-in-python/56434301#56434301
        # nest_asyncio.apply()
        # FIXME: This breaks pytest-asyncio tests (POSSIBLY?)
        self.task = loop.create_task(self.get_or_init_checkpoint())
        # loop.run_until_complete(self.get_or_init_checkpoint())

    async def get_or_init_checkpoint(self) -> Optional[str]:
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
            self.logger.log(f"No checkpoint found for type: {self.type}", "debug")

            # fetch latest sync token
            res = await self.client.sync(timeout=0, sync_filter=EMPTY_FILTER)
            if not isinstance(res, SyncResponse):
                raise MatrixSyncError(f"Failed to sync: {res.message}")

            # update the checkpoint state in the Matrix room
            await self.put_checkpoint_state(res.next_batch)

            self.since_token = res.next_batch
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
            async with MatrixLock().lock(key=self.type):
                self.logger.log(f"Setting checkpoint for type {self.type}", "debug")
                # set checkpoint
                resp = await self.client.room_put_state(
                    self.room_id,
                    self.type,
                    {"checkpoint": since_token},
                )
                if isinstance(resp, RoomPutStateError):
                    self.logger.log(
                        f"Failed to set checkpoint for type {self.type}: {resp}", "error"
                    )
                    return False
                else:
                    self.since_token = since_token
                    return True

        except LockAcquireError as e:
            self.logger.log(f"Failed to add checkpoint: {e}\n")
            return False

    @classmethod
    async def create(cls, type: str, client: AsyncClient, room_id: str) -> "Checkpoint":
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
    client: AsyncClient
    checkpoint: Checkpoint
    task_types: TaskTypes
    logger: Logger = Logger()

    def __init__(
        self,
        name: str,
        homeserver_url: str = os.environ["MATRIX_HOMESERVER_URL"],
        access_token: str = os.environ["MATRIX_ACCESS_TOKEN"],
        room_id: str = os.environ["MATRIX_ROOM_ID"],
    ):
        self.client = AsyncClient(homeserver_url)
        self.client.access_token = access_token

        self.name = name
        self.checkpoint = Checkpoint(type=name, client=self.client, room_id=room_id)
        self.task_types = TaskTypes(name)
        self.device_name = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname())
        self.room_id = room_id

    async def verify_room_exists(self) -> None:
        """
        Verifies that the configured room exists
        """
        # verify room exists by fetching a piece of its room state
        res = await self.client.room_get_state_event(self.room_id, "m.room.create")
        if isinstance(res, RoomGetStateEventError):
            raise Exception(f"Matrix room {self.room_id} not found: {res.message}")

    async def get_tasks(
        self, timeout: int = 30000, since_token: Optional[str] = None
    ) -> list[Task]:
        """
        Returns a list of tasks and acks.

        Args:
            timeout (int): The timeout to use when fetching tasks.
            since_token (str): The next batch token to use when fetching tasks.
                               Defaults to the current checkpoint of the broker.

        Returns:
            A list of tasks and acks.
        """
        next_batch = since_token or self.checkpoint.since_token
        task_filter = create_filter(
            self.room_id, types=[self.task_types.task, f"{self.task_types.ack}.*"]
        )
        task_events = await run_sync_filter(
            self.client, task_filter, timeout=timeout, since=next_batch
        )
        tasks = [Task(**task) for task in task_events.get(self.room_id, [])]
        return tasks

    def filter_acked_tasks(self, tasks: list["Task"]) -> list["Task"]:
        """
        Filter out all events that have been acked
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
        unacked = [task for task in task_dict.values() if not task.acknowledged]
        print(f"{self.name} Unacked tasks: {unacked}")
        return unacked

    async def get_unacked_tasks(self, timeout: int = 30000) -> Tuple[str, List[Task]]:
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
        tasks = await self.get_tasks(timeout=timeout, since_token=self.checkpoint.since_token)
        unacked = self.filter_acked_tasks(tasks)
        return self.name, unacked

    async def all_tasks_acked(self) -> bool:
        """
        Returns if all tasks have been acked and if so returns True else False.
        """
        unacked_tasks = await self.get_unacked_tasks()
        return len(unacked_tasks) == 0

    async def task_is_acked(self, task_id: str, since: Optional[str] = None) -> bool:
        """
        Returns a boolean for a task being acked or not.
        """
        next_batch = since or await self.checkpoint.get_or_init_checkpoint()

        # FIXME: Maybe this should be a method on a task type?
        queue_ack_type = self.task_types.ack
        expected_ack = f"{queue_ack_type}.{task_id}"

        # attempt to fetch the given task_id's ack from the room
        task_filter = create_filter(self.room_id, types=[expected_ack])
        tasks = await run_sync_filter(self.client, task_filter, timeout=0, since=next_batch)

        # if anything for the room was returned, then the task was acked
        return tasks.get(self.room_id) is not None

    async def ack_msg(self, task_id: str) -> None:
        """
        Acks a message
        """
        message = json.dumps(
            {
                "task_id": task_id,
                "task": "{}",
            }
        )
        await send_message(
            self.client,
            self.room_id,
            message=message,
            msgtype=f"{self.task_types.ack}.{task_id}",
            task_id=task_id,
            queue=self.name,
        )

    async def yield_task(self, task: Task) -> AckableMessage:
        try:
            async with MatrixLock().lock(f"{self.task_types.lock}.{task.id}"):
                # ensure that task has not been acked since lock was acquired
                acked = await self.task_is_acked(task.id)
                if acked:
                    raise Exception(f"Task {task.id} has already been acked")

                # encode task data
                task_data = json.dumps(task.data).encode("utf-8")

                self.logger.log(
                    f"Yielding message {task.data} for task {task.id} to worker {self.device_name}",
                    "info",
                )
                return AckableMessage(
                    data=task_data,
                    ack=partial(self.ack_msg, task.id),
                )
        except LockAcquireError as e:
            raise Exception(e)

    def __del__(self):
        asyncio.run(self.client.close())


class BroadcastQueue(MatrixQueue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_types.ack = f"{self.task_types.ack}.{self.device_name}"
        self.task_types.lock = f"{self.task_types.lock}.{self.device_name}"
