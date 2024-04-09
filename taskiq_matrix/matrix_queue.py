import asyncio
import json
import logging
import os
import socket
from functools import partial
from typing import Any, Dict, List, Optional, Self, Tuple, Union

import appdirs
from aiofiles import open as aopen
from aiofiles.os import makedirs
from fractal.matrix.async_client import FractalAsyncClient
from nio import (
    MessageDirection,
    RoomGetStateEventError,
    RoomMessagesResponse,
    RoomPutStateError,
    SyncError,
    WhoamiError,
)
from taskiq import AckableMessage

from .exceptions import CheckpointGetOrInitError, LockAcquireError, TaskAlreadyAcked
from .filters import (
    EMPTY_FILTER,
    create_room_message_filter,
    create_sync_filter,
    get_content_only,
    run_room_message_filter,
    run_sync_filter,
    sync_room_timelines,
)
from .lock import AsyncFileLock, MatrixLock
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

    def all(self) -> List[str]:  # pragma: no cover
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
    room_id: str

    def __init__(self, *args, **event):
        # FIXME: if event is malformed this will fail resulting in
        # a task that can never be acked which breaks the queue
        self.id = event["body"]["task_id"]
        self.type = event["msgtype"]
        self.data = json.loads(event["body"]["task"])
        self.queue = event["body"]["queue"]
        self.sender = event["sender"]
        self.room_id = event["room_id"]

    def __repr__(
        self,
    ):
        return f"Task(id={self.id}, type={self.type}, queue={self.queue})"

    async def yield_task(self, *args, **kwargs) -> AckableMessage:
        raise NotImplementedError


class FileSystemCheckpoint:

    type: str
    since_token: Optional[str] = None
    client: FractalAsyncClient
    CHECKPOINT_DIR = os.environ.get("CHECKPOINT_PATH", appdirs.user_data_dir("taskiq-matrix"))

    def __init__(
        self,
        type: str,
        client: FractalAsyncClient,
        since_token: Optional[str] = None,
    ):
        self.type = type
        self.checkpoint_lock_type = f"{type}.lock"
        self.checkpoint_path = os.path.join(self.CHECKPOINT_DIR, f"{type}.checkpoint")
        self.client = client
        self.since_token = since_token

    async def get_or_init_checkpoint(self, full_sync: bool = False) -> Optional[str]:
        """
        Gets the current checkpoint from the Matrix server. If it doesn't exist,
        it will be initialized.

        Returns:
            The current checkpoint or None if not found in room state.
        """
        # ensure checkpoint directory exists
        await makedirs(self.CHECKPOINT_DIR, exist_ok=True)

        # attempt to read checkpoint from file
        try:
            async with aopen(self.checkpoint_path, "r") as f:
                self.since_token = await f.read()
                logger.info("Read checkpoint from %s file: %s" % (self.type, self.since_token))
                return self.since_token
        except FileNotFoundError:
            pass

        logger.info("Checkpoint not found in %s file. Fetching latest sync token" % self.type)

        if full_sync:
            self.since_token = ""
        else:
            # fetch latest since token
            resp = await self.client.sync(timeout=0, sync_filter=EMPTY_FILTER, since=None)
            if isinstance(resp, SyncError):
                raise Exception(resp.message)

            self.since_token = resp.next_batch

        await self.put_checkpoint_state(self.since_token)

        return self.since_token

    async def put_checkpoint_state(self, since_token: str) -> bool:
        """
        Writes current checkpoint to the file system

        Returns:
            True if the checkpoint was set, else False.
        """
        # ensure checkpoint directory exists
        await makedirs(self.CHECKPOINT_DIR, exist_ok=True)

        # acquire lock on checkpoint
        try:
            async with AsyncFileLock(self.checkpoint_lock_type).acquire_lock():
                async with aopen(self.checkpoint_path, "w") as f:
                    await f.write(since_token)
                logger.info(f"Successfully set checkpoint for type: {self.type}")
            return True
        except LockAcquireError as e:
            logger.warning(f"Failed to set checkpoint: {e}\n")
            return False

    async def update_checkpoint(self, new_checkpoint: str) -> str:
        """
        Updates the current checkpoint on the file system.
        """
        # update the checkpoint state in the configured Matrix room
        await self.put_checkpoint_state(new_checkpoint)
        self.since_token = new_checkpoint
        return new_checkpoint


class MatrixRoomCheckpoint:
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
            async with MatrixLock(
                room_id=self.room_id,
                homeserver_url=self.client.homeserver,
                access_token=self.client.access_token,
            ).lock(key=self.type):
                logger.info(f"Got {self.type} lock. Setting checkpoint for type {self.type}")
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
                    logger.info(f"Successfully set checkpoint for type: {self.type}")
                    return True

        except LockAcquireError as e:
            logger.info(f"Failed to set checkpoint: {e}\n")
            return False

    async def update_checkpoint(self, new_checkpoint: str) -> str:
        """
        Updates the current checkpoint on the Matrix server.
        """
        # update the checkpoint state in the configured Matrix room
        await self.put_checkpoint_state(new_checkpoint)
        return new_checkpoint

    @classmethod
    async def create(cls, type: str, client: FractalAsyncClient, room_id: str) -> Self:
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
    client: FractalAsyncClient
    checkpoint: FileSystemCheckpoint
    task_types: TaskTypes

    def __init__(
        self,
        name: str,
        homeserver_url: str,
        access_token: str,
        device_name: str = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname()),
    ):
        self.client = FractalAsyncClient(homeserver_url=homeserver_url, access_token=access_token)
        self.name = name
        self.checkpoint = FileSystemCheckpoint(type=name, client=self.client)
        self.task_types = TaskTypes(name)
        self.device_name = device_name

    async def get_tasks_from_room(
        self,
        room_id: str,
        task_filter: Optional[Dict[str, Any]] = None,
        start: str = "",
        end: str = "",
    ) -> Tuple[list[Task], Optional[str]]:
        """
        Fetches tasks from a room using a task filter.

        Args:
            room_id (str): The room id to fetch tasks from.
            task_filter (dict): The filter to use when fetching tasks.

        Returns:
            A dictionary of tasks.
        """
        if not task_filter:
            task_filter = create_room_message_filter(
                room_id, types=[self.task_types.task, f"{self.task_types.ack}.*"]
            )

        events, next_batch = await run_room_message_filter(
            self.client,
            room_id,
            task_filter,
            start=start,
            end=end,
            content_only=False,
        )

        if not events:
            return [], None

        tasks = [Task(room_id=room_id, **get_content_only(event)) for event in events[room_id]]

        return tasks, next_batch

    async def get_tasks(
        self,
        timeout: int = 30000,
        since_token: Optional[str] = None,
        task_filter: Optional[Dict[str, Any]] = None,
    ) -> Tuple[list[Task], str]:
        """
        Returns a list of tasks and acks.

        Args:
            timeout (int): The timeout to use when fetching tasks.
            since_token (str): The next batch token to use when fetching tasks.
                               Defaults to the current checkpoint of the broker.
            task_filter (dict): A filter to use when fetching tasks. Defaults to
                                a filter that fetches all tasks and acks for the
                                queue.

        Returns:
            Tuple[list[Task], next_batch returned from sync request]
        """
        next_batch = since_token or self.checkpoint.since_token or ""

        if not task_filter:
            task_filter = create_sync_filter(
                types=[self.task_types.task, f"{self.task_types.ack}.*"],
            )

        # fetch all room timelines filtered by the task filter
        room_timelines = await sync_room_timelines(
            self.client, task_filter, timeout=timeout, since=next_batch
        )

        tasks = []
        for room_id, timeline in room_timelines.items():
            events = timeline.events
            if not events:
                # shouldn't be possible to get here since the room wouldn't be
                # returned in the sync response, but if somehow this does occur, then continue
                continue

            if timeline.limited:
                # if the timeline is limited, then we need to fetch the rest of the events
                # that have happened in this room since the checkpoint
                # the prev_batch on the timeline is where to fetch events up to
                prev_batch = timeline.prev_batch

                # use room_messages to fetch the rest of the events that have happened since the checkpoint
                task_filter = create_room_message_filter(
                    room_id, types=[self.task_types.task, f"{self.task_types.ack}.*"]
                )
                task_events, more_messages = await run_room_message_filter(
                    self.client,
                    room_id,
                    task_filter,
                    start=next_batch,
                    end=prev_batch,
                    content_only=False,
                )

                # prepend the events to the list of events since these events are older
                events = task_events.get(room_id, []) + events

                # if there are still more events to fetch, then keep fetching
                # until all the events have been fetched
                while more_messages:
                    task_events, more_messages = await run_room_message_filter(
                        self.client,
                        room_id,
                        task_filter,
                        start=more_messages,
                        end=prev_batch,
                        content_only=False,
                    )
                    events = task_events.get(room_id, []) + events

            # create tasks from the list of events
            tasks.extend([Task(room_id=room_id, **get_content_only(event)) for event in events])

        return tasks, self.client.next_batch

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
                # if exclude_self is False, then append unacked task
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

        # save current next batch in the event that tasks are returned.
        # we don't want to skip over tasks if some error occurs.
        prev_batch = self.client.next_batch

        tasks, next_batch = await self.get_tasks(
            timeout=timeout, since_token=self.checkpoint.since_token
        )
        unacked_tasks = self.filter_acked_tasks(tasks, exclude_self=exclude_self)
        if not unacked_tasks:
            # if there are no unacked tasks, then update the checkpoint to the next batch
            self.client.next_batch = next_batch
            logger.debug(
                f"No unacked tasks, updating {self.checkpoint.type} checkpoint in room to: {self.client.next_batch}"
            )
            await self.checkpoint.update_checkpoint(self.client.next_batch)
        else:
            # some tasks were received, so reset the next batch to the previous next batch
            # for the next fetch. This is so that we keep fetching the same tasks until they
            # are all acked and only then should we update the next batch (checkpoint).
            self.client.next_batch = prev_batch

        return self.name, unacked_tasks

    async def all_tasks_acked(self) -> bool:
        """
        Returns a boolean for all tasks being acked or not.
        """
        _, unacked_tasks = await self.get_unacked_tasks(timeout=0)
        return len(unacked_tasks) == 0

    async def task_is_acked(
        self, task_id: str, task_room_id: str, since: Optional[str] = None
    ) -> bool:
        """
        Returns a boolean for a task being acked or not.
        """
        queue_ack_type = self.task_types.ack
        expected_ack = f"{queue_ack_type}.{task_id}"

        task_filter = create_room_message_filter(task_room_id, types=[expected_ack])
        tasks, _ = await run_room_message_filter(
            self.client, task_room_id, task_filter, start="", end=""
        )

        # if anything for the room was returned, then the task was acked
        return tasks.get(task_room_id) is not None

    async def ack_msg(self, task_id: str, room_id: str) -> None:
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
            f"Sending ack for task {task_id} to room: {room_id}\nAck type: {self.task_types.ack}.{task_id}",
        )
        await send_message(
            self.client,
            room_id,
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
                acked = await self.task_is_acked(task.id, task.room_id)
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
                ack=partial(self.ack_msg, task.id, task.room_id),
            )

        if not lock:
            return await _yield_task()

        async with MatrixLock(
            room_id=task.room_id,
            homeserver_url=self.client.homeserver,
            access_token=self.client.access_token,
        ).lock(f"{self.task_types.lock}.{task.id}"):
            return await _yield_task()

    async def shutdown(self) -> None:
        """
        Closes the Queue's Matrix client session.
        """
        return await self.client.close()


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
        *args,
        **kwargs,
    ):
        super().__init__(name, homeserver_url, access_token, *args, **kwargs)
        self.checkpoint.type = f"{self.checkpoint.type}.{self.device_name}"
