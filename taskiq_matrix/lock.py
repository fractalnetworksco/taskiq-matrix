import asyncio
import functools
import json
import logging
import os
import tempfile
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from uuid import uuid4

from filelock import FileLock, UnixFileLock
from fractal.matrix.async_client import FractalAsyncClient
from nio import (
    MatrixRoom,
    MessageDirection,
    RoomMessagesError,
    RoomSendResponse,
    SyncError,
)

from .exceptions import LockAcquireError
from .filters import create_room_message_filter, run_room_message_filter
from .utils import setup_console_logging

logger = logging.getLogger(__name__)


class MatrixLock:
    """
    Make building decentralized applications with Matrix easier.
    Key/Value store.
    """

    next_batch = None

    def __init__(
        self,
        homeserver_url: str = os.environ.get("MATRIX_HOMESERVER_URL", ""),
        access_token: str = os.environ.get("MATRIX_ACCESS_TOKEN", ""),
        room_id: str = os.environ.get("MATRIX_ROOM_ID", ""),
    ):
        # raise an exception if any of the required env vars are missing
        if not homeserver_url:
            raise Exception("MATRIX_HOMESERVER_URL is required if not passed explicitly")
        if not access_token:
            raise Exception("MATRIX_ACCESS_TOKEN is required if not passed explicitly")
        if not room_id:
            raise Exception("MATRIX_ROOM_ID is required if not passed explicitly")

        self.client = FractalAsyncClient(
            homeserver_url=homeserver_url, access_token=access_token, room_id=room_id
        )
        self.client.access_token = access_token
        self.room_id = room_id
        self.lock_id = str(uuid4())
        self.next_batch = None
        setup_console_logging()

    def create_filter(
        self, room_id: Optional[str] = None, types: List[str] = [], limit: Union[int, None] = None
    ) -> dict:
        """
        Create a filter for a room and/or specific message types.

        Returns:
            filter dict
        """
        if not room_id:
            room_id = self.room_id

        return create_room_message_filter(room_id, types=types, limit=limit)

    async def send_message(
        self,
        message: Union[bytes, str, Dict[Any, Any]],
        msgtype: str = "m.room.message",
        room: Optional[Union[MatrixRoom, str]] = None,
    ) -> bool:
        """
        Send a message to a room.

        Note: Encrypted rooms are not supported for now.

        Args:
            message (bytes | str): The message to send.
            msgtype (str): The message type to send. Defaults to "m.room.message".
            room (Optional[Union[MatrixRoom, str]]): The room to send the message to. Defaults to the client's room.
        """
        is_bytes = False
        if isinstance(message, bytes):
            message = b64encode(message).decode("utf-8")
            is_bytes = True
        if not isinstance(message, str):
            message = json.dumps(message)

        if room is None:
            room = self.room_id
        elif isinstance(room, MatrixRoom):
            room = room.room_id

        msgcontent: Dict[str, Union[str, bool]] = {"msgtype": msgtype, "body": message}
        if is_bytes:
            msgcontent["bytes"] = True

        logger.debug(f"Sending message: {msgcontent} to room {room}")

        try:
            response = await self.client.room_send(room, msgtype, msgcontent)
        except Exception as err:
            raise Exception(f"Error sending message type {msgtype}: {err}")

        if not isinstance(response, RoomSendResponse):
            raise Exception(f"Got error response when sending message: {response}", "error")
        else:
            return True

    @asynccontextmanager
    async def lock(
        self, key: Optional[str] = None, wait: bool = False
    ) -> AsyncGenerator[str, None]:
        """
        lock room state, optionally on a specific key
        Args:
            key (str): the key to lock on
            wait (bool): whether to wait for the lock to be available

        Yields:
            lock_id (str): the acquired lock id
        Raises:
            LockAcquireError: if the lock could not be acquired
        """
        try:
            lock = await self._acquire_lock(key)
            if not lock and wait is False:
                await self.client.close()
                raise LockAcquireError("Could not acquire lock on %s" % key)
        except Exception as err:
            await self.client.close()
            raise LockAcquireError(f"Error acquiring lock on {key}: {err}")

        try:
            yield self.lock_id
        finally:
            logger.debug(f"Worker ({self.lock_id}) releasing lock: {key}")
            await self.send_message(
                {"type": f"fn.lock.release.{key}"}, msgtype=f"fn.lock.release.{key}"
            )
            await self.client.close()

    async def _acquire_lock(self, key: Optional[str] = None) -> bool:
        """
        acquire a lock on a specific key
        Args:
            key (str): the key to lock on
            wait (bool): whether to wait for the lock to be available
        """
        lock_types = [f"fn.lock.acquire.{key}", f"fn.lock.release.{key}"]
        # because we create a new instance of a lock each time, we cache
        # a next batch that we can use for subsequent invocations of locks.
        # FIXME: this should be advanced
        res, next_batch = await self.filter(
            self.create_filter(types=lock_types), limit=1, message_direction=MessageDirection.back
        )
        self.next_batch = next_batch

        # if last event is a lock release or the lock types dont exist in the room,
        # we can acquire the lock
        if self.room_id not in res or res[self.room_id][0]["type"] == f"fn.lock.release.{key}":
            logger.debug(
                f"Worker {self.lock_id} will attempt to acquire since Got back response for {key}: {res}"
            )
            await self.send_message(
                {"type": f"fn.lock.acquire.{key}", "lock_id": self.lock_id},
                msgtype=f"fn.lock.acquire.{key}",
            )

            # filter again to make sure that we got the lock
            res, _ = await self.filter(self.create_filter(types=[f"fn.lock.acquire.{key}"]))
            if res[self.room_id] and res[self.room_id][0]["lock_id"] == self.lock_id:
                logger.debug(f"Worker {self.lock_id} acquired lock {key}")
                return True
            else:
                logger.info(
                    f'Unable to acquire lock {key}, worker {res[self.room_id][0]["lock_id"]} got it.'
                )
                return False
        else:
            return False

    async def filter(
        self,
        filter: dict,
        limit: int = 100,
        message_direction: MessageDirection = MessageDirection.front,
    ) -> tuple[Dict[str, Any], Optional[str]]:
        """
        execute a filter with the client, optionally filter message body by kwargs
        attempts to deserialize json
        """
        logger.debug("Next batch is %s" % self.next_batch)
        result, next_batch = await run_room_message_filter(
            self.client,
            self.room_id,
            filter,
            start=self.next_batch or "",
            content_only=True,
            direction=message_direction,
            limit=limit,
        )
        rooms = list(result.keys())
        d = {}
        for room in rooms:
            d[room] = list(
                map(
                    json.loads,
                    [event["body"] for event in result[room]],
                )
            )
        return d, next_batch

    async def get_latest_sync_token(self) -> str:
        """
        Returns the latest sync token for a room in constant time, using /sync with an empty filter takes longer as the room grows
        """
        res = await self.client.room_messages(
            self.room_id, start="", limit=1, direction=MessageDirection.back
        )
        if not isinstance(res, RoomMessagesError):
            return res.start
        raise Exception(f"Failed to get sync token for room {self.room_id}")


class AsyncFileLock:
    lock_path = tempfile.gettempdir()

    def __init__(self, lock_file: str):
        self.type = lock_file
        self.lock = UnixFileLock(
            f"{self.lock_path}{os.path.sep}{self.type}", timeout=0, is_singleton=True
        )
        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=1)

    @asynccontextmanager
    async def acquire_lock(self):
        acquire = functools.partial(self.lock.acquire, blocking=False)
        try:
            await self.loop.run_in_executor(self.executor, acquire)
        except TimeoutError:
            raise LockAcquireError(f"Could not acquire lock for {self.type}")

        logger.debug("Acquired lock on %s" % self.lock.lock_file)
        try:
            yield self.lock
        finally:
            logger.debug("Releasing lock on %s" % self.type)
            await self.loop.run_in_executor(self.executor, self.lock.release)
