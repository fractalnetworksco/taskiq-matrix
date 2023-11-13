import asyncio
import json
import logging
import os
from base64 import b64decode, b64encode
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from uuid import uuid4

from nio import (
    AsyncClient,
    MatrixRoom,
    RoomGetStateEventError,
    RoomPutStateResponse,
    RoomSendResponse,
    SyncError,
)

from .exceptions import LockAcquireError
from .filters import create_filter
from .utils import setup_console_logging


class MatrixLock:
    """
    Make building decentralized applications with Matrix easier.
    Key/Value store.
    """

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        homeserver_url: str = os.environ["HS_MATRIX_URL"],
        access_token: str = os.environ["HS_ACCESS_TOKEN"],
        user_id: str = os.environ["HS_USER_ID"],
        room_id: str = os.environ["HS_ROOM_ID"],
    ):
        self.client = AsyncClient(homeserver_url)
        self.client.access_token = access_token
        self.client.user_id = user_id
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

        return create_filter(room_id, types=types, limit=limit)

    async def set(self, key: str, value: Union[bytes, str], state_key: str = "") -> None:
        """
        TODO: should be moved to a Homeserver Client
        set a key/value pair in the client's room state
        """
        if isinstance(value, bytes):
            value = b64encode(value).decode("utf-8")
            msgcontent = {key: value, "bytes": True}
        else:
            msgcontent = {key: value}
        resp = await self.client.room_put_state(self.room_id, key, msgcontent, state_key)
        if isinstance(resp, RoomPutStateResponse):
            return None
        raise Exception(resp.message)

    async def get(
        self, key: str, default: Any = None, state_key: str = ""
    ) -> Union[dict, bytes, str]:
        """
        TODO: should be moved to a Homeserver Client

        get a key/value pair from the client's room state
        """
        resp = await self.client.room_get_state_event(self.room_id, key, state_key)
        if isinstance(resp, RoomGetStateEventError):
            # if not default:
            #     raise KeyError(f"key {key} not found in room state")
            return default

        # if the value is a base64 encoded string, decode and return bytes
        val_is_bytes = resp.content.get("bytes", False)
        if val_is_bytes is True:
            return b64decode(resp.content[key])
        else:
            return resp.content[key]

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

        self.logger.debug(f"Sending message: {msgcontent} to room {room}")

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
                raise LockAcquireError("Could not acquire lock on %s" % key)
        except Exception as err:
            raise LockAcquireError(f"Error acquiring lock on {key}: {err}")
        finally:
            await self.client.close()

        try:
            yield self.lock_id
        finally:
            print(f"Worker ({self.lock_id}) releasing lock: {key}")
            # update sync token before we release
            await self.filter(self.create_filter(limit=0), timeout=0)
            self.next_batch = self.client.next_batch
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
        res = await self.filter(self.create_filter(types=lock_types), timeout=0)
        self.next_batch = self.client.next_batch

        # if last event is a lock release or the lock types dont exist in the room,
        # we can acquire the lock
        if self.room_id not in res or res[self.room_id][-1]["type"] == f"fn.lock.release.{key}":
            await self.send_message(
                {"type": f"fn.lock.acquire.{key}", "lock_id": self.lock_id},
                msgtype=f"fn.lock.acquire.{key}",
            )

            # filter again to make sure that we got the lock
            res = await self.filter(self.create_filter(types=[f"fn.lock.acquire.{key}"]))
            if res[self.room_id] and res[self.room_id][0]["lock_id"] == self.lock_id:
                return True
            else:
                print(
                    f'Someone else got the {key} lock: Worker {res[self.room_id][0]["lock_id"]}'
                )
                return False
        else:
            return False

    async def filter(
        self, filter: dict, timeout: int = 3000, since: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        execute a filter with the client, optionally filter message body by kwargs
        attempts to deserialize json
        """
        res = await self.client.sync(timeout, sync_filter=filter, since=self.next_batch)
        if isinstance(res, SyncError):
            raise Exception(res.message)

        rooms = list(res.rooms.join.keys())
        filter_keys = kwargs.keys()
        d = {}
        for room in rooms:
            d[room] = list(
                map(
                    json.loads,
                    [
                        event.source["content"]["body"]
                        for event in res.rooms.join[room].timeline.events
                    ],
                )
            )
        if kwargs:
            # filter out all keys by value from kwargs
            for key in filter_keys:
                d = {k: [i for i in v if i.get(key) == kwargs[key]] for k, v in d.items()}
        return d

    def __del__(self):
        asyncio.run(self.client.close())
