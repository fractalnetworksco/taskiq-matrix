import logging
import os
import pickle
import socket
from base64 import b64decode, b64encode
from typing import Any, Dict, Optional, TypeVar, Union
from uuid import uuid4

from async_lru import alru_cache
from fractal.matrix.async_client import FractalAsyncClient
from nio import MessageDirection, RoomMessagesError
from taskiq import AsyncResultBackend
from taskiq.result import TaskiqResult

from .exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultDecodeError,
)
from .filters import create_room_message_filter, run_room_message_filter
from .utils import send_message

_ReturnType = TypeVar("_ReturnType")


logger = logging.getLogger(__name__)


class MatrixResultBackend(AsyncResultBackend):
    def __init__(
        self,
        homeserver_url: str,
        access_token: str,
        room_id: str,
        result_ex_time: Optional[int] = None,
        result_px_time: Optional[int] = None,
    ):
        """
        Constructs a new Matrix result backend.

        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        """
        self.room = room_id
        self.homeserver_url = homeserver_url
        self.access_token = access_token
        self.matrix_client = FractalAsyncClient(
            homeserver_url=homeserver_url,
            access_token=access_token,
            room_id=self.room,
        )
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.device_name = os.environ.get("MATRIX_DEVICE_NAME", socket.gethostname())
        self.next_batch: Optional[str] = None

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError(
                "You must select one expire time param and it must be more than zero.",
            )

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError(
                "Choose either result_ex_time or result_px_time.",
            )

    async def shutdown(self) -> None:
        await self.matrix_client.close()
        return await super().shutdown()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Sets task result in matrix.

        Dumps TaskiqResult instance into the bytes and writes
        it to the configured Matrix room.

        :param task_id: ID of the task.
        :param result: TaskiqResult instance.
        """
        # ensure that the device name is set in labels for the result
        result.labels.update({"device": self.device_name})

        message: Dict[str, Union[str, bytes, int]] = {
            "name": task_id,
            # FIXME: Move away from pickling due to security concerns
            "value": b64encode(pickle.dumps(result)).decode(),
        }

        if self.result_ex_time:
            message["ex"] = self.result_ex_time
        elif self.result_px_time:
            message["px"] = self.result_px_time

        await send_message(
            self.matrix_client,
            self.room,
            message,
            msgtype=f"taskiq.result.{task_id}",
        )

    @alru_cache(maxsize=64, ttl=60)
    async def _fetch_result_from_matrix(self, task_id: str) -> dict[str, Any]:
        """
        Fetches task result from matrix. Caches the result for 60 seconds.

        TODO: Handle waiting for a number of results for a task

        :param task_id: ID of the task.
        :return: list of task results.
        """
        message_filter = create_room_message_filter(self.room, types=[f"taskiq.result.{task_id}"])
        result, next_batch = await run_room_message_filter(
            self.matrix_client,
            self.room,
            message_filter,
            since="",  # we can simply search the full room history since we are filtering by a specific type
            direction=MessageDirection.back,
        )
        return result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        result = await self._fetch_result_from_matrix(task_id)
        if not result.get(self.room):
            # invalidate the cache if the result is not found
            # we dont want to cache an empty result as it may
            # be a temporary condition (the result hasn't been pushed yet)
            self._fetch_result_from_matrix.cache_invalidate(task_id)
        return True if result.get(self.room) else False

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :raises MatrixSyncError: if there is an error syncing with the Matrix server.
        :raises ResultIsMissingError: if there is no result when trying to get it.
        :raises ResultDecodeError: if there is an error decoding the result.
        :return: task's return value.
        """
        result_object = await self._fetch_result_from_matrix(task_id)

        try:
            result = result_object[self.room][0]
        except Exception as e:
            logger.error(f"Error getting task result from Matrix {e}")
            raise ResultDecodeError()

        try:
            result = b64decode(result["body"]["task"]["value"])
        except Exception as e:
            logger.error(f"Error loading result from returned task {e}")
            raise ResultDecodeError()

        try:
            taskiq_result: TaskiqResult[_ReturnType] = pickle.loads(result)
        except Exception as e:
            logger.error(f"Error loading result as taskiq result: {e}")
            raise ResultDecodeError()

        if not with_logs:
            taskiq_result.log = None

        return taskiq_result
