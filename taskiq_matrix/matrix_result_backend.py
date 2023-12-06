import logging
import os
import pickle
import socket
from base64 import b64decode, b64encode
from typing import Dict, Optional, TypeVar, Union

from fractal.matrix.async_client import FractalAsyncClient
from nio import MessageDirection, RoomMessagesError
from taskiq import AsyncResultBackend
from taskiq.result import TaskiqResult

from .exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultDecodeError,
)
from .filters import create_filter, run_sync_filter
from .utils import send_message

_ReturnType = TypeVar("_ReturnType")


logger = logging.getLogger(__name__)


class MatrixResultBackend(AsyncResultBackend):
    def __init__(
        self,
        result_ex_time: Optional[int] = None,
        result_px_time: Optional[int] = None,
    ):
        """
        Constructs a new Matrix result backend.

        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        """
        self.matrix_client = FractalAsyncClient(
            homeserver_url=os.environ["MATRIX_HOMESERVER_URL"],
            access_token=os.environ["MATRIX_ACCESS_TOKEN"],
            room_id=os.environ["MATRIX_ROOM_ID"],
        )
        self.room = os.environ["MATRIX_ROOM_ID"]
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

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Returns whether the result is ready.

        :param task_id: ID of the task.

        :returns: True if the result is ready else False.
        """
        if not self.next_batch:
            res = await self.matrix_client.room_messages(
                self.room, start="", limit=1, direction=MessageDirection.back
            )
            if not isinstance(res, RoomMessagesError):
                self.next_batch = res.start
                self.matrix_client.next_batch = res.start

        sync_filter = create_filter(self.room, types=[f"taskiq.result.{task_id}"])
        # cache the next batch token from kick so we can use it later when getting the result
        # need to do this because when we sync below here, the client's next_batch token will
        # be updated to the latest sync token, which will be after the result we're looking for
        result = await run_sync_filter(
            self.matrix_client, sync_filter, timeout=0, since=self.matrix_client.next_batch
        )
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

        sync_filter = create_filter(self.room, types=[f"taskiq.result.{task_id}"])
        result_object = await run_sync_filter(
            self.matrix_client, sync_filter, timeout=0, since=self.next_batch
        )
        # TODO: handle waiting for a number of results for a task

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
