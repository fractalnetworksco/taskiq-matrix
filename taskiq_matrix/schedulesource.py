import logging
from typing import Any, Dict, List

from fractal.matrix.async_client import FractalAsyncClient
from nio.responses import RoomGetStateEventError
from taskiq import ScheduledTask
from taskiq.abc.schedule_source import ScheduleSource

from .filters import create_state_filter, create_sync_filter, run_sync_filter

SCHEDULE_DIR = "/schedules"

logging.getLogger("nio").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

SCHEDULE_STATE_TYPE = "taskiq.schedules"


# class FileScheduleSource(ScheduleSource):
#     """File schedule source."""

#     def __init__(self, broker: "MatrixBroker") -> None:
#         self.broker = broker

#     async def shutdown(self) -> None:
#         await self.broker.shutdown()
#         await super().shutdown()

#     async def get_schedules(self) -> List["ScheduledTask"]:
#         """
#         Collect schedules for all tasks.

#         This function checks labels for all tasks available to the broker.

#         If task has a schedule label, it will be parsed and returned.

#         :return: list of schedules.
#         """
#         schedules = []
#         schedule_files = await self.load_schedule_files()
#         broker_tasks = self.broker.get_all_tasks()
#         for task in schedule_files:
#             if task["name"] not in broker_tasks.keys():
#                 raise Exception("Got schedule for non-existant task: {}".format(task["name"]))
#             if broker_tasks[task["name"]].broker != self.broker:
#                 continue
#             if "cron" not in task and "time" not in task:
#                 raise Exception("Schedule for task {} has no cron or time".format(task["name"]))
#             labels = task.get("labels", {})
#             labels.update(broker_tasks[task["name"]].labels)
#             schedules.append(
#                 ScheduledTask(
#                     task_name=task["name"],
#                     labels=labels,
#                     args=task.get("args", []),
#                     kwargs=task.get("kwargs", {}),
#                     cron=task.get("cron"),
#                     time=task.get("time"),
#                 ),
#             )
#         return schedules

#     async def list_schedule_files(self) -> List[str]:
#         return [f for f in await listdir(SCHEDULE_DIR) if ".yml" in f or ".yaml" in f]

#     def pre_send(self, task: ScheduledTask) -> Coroutine[Any, Any, None] | None:
#         print(f"Scheduled task: {task}")
#         return super().pre_send(task)

#     async def load_schedule_files(self) -> List[dict]:
#         task_files = await self.list_schedule_files()
#         tasks = []
#         for task_file in task_files:
#             async with aopen(os.path.join(SCHEDULE_DIR, task_file), "r") as f:
#                 loop = asyncio.get_event_loop()
#                 schedule = await loop.run_in_executor(None, yaml.safe_load, await f.read())
#                 tasks.append(schedule)
#         return tasks


class MatrixRoomScheduleSource(ScheduleSource):
    """Schedule source based on the `taskiq.schedules` state key in a Matrix Room."""

    def __init__(self, broker: Any) -> None:
        from .matrix_broker import MatrixBroker

        if not isinstance(broker, MatrixBroker):
            raise TypeError(
                f"MatrixRoomScheduleSource expected broker of type MatrixBroker, got {type(broker)}"
            )

        self.broker = broker
        if not hasattr(self.broker, "homeserver_url") or not hasattr(self.broker, "access_token"):
            raise Exception("MatrixBroker must be configured using .with_matrix_config()")

        self.client = FractalAsyncClient(self.broker.homeserver_url, self.broker.access_token)

    async def startup(self) -> None:
        await super().startup()
        self._initial = True
        return None

    async def shutdown(self) -> None:
        await super().shutdown()
        await self.client.close()
        return None

    async def get_schedules(self) -> List["ScheduledTask"]:
        """
        Collect schedules for all tasks.

        :return: list of schedules.
        """
        if self._initial is True:
            # NOTE: this is a hack to prevent the scheduler from sending
            # schedules (importantly ones that occur on the minute) when the
            # scheduler starts up at a time in between the minute (1-59 seconds).
            # This basically eliminates duplicate schedules being fired since
            # the scheduler can acquire the task's schedule lock since it is
            # out of phase from other schedulers.
            self._initial = False
            return []

        schedules = []
        schedule_state = await self.get_schedules_from_rooms()
        broker_tasks = self.broker.get_all_tasks()
        for room_id, tasks in schedule_state.items():
            for task in tasks:
                if task["name"] not in broker_tasks.keys():
                    logger.warning("Got schedule for non-existant task: %s", task["name"])
                    continue
                if broker_tasks[task["name"]].broker != self.broker:
                    continue
                if "cron" not in task and "time" not in task:
                    logger.warning("Schedule for task %s has no cron or time", task["name"])
                    continue
                labels = task.get("labels", {})
                labels.update(broker_tasks[task["name"]].labels)

                # ensure scheduled task label is set so that kick knows to lock
                labels["scheduled_task"] = True
                labels["room_id"] = room_id

                schedules.append(
                    ScheduledTask(
                        task_name=task["name"],
                        labels=labels,
                        args=task.get("args", []),
                        kwargs=task.get("kwargs", {}),
                        cron=task.get("cron"),
                        time=task.get("time"),
                    ),
                )
        logger.info("Returning schedules: %s" % schedules)
        return schedules

    async def get_schedules_from_rooms(self) -> dict[str, list[dict[str, Any]]]:
        sync_filter = create_state_filter(types=[SCHEDULE_STATE_TYPE])
        try:
            resp = await run_sync_filter(self.client, sync_filter, state=True, since=None)
        except Exception as e:
            logger.error("Error fetching schedules from rooms: %s", e)
            return {}

        scheduled_tasks = {}

        for room_id, state in resp.items():
            if not state:
                logger.info("No schedules found in room %s", room_id)
                continue

            try:
                state_event = state[0]
            except IndexError:
                logger.error("No state event found in room %s", room_id)
                continue

            tasks = state_event.get("tasks", [])
            if not tasks:
                logger.info("No schedules found in room %s", room_id)
                continue

            scheduled_tasks[room_id] = tasks

        return scheduled_tasks
