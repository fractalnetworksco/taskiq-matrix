import logging
from typing import Any, Dict, List

from nio.responses import RoomGetStateEventError
from taskiq import ScheduledTask
from taskiq.abc.schedule_source import ScheduleSource

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
            raise TypeError(f"MatrixRoomScheduleSource expected MatrixBroker, got {type(broker)}")

        self.broker = broker

    async def startup(self) -> None:
        await super().startup()
        self.initial = True
        return None

    async def get_schedules(self) -> List["ScheduledTask"]:
        """
        Collect schedules for all tasks.

        :return: list of schedules.
        """
        if self.initial is True:
            # NOTE: this is a hack to prevent the scheduler from sending
            # schedules (importantly ones that occur on the minute) when the
            # scheduler starts up at a time in between the minute (1-59 seconds).
            # This basically eliminates duplicate schedules being fired since
            # the scheduler can acquire the task's schedule lock since it is
            # out of phase from other schedulers.
            self.initial = False
            return []

        schedules = []
        schedule_state = await self.get_schedules_from_room()
        broker_tasks = self.broker.get_all_tasks()
        for task in schedule_state:
            if task["name"] not in broker_tasks.keys():
                logger.error(f'Got schedule for non-existant task: {task["name"]}')
                raise Exception("Got schedule for non-existant task: {}".format(task["name"]))
            if broker_tasks[task["name"]].broker != self.broker:
                continue
            if "cron" not in task and "time" not in task:
                raise Exception("Schedule for task {} has no cron or time".format(task["name"]))
            labels = task.get("labels", {})
            labels.update(broker_tasks[task["name"]].labels)

            # ensure scheduled task label is set so that kick knows to lock
            labels["scheduled_task"] = True

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
        logger.debug(f"Returning schedules: {schedules}")
        return schedules

    async def get_schedules_from_room(self) -> List[Dict[str, Any]]:
        # FIXME: Should each queue have its own schedule state?
        # for now all schedules for all queues are stored in the same state
        resp = await self.broker.mutex_queue.client.room_get_state_event(
            self.broker.room_id,
            SCHEDULE_STATE_TYPE,
        )
        if isinstance(resp, RoomGetStateEventError):
            if resp.status_code == "M_NOT_FOUND":
                logger.info(f"No schedules found for room {self.broker.room_id}")
            else:
                logger.warn(
                    f"Encountered error when fetching schedules from room {self.broker.room_id}: {resp}"
                )
            return []
        else:
            return resp.content.get("tasks", [])
