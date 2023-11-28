from taskiq.exceptions import (
    ResultBackendError,
    ResultGetError,
    SendTaskError,
    TaskiqError,
)


class TaskIQMatrixError(TaskiqError):
    """Base error for all taskiq-matrix exceptions."""


class DuplicateExpireTimeSelectedError(ResultBackendError, TaskIQMatrixError):
    """Error if two lifetimes are selected."""


class ExpireTimeMustBeMoreThanZeroError(ResultBackendError, TaskIQMatrixError):
    """Error if two lifetimes are less or equal zero."""


class ResultIsMissingError(TaskIQMatrixError, ResultGetError):
    """Error if there is no result when trying to get it."""


class ResultDecodeError(TaskIQMatrixError):
    """Error if result can't be decoded."""


class MatrixSyncError(TaskIQMatrixError):
    """Error if Matrix sync fails."""


class LockAcquireError(TaskIQMatrixError):
    """Error if lock can't be acquired."""


class TaskAlreadyAcked(LockAcquireError):
    """Error if a task has already been acked since the lock was acquired."""

    def __init__(self, task_id: str):
        super().__init__(f"Task {task_id} is already acked")


class MatrixRoomNotFound(TaskIQMatrixError):
    """Error if Matrix room can't be found."""


class QueueDoesNotExist(TaskIQMatrixError):
    """Error if a Matrix queue does not exist."""


class ScheduledTaskRequiresTaskIdLabel(TaskIQMatrixError, SendTaskError):
    """Error if a scheduled task is missing the task_id label."""

    def __init__(self, task_id: str):
        super().__init__(
            f"Scheduled task {task_id} is missing the task_id label. Scheduled tasks must have a task_id label."
        )


class DeviceQueueRequiresDeviceLabel(TaskIQMatrixError, SendTaskError):
    """Error if a device queue task is missing the device label."""

    def __init__(self, task_id: str):
        super().__init__(
            f'Device queue task {task_id} is missing the device label. When Device queue is specified, the task must have a "device" label.'
        )


class CheckpointGetOrInitError(TaskIQMatrixError):
    """Error if get_or_init_checkpoint() fails."""

    def __init__(self, queue: str):
        super().__init__(f"Unable to get or init checkpoint for queue {queue}")
