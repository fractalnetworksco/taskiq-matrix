from nio import SyncError
from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError


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


class MatrixRoomNotFound(TaskIQMatrixError):
    """Error if Matrix room can't be found."""


class QueueDoesNotExist(TaskIQMatrixError):
    """Error if a Matrix queue does not exist."""
