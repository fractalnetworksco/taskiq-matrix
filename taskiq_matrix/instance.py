from taskiq import TaskiqScheduler
from taskiq.middlewares.retry_middleware import SimpleRetryMiddleware

from .matrix_broker import MatrixBroker
from .matrix_result_backend import MatrixResultBackend
from .schedulesource import MatrixRoomScheduleSource

broker = (
    MatrixBroker()
    .with_result_backend(MatrixResultBackend(result_ex_time=60))
    .with_middlewares(SimpleRetryMiddleware(default_retry_count=3))
)

scheduler = TaskiqScheduler(broker=broker, sources=[MatrixRoomScheduleSource(broker)])
