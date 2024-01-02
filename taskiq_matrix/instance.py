import os

from taskiq import TaskiqScheduler
from taskiq.middlewares.retry_middleware import SimpleRetryMiddleware
from taskiq_matrix.matrix_broker import MatrixBroker
from taskiq_matrix.matrix_result_backend import MatrixResultBackend
from taskiq_matrix.schedulesource import MatrixRoomScheduleSource

broker = (
    MatrixBroker()
    .with_matrix_config(
        room_id=os.environ.get("MATRIX_ROOM_ID"),
        homeserver_url=os.environ.get("MATRIX_HOMESERVER_URL"),
        access_token=os.environ.get("MATRIX_ACCESS_TOKEN"),
    )
    .with_result_backend(
        MatrixResultBackend(
            room_id=os.environ.get("MATRIX_ROOM_ID"),
            homeserver_url=os.environ.get("MATRIX_HOMESERVER_URL"),
            access_token=os.environ.get("MATRIX_ACCESS_TOKEN"),
            result_ex_time=60,
        )
    )
    .with_middlewares(SimpleRetryMiddleware(default_retry_count=3))
)

scheduler = TaskiqScheduler(broker=broker, sources=[MatrixRoomScheduleSource(broker)])

