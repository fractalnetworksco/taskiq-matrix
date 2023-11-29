import pytest

from taskiq_matrix.tasks import update_checkpoint, QueueDoesNotExist


async def test_update_checkpoint_attribute_error(test_matrix_broker):
    """ """
    broker = await test_matrix_broker()

    with pytest.raises(QueueDoesNotExist):
        res = await broker.update_checkpoint("test")

