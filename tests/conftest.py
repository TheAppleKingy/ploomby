import pytest_asyncio
import pytest
from aio_pika import connect_robust
from pydantic import BaseModel


CONN_URL = "amqp://admin:admin@localhost:5672"


@pytest_asyncio.fixture(scope="function", autouse=True)
async def rabbitmq_cleanup():
    yield  # Тест выполняется

    chan = await get_publish_chan()

    # Безопасное удаление (не падает если нет)
    for q in ["test", "test_dlq"]:
        try:
            await chan.queue_delete(q)
        except Exception:
            pass  # Очередь не существует — ок

    # Exchange тоже
    try:
        await chan.exchange_delete("dlx")
    except Exception:
        pass


async def get_publish_chan():
    conn = await connect_robust(CONN_URL)
    return await conn.channel()


class TestModel(BaseModel):
    id: int


class Sensor:
    def __init__(self, handled: bool = False, exc: bool = False):
        self.handled = handled
        self.exc = exc


pmr = pytest.mark.asyncio
