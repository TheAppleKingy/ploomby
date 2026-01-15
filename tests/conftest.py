import pytest_asyncio
import pytest
from aio_pika import connect_robust
from pydantic import BaseModel


CONN_URL = "amqp://admin:admin@localhost:5672"


@pytest_asyncio.fixture(scope="module", autouse=True)
async def teardown():
    yield
    chan = await get_publish_chan()
    await chan.queue_delete("test")


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
