import pytest
import pytest_asyncio
import asyncio

from pydantic import ValidationError
from aio_pika import connect_robust, Message
from ploomby.rabbit import RabbitConsumer, RabbitConsumerFactory
from ploomby.abc import NoConnectionError
from ..conftest import TestModel, teardown, get_publish_chan, CONN_URL, Sensor, pmr


def get_consumer(reconnect: bool = True):
    async def get_conn():
        return await connect_robust(CONN_URL)

    return RabbitConsumer(get_conn, "task_name", prefetch_count=1, reconnect=reconnect)


async def ok_handler(dto: TestModel):
    await asyncio.sleep(0)
    return dto.id


async def fail_handler(dto: TestModel):
    await asyncio.sleep(0)
    raise Exception("fail exec handler")


def get_handler_func(sensor: Sensor):
    def get_raw_handler_func(key: str):
        def raw_handler(data: str | bytes):
            try:
                dto = TestModel.model_validate_json(data)
            except ValidationError:
                sensor.exc = True
                raise

            async def ok_handler(dto: TestModel):
                await asyncio.sleep(0)
                sensor.handled = True
                return dto.id
            return ok_handler(dto)
        handlrs = {"test_task": raw_handler}
        return handlrs.get(key)
    return get_raw_handler_func


def get_handler_func_with_exc(sensor: Sensor):
    def get_raw_handler_func(key: str):
        def raw_handler(data: str | bytes):
            try:
                dto = TestModel.model_validate_json(data)
            except ValidationError:
                sensor.exc = True
                raise

            async def fail_handler(dto: TestModel):
                await asyncio.sleep(0)
                sensor.exc = True
                raise Exception("fail exec handler")
            return fail_handler(dto)
        handlrs = {"test_task": raw_handler}
        return handlrs.get(key)
    return get_raw_handler_func


@pmr
async def test_consumer_connect_ok():
    consumer = get_consumer()
    await consumer.connect()
    assert consumer._connection
    assert not consumer._connection.is_closed


@pmr
async def test_disconn():
    consumer = get_consumer()
    await consumer.connect()
    assert consumer._connection
    assert not consumer._connection.is_closed
    await consumer.disconnect()
    assert not consumer._connection


@pmr
async def test_check_conn_reconn():
    consumer = get_consumer()
    await consumer.connect()
    assert not consumer._connection.is_closed
    await consumer.disconnect()
    assert not consumer._connection
    await consumer._check_connection()
    assert consumer._connection


@pmr
async def test_check_conn_reconn_raises():
    consumer = get_consumer(False)
    await consumer.connect()
    assert not consumer._connection.is_closed
    await consumer.disconnect()
    assert not consumer._connection
    with pytest.raises(NoConnectionError):
        await consumer._check_connection()


@pmr
async def test_init_channel_ok():
    consumer = get_consumer()
    await consumer.connect()
    await consumer._init_channel()
    assert consumer._channel
    assert not consumer._channel.is_closed


@pmr
async def test_init_channel_fail():
    consumer = get_consumer(False)
    with pytest.raises(NoConnectionError):
        await consumer._init_channel()


@pmr
async def test_get_channel_ok():
    consumer = get_consumer()
    await consumer._init_channel()
    assert await consumer._get_channel()


@pmr
async def test_get_channel_fail():
    consumer = get_consumer(False)
    with pytest.raises(NoConnectionError):
        assert await consumer._get_channel()


@pmr
async def test_declare_queue_ok():
    consumer = get_consumer()
    queue = await consumer._declare_queue("test")
    assert queue.name == "test"


@pmr
async def test_handle_message():
    consumer = get_consumer()
    pub_chan = await get_publish_chan()
    await consumer.connect()
    sensor = Sensor()
    await consumer.consume("test", get_handler_func(sensor))
    assert not sensor.handled
    await pub_chan.default_exchange.publish(
        Message('{"id": 1}'.encode(), headers={"task_name": "test_task"}), "test"
    )
    await asyncio.sleep(0.1)
    assert sensor.handled


@pmr
async def test_handle_message_fail_validation():
    consumer = get_consumer()
    await consumer.connect()
    sensor = Sensor()
    await consumer.consume("test", get_handler_func(sensor))
    chan = await get_publish_chan()
    msg = Message('{"idasd": 1}'.encode(), headers={"task_name": "test_task"})
    await chan.default_exchange.publish(msg, "test")
    await asyncio.sleep(0.1)
    assert sensor.exc
    assert not sensor.handled  # exc raised before handler work


@pmr
async def test_handle_message_fail():
    consumer = get_consumer()
    await consumer.connect()
    sensor = Sensor()
    await consumer.consume("test", get_handler_func_with_exc(sensor))
    chan = await get_publish_chan()
    msg = Message('{"id": 1}'.encode(), headers={"task_name": "test_task"})
    await chan.default_exchange.publish(msg, "test")
    await asyncio.sleep(0.1)
    assert not sensor.handled  # exc raised in handler
    assert sensor.exc


@pmr
async def test_handle_message_unregistered_handler():
    consumer = get_consumer()
    await consumer.connect()
    sensor = Sensor()
    await consumer.consume("test", get_handler_func(sensor))
    chan = await get_publish_chan()
    msg = Message('{"id": 1}'.encode(), headers={"task_name": "test_"})
    await chan.default_exchange.publish(msg, "test")
    await asyncio.sleep(0.1)
    assert not sensor.handled  # exc UnregisteredHandler raised in consumer


@pmr
async def test_factory_ok():
    fac = RabbitConsumerFactory(CONN_URL)
    consumer = await fac.create("task_name")
    assert consumer._connection
    await consumer._init_channel()
    await consumer._channel.queue_delete("test")


@pmr
async def test_factory_shared_conn():
    fac = RabbitConsumerFactory(CONN_URL)
    c1 = await fac.create("k1")
    c2 = await fac.create("k2")
    assert (fac._connection is c1._connection) and (c1._connection is c2._connection)


@pmr
async def test_factory_nonShared_conn():
    fac = RabbitConsumerFactory(CONN_URL, False)
    c1 = await fac.create("k1")
    c2 = await fac.create("k2")
    assert not ((fac._connection is c1._connection) and (c1._connection is c2._connection))
