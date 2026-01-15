import pytest
import asyncio

from aio_pika import Message
from ploomby.rabbit import RabbitConsumerFactory
from ploomby.abc import ConsumerAlreadyRegistered
from ploomby.registry import HandlersRegistry, MessageConsumerRegistry

from .conftest import CONN_URL, Sensor, TestModel, get_publish_chan, pmr


def get_consumer_factory():
    return RabbitConsumerFactory(CONN_URL)


@pmr
async def test_handlers_registry():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    sensor = Sensor()
    await consumer_registry.register("test", "task_name")

    @h_reg.register("test_task")
    async def handler_ok(dto: TestModel):
        await asyncio.sleep(0)
        sensor.handled = True
        return dto.id

    chan = await get_publish_chan()
    await chan.default_exchange.publish(Message('{"id": 1}'.encode(), headers={"task_name": "test_task"}), "test")
    await asyncio.sleep(0.1)
    assert sensor.handled


@pmr
async def test_unregistered_handler():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    sensor = Sensor()
    await consumer_registry.register("test", "task_name")

    @h_reg.register("test_task")
    async def handler_ok(dto: TestModel):
        await asyncio.sleep(0)
        sensor.handled = True
        return dto.id

    chan = await get_publish_chan()
    await chan.default_exchange.publish(Message('{"id": 1}'.encode(), headers={"task_name": "test_tas"}), "test")
    await asyncio.sleep(0.1)
    assert not sensor.handled


@pmr
async def test_unexpected_message_key_name():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    sensor = Sensor()
    await consumer_registry.register("test", "task_name")

    @h_reg.register("test_task")
    async def handler_ok(dto: TestModel):
        await asyncio.sleep(0)
        sensor.handled = True
        return dto.id

    chan = await get_publish_chan()
    await chan.default_exchange.publish(Message('{"id": 1}'.encode(), headers={"task_nae": "test_task"}), "test")
    await asyncio.sleep(0.1)
    assert not sensor.handled


@pmr
async def test_more_handlers_consumers():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    sensor = Sensor()
    sensor1 = Sensor()
    sensor2 = Sensor()
    await consumer_registry.register("test", "task_name")
    await consumer_registry.register("test1", "task_name_")

    @h_reg.register("test_task")
    async def handler(dto: TestModel):
        await asyncio.sleep(0)
        sensor.handled = True
        return dto.id

    @h_reg.register("test_task1")
    async def handler1(dto: TestModel):
        await asyncio.sleep(0)
        sensor1.handled = True

    @h_reg.register("test_task2")
    async def handler2(dto: TestModel):
        await asyncio.sleep(0)
        sensor2.handled = True

    chan = await get_publish_chan()
    msg = Message('{"id": 1}'.encode(), headers={"task_name": "test_task"})
    msg1 = Message('{"id": 1}'.encode(), headers={"task_name": "test_task1"})
    msg2 = Message('{"id": 1}'.encode(), headers={"task_name_": "test_task2"})
    await chan.default_exchange.publish(msg, "test")
    await chan.default_exchange.publish(msg1, "test")
    await chan.default_exchange.publish(msg2, "test1")
    await asyncio.sleep(0.1)
    assert sensor.handled
    assert sensor1.handled
    assert sensor2.handled


@pmr
async def test_more_that_one_consumer_for_queue():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    await consumer_registry.register("test", "task_name", prefetch_count=0)
    with pytest.raises(ConsumerAlreadyRegistered):
        await consumer_registry.register("test", "task_name_", prefetch_count=0)


@pmr
async def test_invalid_data():
    h_reg = HandlersRegistry()
    consumer_factory = get_consumer_factory()
    consumer_registry = MessageConsumerRegistry(h_reg, consumer_factory)
    await consumer_registry.register("test", "task_name", prefetch_count=0)
    sensor = Sensor()

    @h_reg.register("test_task")
    async def handler_ok(dto: TestModel):
        await asyncio.sleep(0)
        sensor.handled = True
        return dto.id

    chan = await get_publish_chan()
    await chan.default_exchange.publish(Message('{"id":asf'.encode(), headers={"task_name": "test_task"}), "test")
    await asyncio.sleep(0.1)
    assert not sensor.handled
