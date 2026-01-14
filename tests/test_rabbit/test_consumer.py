import pytest

from ploomby.rabbit import RabbitConsumer, RabbitConsumerFactory, NoConnectionError


def get_consumer(reconnect: bool = True):
    return RabbitConsumer("amqp://admin:admin@localhost:5672", "task_name", 1, reconnect=reconnect)


pmr = pytest.mark.asyncio


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
