from aio_pika import Message, connect_robust
from aio_pika.abc import ConsumerTag, AbstractRobustChannel, AbstractRobustQueue, AbstractIncomingMessage, AbstractRobustConnection

from ploomby.abc.exceptions import UnregisteredHandler
from ploomby.abc import IncomingMessageHandler, RawDataHandler, MessageKeyType, HandlerDependencyType
from ploomby.logger import logger
from .exceptions import NoConnectionError


class RabbitConsumer:
    """
    Simplest consumer for RabbitMQ implemented by aio_pika.  
    """

    def __init__(
            self,
            conn_url: str,
            message_key_name: str,
            prefetch_count: int = 0,
            reconnect: bool = True,
    ):
        """
        :param conn_url: URL for connection with RbbitMQ
        :type conn_url: str
        :param message_key_name: The key is in the header dictionary, which can be used to uniquely identify the incoming message. For example if message_key_name is 'task_name' that means consumer will try to get value from message headers by key 'task_name' and use this value as key to get handler from handlers map
        :type message_key_name: str
        :param prefetch_count: Number of unacknowledged messages that consumer can keep at the same time
        :type prefetch_count: int
        :param reconnect: If false an exception will be raised when connection will become lost
        :type reconnect: bool
        """
        self._connection: AbstractRobustConnection = None
        self._conn_url = conn_url
        self._queue: AbstractRobustQueue = None
        self._tag: ConsumerTag = None
        self._channel: AbstractRobustChannel = None
        self._prefetch_count = prefetch_count
        self._reconnect = reconnect
        self.message_key_name = message_key_name
        self._handlers_map: dict[MessageKeyType, RawDataHandler] = {}

    async def connect(self) -> None:
        if not self._connection or self._connection.is_closed:
            self._connection = await connect_robust(self._conn_url)

    async def disconnect(self) -> None:
        if self._channel:
            if not self._channel.is_closed:
                await self._channel.close()
            self._channel = None
            await self._queue.cancel(self._tag)
            self._tag = None

        if self._connection:
            await self._connection.close()
            self._connection = None

    async def _check_connection(self):
        if not self._connection or self._connection.is_closed:
            if self._reconnect:
                await self.connect()
            else:
                raise NoConnectionError("Connection lost")

    async def _init_channel(self):
        await self._check_connection()
        self._channel = await self._connection.channel(publisher_confirms=False)

    async def _get_channel(self):
        if not self._channel or self._channel.is_closed:
            await self._init_channel()
        return self._channel

    async def _declare_queue(self, queue_name: str):
        channel = await self._get_channel()
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-max-priority": 10,
            },
        )
        return queue

    def _on_message(self, get_handler_func: HandlerDependencyType) -> IncomingMessageHandler:
        async def handle_message(message: AbstractIncomingMessage) -> None:
            publisher_answer = b"ACK"
            try:
                message_key = message.headers.get(self.message_key_name)
                handler = get_handler_func(message_key)
                if not handler:
                    raise UnregisteredHandler(
                        f"Handler with key '{self.message_key_name}' was not registered")
                await handler(message.body.decode())
            except Exception as e:
                logger.error(
                    f"Error occured when handled marked '{message_key}' retrieving from '{self._queue.name}': {e}")
                publisher_answer = b"NACK"
            await message.ack()
            if message.reply_to:
                channel = await self._get_channel()
                await channel.default_exchange.publish(
                    Message(body=publisher_answer), routing_key=message.reply_to
                )
        return handle_message

    async def consume(self, listen_for: str, get_handler_func: HandlerDependencyType):
        self._queue = await self._declare_queue(listen_for)
        self._tag = await self._queue.consume(self._on_message(get_handler_func))
