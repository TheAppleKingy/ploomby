import json

from typing import Callable, Awaitable, Optional

from aio_pika import Message, ExchangeType
from aio_pika.abc import ConsumerTag, AbstractRobustChannel, AbstractRobustQueue, AbstractIncomingMessage, AbstractRobustConnection

from ploomby.abc.exceptions import UnregisteredHandler, NoConnectionError, NoMessageKeyError
from ploomby.abc import (
    IncomingMessageHandler,
    RawDataHandler,
    MessageKeyType,
    HandlerDependencyType,
    MessageConsumer
)
from ploomby.logger import logger


class RabbitConsumer(MessageConsumer):
    """
    Simplest consumer for RabbitMQ implemented by aio_pika.  
    """

    def __init__(
        self,
        connection_dep: Callable[[], Awaitable[AbstractRobustConnection]],
        message_key_name: str,
        conn_is_shared: bool = True,
        prefetch_count: int = 0,
        reconnect: bool = True,
        with_dead_letter_policy: bool = False,
        queue_args: dict[str, str | int] = {},
        rpc: bool = False
    ):
        """
        :param connection_dep: Dependency that should returns a coroutine for getting connection 
        :type connection_dep: Callable[[], Awaitable[AbstractRobustConnection]]
        :param message_key_name: The key is in the header dictionary, which can be used to uniquely identify the incoming message. For example if message_key_name is 'task_name' that means consumer will try to get value from message headers by key 'task_name' and use this value as key to get handler from handlers map
        :type message_key_name: str
        :param conn_is_shared: Should be True if connection is shared for consumers produced by one factory else False
        :type conn_is_shared: bool
        :param prefetch_count: Number of unacknowledged messages that consumer can keep at the same time
        :type prefetch_count: int
        :param reconnect: If false an exception will be raised when connection will become lost
        :type reconnect: bool
        :param with_dead_letter_policy: If true dead-letter queue will be created for listening queue
        :type with_dead_letter_policy: bool
        :param queue_args: Arguments for declaring queue
        :type queue_args: dict[str, str | int]
        :param rpc: If true consumer will send handler's return value to a reply queue
        :type rpc: bool
        """
        self._connection_dep = connection_dep
        self._connection: AbstractRobustConnection = None  # type: ignore
        self._conn_is_shared = conn_is_shared
        self._queue: AbstractRobustQueue = None  # type: ignore
        self._tag: ConsumerTag = None  # type: ignore
        self._channel: AbstractRobustChannel = None  # type: ignore
        self._prefetch_count = prefetch_count
        self._reconnect = reconnect
        self.message_key_name = message_key_name
        self._handlers_map: dict[MessageKeyType, RawDataHandler] = {}
        self._with_dead_letter_policy = with_dead_letter_policy
        self._queue_args = queue_args
        self._rpc = rpc

    async def _cancel_queue(self):
        if self._queue:
            await self._queue.cancel(self._tag)
            self._tag = None
            self._queue = None

    async def _close_channel(self):
        if self._channel:
            if not self._channel.is_closed:
                await self._channel.close()
            self._channel = None

    async def _close_conn(self):
        if self._connection:
            if not self._connection.is_closed:
                if not self._conn_is_shared:
                    await self._connection.close()
            self._connection = None

    async def connect(self):
        self._connection = await self._connection_dep()

    async def disconnect(self) -> None:
        await self._cancel_queue()
        await self._close_channel()
        await self._close_conn()

    async def _check_connection(self):
        if not self._connection or self._connection.is_closed:
            if self._reconnect:
                await self.connect()
            else:
                raise NoConnectionError("Connection lost")

    async def _get_channel(self):
        await self._check_connection()
        if not self._channel or self._channel.is_closed:
            self._channel = await self._connection.channel(publisher_confirms=False)
        return self._channel

    async def _setup_dead_letter_policy(self, queue_name: str):
        channel = await self._get_channel()
        dlx = await channel.declare_exchange(
            "dlx",
            durable=True,
            type=ExchangeType.DIRECT
        )
        dlq = await channel.declare_queue(
            f"{queue_name}_dlq",
            durable=True,
            arguments={
                "x-message-ttl": 3600*24
            }
        )
        await dlq.bind(dlx, f"{queue_name}_dlq")

    async def _declare_queue(self, queue_name: str):
        channel = await self._get_channel()
        args = self._queue_args.copy()
        if self._with_dead_letter_policy:
            await self._setup_dead_letter_policy(queue_name)
            args.update({"x-dead-letter-exchange": "dlx", "x-dead-letter-routing-key": f"{queue_name}_dlq"})
        self._queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments=args,
        )
        return self._queue

    def _on_message(self, get_handler_func: HandlerDependencyType) -> IncomingMessageHandler:
        async def handle_message(message: AbstractIncomingMessage) -> None:
            try:
                message_key = message.headers.get(self.message_key_name)
                if not message_key:
                    raise NoMessageKeyError(
                        f"Headers do not contain value by key '{self.message_key_name}'")
                validated_coro = get_handler_func(message_key)(message.body.decode(errors="ignore"))  # type: ignore
                if not validated_coro:
                    raise UnregisteredHandler(
                        f"Handler for message identified as '{message_key!r}' was not registered")
                result = await validated_coro
                if self._rpc:
                    try:
                        rpc_answer = json.dumps(result)
                    except TypeError as e:
                        rpc_answer = str(e)
                await message.ack()
            except Exception as e:
                logger.error(
                    f"Error occured when handled message retrieved from '{self._queue.name}': {e}")
                if self._with_dead_letter_policy:
                    await message.nack(requeue=False)
                else:
                    logger.error(f"Dead letter policy was not selected. Mesage will be killed")
                    await message.ack()
                rpc_answer = str(e)
            if self._rpc:
                if message.reply_to:
                    channel = await self._get_channel()
                    await channel.default_exchange.publish(
                        Message(body=json.dumps({"detail": rpc_answer}).encode()), routing_key=message.reply_to
                    )
                else:
                    logger.error(f"Consumer {self._tag} was setup for RPC but reply queue was no specified")
        return handle_message

    async def consume(self, listen_for: str, get_handler_func: HandlerDependencyType):
        self._queue = await self._declare_queue(listen_for)
        self._tag = await self._queue.consume(self._on_message(get_handler_func))
