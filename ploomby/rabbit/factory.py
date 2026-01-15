from typing import Callable, Awaitable

from aio_pika.abc import AbstractRobustConnection
from aio_pika import connect_robust

from ploomby.abc import MessageConsumer

from .consumer import RabbitConsumer


class RabbitConsumerFactory:
    """
    Factory producing instances of consumers per queue.
    """

    def __init__(self, conn_url: str, shared_conn: bool = True):
        """
        :param conn_url: URL to connect RabbitMQ
        :type conn_url: str
        :param shared_conn: If True all created consumers will have tha same instance of connection
        :type shared_conn: bool
        """
        self._conn_url = conn_url
        self._shared_conn = shared_conn
        self._connection: AbstractRobustConnection = None

    async def _get_connection(self) -> AbstractRobustConnection:
        if self._shared_conn:
            if not self._connection or self._connection.is_closed:
                self._connection = await connect_robust(self._conn_url)
            return self._connection
        return await connect_robust(self._conn_url)

    async def create(self, message_key_name: str, prefetch_count: int = 1, reconnect: bool = True) -> MessageConsumer:
        consumer = RabbitConsumer(
            self._get_connection,
            message_key_name,
            conn_is_shared=self._shared_conn,
            prefetch_count=prefetch_count,
            reconnect=reconnect,
        )
        await consumer.connect()
        return consumer
