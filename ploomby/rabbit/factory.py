from aio_pika.abc import AbstractRobustConnection
from aio_pika import connect_robust

from ploomby.abc import MessageConsumerFactory

from .consumer import RabbitConsumer


class RabbitConsumerFactory(MessageConsumerFactory):
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
        self._connection: AbstractRobustConnection = None  # type: ignore

    async def _get_connection(self) -> AbstractRobustConnection:
        if self._shared_conn:
            if not self._connection or self._connection.is_closed:
                self._connection = await connect_robust(self._conn_url)
            return self._connection
        return await connect_robust(self._conn_url)

    async def create(
        self,
        message_key_name: str,
        prefetch_count: int = 1,
        reconnect: bool = True,
        with_dead_letter_policy: bool = False,
        queue_args: dict[str, str | int] = {},
        rpc: bool = False
    ) -> RabbitConsumer:
        consumer = RabbitConsumer(
            self._get_connection,
            message_key_name,
            conn_is_shared=self._shared_conn,
            prefetch_count=prefetch_count,
            reconnect=reconnect,
            with_dead_letter_policy=with_dead_letter_policy,
            queue_args=queue_args,
            rpc=rpc
        )
        await consumer.connect()
        return consumer
