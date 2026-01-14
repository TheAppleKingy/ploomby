from ploomby.abc import MessageConsumer

from .consumer import RabbitConsumer


class RabbitConsumerFactory:
    def __init__(self, conn_url: str, message_key_name: str):
        self._conn_url = conn_url
        self._message_key_name = message_key_name

    async def create(self, prefetch_count: int = 1, reconnect: bool = False) -> MessageConsumer:
        consumer = RabbitConsumer(
            self._conn_url,
            self._message_key_name,
            prefetch_count=prefetch_count,
            reconnect=reconnect
        )
        await consumer.connect()
        return consumer
