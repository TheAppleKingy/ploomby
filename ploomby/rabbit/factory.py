from ploomby.abc import MessageConsumer

from .consumer import RabbitConsumer


class RabbitConsumerFactory:
    def __init__(self, conn_url: str):
        self._conn_url = conn_url

    async def create(self, message_key_name: str, prefetch_count: int = 1, reconnect: bool = True) -> MessageConsumer:
        consumer = RabbitConsumer(
            self._conn_url,
            message_key_name,
            prefetch_count=prefetch_count,
            reconnect=reconnect
        )
        await consumer.connect()
        return consumer
