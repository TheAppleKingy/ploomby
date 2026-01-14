from typing import Protocol

from .consumer import MessageConsumer


class MessageConsumerFactory(Protocol):
    async def create(self, message_key_name: str, *args, **kwargs) -> MessageConsumer: ...
