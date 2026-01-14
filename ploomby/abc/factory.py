from typing import Protocol

from .consumer import MessageConsumer


class MessageConsumerFactory(Protocol):
    async def create(self, *args, **kwargs) -> MessageConsumer: ...
