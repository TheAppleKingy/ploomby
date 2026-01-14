from typing import Protocol

from .types import HandlerDependencyType


class MessageConsumer(Protocol):
    def connect(self): ...

    async def consume(self, listen_for: str, get_handler_func: HandlerDependencyType):
        """        
        :param listen_for: Name of representation of what the consumer is subscribed to
        :type listen_for: str
        :param get_handler_func: Wrapper that retrieves message key and returns function, that handles raw data from broker
        :type get_handler_func: HandlerDependencyType
        """
