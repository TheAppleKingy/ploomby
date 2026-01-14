from typing import Protocol

from .types import HandlerDependencyType


class MessageConsumer(Protocol):
    """
    Interface of consumer that should be implemented to use in registries.
    Message key name is a value that consumer should to use to get value from message headers(or other metadata)
    to identify incoming messsage and get corresponding handler using get_handler_func provided in consume() 
    """
    message_key_name: str

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...

    async def consume(self, listen_for: str, get_handler_func: HandlerDependencyType):
        """
        Starts consume events/messages from resourse

        :param listen_for: Name of representation of what the consumer is subscribed to
        :type listen_for: str
        :param get_handler_func: Wrapper that retrieves message key and returns function, that handles raw data from broker
        :type get_handler_func: HandlerDependencyType

        examples get_handler_func::

            def get_order_handler(key: str) -> Callable[[str | bytes], Awaitable[None]]:
                handlers = {
                    "order.created": handle_order_created,
                    "order.updated": handle_order_updated,
                    "order.cancelled": handle_order_cancelled,
                }
                return handlers.get(key.decode("utf-8"), default_handler)

            async def handle_order_created(raw_data: str | bytes) -> None:
                order_data = json.loads(raw_data)
                # Process order creation logic
                await process_new_order(order_data)

            async def handle_order_updated(raw_data: str | bytes) -> None:
                order_data = json.loads(raw_data)
                # Process order update logic
                await update_order(order_data)

            async def default_handler(raw_data: str | bytes) -> None:
                logger.warning(f"Unhandled message key with data: {raw_data}")
        """
