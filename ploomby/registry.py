from typing import Type, Awaitable, Any
from functools import wraps

from pydantic import BaseModel

from ploomby.abc import (
    MessageKeyType,
    HandlerType,
    MessageConsumer,
    MessageConsumerFactory
)
from ploomby.logger import logger


class HandlersRegistry:
    def __init__(self):
        self._handlers: dict[MessageKeyType, HandlerType] = {}
        self._models: dict[MessageKeyType, Type[BaseModel]] = {}

    def register(self, key: MessageKeyType, model: type[BaseModel]):
        def decorator(handler_func: HandlerType):
            self._handlers[key] = handler_func
            self._models[key] = model
            logger.info(f"Handler '{handler_func.__name__}' registered by key '{key}'")

            @wraps(handler_func)
            def _(dto: BaseModel, *args, **kwargs):
                pass
            return _
        return decorator

    def generate_handler_with_validation_coro(self, handler_key: MessageKeyType):
        def handler_with_validation(data: str | bytes) -> Awaitable[Any]:
            dto = self._models[handler_key].model_validate_json(data)
            return self._handlers[handler_key](dto)
        return handler_with_validation


class MessageConsumerRegistry:
    def __init__(
            self,
            handlers_registry: HandlersRegistry,
            consumer_factory: MessageConsumerFactory
    ):
        self._handlers_registry = handlers_registry
        self._consumers: dict[str, MessageConsumer] = {}
        self._consumer_factory = consumer_factory

    async def register(
            self,
            listen_for: str,
            message_key_name: str,
            *args,
            **kwargs
    ):
        """
        Uses provided factory to create consumer instance and subscribe it on provided resource. If want to use not built-in factories just define it according to required interface of factore and provide to registry

        :param listen_for: Description
        :type listen_for: str
        :param consumer: Description
        :type consumer: MessageConsumer
        :param args: Args using to provide to create method of consumer factory
        :param kwargs: Kwargs using to provide to create method of consumer factory
        """
        consumer = await self._consumer_factory.create(message_key_name, *args, **kwargs)
        await consumer.consume(listen_for, self._handlers_registry.generate_handler_with_validation_coro)
        self._consumers[listen_for] = consumer
