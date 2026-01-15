from typing import Type, Awaitable, Any
from functools import wraps

from pydantic import BaseModel

from ploomby.abc import (
    MessageKeyType,
    HandlerType,
    MessageConsumer,
    MessageConsumerFactory,
    NoModelProvidedError,
    ConsumerAlreadyRegistered
)
from ploomby.logger import logger

__all__ = [
    "HandlersRegistry",
    "MessageConsumerRegistry"
]


class HandlersRegistry:
    def __init__(self):
        self._handlers: dict[MessageKeyType, HandlerType] = {}
        self._models: dict[MessageKeyType, Type[BaseModel]] = {}

    def _find_model(self, annotations: dict):
        for arg in annotations.values():
            if issubclass(arg, BaseModel):
                return arg
        return None

    def register(self, key: MessageKeyType):
        def decorator(handler_func: HandlerType):
            model = self._find_model(handler_func.__annotations__)
            if not model:
                raise NoModelProvidedError(
                    f"No data model provided into handler '{handler_func.__name__}'")
            self._handlers[key] = handler_func
            self._models[key] = model
            logger.info(
                f"Handler '{handler_func.__name__}' registered by key '{key}'. Handler excpects data as model of {model.__name__}")

            @wraps(handler_func)
            def _(dto: BaseModel, *args, **kwargs):
                pass
            return _
        return decorator

    def generate_handler_with_validation_coro(self, handler_key: MessageKeyType):
        def handler_with_validation(data: str | bytes) -> Awaitable[Any]:
            model = self._models.get(handler_key)
            handler = self._handlers.get(handler_key)
            if not (model and handler):
                return None
            dto = model.model_validate_json(data)
            return handler(dto)
        return handler_with_validation


class MessageConsumerRegistry:
    def __init__(
            self,
            handlers_registry: HandlersRegistry,
            consumer_factory: MessageConsumerFactory
    ):
        self._handlers_registry = handlers_registry
        self._consumers_map: dict[str, MessageConsumer] = {}
        self._consumer_factory = consumer_factory

    async def register(
            self,
            listen_for: str,
            message_key_name: str,
            *args,
            **kwargs
    ):
        """
        Uses provided factory to create consumer instance and subscribe it on provided resource.
        If want to use not built-in factories just define it according to required interface of factore and provide to registry

        :param listen_for: Name of representation of what the consumer is subscribed to
        :type listen_for: str
        :param message_key_name: Value that consumer should to use to get value from message headers(or other metadata)
        to identify incoming messsage and get corresponding handler using get_handler_func provided in consume() 
        :type message_key_name: str
        :param args: Args using to provide to create method of consumer factory
        :param kwargs: Kwargs using to provide to create method of consumer factory
        """
        current = self._consumers_map.get(listen_for)
        if current:
            raise ConsumerAlreadyRegistered(f"Consumer for '{listen_for}' already registered")
        consumer = await self._consumer_factory.create(message_key_name, *args, **kwargs)
        await consumer.consume(listen_for, self._handlers_registry.generate_handler_with_validation_coro)
        self._consumers_map[listen_for] = consumer
        logger.info(
            f"Consumer registered with message key name '{consumer.message_key_name}'. Listening for '{listen_for}'")
