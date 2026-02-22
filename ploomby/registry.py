from typing import Type, Awaitable, Any, Optional
from functools import wraps

from pydantic import BaseModel

from ploomby.abc import (
    MessageKeyType,
    HandlerType,
    MessageConsumer,
    MessageConsumerFactory,
    NoModelProvidedError,
    ConsumerAlreadyRegistered,
    HandlerAlreadyExistsError
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

    def register(self, key: Optional[MessageKeyType] = None):
        """
        Register message handler in handlers registry

        :param key: Unique key that should be provided in broker message and used to get handler for th9is message.
        If not provided handler function name will be used 
        :type key: MessageKeyType | None
        """
        def decorator(handler_func: HandlerType):
            @wraps(handler_func)
            def _(dto: BaseModel, *args, **kwargs):
                pass
            reg_key = key or handler_func.__name__
            for key_, handler in self._handlers.items():
                if handler is handler_func:
                    logger.warning(
                        f"Function '{handler_func.__name__}' already registered as handler with key '{key_}'. New handler will not be registered")
                    return _
                if key_ == reg_key:
                    raise HandlerAlreadyExistsError(
                        f"Function '{handler.__name__}' already registered as handler with key '{key_}'. Resolve handlers keys")
            model = self._find_model(handler_func.__annotations__)
            if not model:
                raise NoModelProvidedError(
                    f"No data model provided into handler '{handler_func.__name__}'")
            self._handlers[reg_key] = handler_func
            self._models[reg_key] = model
            logger.info(
                f"Handler '{handler_func.__name__}' registered by key '{reg_key}'. Handler excpects data as model of {model.__name__}")
            return _
        return decorator

    def generate_handler_with_validation_coro(self, handler_key: MessageKeyType):
        def handler_with_validation(data: str | bytes) -> Optional[Awaitable[Any]]:
            model = self._models.get(handler_key)
            handler = self._handlers.get(handler_key)
            if not (model and handler):
                return None
            dto = model.model_validate_json(data)
            return handler(dto)  # type: ignore
        return handler_with_validation


class MessageConsumerRegistry:
    def __init__(
            self,
            handlers_registry: HandlersRegistry,
            consumer_factory: MessageConsumerFactory
    ):
        self._handlers_registry = handlers_registry
        self._consumers_map: dict[str, list[MessageConsumer]] = {}
        self._consumer_factory = consumer_factory

    async def register(
        self,
        listen_for: str,
        message_key_name: str,
        consumers_count: int = 1,
        *factory_create_args,
        **factory_create_kwargs
    ):
        """
        Uses provided factory to create consumer instance and subscribe it on provided resource.
        If want to use not built-in factories just define it according to required interface of factore and provide to registry

        :param listen_for: Name of representation of what the consumer is subscribed to
        :type listen_for: str
        :param message_key_name: Value that consumer should to use to get value from message headers(or other metadata)
        to identify incoming messsage and get corresponding handler using get_handler_func provided in consume() 
        :type message_key_name: str
        :param consumers_count: Define count of consumers that will listen resource and will looking for message key name
        :param factory_create_args: Args using to provide to create method of consumer factory
        :param factory_create_kwargs: Kwargs using to provide to create method of consumer factory
        """
        current_subscribers = self._consumers_map.get(listen_for, [])
        if current_subscribers:
            raise ConsumerAlreadyRegistered(
                f"{len(current_subscribers)} consumer{["", "s"][len(current_subscribers) != 1]} already looking for message key named '{message_key_name}' on resource '{listen_for}'")
        current_subscribers = [
            await self._consumer_factory.create(message_key_name, *factory_create_args, **factory_create_kwargs) for _ in range(consumers_count)
        ]
        for consumer in current_subscribers:
            await consumer.consume(listen_for, self._handlers_registry.generate_handler_with_validation_coro)
        self._consumers_map[listen_for] = current_subscribers
        logger.info(
            f"{len(current_subscribers)} consumer{["", "s"][len(current_subscribers) != 1]} registered and looking for message key name '{consumer.message_key_name}'. Listening for '{listen_for}'")

    async def disconnect_consumers(self):
        for consumers in self._consumers_map.values():
            for consumer in consumers:
                await consumer.disconnect()
