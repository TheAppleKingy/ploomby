# ploomby


[![PyPI version](https://badge.fury.io/py/ploomby.svg)](https://badge.fury.io/py/ploomby)
[![Python](https://img.shields.io/pypi/pyversions/ploomby.svg)](https://pypi.org/project/ploomby)
[![Downloads](https://img.shields.io/pypi/dm/ploomby.svg)](https://pypi.org/project/ploomby/)

**ploomby** is a wrapper over [aio_pika](https://github.com/mosquito/aio-pika), which provides a more intuitive and simpler way to declare message handlers coming from RabbitMQ.

- [Install](#install)
- [Quickstart](#quickstart)
- [Usage](#usage)

---

## ⚙️ Install
```bash
pip install ploomby
```
---

## 🚀 Quickstart

```python
from pydantic import BaseModel

from ploomby.registry import HandlersRegistry, MessageConsumerRegistry
from ploomby.rabbit import RabbitConsumerFactory

handlers_registry = HandlersRegistry()
consumer_factory = RabbitConsumerFactory("amqp://admin:admin@localhost:5672")
consumer_registry = MessageConsumerRegistry(handlers_registry, consumer_factory)


class CreateUserDTO(BaseModel):
    name: str


@handlers_registry.register("create_user")
async def create_user(dto: CreateUserDTO):
    await ...


consumer = consumer_registry.register("queue1", "task_name")
```
 After registering of consumer it starts to listen resourse(in RabbitMQ case it is a queue) immediately.

---

## 📦 Features
This lib let you to manage your handlers too flexible.

Creating registries for handlers requires nothing. It just need to decorate our handlers using **register()** method.
```python
def register(self, key: MessageKeyType)
```
This method requires only key that will be used to identify message coming from broker anf take corresponding handler from registry. 

------------
Consumer registry takes over the management of the consumers and their creation and that is cause we need to use factories create consumers in setup we need. ploompy provides factory for creating simpliest consumer's implementation for RabbitMQ using [aio_pika](https://github.com/mosquito/aio-pika). But you can define your factory to define rules of setuping creating consumers. Factory just should implement interface of
```python
class MessageConsumerFactory(Protocol):
    async def create(self, message_key_name: str, *args, **kwargs) -> MessageConsumer: ...
```
where interface of consumer looks like
```python
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
```
Yes, also you can define implementation of consumer. 

---
Next we make a **MessageConsumerRegistry**. We need literally **register():**
```python
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
```