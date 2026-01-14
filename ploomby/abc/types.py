from typing import Callable, Awaitable, Any

from aio_pika.abc import AbstractIncomingMessage

from pydantic import BaseModel


MessageKeyType = str
HandlerType = Callable[[BaseModel, Any], Awaitable[Any]]
IncomingMessageHandler = Callable[[AbstractIncomingMessage], Awaitable[Any]]
RawDataHandler = Callable[[str | bytes], Awaitable[Any]]
"""Represents function that retreives raw data and handle message"""
HandlerDependencyType = Callable[[MessageKeyType], RawDataHandler]
"""Function wrapper that returns function retrieving raw data and returns coroutine to handle message"""
