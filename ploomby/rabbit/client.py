from typing import Optional

from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustConnection


class RabbitClient:
    def __init__(self, conn_url: str):
        self._connection: Optional[AbstractRobustConnection] = None
        self._conn_url = conn_url

    async def connect(self) -> None:
        if not self._connection or self._connection.is_closed:
            self._connection = await connect_robust(self._conn_url)

    async def disconnect(self) -> None:
        if self._connection:
            await self._connection.close()
            self._connection = None
