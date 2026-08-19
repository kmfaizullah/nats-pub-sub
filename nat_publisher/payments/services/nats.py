import json

import nats
from django.conf import settings


class NATSClient:
    def __init__(self):
        self.nc = None
        self.js = None

    async def connect(self):
        if self.nc and not self.nc.is_closed:
            return

        self.nc = await nats.connect(settings.NATS_URL)
        self.js = self.nc.jetstream()

    async def publish(self, subject, message):
        await self.connect()

        payload = json.dumps(message).encode()

        ack = await self.js.publish(
            subject,
            payload,
        )
        return ack

    async def close(self):
        if self.nc and not self.nc.is_closed:
            await self.nc.drain()
