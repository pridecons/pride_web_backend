# routes/ANGEL_ONE/realtime_hub.py
import asyncio
import os
import time
from typing import Optional

import orjson
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

CHANNEL = "market:snapshot:channel"
LATEST_KEY = "market:snapshot:latest"
LOCK_KEY = "market:leader-lock"

# 8 sec lock; producer refresh loop every 2-5 sec so lock stays alive
LOCK_TTL_MS = 8000

class RealtimeHub:
    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=False)

    async def try_become_leader(self, worker_id: str) -> bool:
        # SET key value NX PX ttl
        ok = await self.r.set(LOCK_KEY, worker_id.encode(), nx=True, px=LOCK_TTL_MS)
        return bool(ok)

    async def renew_leader_lock(self, worker_id: str) -> bool:
        # Renew only if we still own it (simple pattern: compare value)
        val = await self.r.get(LOCK_KEY)
        if val != worker_id.encode():
            return False
        # extend TTL
        await self.r.pexpire(LOCK_KEY, LOCK_TTL_MS)
        return True

    async def publish_snapshot(self, payload: dict) -> None:
        b = orjson.dumps(payload)
        # store latest
        await self.r.set(LATEST_KEY, b)
        # fanout
        await self.r.publish(CHANNEL, b)

    async def get_latest(self) -> Optional[bytes]:
        return await self.r.get(LATEST_KEY)

    async def subscribe(self):
        pubsub = self.r.pubsub()
        await pubsub.subscribe(CHANNEL)
        return pubsub
