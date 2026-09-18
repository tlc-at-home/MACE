import asyncio
from crypto.swarm.orchestrator import execute_swarm_sweep
import logging

logging.basicConfig(level=logging.INFO)
class Args:
    limit = 5

import crypto.swarm.orchestrator as orch

orig_communicate = asyncio.subprocess.Process.communicate
async def mock_communicate(self, input=None):
    if input:
        print("ALLOCATOR INPUT:", input.decode())
    return await orig_communicate(self, input)

asyncio.subprocess.Process.communicate = mock_communicate

asyncio.run(execute_swarm_sweep(Args()))
