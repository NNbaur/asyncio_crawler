from dataclasses import dataclass
import asyncio


@dataclass
class Task:
    task_id: int
    async def perform(self, controller):
        print(f'Start perform {self.task_id}')
        await asyncio.sleep(0.5)
        print(f'Complete perform {self.task_id}')