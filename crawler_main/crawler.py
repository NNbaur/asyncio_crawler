import asyncio
from dataclasses import dataclass
from typing import Optional
from task import Task


# class Controller - controls the number of requests per time unit
class Controller:
    # max_rate - number of requests, interval - time unit,
    # concur_lvl - number of parallel requests running at the same time
    def __init__(self, max_rate: int, interval: int = 1, concur_lvl: Optional[int] = None):
        self.max_rate = max_rate
        self.interval = interval
        self.concur_lvl = concur_lvl
        self.is_running = False
        self._queue = asyncio.Queue()
        self._manager_task: Optional[asyncio.Task] = None
        # Semaphore - synchronization primitive,
        # limits the number of concurrent requests
        self._sem = asyncio.Semaphore(concur_lvl or max_rate)

    # Worker runs the tasks
    async def _worker(self, task: Task):
        # Async context manager
        # Semaphore in worker needs to when
        # task is running, we don't get another tasks
        # we don't pull another tasks until
        # number of concurrent requests reached number of max_rate
        async with self._sem:
            await task.perform(self)
            self._queue.task_done()

    # Manager get up once per second, get tasks and run worker
    async def _manager(self):
        while self.is_running:
            for _ in range(self.max_rate):
                async with self._sem:
                    task = await self._queue.get()
                    asyncio.create_task(self._worker(task))
            await asyncio.sleep(self.interval)

    # method, that runs manager
    def start(self):
        self.is_running = True
        self._manager_task = asyncio.create_task(self._manager())

    # Put task into queue
    async def put(self, task: Task):
        await self._queue.put(task)

    # Standard lib queue hasn't func join
    # We wait until queue is empty
    async def join(self):
        await self._queue.join()

    async def stop(self):
        self.is_running = False
        self._manager_task.cancel()


async def start():
    controller = Controller(3)
    for task_id in range(10):
        await controller.put(Task(task_id))
    controller.start()
    await controller.join()
    await controller.stop()


def main():
    loop = asyncio.new_event_loop()

    try:
        loop.run_until_complete(start())
    except KeyboardInterrupt:
        loop.close()


if __name__ == '__main__':
    main()
