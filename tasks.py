import asyncio
import os

from taskiq_matrix.instance import broker

# from taskiq_matrix.matrix_broker import MatrixBroker
# from taskiq_matrix.matrix_result_backend import MatrixResultBackend


@broker.task()
async def hello():
    print("Hello, world!")
    return 1


async def main():
    task = await hello.kiq()
    print("Task sent. Waiting for result...")
    result = await task.wait_result()
    print(f"Got result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
