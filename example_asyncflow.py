import asyncio
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

from radical.asyncflow import LocalExecutionBackend, WorkflowEngine

from radical.asyncflow.streaming import (
    StreamingServer,
    StreamingClient,
    StreamBackend,
    MQTTBackend,
)

logger = logging.getLogger(__name__)


async def main():
    backend = await LocalExecutionBackend(ThreadPoolExecutor())
    flow = await WorkflowEngine.create(backend)

    stream_backend = StreamBackend()

    # in this case, a "server" is anything that runs in AsyncFlow's engine
    # a "client" is anything that runs inside a task.
    stream_server = StreamingServer()

    mqtt_backend = MQTTBackend(host, port)
    mqtt_handler = StreamingServer(mqtt_backend)

    @flow.streaming_task
    async def my_sensor_task(*args):
        # client is private to the task.
        streaming_client = StreamingClient(stream_backend)

        while True:
            number = random.random()
            streaming_client.publish("/my_sensor", number)
            time.sleep(1)

    # this is a "pseudo task". To asyncflow it will look like any task, but
    # not actually execute anything. When an event comes on the topic, this
    # future will be ready. It will go back to off once all dependent tasks are scheduled.
    sensor_receiving = flow.register(stream_server.task_factory_subscribe("/my_sensor"))

    # For fun, lets add in a MQTT consumer
    mqtt_receiving = flow.register(
        mqtt_handler.task_factory_subscribe("/my_mqtt_sensor")
    )

    @flow.function_task
    async def task1(*args):
        # Simulate processing data from task1 (simple guessing number game)
        input_data = args[0]
        return int(int(input_data) * 100)

    @flow.function_task
    async def task2(*args):
        # Simulate processing data from task2 (e.g., pick number)
        number = int(random.random() * 100)
        return number

    @flow.function_task
    async def task3(*args):
        # Simulate processing data from task3 (e.g., return high or low)
        if args[0] == args[1]:
            return 0

        number = 1 if args[0] > args[1] else -1
        return number

    async def run_wf(wf_id):
        print(f"Starting workflow {wf_id} at {time.time()}")

        num1 = t1(sensor_receiving)
        num2 = t2()

        result = await t3(num1, num2)
        # Only t1 depends on the sensor. After t1 starts, sensor_receiving is cleared. (each task will get event exactly once)
        print(f"Result: {result}")

        num1 = t1(sensor_receiving)  # sensor receiving is already done.
        num2 = t2()

        result = await t3(num1, num2)
        print(f"Result 2: {result}")

        t1 = mqtt_receiving
        num1 = t1(
            mqtt_receiving
        )  # t1 will automatically be called when receiving an MQTT event.
        num2 = t2()

        result = await t3(
            num1, num2
        )  # Only t1 depends on the sensor. After t1 starts, sensor_receiving is cleared.
        print(f"Result MQTT: {result}")

    # Run workflows concurrently
    await run_wf(1)

    await flow.shutdown()

    stream_server.shutdown()
    mqtt_handler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
