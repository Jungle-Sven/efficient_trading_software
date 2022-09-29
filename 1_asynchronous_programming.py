import asyncio

class DataInput:
    def __init__(self):
        pass

    async def connect_to_ws(self):
        #implement websocket connection here
        #for now we just print a message inside infinitive loop
        while True:
            await asyncio.sleep(1)
            print('recieving data from websocket')

    async def task_connect_to_ws(self):
        await asyncio.sleep(0)
        await self.connect_to_ws()


class DataProcessing:
    def __init__(self):
        pass

    async def process_data(self):
        #implement data processing here
        #for now we just print a message inside infinitive loop
        while True:
            await asyncio.sleep(2)
            print('processing recieved data inside our program')

    async def task_process_data(self):
        await asyncio.sleep(0)
        await self.process_data()

class SimpleAsyncProgram:
    def __init__(self):
        self.data_input = DataInput()
        self.data_processing = DataProcessing()

    def run(self):
        #1 - we create event loop, we can consider it is program control stream
        self.ioloop = asyncio.get_event_loop()
        #2 - we add two tasks to our control stream
        tasks = [self.ioloop.create_task(self.data_input.task_connect_to_ws()), \
        self.ioloop.create_task(self.data_processing.task_process_data())]
        #3 - our control stream will wait for our tasks being complete
        wait_tasks = asyncio.wait(tasks)
        self.ioloop.run_until_complete(wait_tasks)
        self.ioloop.close()


our_program = SimpleAsyncProgram()
our_program.run()
