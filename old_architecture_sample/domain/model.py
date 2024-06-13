import random
import time


class StubModel:

    def inference(self, input) -> str:
        time.sleep(random.randint(1, 30))  # simulation of the process of data processing

        return f"hello, {input['name']}!"
