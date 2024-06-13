import random
import time

import arqanmode
import arqanmode.framework as af
import pydantic
from arqanmode.kafka import KafkaConfig
from arqanmode.storage import RedisConfig


class Model(af.ModelInterface):

    def __init__(self):
        self.interface = arqanmode.ModelV1(
            model_name="test_model",
            scheme=arqanmode.SchemeV1(
                name="test_schema_name",
                fields=[
                    arqanmode.SchemeFieldV1(
                        name="test_field_name",
                        value=pydantic.StrictStr,
                    ),
                ]
            )
        )

    def get_interface(self) -> arqanmode.ModelV1:
        return self.interface

    def parse_and_validate(self, raw_request: bytes) -> object:
        obj = self.interface.parse_raw(raw_request)

        fields = {field.name: field.value for (field) in obj.scheme.fields}

        return {
            "name": fields["test_field_name"],
        }

    async def process_task(self, input: object):
        time.sleep(random.randint(1, 30))  # simulation of the process of data processing

        return arqanmode.TaskResult(
            status=arqanmode.TaskStatus.SUCCESS,
            raw_response=f"hello, {input['name']}!".encode(),
        )


model = Model()


async def main():
    print("start main")

    # ModelFramework requires 3 fields for initialisation
    # 1) model that implements ModelInterface
    #
    # 2) config for Kafka consumer
    #
    # 3) config for Redis
    #
    # Here we use default test configs that are provided with the config
    # For Kafka defaults are topic="test", group_id="test", server="localhost:9092"
    # For Redis defaults are url="localhost", port="6379"
    framework = af.ModelFramework(
        model,
        KafkaConfig.TestConfig(),
        RedisConfig.TestConfig(),
    )

    # starting the framework processing
    await framework.process()

    # graceful shutdown of the framework
    await framework.stop()
