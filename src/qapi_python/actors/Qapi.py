from typing import Any

import pykka

from qapi_python.actors.Source import Source
from qapi_python.actors.Sink import Sink
from qapi_python.client.Qapi import QapioGrpcInstance

from qapi_python.client.Qapi import Manifest


class Qapi(pykka.ThreadingActor):
    def __init__(self, endpoint: str, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = QapioGrpcInstance(endpoint)

    def source(self, expression: str, target: pykka.ActorRef):
        return Source.start(expression, self.__client, target)

    def sink(self, expression: str):
        return Sink.start(expression, self.__client)

    def get_manifest(self) -> Manifest:
        return self.__client.get_manifest()
