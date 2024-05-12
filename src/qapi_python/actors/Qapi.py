from typing import Any

import pykka

from qapi_python.actors.Source import Source
from qapi_python.actors.Sink import Sink
from qapi_python.client.Qapi import QapioGrpcInstance


class Qapi(pykka.ThreadingActor):
    def __init__(self, endpoint: str, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = QapioGrpcInstance(endpoint)

    def source(self, expression: str, target: pykka.ActorRef):
        return Source.start(expression, self.__client, target)

    def subscribe(self, inlet: str):
        return Source.start(inlet, self.__client, self.actor_ref)

    def sink(self, expression: str):
        return Sink.start(expression, self.__client)

    def get_subject(self, name: str):
        return Sink.start(name, self.__client).proxy()
