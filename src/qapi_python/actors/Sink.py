from typing import Any

import pykka

from qapi_python.client.Qapi import QapioGrpcInstance


class Sink(pykka.ThreadingActor):
    def __init__(self, expression: str, client: QapioGrpcInstance, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = client
        if "(" in expression:
            self.__sink = client.sink(expression)
        else:
            self.__sink = client.sink(client.get_manifest().outlet(expression))

    def on_next(self, value):
        self.__sink.on_next(value)
