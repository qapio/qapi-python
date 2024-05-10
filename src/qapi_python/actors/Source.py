from typing import Any

import pykka

from qapi_python.client.Qapi import QapioGrpcInstance


class Event:
    def __init__(self, inlet: str, value: Any):
        self.__inlet = inlet
        self.__value = value

    @property
    def inlet(self):
        return self.__inlet

    @property
    def value(self):
        return self.__value


class Source(pykka.ThreadingActor):
    def __init__(self, expression: str, client: QapioGrpcInstance, actor_ref: pykka.ActorRef, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = client
        client.source(expression).subscribe(lambda x: actor_ref.tell(Event(expression, x)))

