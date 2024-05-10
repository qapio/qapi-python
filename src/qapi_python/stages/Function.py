from typing import Any
from pykka import ThreadingActor
import os
import json
from qapi_python.actors import Qapi as QapiActor

from qapi_python.actors.Source import Event


class FlowActor(ThreadingActor):
    def __init__(self, qapi, manifest, func, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__manifest = manifest
        self.__function = func

        qapi.proxy().source(manifest["inlets"]["Request"], self.actor_ref)
        qapi.proxy().source(manifest["inlets"]["Code"], self.actor_ref)

        self.__sink = qapi.proxy().sink(manifest["outlets"]["Response"]).get().proxy()

    def transmit(self, value):
        print("NBOM!")
        self.__sink.on_next(self.__function(value))

    def on_receive(self, message: Event) -> Any:

        if message.inlet is self.__manifest["inlets"]["Request"]:
            self.transmit(message.value)

        if message.inlet is self.__manifest["inlets"]["Code"]:
            print(message.value)


def function(fn):

    endpoint = "127.0.0.1:5021"

    qapi = QapiActor.Qapi.start(endpoint)

    manifest_length = int.from_bytes(
        os.read(0, 4),
        "little"
    )

    manifest = json.loads(os.read(0, manifest_length).decode('utf8'))

    print(manifest)
    
    FlowActor.start(qapi, manifest, fn)
