from typing import Any
from pykka import ThreadingActor
import os
import json
import inspect
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event


class FlowActor(ThreadingActor):
    def __init__(self, qapi, manifest, func, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__manifest = manifest
        self.__function = func
        self.__params = inspect.signature(self.__function).parameters
        self.__spread = False

        if len(self.__params) > 1:
            self.__spread = True

        qapi.proxy().source(manifest["inlets"]["Request"], self.actor_ref)

        self.__sink = qapi.proxy().sink(manifest["outlets"]["Response"]).get().proxy()

    def transmit(self, value):

        if self.__spread and isinstance(value, dict):
            ordered_args = {param: value.get(param) for param in list(self.__params.keys())}
            self.__function(**ordered_args)
        else:
            self.__function(value)

    def on_receive(self, message: Event) -> Any:

        if message.inlet is self.__manifest["inlets"]["Request"]:
            self.transmit(message.value)


def function(fn):

    endpoint = "127.0.0.1:5021"

    qapi = QapiActor.Qapi.start(endpoint)

    manifest_length = int.from_bytes(
        os.read(0, 4),
        "little"
    )

    manifest = json.loads(os.read(0, manifest_length).decode('utf8'))

    FlowActor.start(qapi, manifest, fn)
