from typing import Any
from pykka import ThreadingActor
import os
import json
import inspect
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event


class FlowActor(QapiActor.Qapi):
    def __init__(self, endpoint, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint,*_args, **_kwargs)
        self.__function = func
        self.__params = inspect.signature(self.__function).parameters
        self.__spread = False

        if len(self.__params) > 1:
            self.__spread = True

        self.subscribe("Request")

        self.__sink = self.get_subject("Response")
        print("MAGA")

    def transmit(self, value):

        data = value

        if self.spread and isinstance(value, str):
            try:
                data = json.loads(data)
            except Exception as e:
                print(e)

        if self.__spread and isinstance(data, dict):
            ordered_args = {param: value.get(param) for param in list(self.__params.keys())}
            self.__sink.on_next(self.__function(**ordered_args))
        else:
            self.__sink.on_next(self.__function(data))

    def on_receive(self, message: Event) -> Any:

        print(message)
        if message.inlet == "Request":
            self.transmit(message.value)


def function(fn):

    endpoint = "127.0.0.1:5021"

    FlowActor.start(endpoint, fn)
