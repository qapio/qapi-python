from typing import Any
import json
import inspect
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
import os
from reactivex import operators, interval
from typing import get_type_hints
from qapi_python.client import Qapi
import time
def is_first_param_dict(func):
    # Get the signature of the function
    sig = inspect.signature(func)

    # Get the list of parameters
    params = list(sig.parameters.values())

    # Check if there is at least one parameter
    if not params:
        return False

    # Get the first parameter
    first_param = params[0]

    # Get the type hints for the function
    type_hints = get_type_hints(func)

    # Check if the first parameter has a type hint of dict
    return type_hints.get(first_param.name) == dict


class FlowActor(QapiActor.Qapi):
    def __init__(self, endpoint, endpoint_http, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint, endpoint_http, *_args, **_kwargs)
        self.__function = func
        self.__params = inspect.signature(self.__function).parameters
        self.__spread = False

        if len(self.__params) > 1:
            self.__spread = True

        #self.subscribe("Request")
        self.client().source(self.client().get_manifest().inlet("Variables").pipe(operators.take(1), operators.switch_latest(lambda x: self.client().source(self.client().get_manifest().inlet("Request"))))).subscribe(lambda x: self.transmit(x))


        self.__sink = None

    def transmit(self, value):

        data = value

        if self.__spread and isinstance(value, str):
            try:
                data = json.loads(data)
            except Exception as e:
                print(e)

        if self.__sink is None:
            self.__sink = self.get_subject("Response")

        if self.__spread and isinstance(data, dict):
            ordered_args = {param: value.get(param) for param in list(self.__params.keys())}
            self.__sink.on_next(self.__function(**ordered_args))
        else:
            if len(self.__params) == 0:
                self.__sink.on_next(self.__function())
            else:
                if isinstance(data, str) and is_first_param_dict(self.__function):
                    self.__sink.on_next(self.__function(json.loads(data)))
                else:
                    self.__sink.on_next(self.__function(data))

    def on_receive(self, message: Event) -> Any:

        if message.inlet == "Request":
            self.transmit(message.value)


def function(fn):

    sink = None
    spread = False
    params = inspect.signature(fn).parameters

    def transmit(value, client, manifest):
        nonlocal sink
        data = value
        print(value)
        if spread and isinstance(value, str):
            try:
                data = json.loads(data)
            except Exception as e:
                print(e)

        if sink is None:
            m = manifest.outlet("Response")
            print(m, flush=True)
            sink = client.sink(m)

        if spread and isinstance(data, dict):
            ordered_args = {param: value.get(param) for param in list(params.keys())}
            sink.on_next(fn(**ordered_args))
        else:
            if len(params) == 0:
                sink.on_next(fn)
            else:
                if isinstance(data, str) and is_first_param_dict(fn):
                    sink.on_next(fn(json.loads(data)))
                else:
                    print('ddd', flush=True)
                    sink.on_next(fn(data))

    grpc_endpoint = os.getenv('GRPC_ENDPOINT')
    http_endpoint = os.getenv('HTTP_ENDPOINT')

    client = Qapi.QapioGrpcInstance(grpc_endpoint)

    manifest = client.get_manifest()

    source = client.source(manifest.inlet("Request"))
    variables = client.source(manifest.inlet("Variables"))
    variables.pipe(operators.take(1), operators.compose(operators.map(lambda x: source), operators.switch_latest()),operators.with_latest_from(variables)).subscribe(lambda x: transmit(x, client, manifest))
    #variables.pipe(operators.take(1), operators.compose(operators.map(lambda x: source), operators.switch_latest())).subscribe(lambda x: transmit(x, client, manifest))

    try:
        while True:
            time.sleep(1)  # Keep the program running and alive
    except KeyboardInterrupt:
        print("Program terminated.")