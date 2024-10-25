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


def function(fn, map_reduce):

    sink = None
    spread = False

    op = None
    json_value = None
    config = None
    params = None

    def transmit(value, client, manifest):
        nonlocal sink, op, json_value, config, params
        data = value[0]

        if op is None:
            op = fn(value[1])
            config = value[1]
            json_value = json.dumps(value[1])
            params = inspect.signature(op).parameters
        else:
            config = value[1]
            if json_value == json.dumps(value[1]):
                op = fn(value[1])
                json_value = json.dumps(value[1])

        if spread and isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as e:
                print(e)

        if sink is None:
            m = manifest.outlet("Response")
            sink = client.sink(m)

        if spread and isinstance(data, dict):
            ordered_args = {param: value.get(param) for param in list(params.keys())}
            sink.on_next(map_reduce(ordered_args, config, lambda x: op(**x)))
        else:
            if len(params) == 0:
                sink.on_next(map_reduce(data, config, op))
            else:
                if isinstance(data, str) and is_first_param_dict(op):
                    sink.on_next(map_reduce(json.loads(data), config, op))
                else:
                    sink.on_next(map_reduce(data, config, op))

    client = Qapi.QapioGrpcInstance("localhost:5021")

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