from typing import Any, List
import json
import inspect
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
from pandas import Timestamp
import os

class UniverseResult:
    def __init__(self, timestamp: Timestamp):
        self.__timestamp = timestamp
        self.__results = []

    @property
    def date(self) -> Timestamp:
        return self.__timestamp

    @property
    def results(self):
        return self.__results

    def add_member(self, measurement, meta: dict={}):
        self.__results.append({'Measurement': measurement, 'Meta': meta})


class Context:
    def __init__(self):
        pass


class UniverseActor(QapiActor.Qapi):
    def __init__(self, endpoint, endpoint_http, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint, endpoint_http, *_args, **_kwargs)
        self.__instance = func()

        self.subscribe("Request")

        self.__sink = None

    @staticmethod
    def get_dates(data):
        parsed = []

        data.sort()

        for d in data:
            parsed.append(Timestamp(d, tz='utc'))

        return parsed

    def transmit(self, value):

        data = value

        dates = self.get_dates(data['Dates'])
        guid = data["Guid"]

        results = {}

        context = Context()

        for date in dates:
            result = UniverseResult(date)

            self.__instance.formula(result, context)

            results[date.strftime("%Y-%m-%dT%H:%M:%SZ")] = result.results

        if self.__sink is None:
            self.__sink = self.get_subject("Response")

        self.__sink.on_next({'Guid': guid, 'Results': results})

    def on_receive(self, message: Event) -> Any:

        if message.inlet == "Request":
            self.transmit(message.value)


def universe(fn):

    grpc_endpoint = os.getenv('GRPC_ENDPOINT')
    http_endpoint = os.getenv('HTTP_ENDPOINT')

    UniverseActor.start(grpc_endpoint, http_endpoint, fn)
