from typing import Any, List
import json
import inspect
import traceback
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
from pandas import Timestamp


class Context:
    def __init__(self, timestamp: Timestamp, measurement: str):
        self.__timestamp = timestamp
        self.__measurement = measurement
        self.__results = []

    @property
    def date(self) -> Timestamp:
        return self.__timestamp

    @property
    def measurement(self) -> str:
        return self.__measurement

    @property
    def results(self):
        return self.__results

    def add_member(self, measurement, meta: dict={}):
        self.__results.append({'Measurement': measurement, 'Meta': meta})


class FactorActor(QapiActor.Qapi):
    def __init__(self, endpoint, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint, *_args, **_kwargs)
        self.__instance = func()

        self.subscribe("Request")

        self.__sink = self.get_subject("Response")

    @staticmethod
    def get_dates(data):
        parsed = {}

        keys = list(data.keys())
        keys.sort()

        for d in keys:
            parsed[Timestamp(d, tz='utc')] = data[d]

        return parsed

    def transmit(self, value):

        data = value

        message = data["Universe"]
        universeId = data["Measurement"]
        factor = data["Factor"]
        guid = data["Guid"]

        try:
            results = {}
            dates = self.get_dates(message)

            all_members = []
            for date, value in message.items():
                all_members = all_members + [o.get("measurement") for o in value]

            for date, universe in dates.items():
                dr = []

                for member in universe:
                    context = Context(date, member)
                    self.__instance.formula(context)
                    dr.append([{"Measurement": member, "Time": self.date.strftime("%Y-%m-%dT%H:%M:%SZ"), "Fields": {self.field: self.value}, "Tags": {
                        "FSYM_ID": ''
                    }}])

                results[date.strftime("%Y-%m-%dT%H:%M:%SZ")] = dr

            self.__sink.on_next({'Guid': guid, 'Data': results})

        except Exception as ex:
            traceback.print_exc()

    def on_receive(self, message: Event) -> Any:

        if message.inlet == "Request":
            self.transmit(message.value)


def factor(fn):

    endpoint = "127.0.0.1:5021"

    FactorActor.start(endpoint, fn)
