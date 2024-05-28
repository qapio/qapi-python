from typing import Any, List
import json
import inspect
import traceback
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
from pandas import Timestamp


class Context:
    def __init__(self):
        pass


class Member:
    def __init__(self, measurement: str, meta: dict):
        self.measurement = measurement
        self.meta = meta


class FactorResult:
    def __init__(self, universe_id: str, member: Member, date: Timestamp, field: str):
        self.__universe_id = universe_id
        self.measurement = member.measurement
        self.meta = member.meta
        self.date = date
        self.field = field
        self.value = None
        self.__results = []

    def set_value(self, value):
        self.value = value

    def results(self):
        return [{"measurement": self.measurement, "time": self.date.strftime("%Y-%m-%dT%H:%M:%SZ"), "fields": {self.field: self.value}, "tags": {

        }}]


class FactorActor(QapiActor.Qapi):
    def __init__(self, endpoint, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint, *_args, **_kwargs)
        self.__instance = func()

        self.subscribe("Request")
        self.__sink = None

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

            context = Context()

            for date, universe in dates.items():
                dr = []

                for member in universe:
                    result = FactorResult(universeId, member, date, factor)
                    self.__instance.formula(result, context)
                    for r in result.results():
                        dr.append(r)

                results[date.strftime("%Y-%m-%dT%H:%M:%SZ")] = dr

            if self.__sink is None:
                self.__sink = self.get_subject("Response")

            self.__sink.on_next({'Guid': guid, 'Data': results})

        except Exception as ex:
            traceback.print_exc()

    def on_receive(self, message: Event) -> Any:

        if message.inlet == "Request":
            self.transmit(message.value)


def factor(fn):

    endpoint = "127.0.0.1:5021"

    FactorActor.start(endpoint, fn)
