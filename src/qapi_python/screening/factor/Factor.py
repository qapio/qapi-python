from typing import Any, List, Union
import json
import inspect
import traceback
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
from pandas import Timestamp


class Endpoint:
    def __init__(self, node_id: str, qapi):
        self.__node_id = node_id
        self.__qapi = qapi

    def query(self, api: str, args: dict({}) = dict({})):
        pass

    def time_series(self, bucket: str, measurements: List[str], fields: List[str], from_date: Union[Timestamp, str],
                    to_date: Union[Timestamp, str], tags: dict = dict({})):
        return self.__qapi.first(f"{self.__node_id}.{bucket}({{measurements: {json.dumps(measurements)}, fields: {json.dumps(fields)}, from_date: '{from_date}', to_date: '{to_date}' }})")
        # if type(from_date) == Timestamp:
        #     from_date = timestamp2str(from_date)
        #
        # if type(to_date) == Timestamp:
        #     to_date = timestamp2str(to_date)
        #
        # data =self.__client.query(f"{self.__node_id}.TimeSeries({{ bucket: '{bucket}', measurements: {json.dumps(measurements)}, fields: {json.dumps(fields)}, fromDate: '{from_date}', toDate: '{to_date}' }})")
        #
        # if "Data" not in data:
        #     return
        #
        # data =  json.loads(data["Data"])
        #
        # df = DataFrame(data[1:], columns=data[0])
        #
        # df_unstacked = melt(df, id_vars=['_measurement', "_time"], value_vars=fields, var_name='_field',
        #                     value_name='_value')
        #
        # df_unstacked['_time'] = to_datetime(df_unstacked['_time']).dt.tz_localize(UTC)
        #
        # ds = DataSet(df_unstacked)
        #
        # return ds


class Context:
    def __init__(self, qapi, dates, members):
        self.__qapi = qapi
        self.dates = list(dates.keys())
        self.members = members

    def first(self, expression: str):
        return self.__qapi.first(expression)

    def endpoint(self, endpoint: str):
        return Endpoint(endpoint, self.__qapi)


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
        return [{"Measurement": self.measurement, "Time": self.date.strftime("%Y-%m-%dT%H:%M:%SZ"), "Fields": {self.field: self.value}, "Tags": {

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
                all_members = all_members + [o.get("Measurement") for o in value]

            context = Context(self, dates, list(set(all_members)))

            self.__instance.begin(context)

            for date, universe in dates.items():
                dr = []

                for member in universe:
                    result = FactorResult(universeId, Member(member["Measurement"], member["Meta"]), date, factor)
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
