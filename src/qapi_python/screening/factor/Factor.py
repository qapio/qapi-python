from typing import Any, List, Union
import json
import inspect
import traceback
from qapi_python.actors import Qapi as QapiActor
from qapi_python.actors.Source import Event
from pandas import Timestamp, DataFrame, to_datetime, melt, Series, MultiIndex, NA
from pytz import UTC
from numpy import finfo, float32, nan
from pandas.api.types import is_numeric_dtype


def timestamp2str(date: Timestamp):
    return date.strftime('%Y-%m-%dT%H:%M:%SZ')


class DataSet:
    def __init__(self, data_frame):

        self.__series = dict({})
        if data_frame is None:
            return

        if data_frame.empty:
            return
        nonCatCols = ["_time", "_value"]

        for col in data_frame.columns:
            if col not in nonCatCols:
                data_frame[col] = data_frame[col].astype("category")

        #print(data_frame.dtypes)
        # data_frame["_time"] = data_frame["_time"].map(lambda x: x.tz_convert('UTC'))

        try:
            response = data_frame.set_index(MultiIndex.from_frame(data_frame[[
                "_measurement", "_field", "_time",
            ]   ], names=["_measurement", "_field", "_time", ]))


            #response = response.tz_localize('UTC', level=1)

            response = response.sort_index()
            a = finfo(float32).min
            response = response.replace(to_replace=a,
                                        value=nan)
            self.__data_frame = response

        except Exception as e:
            print(e)


    @property
    def data_frame(self):
        return self.__data_frame

    def series(self, measurement: str, field: str,
               from_date: Timestamp =
               None, to_date: Timestamp = None) -> Series:


        try:
            # series = self.__data_frame[
            #     (self.__data_frame._measurement == measurement) & (self.__data_frame._field == field)]

            if measurement+field not in self.__series:

                colNames = list(self.__data_frame)

                if field in colNames:
                    data = self.__data_frame.loc[measurement, :, : ]
                    series = Series(data=data[field].values, index=data["_time"].values)
                    series = series.tz_localize('UTC', level=0)
                    self.__series[measurement+field] = series
                else:
                    data = self.__data_frame.loc[measurement, field, : ]

                    series = Series(data=data["_value"].values, index=data["_time"].values)
                    series = series.tz_localize('UTC', level=0)
                    self.__series[measurement+field] = series

            series = self.__series.get(measurement+field)
            series = series.mask(series.apply(lambda x: isinstance(x, dict)), NA)
            series = series.dropna()

            #series = Series(data=series["_value"].values, index=series["_time"].values)
            #series = series.tz_localize('UTC', level=0)

            if from_date is None and to_date is None:
                series = series

            if from_date is not None and to_date is not None:
                series = series.loc[from_date:to_date]

            if from_date is not None and to_date is None:
                series = series.loc[from_date:]

            if from_date is None and to_date is not None:
                series = series.loc[:to_date]

            if series is None:
                return None

            if len(series.index) == 0:
                return None

            if is_numeric_dtype(series):
                return series[series < 3.4e38]

            #series = series.mask(df.apply(lambda x: isinstance(x, dict)), NA)
            return series.dropna()
        except:
            return None


    def point_series(self, measurements: List[str], field: str,
                     date: Timestamp):

        data = []

        for ticker in measurements:
            p = self.point(
                ticker,
                field,
                date)

            if p is None:
                p = nan

            data.append(p)

        return Series(data, index=measurements)

    def point(self, measurement: str, field: str, date: Timestamp):
        series = self.series(measurement, field, date, date)

        if series is None:
            return None

        try:
            point = series[date]
            if isinstance(point, Series):
                print("Duplicate found.")
                print(measurement)
                print(field)
                return None
            return point
        except:
            return None

    def last(self, measurement: str, field: str, date: Timestamp):
        series = self.series(measurement, field, None, date)

        #print(series, flush=True)

        if series is None or len(series.tail(1).keys()) == 0:
            return None

        try:
            last = series.tail(1)[0]

            if isinstance(last, Series):
                print("Duplicate found.")
                print(measurement)
                print(field)
                return None

            return last
        except Exception as e:
            return None


class Endpoint:
    def __init__(self, node_id: str, qapi):
        self.__node_id = node_id
        self.__qapi = qapi

    def query(self, api: str, args: dict({}) = dict({})):
        pass

    def time_series(self, bucket: str, measurements: List[str], fields: List[str], from_date: Union[Timestamp, str],
                    to_date: Union[Timestamp, str], tags: dict = dict({})):

        data = self.__qapi.first(f"{self.__node_id}.CompositeSource(Source.Single({{measurements: {json.dumps(measurements)}, fields: {json.dumps(fields)}, from_date: '{from_date}', to_date: '{to_date}' }}).Via({self.__node_id}.{bucket}()))")["result"]

        df = DataFrame(data[1:], columns=data[0])

        df_unstacked = melt(df, id_vars=['_measurement', "_time"], value_vars=fields, var_name='_field',
                            value_name='_value')

        df_unstacked['_time'] = to_datetime(df_unstacked['_time']).dt.tz_localize(UTC)

        ds = DataSet(df_unstacked)

        return ds
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
    def __init__(self, endpoint, endpoint_http, func, *_args: Any, **_kwargs: Any):
        super().__init__(endpoint, endpoint_http, *_args, **_kwargs)
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
    endpoint_http = "http://127.0.0.1:2020"

    FactorActor.start(endpoint, endpoint_http, fn)
