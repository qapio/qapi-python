import json
import requests
import reactivex
from reactivex import operators, Observable
from dataclasses import dataclass
from typing import List, Optional, Any, Tuple
from reactivex import operators
from box import Box
import pickle
import base64
import pandas as pd
from pandas import Timestamp
from numpy import finfo, float32, nan
from pandas.api.types import is_numeric_dtype

class DatasetBuilder:
    def __init__(self, client):
        self.__items = []
        self.__client = client

    def AddTimeSeries(self, key, endpoint, bucket, measurements, fields, from_date, to_date):

        if isinstance(from_date, str):
            from_date = Timestamp(from_date, tz="utc")

        if isinstance(to_date, str):
            to_date = Timestamp(to_date, tz="utc")

        self.__items.append({'key': key, 'type': 'timeseries', 'expression': f"Source.Single({json.dumps({'measurements': measurements, 'fields': fields, 'fromDate': from_date.strftime('%Y-%m-%dT%H:%M:%SZ'), 'toDate': to_date.strftime('%Y-%m-%dT%H:%M:%SZ'), 'bucket': bucket})}).Via({endpoint}())"})

        return self

    def Query(self, key, expression):

        self.__items.append({'key': key, 'type': 'query', 'expression': expression})

        return self

    def GetExpression(self):
        queries = [item['expression'] for item in self.__items]
        return f"Source.CombineFirst({','.join(queries)})"

    def Load(self):
        data = self.__client.query(self.GetExpression())

        result = {}

        for idx, value in enumerate(data):
            if self.__items[idx]['type'] == "timeseries":
                value = value.TimeSeries

            result[self.__items[idx]['key']] = value

        return Box(result)

@pd.api.extensions.register_dataframe_accessor("TimeSeries")
class GeoAccessor:
    def __init__(self, pandas_obj):
        #self._validate(pandas_obj)
        self.__series = dict({})
        pandas_obj["_time"] =  pd.to_datetime(pandas_obj['_time'])
        self._obj = pandas_obj.melt(id_vars=['_time', '_measurement'],
                                    var_name='_field',
                                    value_name='_value')
        nonCatCols = ["_time", "_value"]

        for col in self._obj.columns:
            if col not in nonCatCols:
                self._obj[col] = self._obj[col].astype("category")



        try:
            response = self._obj.set_index(pd.MultiIndex.from_frame(self._obj[[
                "_measurement", "_field", "_time",
            ]   ], names=["_measurement", "_field", "_time", ]))

            #print(response, flush=True)

            #response = response.tz_localize('UTC', level=1)

            response = response.sort_index()
            a = finfo(float32).min
            response = response.replace(to_replace=a,
                                        value=nan)
            self._obj = response

        except Exception as e:
            print(e)

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        if "_measurement" not in obj.columns or "_time" not in obj.columns:
            raise AttributeError("Must have '_measurement' and '_time'.")

    def last(self, measurement: str, field: str, date: pd.Timestamp):
        series = self.series(measurement, field, None, date)

        if series is None or len(series.tail(1).keys()) == 0:
            return None

        try:
            last = series.tail(1).iloc[0]

            if isinstance(last, pd.Series):
                return None

            return last
        except Exception as e:
            return None

    def point(self, measurement: str, field: str, date: pd.Timestamp):
        series = self.series(measurement, field, date, date)

        if series is None:
            return None

        try:
            point = series[date]
            if isinstance(point, pd.Series):
                return None
            return point
        except:
            return None

    def series(self, measurement: str, field: str,
               from_date: pd.Timestamp =
               None, to_date: pd.Timestamp = None) -> pd.Series:


        try:
            # series = self.__data_frame[
            #     (self.__data_frame._measurement == measurement) & (self.__data_frame._field == field)]

            if measurement+field not in self.__series:

                colNames = list(self._obj)

                if field in colNames:
                    data = self._obj.loc[measurement, :, : ]
                    series = pd.Series(data=data[field].values, index=data["_time"].values)
                    series = series.tz_localize('UTC', level=0)
                    self.__series[measurement+field] = series
                else:
                    data = self._obj.loc[measurement, field, : ]

                    series = pd.Series(data=data["_value"].values, index=data["_time"].values)
                    series = series.tz_localize('UTC', level=0)
                    self.__series[measurement+field] = series

            series = self.__series.get(measurement+field)
            series = series.mask(series.apply(lambda x: isinstance(x, dict)), pd.NA)
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
        except Exception as e:
            #print(e)
            return None
    def plot(self):
        # plot this array's data on a map, e.g., using Cartopy
        pass



def assemble_chunks(collected_chunks: List[bytes]) -> Any:
    # Calculate total buffer size
    buffer_size = sum(len(chunk) for chunk in collected_chunks)

    # Create a bytearray to store assembled data
    bytes_data = bytearray(buffer_size)

    # Fill bytearray with chunk data
    cur_index = 0
    for chunk in collected_chunks:
        bytes_data[cur_index:cur_index + len(chunk)] = chunk
        cur_index += len(chunk)

    # Convert to UTF-8 string and parse as JSON
    decoded_string = bytes_data.decode("utf-8")
    data = json.loads(decoded_string)

    if isinstance(data, dict):
        return Box(data)

    return data


def assembler(chunk: Observable) -> Observable:
    def partition(acc: Tuple[List[ChunkDto], Optional[List[ChunkDto]]], i: ChunkDto):
        if i['firstChunk'] and i['lastChunk']:
            return [], [i]

        acc[0].append(i)

        if i['firstChunk']:
            return acc[0], None

        if i['lastChunk']:
            collected = acc[0][:]
            return [], collected

        return acc

    return chunk.pipe(
        operators.map(lambda x: x.split("\n")[2]),
        operators.map(lambda t: json.loads(t.replace("data: ", ""))),
        operators.scan(partition, ([], None)),
        operators.filter(lambda x: x[1] is not None),
        operators.map(lambda x: assemble_chunks([base64.b64decode(y['bytes']) for y in x[1]]))
    )

@dataclass
class ChunkDto:
    bytes: str
    firstChunk: bool
    lastChunk: bool


class Qapi:
    def __init__(self, url: str):
        self.__url = url


    def enhance(self, value):
        if isinstance(value, dict):
            if '__type' in value.keys():
                if value['__type'] == "pickle":
                    return pickle.loads(base64.b64decode(value['data']))
            else:
                return Box(value)
        return value

    def query(self, expression: str):
        data = requests.get(f"{self.__url}/query/{expression}").json()

        if isinstance(data, list):
            return [self.enhance(m) for m in data]

        if isinstance(data, dict):
            return self.enhance(data)

        return data

    def DatasetBuilder(self):
        return DatasetBuilder(self)

    def TimeSeries(self, endpoint, bucket, measurements, fields, from_date, to_date):
        data = self.DatasetBuilder().AddTimeSeries("Single", endpoint, bucket, measurements, fields, from_date, to_date).Load()
        return data.Single

    def source(self, expression: str):

        session = requests.Session()

        def partition(acc, msg):


            acc[0].append(msg)

            if msg == "":
                return [[], "\n".join(acc[0])]
            else:
                return [acc[0], None]

        def prints(p):
            print(p)
            return p

        return reactivex.from_iterable(session.get(
            f"{self.__url}/source/{expression}",
            verify=False, stream=True
        ).iter_lines(decode_unicode=True)).pipe(
            operators.scan(partition, ([], None)),
            operators.filter(lambda x: x[1] is not None),
            operators.map(lambda t: t[1]),
            assembler
        )
