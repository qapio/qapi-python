import json
from pandas import Timestamp
from qapi_python.client import QapiHttp

class DatasetBuilder:
    def __init__(self, client):
        self.__items = []
        self.__client = client

    def AddTimeSeries(self, key, endpoint, bucket, measurements, fields, from_date, to_date):

        if isinstance(from_date, str):
            from_date = Timestamp(from_date, tz="utc")

        if isinstance(to_date, str):
            to_date = Timestamp(to_date, tz="utc")

        self.__items.append({'key': key, 'expression': f"{endpoint}({json.dumps({'measurements': measurements, 'fields': fields, 'fromDate': from_date.strftime('%Y-%m-%dT%H:%M:%SZ'), 'toDate': to_date.strftime('%Y-%m-%dT%H:%M:%SZ'), 'bucket': bucket})})"})

    def GetExpression(self):
        queries = [item['expression'] for item in self.__items]
        return f"Source.CombineFirst({','.join(queries)})"

    def Load(self):
        return self.__client.query(self.GetExpression())

builder = DatasetBuilder(QapiHttp.Qapi("http://localhost:2020"))

builder.AddTimeSeries("A", "KfSql.Dataset", "ff", ["A", "B", "C"], ["O", "H", "L", "C"], "2020-01-01", "2028-01-01")
builder.AddTimeSeries("B", "KfSql.Dataset", "ffff", ["A", "B", "C"], ["O", "H", "L", "C"], "2020-01-01", "2028-01-01")

print(builder.Load())