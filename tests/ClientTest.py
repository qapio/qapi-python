import uuid
from typing import Any
import json
from pykka import ThreadingActor
from reactivex import  operators
from src.qapi_python.client import Qapi


endpoint = "127.0.0.1:5021"


qapi = Qapi.QapioGrpcInstance(endpoint)


#qapi.source("Source.Tick(1000)").subscribe(lambda x: print(x))

a = "MyScreen1.Operators.Generate(10240000, 5000)"
#qapi.source(f"Source.Single({{Guid: '{uuid.uuid4()}', Dates: ['2020-01-01','2020-01-02','2020-01-03','2020-01-04','2020-01-05','2020-01-06','2020-01-07','2020-01-01','2020-01-08','2020-01-09','2020-01-10']}}).Via(Universe11.LoadUniverse().Pack())").subscribe(lambda x: print(len(json.dumps(x))))
#qapi.source("Universe11.LoadUniverse2(100)").subscribe(lambda x: print(len(json.dumps(x))))


def count(acc, i):
    return acc+len(i)/1000000

qapi.source(a).pipe(operators.scan(count, 0)).subscribe(lambda x: print(x))
# sink = qapi.sink("ddd")
#
# def transmit(c):
#     data = []
#     for i in range(0, 10000):
#         print(i)
#         sink.on_next(c)
#         #data.append(c)
#     #sink.on_next(data)
#
#
#
#
# qapi.close()
