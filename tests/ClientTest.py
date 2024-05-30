import random

from src.qapi_python.client import Qapi
import reactivex
from reactivex import operators as ops
import threading
import json
import time
def get_first_value(observable):
    # Create a container to store the result
    result = []
    event = threading.Event()

    def on_next(value):
        result.append(value)
        event.set()  # Signal that the result is ready

    def on_error(error):
        result.append(error)
        event.set()  # Signal that an error occurred

    def on_completed():
        event.set()  # Signal that the observable is completed

    # Subscribe to the observable with the defined callbacks
    observable.pipe(ops.first()).subscribe(
        on_next=on_next,
        on_error=on_error,
        on_completed=on_completed
    )

    # Block until the event is set
    event.wait()

    # Return the first value or raise an error if occurred
    if isinstance(result[0], Exception):
        raise result[0]
    return result[0]

# Example usage
if __name__ == "__main__":
    source = reactivex.from_([1, 2, 3, 4, 5])
    first_value = get_first_value(source)
    print(f"The first value is: {first_value}")

endpoint = "127.0.0.1:5021"


qapi = Qapi.QapioGrpcInstance(endpoint)


node_id = "Source"
measurements = [random.random() for o in range(0, 100)]
fields = ["B"]
from_date="2020-01-01"
to_date = "2024-01-01"
g = f"FACTSETSQL.CompositeSource(Source.Single({{measurements: {json.dumps(measurements)}, fields: {json.dumps(fields)}, from_date: '{from_date}', to_date: '{to_date}' }}).Via(FACTSETSQL.prices()))"

print(qapi.first(g))
print(1)

qapi.close()

#a = "Source.Operators.Generate(102400000, 1000).To(MyScreen1.Operators.Consumer())"
#qapi.source(f"Source.Single({{Guid: '{uuid.uuid4()}', Dates: ['2020-01-01','2020-01-02','2020-01-03','2020-01-04','2020-01-05','2020-01-06','2020-01-07','2020-01-01','2020-01-08','2020-01-09','2020-01-10']}}).Via(Universe11.LoadUniverse().Pack())").subscribe(lambda x: print(len(json.dumps(x))))
#qapi.source("Universe11.LoadUniverse2(10)").subscribe(lambda x: print(len(json.dumps(x))))


#def count(acc, i):
#    return acc+len(i)/1000000

#qapi.source(a).pipe(operators.scan(count, 0)).subscribe(lambda x: print(x))
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
