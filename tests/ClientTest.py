
from reactivex import operators, interval
from qapi_python.client import Qapi

import time

# class QapiHttpClient:
#     def __init__(self, url: str):
#         self.__url = url
#
#     def query(self, expression: str):
#         return requests.get(f"{self.__url}/query/{expression}", verify=False).json()["result"]
#
#     def source(self, expression: str):
#
#         session = requests.Session()
#
#         return reactivex.from_iterable(session.get(
#             f"{self.__url}/source/{expression}",
#             verify=False, stream=True
#         ).iter_lines(decode_unicode=True))
#
# def get_first_value(observable):
#     # Create a container to store the result
#     result = []
#     event = threading.Event()
#
#     def on_next(value):
#         result.append(value)
#         event.set()  # Signal that the result is ready
#
#     def on_error(error):
#         result.append(error)
#         event.set()  # Signal that an error occurred
#
#     def on_completed():
#         event.set()  # Signal that the observable is completed
#
#     # Subscribe to the observable with the defined callbacks
#     observable.pipe(ops.first()).subscribe(
#         on_next=on_next,
#         on_error=on_error,
#         on_completed=on_completed
#     )
#
#     # Block until the event is set
#     event.wait()
#
#     # Return the first value or raise an error if occurred
#     if isinstance(result[0], Exception):
#         raise result[0]
#     return result[0]
#
# # Example usage
# if __name__ == "__main__":
#     source = reactivex.from_([1, 2, 3, 4, 5])
#     first_value = get_first_value(source)
#     print(f"The first value is: {first_value}")

endpoint = "localhost:5021"


qapi = Qapi.QapioGrpcInstance(endpoint)
#qapi2 = QapiHttpClient("http://localhost:2020")

# node_id = "Source"
# measurements = [random.random() for o in range(0, 200)]
# fields = ["B"]
# from_date="2020-01-01"
# to_date = "2024-01-02"
# g = f"Source.Single({{measurements: {json.dumps(measurements)}, fields: {json.dumps(fields)}, from_date: '{from_date}', to_date:'{to_date}' }}).Via(FACTSETSQL.prices({{}}))"
# gk = "FACTSETSQL.Operators.Generate(10000000)"
# fk = "Screen_Ui.Files.ReadAllText('Tdip.Qapio.Service.Ui.Core.db')"
# n = 0



a = qapi.source("Source.Tick(1000)").pipe(operators.take(1), operators.compose(operators.map(lambda x: qapi.source("Source.Tick(1500)")), operators.switch_latest())).subscribe(lambda x: print(x))


try:
    while True:
        time.sleep(1)  # Keep the program running and alive
except KeyboardInterrupt:
    print("Program terminated.")