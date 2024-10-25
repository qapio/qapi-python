
from qapi_python.client import Qapi
import time

endpoint = "localhost:5021"


qapi = Qapi.QapioGrpcInstance(endpoint)




a = qapi.source("Source.Tick(1000)").subscribe(lambda x: print(x))


try:
    while True:
        time.sleep(1)  # Keep the program running and alive
except KeyboardInterrupt:
    print("Program terminated.")