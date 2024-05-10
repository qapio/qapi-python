from typing import Any

from pykka import ThreadingActor

from qapi_python.client import Qapi
from qapi_python.actors import Qapi as QapiActor


endpoint = "127.0.0.1:5021"


#qapi = Qapi.QapioGrpcInstance(endpoint)



class FlowActor(ThreadingActor):
    def __init__(self, qapi, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        qapi.proxy().source("FlightData_Ui.Files.ReadAllText('Index.json').Take(1)", self.actor_ref)
        self.__sink = qapi.proxy().sink("ddd").get().proxy()

    def transmit(self, value):
        for i in range(0, 100):
            self.__sink.on_next(value)

    def on_receive(self, message: Any) -> Any:
        self.transmit(message)

qapi2 = QapiActor.Qapi.start(endpoint)
FlowActor.start(qapi2)
#qapi.source("Source.Tick(1000)").subscribe(lambda x: print(assemble_chunks(x)))


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
# qapi.source("FlightData_Ui.Files.ReadAllText('Index.json').Take(1)").subscribe(lambda x: transmit(x))
#
# qapi.close()
