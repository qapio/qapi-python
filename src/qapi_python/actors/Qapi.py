from typing import Any, Union, List
import pykka
from pandas import Timestamp
from qapi_python.actors.Source import Source
from qapi_python.actors.Sink import Sink
from qapi_python.client.Qapi import QapioGrpcInstance
import requests
import reactivex


def timestamp2str(date: Timestamp):
    return date.strftime('%Y-%m-%dT%H:%M:%SZ')


class QapiHttpClient:
    def __init__(self, url: str):
        self.__url = url

    def query(self, expression: str):
        return requests.get(f"{self.__url}/query/{expression}", verify=False).json()

    def source(self, expression: str):

        session = requests.Session()

        return reactivex.from_iterable(session.get(
            f"{self.__url}/source/{expression}",
            verify=False, stream=True
        ).iter_lines(decode_unicode=True))


class Qapi(pykka.ThreadingActor):
    def __init__(self, endpoint: str, endpoint_http: str, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = QapioGrpcInstance(endpoint)
        self.__http_client = QapiHttpClient(endpoint_http)

    def on_stop(self):
        self.__client.dispose()

    def source(self, expression: str, target: pykka.ActorRef):
        return Source.start(expression, self.__client, target)

    def first(self, expression: str):
        return self.__http_client.query(expression)

    def subscribe(self, inlet: str):
        return Source.start(inlet, self.__client, self.actor_ref)

    def sink(self, expression: str):
        return Sink.start(expression, self.__client)

    def get_subject(self, name: str):
        return Sink.start(name, self.__client).proxy()
