from typing import Any

import pykka

from qapi_python.client.Qapi import QapioGrpcInstance


class Source(pykka.ThreadingActor):
    def __init__(self, expression: str, client: QapioGrpcInstance, actor_ref: pykka.ActorRef, *_args: Any, **_kwargs: Any):
        super().__init__(*_args, **_kwargs)
        self.__client = client
        print("JEE!")
        client.source(expression).subscribe(lambda x: actor_ref.tell(x))


