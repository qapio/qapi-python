from google.protobuf.any_pb2 import Any

import grpc
import reactivex as rx
from reactivex import operators as op, Observable
import json
import base64
import msgpack
from typing import Iterable
from math import ceil
from collections import namedtuple
import uuid
from .Scheduler import scheduler
import os
from . import qapi_pb2 as qapi_pb2
from . import qapi_pb2_grpc as qapi_pb2_grpc


class Manifest:

    def __init__(self, raw):
        self.__raw = raw

    def inlet(self, name: str):
        return self.__raw["inlets"][name]

    def outlet(self, name: str):
        return self.__raw["outlets"][name]


def assemble_chunks(collected_chunks) -> object:
    buffer_size = sum(len(bytes(chunk)) for chunk in collected_chunks)
    bytes_data = bytearray(buffer_size)
    cur_index = 0
    for chunk in collected_chunks:
        b = chunk
        bytes_data[cur_index:cur_index + len(b)] = b
        cur_index += len(b)

    bytes_data = bytes_data[:cur_index]

    return json.loads(bytes_data.decode("utf-8"))



def create_chunks(msg, chunk_size) -> Iterable:
    bytes_data = msgpack.packb(msg)

    if len(bytes_data) <= chunk_size:
        chunked_message = {'Bytes': bytes_data, 'FirstChunk': True, 'LastChunk': True}
        yield chunked_message
    else:
        chunk_count = ceil(len(bytes_data) / chunk_size)
        first = True
        for i in range(chunk_count):
            is_last = i == chunk_count - 1
            next_chunk = min(chunk_size, len(bytes_data) - i * chunk_size)
            chunked_message = {'Bytes': bytes_data[i * chunk_size : i * chunk_size + next_chunk], 'FirstChunk': first, 'LastChunk': is_last}
            first = False
            yield chunked_message

def concat_map(os):

    def partition(acc, i):

        if i["FirstChunk"] is True and i["LastChunk"] is True:
            return ([], [i])

        acc[0].append(i)

        if i["FirstChunk"] is True:
            return (acc[0], None)

        if i["LastChunk"] is True:
            s = [a for a in acc[0]]
            return ([], s)

        return acc

    return os.pipe(
        op.map(lambda x: json.loads(x.bytes[0].value[4:])),
        op.scan(partition, ([], None)),
        op.filter(lambda x: x[1] is not None),
        op.map(lambda x: assemble_chunks([base64.b64decode(y['Bytes']) for y in x[1]])),
        op.subscribe_on(scheduler)

    )

def custom_encoder(x):
    if isinstance(x, bytes):
        return base64.b64encode(x).decode('utf-8')
    else:
        raise TypeError

class Transmitter:
    def __init__(self, expression, stub, session_id):
        self.__stub = stub
        self.__expression = expression
        self.__session_id = session_id

    def to_payload(self, value) -> Iterable[qapi_pb2.Chunk]:
        chunks = create_chunks(value, 64000)
        for chunk in chunks:
            v = json.dumps(chunk, default=custom_encoder)
            c = qapi_pb2.Chunk(expression=self.__expression, bytes=[Any(value=bytes(v, 'utf-8'))])
            yield c

    def on_next(self, value):
        self.__stub.Sink(self.to_payload(value), metadata=[('session_id', self.__session_id)])

class QapioGrpcInstance:

    def __init__(self, endpoint: str):
        self.__manifest = None
        self.__session_id = str(uuid.uuid4())
        self.__channel = channel = grpc.insecure_channel(endpoint)

        self.__stub = qapi_pb2_grpc.QapiStub(channel)

    def sink(self, expression: str):
        return Transmitter(expression, self.__stub, self.__session_id)

    def close(self):
        self.__channel.close()

    def source(self, expression: str) -> Observable:
        args = qapi_pb2.SourceRequest(expression=expression)
        return concat_map(rx.from_iterable(self.__stub.Source(args, metadata=[('session_id', self.__session_id)])))

    def get_manifest(self) -> Manifest:

        if self.__manifest is not None:
            return self.__manifest

        manifest_length = int.from_bytes(
            os.read(0, 4),
            "little"
        )

        manifest = json.loads(os.read(0, manifest_length).decode('utf8'))

        self.__manifest = Manifest(manifest)

        return self.__manifest
