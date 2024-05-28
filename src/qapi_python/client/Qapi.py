from google.protobuf.any_pb2 import Any

import grpc
import reactivex as rx
from reactivex import operators as op, Observable
import json
import base64
from typing import Iterable
from math import ceil
from collections import namedtuple
import uuid
from .Scheduler import scheduler
import os
from . import qapi_pb2 as qapi_pb2
from . import qapi_pb2_grpc as qapi_pb2_grpc
from reactivex.scheduler import ThreadPoolScheduler, EventLoopScheduler, ImmediateScheduler

import reactivex
from reactivex import operators as ops
import threading


class Manifest:

    def __init__(self, raw):
        self.__raw = raw

    def inlet(self, name: str):
        return self.__raw["inlets"][name]

    def outlet(self, name: str):
        return self.__raw["outlets"][name]

    def id(self):
        return self.__raw["id"]

    def deadline(self):
        return self.__raw["deadline"]


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
    v = json.dumps(msg, default=custom_encoder)
    bt = bytes(v, 'utf-8')


    # bytes_data = msgpack.packb(msg)

    if len(bt) <= chunk_size:
        chunked_message = {'Bytes': bt, 'FirstChunk': True, 'LastChunk': True}
        yield chunked_message
    else:
        chunk_count = ceil(len(bt) / chunk_size)
        first = True
        for i in range(chunk_count):
            is_last = i == chunk_count - 1
            next_chunk = min(chunk_size, len(bt) - i * chunk_size)
            chunked_message = {'Bytes': bt[i * chunk_size : i * chunk_size + next_chunk], 'FirstChunk': first, 'LastChunk': is_last}
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
        op.map(lambda x: assemble_chunks([base64.b64decode(y['Bytes']) for y in x[1]]))
    )

def custom_encoder(x):
    if isinstance(x, bytes):
        return base64.b64encode(x).decode('utf-8')
    else:
        raise TypeError

class Transmitter:
    def __init__(self, expression, stub, session_id, principal_id, deadline):
        self.__stub = stub
        self.__deadline = deadline
        self.__expression = expression
        self.__session_id = session_id
        self.__principal_id = principal_id

    def to_payload(self, value) -> Iterable[qapi_pb2.Chunk]:
        chunks = create_chunks(value, 32000)
        for chunk in chunks:
            v = json.dumps(chunk, default=custom_encoder)
            payload_bytes = bytes(v, 'utf-8')
            payload_length = len(payload_bytes).to_bytes(4, byteorder='big')

            #return [payload_length + payload_bytes]
            data = payload_length + payload_bytes
            c = qapi_pb2.Chunk(expression=self.__expression, bytes=[Any(value=data)])
            yield c

    def on_next(self, value):
        self.__stub.Sink(self.to_payload(value), metadata=[('session_id', self.__session_id), ('principal_id', self.__principal_id)], timeout=self.__deadline)

class QapioGrpcInstance:

    def __init__(self, endpoint: str):
        self.__manifest = None
        self.__session_id = str(uuid.uuid4())
        self.__principal_id = self.__session_id
        self.__channel = channel = grpc.insecure_channel(endpoint)

        self.__stub = qapi_pb2_grpc.QapiStub(channel)

    def sink(self, expression: str):
        timeout = None

        if self.__manifest is not None:
            timeout = self.__manifest.deadline()

        return Transmitter(expression, self.__stub, self.__session_id, self.__principal_id, timeout)

    def close(self):
        self.__channel.close()

    def source(self, expression: str, s=None) -> Observable:
        args = qapi_pb2.SourceRequest(expression=expression)
        timeout = None

        if s is None:
            s = scheduler
        if self.__manifest is not None:
            timeout = self.__manifest.deadline()

        return concat_map(rx.from_iterable(self.__stub.Source(args, metadata=[('session_id', self.__session_id), ('principal_id', self.__principal_id)], timeout=timeout))).pipe(op.subscribe_on(s))

    def first(self, expression: str):
        return self.source(expression+".Take(1)", EventLoopScheduler()).run()

    def get_manifest(self) -> Manifest:

        if self.__manifest is not None:
            return self.__manifest

        manifest_length = int.from_bytes(
            os.read(0, 4),
            "little"
        )

        manifest = json.loads(os.read(0, manifest_length).decode('utf8'))

        self.__manifest = Manifest(manifest)
        self.__principal_id = self.__manifest.id()

        return self.__manifest
