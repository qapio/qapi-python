import json
import requests
import reactivex
from reactivex import operators, Observable
from dataclasses import dataclass
import base64
from typing import List, Optional, Any, Tuple
from box import Box

def assemble_chunks(collected_chunks: List[bytes]) -> Any:
    # Calculate total buffer size
    buffer_size = sum(len(chunk) for chunk in collected_chunks)

    # Create a bytearray to store assembled data
    bytes_data = bytearray(buffer_size)

    # Fill bytearray with chunk data
    cur_index = 0
    for chunk in collected_chunks:
        bytes_data[cur_index:cur_index + len(chunk)] = chunk
        cur_index += len(chunk)

    # Convert to UTF-8 string and parse as JSON
    decoded_string = bytes_data.decode("utf-8")
    data = json.loads(decoded_string)

    if isinstance(data, dict):
        return Box(data)

    return data


def assembler(chunk: Observable) -> Observable:
    def partition(acc: Tuple[List[ChunkDto], Optional[List[ChunkDto]]], i: ChunkDto):
        if i['firstChunk'] and i['lastChunk']:
            return [], [i]

        acc[0].append(i)

        if i['firstChunk']:
            return acc[0], None

        if i['lastChunk']:
            collected = acc[0][:]
            return [], collected

        return acc

    return chunk.pipe(
        operators.map(lambda x: x.split("\n")[2]),
        operators.map(lambda t: json.loads(t.replace("data: ", ""))),
        operators.scan(partition, ([], None)),
        operators.filter(lambda x: x[1] is not None),
        operators.map(lambda x: assemble_chunks([base64.b64decode(y['bytes']) for y in x[1]]))
    )

@dataclass
class ChunkDto:
    bytes: str
    firstChunk: bool
    lastChunk: bool


class Qapi:
    def __init__(self, url: str):
        self.__url = url

    def query(self, expression: str):
        data = requests.get(f"{self.__url}/query/{expression}").json()

        if isinstance(data, dict):
            return Box(data)

        return data

    def source(self, expression: str):

        session = requests.Session()

        def partition(acc, msg):


            acc[0].append(msg)

            if msg == "":
                return [[], "\n".join(acc[0])]
            else:
                return [acc[0], None]

        def prints(p):
            print(p)
            return p

        return reactivex.from_iterable(session.get(
            f"{self.__url}/source/{expression}",
            verify=False, stream=True
        ).iter_lines(decode_unicode=True)).pipe(
            operators.scan(partition, ([], None)),
            operators.filter(lambda x: x[1] is not None),
            operators.map(lambda t: t[1]),
            assembler
        )
