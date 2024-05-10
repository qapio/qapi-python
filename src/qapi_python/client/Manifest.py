import json
import os

INPUT_STREAMS = 'inputStreams'
OUTPUT_STREAMS = 'outputStreams'
GRAPH_ID = 'processId'
REQUEST_ID = 'requestId'


class Manifest:

    def __init__(self, raw):
        self.__raw = raw

    @property
    def graph_id(self):
        return self.__raw[GRAPH_ID]

    @property
    def request_id(self):
        return self.__raw[REQUEST_ID]

    @property
    def input_streams(self):
        return self.__raw[INPUT_STREAMS]

    @property
    def output_streams(self):
        return self.__raw[OUTPUT_STREAMS]


def load_qapio_manifest():
    # Receive the manifest for the standard input
    # This is an instance of "PythonQapioManifest"
    # serialized to JSON.
    # Qapio provides the manifest through the standard
    # input, binary serialized.

    # The first bytes contain the length
    # of the manifest
    manifest_length = int.from_bytes(
        os.read(0, 4),
        "little"
    )

    # The next "manifest_length" bytes contain the
    # UTF-8 encoded JSON representation of the
    # manifest.
    manifest = json.loads(
        os.read(0, manifest_length).decode('utf8')
    )

    #print("loaded python manifest", manifest)

    return manifest
