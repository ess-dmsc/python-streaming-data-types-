from streaming_data_types.fbschemas.timestamps_tdct.timestamp import (
    timestamp,
    timestampStart,
    timestampAddName,
    timestampAddTimestamps,
    timestampAddSequenceCounter,
    timestampEnd,
)
import flatbuffers
import numpy as np
from collections import namedtuple
from typing import Optional, Union, List
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"tdct"


def serialise_tdct(
    name: str,
    timestamps: Union[np.ndarray, List],
    sequence_counter: Optional[int] = None,
) -> bytes:
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    timestamps = np.atleast_1d(np.array(timestamps)).astype(np.uint64)

    name_offset = builder.CreateString(name)

    array_offset = builder.CreateNumpyVector(timestamps)

    timestampStart(builder)
    timestampAddName(builder, name_offset)
    timestampAddTimestamps(builder, array_offset)
    if sequence_counter is not None:
        timestampAddSequenceCounter(builder, sequence_counter)
    timestamps_message = timestampEnd(builder)

    builder.Finish(timestamps_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


Timestamps = namedtuple("Timestamps", ("name", "timestamps", "sequence_counter"))


def deserialise_tdct(buffer: Union[bytearray, bytes]) -> Timestamps:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    timestamps = timestamp.GetRootAstimestamp(buffer, 0)
    name = timestamps.Name() if timestamps.Name() else b""

    timestamps_array = timestamps.TimestampsAsNumpy()

    return Timestamps(name.decode(), timestamps_array, timestamps.SequenceCounter())
