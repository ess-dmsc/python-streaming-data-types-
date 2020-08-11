from streaming_data_types.fbschemas.timestamps_tdct.timestamp import (
    timestamp,
    timestampStart,
    timestampAddName,
    timestampAddTimestamps,
    timestampAddSequenceCounter,
    timestampStartTimestampsVector,
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
    builder = flatbuffers.Builder(136)

    timestamps = np.atleast_1d(np.array(timestamps)).astype(np.uint64)

    name_offset = builder.CreateString(name)

    timestampStartTimestampsVector(builder, len(timestamps))
    for single_value in reversed(timestamps):
        builder.PrependUint64(single_value)
    array_offset = builder.EndVector(len(timestamps))

    timestampStart(builder)
    timestampAddName(builder, name_offset)
    timestampAddTimestamps(builder, array_offset)
    if sequence_counter is not None:
        timestampAddSequenceCounter(builder, sequence_counter)
    timestamps_message = timestampEnd(builder)
    builder.Finish(timestamps_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)


Timestamps = namedtuple("Timestamps", ("name", "timestamps", "sequence_counter",),)


def deserialise_tdct(buffer: Union[bytearray, bytes]) -> Timestamps:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    timestamps = timestamp.GetRootAstimestamp(buffer, 0)
    name = timestamps.Name() if timestamps.Name() else b""

    timestamps_array = timestamps.TimestampsAsNumpy()

    return Timestamps(name.decode(), timestamps_array, timestamps.SequenceCounter(),)
