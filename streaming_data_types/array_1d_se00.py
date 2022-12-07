from typing import List, NamedTuple, Optional, Union

import flatbuffers
import numpy as np
from flatbuffers.number_types import (
    Float32Flags,
    Float64Flags,
    Int8Flags,
    Int16Flags,
    Int32Flags,
    Int64Flags,
    Uint8Flags,
    Uint16Flags,
    Uint32Flags,
    Uint64Flags,
)

from streaming_data_types.fbschemas.array_1d_se00.Location import Location
from streaming_data_types.fbschemas.array_1d_se00.se00_SampleEnvironmentData import (
    se00_SampleEnvironmentData,
    se00_SampleEnvironmentDataAddChannel,
    se00_SampleEnvironmentDataAddMessageCounter,
    se00_SampleEnvironmentDataAddName,
    se00_SampleEnvironmentDataAddPacketTimestamp,
    se00_SampleEnvironmentDataAddTimeDelta,
    se00_SampleEnvironmentDataAddTimestampLocation,
    se00_SampleEnvironmentDataAddTimestamps,
    se00_SampleEnvironmentDataAddValues,
    se00_SampleEnvironmentDataAddValuesType,
    se00_SampleEnvironmentDataEnd,
    se00_SampleEnvironmentDataStart,
)
from streaming_data_types.fbschemas.array_1d_se00.ValueUnion import ValueUnion
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"se00"

flag_map = {
    ValueUnion.Int8Array: Int8Flags,
    ValueUnion.UInt8Array: Uint8Flags,
    ValueUnion.Int16Array: Int16Flags,
    ValueUnion.UInt16Array: Uint16Flags,
    ValueUnion.Int32Array: Int32Flags,
    ValueUnion.UInt32Array: Uint32Flags,
    ValueUnion.Int64Array: Int64Flags,
    ValueUnion.UInt64Array: Uint64Flags,
    ValueUnion.DoubleArray: Float64Flags,
    ValueUnion.FloatArray: Float32Flags,
}

numpy_type_map = {
    np.dtype("int8"): ValueUnion.Int8Array,
    np.dtype("uint8"): ValueUnion.UInt8Array,
    np.dtype("int16"): ValueUnion.Int16Array,
    np.dtype("uint16"): ValueUnion.UInt16Array,
    np.dtype("int32"): ValueUnion.Int32Array,
    np.dtype("uint32"): ValueUnion.UInt32Array,
    np.dtype("int64"): ValueUnion.Int64Array,
    np.dtype("uint64"): ValueUnion.UInt64Array,
    np.dtype("float64"): ValueUnion.DoubleArray,
    np.dtype("float32"): ValueUnion.FloatArray,
}

Response = NamedTuple(
    "SampleEnvironmentData",
    (
        ("name", str),
        ("channel", int),
        ("timestamp_unix_ns", int),
        ("sample_ts_delta", int),
        ("ts_location", Location),
        ("message_counter", int),
        ("values", np.ndarray),
        ("value_ts", Optional[np.ndarray]),
    ),
)


def serialise_se00(
    name: str,
    channel: int,
    timestamp_unix_ns: int,
    sample_ts_delta: int,
    message_counter: int,
    values: Union[np.ndarray, List],
    ts_location: Location = Location.Middle,
    value_timestamps: Union[np.ndarray, List, None] = None,
) -> bytes:
    builder = flatbuffers.Builder(1024)

    if value_timestamps is not None:
        used_timestamps = np.atleast_1d(np.asarray(value_timestamps)).astype(np.uint64)
        timestamps_offset = builder.CreateNumpyVector(used_timestamps)

    temp_values = np.atleast_1d(np.asarray(values))

    value_array_offset = builder.CreateNumpyVector(temp_values)

    # Some flatbuffer fu in order to avoid >200 lines of code
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(value_array_offset), 0
    )
    value_offset = builder.EndObject()

    name_offset = builder.CreateString(name)

    se00_SampleEnvironmentDataStart(builder)
    se00_SampleEnvironmentDataAddName(builder, name_offset)
    se00_SampleEnvironmentDataAddTimeDelta(builder, sample_ts_delta)
    se00_SampleEnvironmentDataAddTimestampLocation(builder, ts_location)
    se00_SampleEnvironmentDataAddMessageCounter(builder, message_counter)
    se00_SampleEnvironmentDataAddChannel(builder, channel)
    se00_SampleEnvironmentDataAddPacketTimestamp(builder, timestamp_unix_ns)
    se00_SampleEnvironmentDataAddValues(builder, value_offset)
    se00_SampleEnvironmentDataAddValuesType(builder, numpy_type_map[temp_values.dtype])
    if value_timestamps is not None:
        se00_SampleEnvironmentDataAddTimestamps(builder, timestamps_offset)

    SE_Message = se00_SampleEnvironmentDataEnd(builder)

    builder.Finish(SE_Message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


def deserialise_se00(buffer: Union[bytearray, bytes]) -> Response:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    SE_data = se00_SampleEnvironmentData.GetRootAsse00_SampleEnvironmentData(buffer, 0)

    value_timestamps = None
    if not SE_data.TimestampsIsNone():
        value_timestamps = SE_data.TimestampsAsNumpy()

    # Some flatbuffers fu in order to avoid >200 lines of code
    value_offset = SE_data.Values()
    value_type = SE_data.ValuesType()
    values = value_offset.GetVectorAsNumpy(flag_map[value_type], 4)

    return Response(
        name=SE_data.Name().decode(),
        channel=SE_data.Channel(),
        timestamp_unix_ns=SE_data.PacketTimestamp(),
        sample_ts_delta=SE_data.TimeDelta(),
        ts_location=SE_data.TimestampLocation(),
        message_counter=SE_data.MessageCounter(),
        values=values,
        value_ts=value_timestamps,
    )
