from datetime import datetime, timezone
from typing import List, NamedTuple, Optional, Union

import flatbuffers
import numpy as np

from streaming_data_types.fbschemas.sample_environment_senv.Location import Location
from streaming_data_types.fbschemas.sample_environment_senv.SampleEnvironmentData import (
    SampleEnvironmentData,
    SampleEnvironmentDataAddChannel,
    SampleEnvironmentDataAddMessageCounter,
    SampleEnvironmentDataAddName,
    SampleEnvironmentDataAddPacketTimestamp,
    SampleEnvironmentDataAddTimeDelta,
    SampleEnvironmentDataAddTimestampLocation,
    SampleEnvironmentDataAddTimestamps,
    SampleEnvironmentDataAddValues,
    SampleEnvironmentDataAddValuesType,
    SampleEnvironmentDataEnd,
    SampleEnvironmentDataStart,
)
from streaming_data_types.fbschemas.sample_environment_senv.ValueUnion import ValueUnion
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"senv"


def serialise_senv(
    name: str,
    channel: int,
    timestamp: datetime,
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

    numpy_type_map = {
        np.dtype("int8"): ValueUnion.Int8Array,
        np.dtype("uint8"): ValueUnion.UInt8Array,
        np.dtype("int16"): ValueUnion.Int16Array,
        np.dtype("uint16"): ValueUnion.UInt16Array,
        np.dtype("int32"): ValueUnion.Int32Array,
        np.dtype("uint32"): ValueUnion.UInt32Array,
        np.dtype("int64"): ValueUnion.Int64Array,
        np.dtype("uint64"): ValueUnion.UInt64Array,
    }

    temp_values = np.atleast_1d(np.asarray(values))

    value_array_offset = builder.CreateNumpyVector(temp_values)

    # Some flatbuffer fu in order to avoid >200 lines of code
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(value_array_offset), 0
    )
    value_offset = builder.EndObject()

    name_offset = builder.CreateString(name)

    SampleEnvironmentDataStart(builder)
    SampleEnvironmentDataAddName(builder, name_offset)
    SampleEnvironmentDataAddTimeDelta(builder, sample_ts_delta)
    SampleEnvironmentDataAddTimestampLocation(builder, ts_location)
    SampleEnvironmentDataAddMessageCounter(builder, message_counter)
    SampleEnvironmentDataAddChannel(builder, channel)
    SampleEnvironmentDataAddPacketTimestamp(builder, int(timestamp.timestamp() * 1e9))
    SampleEnvironmentDataAddValues(builder, value_offset)
    SampleEnvironmentDataAddValuesType(builder, numpy_type_map[temp_values.dtype])
    if value_timestamps is not None:
        SampleEnvironmentDataAddTimestamps(builder, timestamps_offset)

    SE_Message = SampleEnvironmentDataEnd(builder)

    builder.Finish(SE_Message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


Response = NamedTuple(
    "SampleEnvironmentData",
    (
        ("name", str),
        ("channel", int),
        ("timestamp", datetime),
        ("sample_ts_delta", int),
        ("ts_location", Location),
        ("message_counter", int),
        ("values", np.ndarray),
        ("value_ts", Optional[np.ndarray]),
    ),
)


def deserialise_senv(buffer: Union[bytearray, bytes]) -> Response:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    SE_data = SampleEnvironmentData.GetRootAsSampleEnvironmentData(buffer, 0)

    max_time = datetime(
        year=9000, month=1, day=1, hour=0, minute=0, second=0
    ).timestamp()
    used_timestamp = SE_data.PacketTimestamp() / 1e9
    if used_timestamp > max_time:
        used_timestamp = max_time

    value_timestamps = None
    if not SE_data.TimestampsIsNone():
        value_timestamps = SE_data.TimestampsAsNumpy()

    from flatbuffers.number_types import (
        Int8Flags,
        Int16Flags,
        Int32Flags,
        Int64Flags,
        Uint8Flags,
        Uint16Flags,
        Uint32Flags,
        Uint64Flags,
    )

    flag_map = {
        ValueUnion.Int8Array: Int8Flags,
        ValueUnion.UInt8Array: Uint8Flags,
        ValueUnion.Int16Array: Int16Flags,
        ValueUnion.UInt16Array: Uint16Flags,
        ValueUnion.Int32Array: Int32Flags,
        ValueUnion.UInt32Array: Uint32Flags,
        ValueUnion.Int64Array: Int64Flags,
        ValueUnion.UInt64Array: Uint64Flags,
    }

    # Some flatbuffers fu in order to avoid >200 lines of code
    value_offset = SE_data.Values()
    value_type = SE_data.ValuesType()
    values = value_offset.GetVectorAsNumpy(flag_map[value_type], 4)

    return Response(
        name=SE_data.Name().decode(),
        channel=SE_data.Channel(),
        timestamp=datetime.fromtimestamp(used_timestamp, tz=timezone.utc),
        sample_ts_delta=SE_data.TimeDelta(),
        ts_location=SE_data.TimestampLocation(),
        message_counter=SE_data.MessageCounter(),
        values=values,
        value_ts=value_timestamps,
    )
