from streaming_data_types.fbschemas.sample_environment_senv.SampleEnvironmentData import SampleEnvironmentData, SampleEnvironmentDataStart, SampleEnvironmentDataEnd, SampleEnvironmentDataAddName, SampleEnvironmentDataAddChannel, SampleEnvironmentDataAddMessageCounter, SampleEnvironmentDataAddTimeDelta, SampleEnvironmentDataAddTimestampLocation, SampleEnvironmentDataAddValues, SampleEnvironmentDataAddTimestamps, SampleEnvironmentDataAddPacketTimestamp
from streaming_data_types.fbschemas.sample_environment_senv.Location import Location
import flatbuffers
import numpy as np
from collections import namedtuple
from typing import Optional, Union, List, NamedTuple
from streaming_data_types.utils import check_schema_identifier
from datetime import datetime

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
        used_timestamps = np.atleast_1d(np.array(value_timestamps)).astype(np.uint64)

    temp_values = np.atleast_1d(np.array(values)).astype(np.uint16)
    value_offset = builder.CreateNumpyVector(temp_values)

    name_offset = builder.CreateString(name)


    SampleEnvironmentDataStart(builder)
    SampleEnvironmentDataAddName(builder, name_offset)
    SampleEnvironmentDataAddTimeDelta(builder, sample_ts_delta)
    SampleEnvironmentDataAddTimestampLocation(builder, ts_location)
    SampleEnvironmentDataAddMessageCounter(builder, message_counter)
    SampleEnvironmentDataAddChannel(builder, channel)
    SampleEnvironmentDataAddPacketTimestamp(builder, int(timestamp.timestamp() * 1e9))
    SampleEnvironmentDataAddValues(builder, value_offset)
    if value_timestamps is not None:
        SampleEnvironmentDataAddTimestamps(builder, used_timestamps)

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

    return Response(name=SE_data.Name().decode(), channel=SE_data.Channel(), timestamp=datetime.fromtimestamp(used_timestamp), sample_ts_delta=SE_data.TimeDelta(), ts_location=SE_data.TimestampLocation(), message_counter=SE_data.MessageCounter(), values=SE_data.ValuesAsNumpy(), value_ts=None)
