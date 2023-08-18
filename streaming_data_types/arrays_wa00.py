from typing import Union, List, NamedTuple
from datetime import datetime, timezone

import flatbuffers
import numpy as np
from streaming_data_types.fbschemas.arrays_wav_00.WaveFormArray import WaveFormArray
from streaming_data_types.fbschemas.arrays_wav_00.DType import DType
from streaming_data_types.fbschemas.arrays_wav_00 import (
    WaveFormArrayStart,
    WaveFormArrayAddXData,
    WaveFormArrayAddYData,
    WaveFormArrayAddXDataType,
    WaveFormArrayAddYDataType,
    WaveFormArrayAddTimestamp,
    WaveFormArrayEnd,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"wa00"


def serialise_wa00(
    values_x_array: Union[np.ndarray, List],
    values_y_array: Union[np.ndarray, List],
    timestamp: datetime,
) -> bytes:
    builder = flatbuffers.Builder(1024)

    type_map = {
        np.dtype("uint8"): DType.uint8,
        np.dtype("int8"): DType.int8,
        np.dtype("uint16"): DType.uint16,
        np.dtype("int16"): DType.int16,
        np.dtype("uint32"): DType.uint32,
        np.dtype("int32"): DType.int32,
        np.dtype("uint64"): DType.uint64,
        np.dtype("int64"): DType.int64,
        np.dtype("float32"): DType.float32,
        np.dtype("float64"): DType.float64,
    }

    if type(values_x_array) is str:
        values_x_array = np.frombuffer(values_x_array.encode(), np.uint8)
        datatype_x_array = DType.c_string
    else:
        datatype_x_array = type_map[values_x_array.dtype]

    if type(values_y_array) is str:
        values_y_array = np.frombuffer(values_y_array.encode(), np.uint8)
        datatype_y_array = DType.c_string
    else:
        datatype_y_array = type_map[values_y_array.dtype]

    # Build data
    x_data_offset = builder.CreateByteVector(values_x_array.view(np.uint8))
    y_data_offset = builder.CreateByteVector(values_y_array.view(np.uint8))

    # Build the buffer
    WaveFormArrayStart(builder)
    WaveFormArrayAddXData(builder, values_x_array)
    WaveFormArrayAddYData(builder, values_y_array)
    WaveFormArrayAddXDataType(builder, datatype_x_array)
    WaveFormArrayAddYDataType(builder, datatype_y_array)
    if timestamp is not None:
        WaveFormArrayAddTimestamp(builder, int(timestamp.timestamp() * 1e9))
    WA_Message = WaveFormArrayEnd(builder)
    builder.Finish(WA_Message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


wa00_t = NamedTuple(
    "wa00",
    (
        ("values_x_array", np.ndarray),
        ("values_y_array", np.ndarray),
        ("timestamp", datetime),
    ),
)


def get_data(raw_data, datatype) -> np.ndarray:
    """
    Converts the data array into the correct type.
    """
    type_map = {
        DType.uint8: np.uint8,
        DType.int8: np.int8,
        DType.uint16: np.uint16,
        DType.int16: np.int16,
        DType.uint32: np.uint32,
        DType.int32: np.int32,
        DType.uint64: np.uint64,
        DType.int64: np.int64,
        DType.float32: np.float32,
        DType.float64: np.float64,
    }
    return raw_data.view(type_map[datatype])


def deserialise_wav00(buffer: Union[bytearray, bytes]) -> wa00_t:
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    waveform_array = WaveFormArray.GetRootAsWaveFormArray(buffer, 0)
    max_time = datetime(
        year=3001, month=1, day=1, hour=0, minute=0, second=0
    ).timestamp()
    used_timestamp = waveform_array.Timestamp() / 1e9

    if used_timestamp > max_time:
        used_timestamp = max_time
    if waveform_array.XDataType() == DType.c_string:
        x_array = waveform_array.XDataAsNumpy().to_bytes().decode()
    else:
        x_array = get_data(waveform_array.XDataAsNumpy(), waveform_array.XDataType)

    if waveform_array.YDataType() == DType.c_string:
        y_array = waveform_array.XDataAsNumpy().to_bytes().decode()
    else:
        y_array = get_data(waveform_array.YDataAsNumpy(), waveform_array.YDataType)
    return wa00_t(
        values_x_array=x_array,
        values_y_array=y_array,
        timestamp=datetime.fromtimestamp(used_timestamp, tz=timezone.utc),
    )
