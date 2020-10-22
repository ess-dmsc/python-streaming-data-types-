import flatbuffers
from streaming_data_types.fbschemas.logdata_f142 import LogData
from streaming_data_types.fbschemas.logdata_f142.Value import Value
from streaming_data_types.fbschemas.logdata_f142.UByte import (
    UByte,
    UByteStart,
    UByteAddValue,
    UByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayUByte import (
    ArrayUByte,
    ArrayUByteStart,
    ArrayUByteAddValue,
    ArrayUByteEnd,
    ArrayUByteStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Byte import (
    Byte,
    ByteStart,
    ByteAddValue,
    ByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayByte import (
    ArrayByte,
    ArrayByteStart,
    ArrayByteAddValue,
    ArrayByteEnd,
    ArrayByteStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.UShort import (
    UShort,
    UShortStart,
    UShortAddValue,
    UShortEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayUShort import (
    ArrayUShort,
    ArrayUShortStart,
    ArrayUShortAddValue,
    ArrayUShortEnd,
    ArrayUShortStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Short import (
    Short,
    ShortStart,
    ShortAddValue,
    ShortEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayShort import (
    ArrayShort,
    ArrayShortStart,
    ArrayShortAddValue,
    ArrayShortEnd,
    ArrayShortStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.UInt import (
    UInt,
    UIntStart,
    UIntAddValue,
    UIntEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayUInt import (
    ArrayUInt,
    ArrayUIntStart,
    ArrayUIntAddValue,
    ArrayUIntEnd,
    ArrayUIntStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Int import (
    Int,
    IntStart,
    IntAddValue,
    IntEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayInt import (
    ArrayInt,
    ArrayIntStart,
    ArrayIntAddValue,
    ArrayIntEnd,
    ArrayIntStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.ULong import (
    ULong,
    ULongStart,
    ULongAddValue,
    ULongEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayULong import (
    ArrayULong,
    ArrayULongStart,
    ArrayULongAddValue,
    ArrayULongEnd,
    ArrayULongStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Long import (
    Long,
    LongStart,
    LongAddValue,
    LongEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayLong import (
    ArrayLong,
    ArrayLongStart,
    ArrayLongAddValue,
    ArrayLongEnd,
    ArrayLongStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Float import (
    Float,
    FloatStart,
    FloatAddValue,
    FloatEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayFloat import (
    ArrayFloat,
    ArrayFloatStart,
    ArrayFloatAddValue,
    ArrayFloatEnd,
    ArrayFloatStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.Double import (
    Double,
    DoubleStart,
    DoubleAddValue,
    DoubleEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayDouble import (
    ArrayDouble,
    ArrayDoubleStart,
    ArrayDoubleAddValue,
    ArrayDoubleEnd,
    ArrayDoubleStartValueVector,
)
from streaming_data_types.fbschemas.logdata_f142.String import (
    String,
    StringStart,
    StringAddValue,
    StringEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayString import (
    ArrayString,
    ArrayStringStart,
    ArrayStringAddValue,
    ArrayStringEnd,
    ArrayStringStartValueVector,
)
from streaming_data_types.utils import check_schema_identifier
import numpy as np
from typing import Any, Tuple, Callable, Dict, Union
from collections import namedtuple


FILE_IDENTIFIER = b"f142"


def _complete_buffer(
    builder,
    timestamp_unix_ns: int,
    alarm_status: Union[int, None] = None,
    alarm_severity: Union[int, None] = None,
) -> bytearray:
    LogData.LogDataAddTimestamp(builder, timestamp_unix_ns)

    if alarm_status is not None:
        LogData.LogDataAddStatus(builder, alarm_status)
        # Only include severity if status was provided, it would be meaningless by itself
        if alarm_severity is not None:
            LogData.LogDataAddSeverity(builder, alarm_severity)

    log_msg = LogData.LogDataEnd(builder)

    builder.Finish(log_msg, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


def _setup_builder(source_name: str) -> Tuple[flatbuffers.Builder, int]:
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)
    source = builder.CreateString(source_name)
    return builder, source


def _serialise_byte(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ByteStart(builder)
    ByteAddValue(builder, data.item())
    value_position = ByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Byte)


def _serialise_bytearray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayByteStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependInt8(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayByteStart(builder)
    ArrayByteAddValue(builder, array_offset)
    value_position = ArrayByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayByte)


def _serialise_ubyte(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UByteStart(builder)
    UByteAddValue(builder, data.item())
    value_position = UByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UByte)


def _serialise_ubytearray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayUByteStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependUint8(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayUByteStart(builder)
    ArrayUByteAddValue(builder, array_offset)
    value_position = ArrayUByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayUByte)


def _serialise_short(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ShortStart(builder)
    ShortAddValue(builder, data.item())
    value_position = ShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Short)


def _serialise_shortarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayShortStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependInt16(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayShortStart(builder)
    ArrayShortAddValue(builder, array_offset)
    value_position = ArrayShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayShort)


def _serialise_ushort(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UShortStart(builder)
    UShortAddValue(builder, data.item())
    value_position = UShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UShort)


def _serialise_ushortarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayUShortStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependUint16(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayUShortStart(builder)
    ArrayUShortAddValue(builder, array_offset)
    value_position = ArrayUShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayUShort)


def _serialise_int(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    IntStart(builder)
    IntAddValue(builder, data.item())
    value_position = IntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Int)


def _serialise_intarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayIntStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependInt32(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayIntStart(builder)
    ArrayIntAddValue(builder, array_offset)
    value_position = ArrayIntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayInt)


def _serialise_uint(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UIntStart(builder)
    UIntAddValue(builder, data.item())
    value_position = UIntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UInt)


def _serialise_uintarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayUIntStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependUint32(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayUIntStart(builder)
    ArrayUIntAddValue(builder, array_offset)
    value_position = ArrayUIntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayUInt)


def _serialise_long(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    LongStart(builder)
    LongAddValue(builder, data.item())
    value_position = LongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Long)


def _serialise_longarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayLongStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependInt64(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayLongStart(builder)
    ArrayLongAddValue(builder, array_offset)
    value_position = ArrayLongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayLong)


def _serialise_ulong(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ULongStart(builder)
    ULongAddValue(builder, data.item())
    value_position = ULongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ULong)


def _serialise_ulongarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayULongStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependUint64(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayULongStart(builder)
    ArrayULongAddValue(builder, array_offset)
    value_position = ArrayULongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayULong)


def _serialise_float(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    FloatStart(builder)
    FloatAddValue(builder, data.item())
    value_position = FloatEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Float)


def _serialise_floatarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayFloatStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependFloat32(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayFloatStart(builder)
    ArrayFloatAddValue(builder, array_offset)
    value_position = ArrayFloatEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayFloat)


def _serialise_double(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    DoubleStart(builder)
    DoubleAddValue(builder, data.item())
    value_position = DoubleEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Double)


def _serialise_doublearray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ArrayDoubleStartValueVector(builder, len(data))
    for single_value in reversed(data):
        builder.PrependFloat64(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayDoubleStart(builder)
    ArrayDoubleAddValue(builder, array_offset)
    value_position = ArrayDoubleEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayDouble)


def _serialise_string(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    string_offset = builder.CreateString(data.item())
    StringStart(builder)
    StringAddValue(builder, string_offset)
    value_position = StringEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.String)


def _serialise_stringarray(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    string_offsets = [
        builder.CreateString(string_item) for string_item in reversed(data)
    ]
    ArrayStringStartValueVector(builder, len(data))
    for string_offset in string_offsets:
        builder.PrependSOffsetTRelative(string_offset)
    string_array_offset = builder.EndVector(len(data))
    ArrayStringStart(builder)
    ArrayStringAddValue(builder, string_array_offset)
    value_position = ArrayStringEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ArrayString)


_map_scalar_type_to_serialiser = {
    np.dtype("byte"): _serialise_byte,
    np.dtype("ubyte"): _serialise_ubyte,
    np.dtype("int8"): _serialise_short,
    np.dtype("int16"): _serialise_short,
    np.dtype("int32"): _serialise_int,
    np.dtype("int64"): _serialise_long,
    np.dtype("uint8"): _serialise_ushort,
    np.dtype("uint16"): _serialise_ushort,
    np.dtype("uint32"): _serialise_uint,
    np.dtype("uint64"): _serialise_ulong,
    np.dtype("float32"): _serialise_float,
    np.dtype("float64"): _serialise_double,
}

_map_array_type_to_serialiser = {
    np.dtype("byte"): _serialise_bytearray,
    np.dtype("ubyte"): _serialise_ubytearray,
    np.dtype("int8"): _serialise_shortarray,
    np.dtype("int16"): _serialise_shortarray,
    np.dtype("int32"): _serialise_intarray,
    np.dtype("int64"): _serialise_longarray,
    np.dtype("uint8"): _serialise_ushortarray,
    np.dtype("uint16"): _serialise_ushortarray,
    np.dtype("uint32"): _serialise_uintarray,
    np.dtype("uint64"): _serialise_ulongarray,
    np.dtype("float32"): _serialise_floatarray,
    np.dtype("float64"): _serialise_doublearray,
}


def serialise_f142(
    value: Any,
    source_name: str,
    timestamp_unix_ns: int = 0,
    alarm_status: Union[int, None] = None,
    alarm_severity: Union[int, None] = None,
) -> bytes:
    """
    Serialise value and corresponding timestamp as an f142 Flatbuffer message.
    Should automagically use a sensible type for value in the message, but if
    in doubt pass value in as a numpy ndarray of a carefully chosen dtype.

    :param value: only scalar value currently supported; if ndarray then ndim must be 0
    :param source_name: name of the data source
    :param timestamp_unix_ns: timestamp corresponding to value, e.g. when value was measured, in nanoseconds
    :param alarm_status: EPICS alarm status, best to provide using enum-like class defined in logdata_f142.AlarmStatus
    :param alarm_severity: EPICS alarm severity, best to provide using enum-like class defined in logdata_f142.AlarmSeverity
    """
    builder, source = _setup_builder(source_name)
    value = np.array(value)

    if value.ndim == 0:
        _serialise_value(
            builder, source, value, _serialise_string, _map_scalar_type_to_serialiser
        )
    elif value.ndim == 1:
        _serialise_value(
            builder,
            source,
            value,
            _serialise_stringarray,
            _map_array_type_to_serialiser,
        )
    else:
        raise NotImplementedError("f142 only supports scalars or 1D array values")

    return bytes(
        _complete_buffer(builder, timestamp_unix_ns, alarm_status, alarm_severity)
    )


def _serialise_value(
    builder: flatbuffers.Builder,
    source: int,
    value: Any,
    string_serialiser: Callable,
    serialisers_map: Dict,
):
    # We can use a dictionary to map most numpy types to one of the types defined in the flatbuffer schema
    # but we have to handle strings separately as there are many subtypes
    if np.issubdtype(value.dtype, np.unicode_) or np.issubdtype(
        value.dtype, np.string_
    ):
        string_serialiser(builder, value, source)
    else:
        try:
            serialisers_map[value.dtype](builder, value, source)
        except KeyError:
            # There are a few numpy types we don't try to handle, for example complex numbers
            raise NotImplementedError(
                f"Cannot serialise data of type {value.dtype}, must use one of "
                f"{list(_map_scalar_type_to_serialiser.keys()).append(np.unicode_)}"
            )


_map_fb_enum_to_type = {
    Value.Byte: Byte,
    Value.UByte: UByte,
    Value.Short: Short,
    Value.UShort: UShort,
    Value.Int: Int,
    Value.UInt: UInt,
    Value.Long: Long,
    Value.ULong: ULong,
    Value.Float: Float,
    Value.Double: Double,
    Value.String: String,
    Value.ArrayByte: ArrayByte,
    Value.ArrayUByte: ArrayUByte,
    Value.ArrayShort: ArrayShort,
    Value.ArrayUShort: ArrayUShort,
    Value.ArrayInt: ArrayInt,
    Value.ArrayUInt: ArrayUInt,
    Value.ArrayLong: ArrayLong,
    Value.ArrayULong: ArrayULong,
    Value.ArrayFloat: ArrayFloat,
    Value.ArrayDouble: ArrayDouble,
    Value.ArrayString: ArrayString,
}


LogDataInfo = namedtuple(
    "LogDataInfo",
    ("value", "source_name", "timestamp_unix_ns", "alarm_status", "alarm_severity"),
)


def _decode_if_scalar_string(value: np.ndarray) -> Union[str, np.ndarray]:
    if value.ndim == 0 and (
        np.issubdtype(value.dtype, np.unicode_)
        or np.issubdtype(value.dtype, np.string_)
    ):
        return value.item().decode()
    return value


def deserialise_f142(buffer: Union[bytearray, bytes]) -> LogDataInfo:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    log_data = LogData.LogData.GetRootAsLogData(buffer, 0)
    source_name = log_data.SourceName() if log_data.SourceName() else b""

    value_offset = log_data.Value()
    value_fb = _map_fb_enum_to_type[log_data.ValueType()]()
    value_fb.Init(value_offset.Bytes, value_offset.Pos)
    try:
        value = value_fb.ValueAsNumpy()
    except AttributeError:
        try:
            # Must be a scalar value then, so we'll get it like this
            value = np.array(value_fb.Value())
        except TypeError:
            # In that case it is an array of strings, which for some reason doesn't get a generated ValueAsNumpy method
            # So we'll have to extract each element from the buffer manually and construct our own numpy array
            value = np.array(
                [str(value_fb.Value(n), "utf-8") for n in range(value_fb.ValueLength())]
            )

    value = _decode_if_scalar_string(value)

    timestamp = log_data.Timestamp()

    return LogDataInfo(
        value, source_name.decode(), timestamp, log_data.Status(), log_data.Severity()
    )
