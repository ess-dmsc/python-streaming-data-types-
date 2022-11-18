from collections import namedtuple
from datetime import datetime
from typing import Any, Union, NamedTuple

import flatbuffers
import numpy as np

from streaming_data_types.fbschemas.logdata_f144 import LogData
from streaming_data_types.fbschemas.logdata_f144.ArrayByte import (
    ArrayByte,
    ArrayByteAddValue,
    ArrayByteEnd,
    ArrayByteStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayDouble import (
    ArrayDouble,
    ArrayDoubleAddValue,
    ArrayDoubleEnd,
    ArrayDoubleStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayFloat import (
    ArrayFloat,
    ArrayFloatAddValue,
    ArrayFloatEnd,
    ArrayFloatStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayInt import (
    ArrayInt,
    ArrayIntAddValue,
    ArrayIntEnd,
    ArrayIntStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayLong import (
    ArrayLong,
    ArrayLongAddValue,
    ArrayLongEnd,
    ArrayLongStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayShort import (
    ArrayShort,
    ArrayShortAddValue,
    ArrayShortEnd,
    ArrayShortStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayUByte import (
    ArrayUByte,
    ArrayUByteAddValue,
    ArrayUByteEnd,
    ArrayUByteStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayUInt import (
    ArrayUInt,
    ArrayUIntAddValue,
    ArrayUIntEnd,
    ArrayUIntStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayULong import (
    ArrayULong,
    ArrayULongAddValue,
    ArrayULongEnd,
    ArrayULongStart,
)
from streaming_data_types.fbschemas.logdata_f144.ArrayUShort import (
    ArrayUShort,
    ArrayUShortAddValue,
    ArrayUShortEnd,
    ArrayUShortStart,
)
from streaming_data_types.fbschemas.logdata_f144.Byte import (
    Byte,
    ByteAddValue,
    ByteEnd,
    ByteStart,
)
from streaming_data_types.fbschemas.logdata_f144.Double import (
    Double,
    DoubleAddValue,
    DoubleEnd,
    DoubleStart,
)
from streaming_data_types.fbschemas.logdata_f144.Float import (
    Float,
    FloatAddValue,
    FloatEnd,
    FloatStart,
)
from streaming_data_types.fbschemas.logdata_f144.Int import (
    Int,
    IntAddValue,
    IntEnd,
    IntStart,
)
from streaming_data_types.fbschemas.logdata_f144.Long import (
    Long,
    LongAddValue,
    LongEnd,
    LongStart,
)
from streaming_data_types.fbschemas.logdata_f144.Short import (
    Short,
    ShortAddValue,
    ShortEnd,
    ShortStart,
)
from streaming_data_types.fbschemas.logdata_f144.UByte import (
    UByte,
    UByteAddValue,
    UByteEnd,
    UByteStart,
)
from streaming_data_types.fbschemas.logdata_f144.UInt import (
    UInt,
    UIntAddValue,
    UIntEnd,
    UIntStart,
)
from streaming_data_types.fbschemas.logdata_f144.ULong import (
    ULong,
    ULongAddValue,
    ULongEnd,
    ULongStart,
)
from streaming_data_types.fbschemas.logdata_f144.UShort import (
    UShort,
    UShortAddValue,
    UShortEnd,
    UShortStart,
)
from streaming_data_types.fbschemas.logdata_f144.Value import Value
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"f144"

SerialiserFunctions = namedtuple(
    "SerialiserFunctionMap",
    ("StartFunction", "AddValueFunction", "EndFunction", "value_type_enum"),
)


def _serialise_value(
    builder: flatbuffers.Builder, value: Any, function_map: SerialiserFunctions
):
    function_map.StartFunction(builder)
    function_map.AddValueFunction(builder, value)
    return function_map.EndFunction(builder)


_map_scalar_type_to_serialiser = {
    np.dtype("byte"): SerialiserFunctions(
        ByteStart, ByteAddValue, ByteEnd, Value.Byte
    ),
    np.dtype("ubyte"): SerialiserFunctions(
        UByteStart, UByteAddValue, UByteEnd, Value.UByte
    ),
    np.dtype("int16"): SerialiserFunctions(
        ShortStart, ShortAddValue, ShortEnd, Value.Short
    ),
    np.dtype("uint16"): SerialiserFunctions(
        UShortStart, UShortAddValue, UShortEnd, Value.UShort
    ),
    np.dtype("int32"): SerialiserFunctions(
        IntStart, IntAddValue, IntEnd, Value.Int
    ),
    np.dtype("uint32"): SerialiserFunctions(
        UIntStart, UIntAddValue, UIntEnd, Value.UInt
    ),
    np.dtype("int64"): SerialiserFunctions(
        LongStart, LongAddValue, LongEnd, Value.Long
    ),
    np.dtype("uint64"): SerialiserFunctions(
        ULongStart, ULongAddValue, ULongEnd, Value.ULong
    ),
    np.dtype("float32"): SerialiserFunctions(
        FloatStart, FloatAddValue, FloatEnd, Value.Float
    ),
    np.dtype("float64"): SerialiserFunctions(
        DoubleStart, DoubleAddValue, DoubleEnd, Value.Double
    ),
}

_map_array_type_to_serialiser = {
    np.dtype("byte"): SerialiserFunctions(
        ArrayByteStart, ArrayByteAddValue, ArrayByteEnd, Value.ArrayByte
    ),
    np.dtype("int16"): SerialiserFunctions(
        ArrayShortStart, ArrayShortAddValue, ArrayShortEnd, Value.ArrayShort
    ),
    np.dtype("int32"): SerialiserFunctions(
        ArrayIntStart, ArrayIntAddValue, ArrayIntEnd, Value.ArrayInt
    ),
    np.dtype("int64"): SerialiserFunctions(
        ArrayLongStart, ArrayLongAddValue, ArrayLongEnd, Value.ArrayLong
    ),
    np.dtype("ubyte"): SerialiserFunctions(
        ArrayUByteStart, ArrayUByteAddValue, ArrayUByteEnd, Value.ArrayUByte
    ),
    np.dtype("uint16"): SerialiserFunctions(
        ArrayUShortStart, ArrayUShortAddValue, ArrayUShortEnd, Value.ArrayUShort
    ),
    np.dtype("uint32"): SerialiserFunctions(
        ArrayUIntStart, ArrayUIntAddValue, ArrayUIntEnd, Value.ArrayUInt
    ),
    np.dtype("uint64"): SerialiserFunctions(
        ArrayULongStart, ArrayULongAddValue, ArrayULongEnd, Value.ArrayULong
    ),
    np.dtype("float32"): SerialiserFunctions(
        ArrayFloatStart, ArrayFloatAddValue, ArrayFloatEnd, Value.ArrayFloat
    ),
    np.dtype("float64"): SerialiserFunctions(
        ArrayDoubleStart, ArrayDoubleAddValue, ArrayDoubleEnd, Value.ArrayDouble
    ),
}

DoubleFuncMap = SerialiserFunctions(
    DoubleStart, DoubleAddValue, DoubleEnd, Value.Double
)
IntFuncMap = SerialiserFunctions(LongStart, LongAddValue, LongEnd, Value.Long)


def serialise_f144(
    source_name: str,
    value: Any,
    timestamp: datetime,
) -> bytes:
    builder = flatbuffers.Builder(1024)
    source_name_offset = builder.CreateString(source_name)
    if isinstance(value, np.ndarray):
        try:
            c_func_map = _map_array_type_to_serialiser[value.dtype]
            value_offset = _serialise_value(
                builder, builder.CreateNumpyVector(value), c_func_map
            )
            value_type = c_func_map.value_type_enum
        except KeyError:
            raise NotImplementedError(
                f'f144 flatbuffer does not support values of type {value.dtype}.'
            )
    else:
        try:
            c_func_map = _map_scalar_type_to_serialiser[value.dtype]
            value_offset = _serialise_value(builder, value, DoubleFuncMap)
            value_type = c_func_map.value_type_enum
        except KeyError:
            raise NotImplementedError(
                f'f144 flatbuffer does not support values of type {value.dtype}.'
            )
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source_name_offset)
    LogData.LogDataAddValue(builder, value_offset)
    LogData.LogDataAddValueType(builder, value_type)
    LogData.LogDataAddTimestamp(builder, int(timestamp.timestamp() * 1e9))
    end = LogData.LogDataEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


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
}


ExtractedLogData = NamedTuple(
    "LogData",
    (
        ("source_name", str),
        ("value", Any),
        ("timestamp", datetime),
    ),
)


def deserialise_f144(buffer: Union[bytearray, bytes]) -> ExtractedLogData:
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    log_data = LogData.LogData.GetRootAs(buffer, 0)
    source_name = log_data.SourceName() if log_data.SourceName() else b""

    value_offset = log_data.Value()
    value_fb = _map_fb_enum_to_type[log_data.ValueType()]()
    value_fb.Init(value_offset.Bytes, value_offset.Pos)
    if hasattr(value_fb, "ValueAsNumpy"):
        value = value_fb.ValueAsNumpy()
    else:
        value = value_fb.Value()
    return ExtractedLogData(
        source_name=source_name.decode(),
        value=value,
        timestamp=log_data.Timestamp()
        )
