from typing import Any, Union, NamedTuple
from collections import namedtuple
import flatbuffers
import numpy as np
from datetime import datetime, timezone

from streaming_data_types.fbschemas.epics_scalar_data_scal import ScalarData
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayInt8 import (
    ArrayInt8,
    ArrayInt8Start,
    ArrayInt8AddValue,
    ArrayInt8End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayUInt8 import (
    ArrayUInt8,
    ArrayUInt8Start,
    ArrayUInt8AddValue,
    ArrayUInt8End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayInt16 import (
    ArrayInt16,
    ArrayInt16Start,
    ArrayInt16AddValue,
    ArrayInt16End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayUInt16 import (
    ArrayUInt16,
    ArrayUInt16Start,
    ArrayUInt16AddValue,
    ArrayUInt16End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayInt32 import (
    ArrayInt32,
    ArrayInt32Start,
    ArrayInt32AddValue,
    ArrayInt32End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayUInt32 import (
    ArrayUInt32,
    ArrayUInt32Start,
    ArrayUInt32AddValue,
    ArrayUInt32End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayInt64 import (
    ArrayInt64,
    ArrayInt64Start,
    ArrayInt64AddValue,
    ArrayInt64End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayUInt64 import (
    ArrayUInt64,
    ArrayUInt64Start,
    ArrayUInt64AddValue,
    ArrayUInt64End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayFloat32 import (
    ArrayFloat32,
    ArrayFloat32Start,
    ArrayFloat32AddValue,
    ArrayFloat32End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.ArrayFloat64 import (
    ArrayFloat64,
    ArrayFloat64Start,
    ArrayFloat64AddValue,
    ArrayFloat64End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.Int8 import Int8
from streaming_data_types.fbschemas.epics_scalar_data_scal.UInt8 import UInt8
from streaming_data_types.fbschemas.epics_scalar_data_scal.Int16 import Int16
from streaming_data_types.fbschemas.epics_scalar_data_scal.UInt16 import UInt16
from streaming_data_types.fbschemas.epics_scalar_data_scal.Int32 import Int32
from streaming_data_types.fbschemas.epics_scalar_data_scal.UInt32 import UInt32
from streaming_data_types.fbschemas.epics_scalar_data_scal.UInt64 import UInt64
from streaming_data_types.fbschemas.epics_scalar_data_scal.Float32 import Float32
from streaming_data_types.fbschemas.epics_scalar_data_scal.Float64 import (
    Float64,
    Float64Start,
    Float64AddValue,
    Float64End,
)
from streaming_data_types.fbschemas.epics_scalar_data_scal.Int64 import (
    Int64,
    Int64Start,
    Int64AddValue,
    Int64End,
)

from streaming_data_types.fbschemas.epics_scalar_data_scal.Value import Value
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"scal"

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


_map_array_type_to_serialiser = {
    np.dtype("int8"): SerialiserFunctions(
        ArrayInt8Start, ArrayInt8AddValue, ArrayInt8End, Value.ArrayInt8
    ),
    np.dtype("int16"): SerialiserFunctions(
        ArrayInt16Start, ArrayInt16AddValue, ArrayInt16End, Value.ArrayInt16
    ),
    np.dtype("int32"): SerialiserFunctions(
        ArrayInt32Start, ArrayInt32AddValue, ArrayInt32End, Value.ArrayInt32
    ),
    np.dtype("int64"): SerialiserFunctions(
        ArrayInt64Start, ArrayInt64AddValue, ArrayInt64End, Value.ArrayInt64
    ),
    np.dtype("uint8"): SerialiserFunctions(
        ArrayUInt8Start, ArrayUInt8AddValue, ArrayUInt8End, Value.ArrayUInt8
    ),
    np.dtype("uint16"): SerialiserFunctions(
        ArrayUInt16Start, ArrayUInt16AddValue, ArrayUInt16End, Value.ArrayUInt16
    ),
    np.dtype("uint32"): SerialiserFunctions(
        ArrayUInt32Start, ArrayUInt32AddValue, ArrayUInt32End, Value.ArrayUInt32
    ),
    np.dtype("uint64"): SerialiserFunctions(
        ArrayUInt64Start, ArrayUInt64AddValue, ArrayUInt64End, Value.ArrayUInt64
    ),
    np.dtype("float32"): SerialiserFunctions(
        ArrayFloat32Start, ArrayFloat32AddValue, ArrayFloat32End, Value.ArrayFloat32
    ),
    np.dtype("float64"): SerialiserFunctions(
        ArrayFloat64Start, ArrayFloat64AddValue, ArrayFloat64End, Value.ArrayFloat64
    ),
}

DoubleFuncMap = SerialiserFunctions(
    Float64Start, Float64AddValue, Float64End, Value.Float64
)
IntFuncMap = SerialiserFunctions(Int64Start, Int64AddValue, Int64End, Value.Int64)


def serialise_scal(
    source_name: str,
    value: Any,
    timestamp: datetime,
) -> bytes:
    builder = flatbuffers.Builder(1024)
    source_name_offset = builder.CreateString(source_name)
    if isinstance(value, int):
        value_offset = _serialise_value(builder, value, IntFuncMap)
        value_type = Value.Int64
    elif isinstance(value, float):
        value_offset = _serialise_value(builder, value, DoubleFuncMap)
        value_type = Value.Float64
    elif isinstance(value, np.ndarray):
        c_func_map = _map_array_type_to_serialiser[value.dtype]
        value_offset = _serialise_value(
            builder, builder.CreateNumpyVector(value), c_func_map
        )
        value_type = c_func_map.value_type_enum
    else:
        raise NotImplementedError(
            f'scal flatbuffer does not support values of type "{type(value)}".'
        )
    ScalarData.ScalarDataStart(builder)
    ScalarData.ScalarDataAddSourceName(builder, source_name_offset)
    ScalarData.ScalarDataAddValue(builder, value_offset)
    ScalarData.ScalarDataAddValueType(builder, value_type)
    ScalarData.ScalarDataAddTimestamp(builder, int(timestamp.timestamp() * 1e6))

    end = ScalarData.ScalarDataEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


_map_fb_enum_to_type = {
    Value.Int8: Int8,
    Value.UInt8: UInt8,
    Value.Int16: Int16,
    Value.UInt16: UInt16,
    Value.Int32: Int32,
    Value.UInt32: UInt32,
    Value.Int64: Int64,
    Value.UInt64: UInt64,
    Value.Float32: Float32,
    Value.Float64: Float64,
    Value.ArrayInt8: ArrayInt8,
    Value.ArrayUInt8: ArrayUInt8,
    Value.ArrayInt16: ArrayInt16,
    Value.ArrayUInt16: ArrayUInt16,
    Value.ArrayInt32: ArrayInt32,
    Value.ArrayUInt32: ArrayUInt32,
    Value.ArrayInt64: ArrayInt64,
    Value.ArrayUInt64: ArrayUInt64,
    Value.ArrayFloat32: ArrayFloat32,
    Value.ArrayFloat64: ArrayFloat64,
}


ExtractedScalarData = NamedTuple(
    "ScalarData",
    (
        ("source_name", str),
        ("value", Any),
        ("timestamp", datetime),
    ),
)


def deserialise_scal(buffer: Union[bytearray, bytes]) -> ExtractedScalarData:
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    scalar_data = ScalarData.ScalarData.GetRootAs(buffer, 0)
    source_name = scalar_data.SourceName() if scalar_data.SourceName() else b""

    value_offset = scalar_data.Value()
    value_fb = _map_fb_enum_to_type[scalar_data.ValueType()]()
    value_fb.Init(value_offset.Bytes, value_offset.Pos)
    if hasattr(value_fb, "ValueAsNumpy"):
        value = value_fb.ValueAsNumpy()
    else:
        value = value_fb.Value()
    return ExtractedScalarData(
        source_name=source_name.decode(),
        value=value,
        timestamp=datetime.fromtimestamp(
            scalar_data.Timestamp() / 1e6, tz=timezone.utc
        ),
    )
