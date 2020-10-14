from collections import namedtuple
from typing import Any, Callable, Dict, Union, List, Set, Tuple

import flatbuffers
import numpy as np
from streaming_data_types.fbschemas.nicos_typed_cache_ns11 import TypedCacheEntry
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Array import (
    ArrayStartValueVector,
    ArrayStart,
    ArrayAddArrayType,
    ArrayEnd,
    ArrayAddValue,
    Array,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.ArrayElement import (
    ArrayElementStart,
    ArrayElementAddV,
    ArrayElementAddVType,
    ArrayElementEnd,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.ArrayType import ArrayType
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Bool import (
    BoolStart,
    BoolAddValue,
    BoolEnd,
    Bool,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Double import (
    DoubleStart,
    DoubleAddValue,
    DoubleEnd,
    Double,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Long import (
    LongStart,
    LongAddValue,
    LongEnd,
    Long,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.String import (
    StringStart,
    StringAddValue,
    StringEnd,
    String,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Value import Value
from streaming_data_types.utils import check_schema_identifier


FILE_IDENTIFIER = b"ns11"


def _complete_buffer(
    builder,
    key: str,
    time_stamp: float = 0,
    ttl: float = 0,
    expired: bool = False,
):
    TypedCacheEntry.TypedCacheEntryAddExpired(builder, expired)
    TypedCacheEntry.TypedCacheEntryAddTtl(builder, ttl)
    TypedCacheEntry.TypedCacheEntryAddTime(builder, time_stamp)
    TypedCacheEntry.TypedCacheEntryAddKey(builder, key)
    cache_entry_message = TypedCacheEntry.TypedCacheEntryEnd(builder)
    builder.Finish(cache_entry_message)

    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return buffer


def _serialise_long(builder: flatbuffers.Builder, data: np.ndarray):
    LongStart(builder)
    LongAddValue(builder, data.item())
    value_position = LongEnd(builder)
    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, value_position)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, Value.Long)


def _serialise_string(builder: flatbuffers.Builder, data: np.ndarray):
    string_offset = builder.CreateString(data.item())
    StringStart(builder)
    StringAddValue(builder, string_offset)
    value_position = StringEnd(builder)
    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, value_position)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, Value.String)


def _serialise_double(builder: flatbuffers.Builder, data: np.ndarray):
    DoubleStart(builder)
    DoubleAddValue(builder, data.item())
    value_position = DoubleEnd(builder)
    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, value_position)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, Value.Double)


def _serialise_double_array(
    builder: flatbuffers.Builder, data: np.ndarray, raw_data: Union[List, Set, Tuple]
):
    if isinstance(raw_data, list):
        _serialise_double_list(builder, data)
    if isinstance(raw_data, set):
        _serialise_double_set(builder, data)
    if isinstance(raw_data, tuple):
        _serialise_double_tuple(builder, data)


def _serialise_double_list(builder: flatbuffers.Builder, data: np.ndarray):
    _init_double_array_serialisation(builder, data)

    ArrayElementAddVType(builder, Value.Double)
    value_offset = ArrayElementEnd(builder)

    ArrayStart(builder)
    ArrayAddArrayType(builder, ArrayType.ListType)
    ArrayAddValue(builder, value_offset)

    _end_array_serialisation(builder)


def _serialise_double_tuple(builder: flatbuffers.Builder, data: np.ndarray):
    _init_double_array_serialisation(builder, data)

    ArrayElementAddVType(builder, Value.Double)
    value_offset = ArrayElementEnd(builder)

    ArrayStart(builder)
    ArrayAddArrayType(builder, ArrayType.TupleType)
    ArrayAddValue(builder, value_offset)

    _end_array_serialisation(builder)


def _serialise_double_set(builder: flatbuffers.Builder, data: np.ndarray):
    _init_double_array_serialisation(builder, data)

    ArrayElementAddVType(builder, Value.Double)
    value_offset = ArrayElementEnd(builder)

    ArrayStart(builder)
    ArrayAddArrayType(builder, ArrayType.SetType)
    ArrayAddValue(builder, value_offset)

    _end_array_serialisation(builder)


def _init_double_array_serialisation(builder: flatbuffers.Builder, data: np.ndarray):
    ArrayStartValueVector(builder, len(data))
    if data.dtype == "float64":
        for single_value in reversed(data):
            builder.PrependFloat64(single_value)
    if data.dtype == "float32":
        for single_value in reversed(data):
            builder.PrependFloat32(single_value)
    array_offset = builder.EndVector(len(data))
    ArrayElementStart(builder)
    ArrayElementAddV(builder, array_offset)


def _end_array_serialisation(builder):
    array_position = ArrayEnd(builder)
    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, array_position)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, Value.Array)


def _serialise_bool(builder: flatbuffers.Builder, data: np.ndarray):
    BoolStart(builder)
    BoolAddValue(builder, data.item())
    value_position = BoolEnd(builder)
    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, value_position)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, Value.Bool)


_map_scalar_type_to_serialiser = {
    np.dtype("int32"): _serialise_long,
    np.dtype("int64"): _serialise_long,
    np.dtype("float32"): _serialise_double,
    np.dtype("float64"): _serialise_double,
    np.dtype("bool"): _serialise_bool,
}

_map_array_type_to_serialiser = {
    np.dtype("float32"): _serialise_double_array,
    np.dtype("float64"): _serialise_double_array,
}


def serialise_ns11(
    value: Any,
    key: str,
    time_stamp: float = 0,
    ttl: float = 0,
    expired: bool = False,
):
    builder = flatbuffers.Builder(128)
    key_offset = builder.CreateString(key)
    raw_value = value

    if isinstance(raw_value, set):
        np_value = np.array(list(raw_value))
    else:
        np_value = np.array(raw_value)

    if np_value.ndim == 0:
        _serialise_value(
            builder,
            np_value,
            raw_value,
            _serialise_string,
            _map_scalar_type_to_serialiser,
        )
    elif np_value.ndim == 1:
        _serialise_value(
            builder,
            np_value,
            raw_value,
            _serialise_string,
            _map_array_type_to_serialiser,
        )
    return bytes(_complete_buffer(builder, key_offset, time_stamp, ttl, expired))


def _serialise_value(
    builder: flatbuffers.Builder,
    value: Any,
    raw_value: Any,
    string_serialiser: Callable,
    serialisers_map: Dict,
):
    if np.issubdtype(value.dtype, np.unicode_) or np.issubdtype(
        value.dtype, np.string_
    ):
        string_serialiser(builder, value)
    else:
        try:
            serialisers_map[value.dtype](builder, value, raw_value)
        except KeyError:
            raise NotImplementedError(
                f"Cannot serialise data of type {value.dtype}, must use one of "
                f"{list(_map_scalar_type_to_serialiser.keys()).append(np.unicode_)}"
            )


_map_fb_enum_to_type = {
    Value.Long: Long,
    Value.Double: Double,
    Value.String: String,
    Value.Bool: Bool,
    Value.Array: Array,
}


def _decode_if_scalar_string(value: np.ndarray):
    if value.ndim == 0 and (
        np.issubdtype(value.dtype, np.unicode_)
        or np.issubdtype(value.dtype, np.string_)
    ):
        return value.item().decode()
    return value


def deserialise_ns11(buffer):
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    typed_entry = TypedCacheEntry.TypedCacheEntry()

    entry = typed_entry.GetRootAsTypedCacheEntry(buffer, 0)

    value_offset = entry.Value()
    value_fb = _map_fb_enum_to_type[entry.ValueType()]()
    value_fb.Init(value_offset.Bytes, value_offset.Pos)

    try:
        value = value_fb.ValueAsNumpy()
    except AttributeError:
        try:
            value = np.array(value_fb.Value())
        except TypeError:
            value = np.array(
                [str(value_fb.Value(n), "utf-8") for n in range(value_fb.ValueLength())]
            )

    value = _decode_if_scalar_string(value)

    key = entry.Key() if entry.Key() else b""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    valueType = entry.ValueType() if entry.ValueType() else 0

    Entry = namedtuple(
        "Entry", ("key", "time_stamp", "ttl", "expired", "value", "valueType")
    )

    return Entry(key.decode().strip(), time_stamp, ttl, expired, value, valueType)
