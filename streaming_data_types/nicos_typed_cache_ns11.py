from collections import namedtuple
from typing import Any, Callable, Dict

import flatbuffers
import numpy as np
from streaming_data_types.fbschemas.nicos_typed_cache_ns11 import TypedCacheEntry
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Bool import (
    BoolStart,
    BoolAddValue,
    BoolEnd,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Double import (
    DoubleStart,
    DoubleAddValue,
    DoubleEnd,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.Long import (
    LongStart,
    LongAddValue,
    LongEnd,
)
from streaming_data_types.fbschemas.nicos_typed_cache_ns11.String import (
    StringStart,
    StringAddValue,
    StringEnd,
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


def serialise_ns11(
    value: Any,
    key: str,
    time_stamp: float = 0,
    ttl: float = 0,
    expired: bool = False,
):
    builder = flatbuffers.Builder(128)
    key_offset = builder.CreateString(key)
    value = np.array(value)

    if value.ndim == 0:
        _serialise_value(
            builder, value, _serialise_string, _map_scalar_type_to_serialiser
        )
    return bytes(_complete_buffer(builder, key_offset, time_stamp, ttl, expired))


def _serialise_value(
    builder: flatbuffers.Builder,
    value: Any,
    string_serialiser: Callable,
    serialisers_map: Dict,
):
    if np.issubdtype(value.dtype, np.unicode_) or np.issubdtype(
        value.dtype, np.string_
    ):
        string_serialiser(builder, value)
    else:
        try:
            serialisers_map[value.dtype](builder, value)
        except KeyError:
            raise NotImplementedError(
                f"Cannot serialise data of type {value.dtype}, must use one of "
                f"{list(_map_scalar_type_to_serialiser.keys()).append(np.unicode_)}"
            )


# TODO: Following function should be rewritten.
def deserialise_ns11(buffer):
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    entry = TypedCacheEntry.TypedCacheEntry.GetRootAsTypedCacheEntry(buffer, 0)

    key = entry.Key() if entry.Key() else b""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    value = entry.Value() if entry.Value() else b""
    valueType = entry.ValueType() if entry.ValueType() else 0

    Entry = namedtuple(
        "Entry", ("key", "time_stamp", "ttl", "expired", "value", "valueType")
    )

    return Entry(
        key.decode().strip(), time_stamp, ttl, expired, value.decode(), valueType
    )
