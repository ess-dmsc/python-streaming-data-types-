from collections import namedtuple
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


def serialise_ns11(
    key: str,
    valueType: int,
    value: str,
    time_stamp: float = 0,
    ttl: float = 0,
    expired: bool = False,
):
    builder = flatbuffers.Builder(128)

    value_offset = builder.CreateString(value)
    key_offset = builder.CreateString(key)

    TypedCacheEntry.TypedCacheEntryStart(builder)
    TypedCacheEntry.TypedCacheEntryAddValue(builder, value_offset)
    TypedCacheEntry.TypedCacheEntryAddValueType(builder, valueType)
    TypedCacheEntry.TypedCacheEntryAddExpired(builder, expired)
    TypedCacheEntry.TypedCacheEntryAddTtl(builder, ttl)
    TypedCacheEntry.TypedCacheEntryAddTime(builder, time_stamp)
    TypedCacheEntry.TypedCacheEntryAddKey(builder, key_offset)
    cache_entry_message = TypedCacheEntry.TypedCacheEntryEnd(builder)
    builder.Finish(cache_entry_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER

    return bytes(buffer)


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
