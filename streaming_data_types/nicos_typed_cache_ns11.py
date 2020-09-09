from collections import namedtuple
import flatbuffers
from streaming_data_types.fbschemas.nicos_typed_cache_ns11 import TypedCacheEntry
from streaming_data_types.utils import check_schema_identifier


FILE_IDENTIFIER = b"ns11"


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
