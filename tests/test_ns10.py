import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.nicos_cache_ns10 import deserialise_ns10, serialise_ns10


class TestSerialisationNs10:
    def test_serialises_and_deserialises_ns10_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "key": "some_key",
            "time_stamp": 123456,
            "ttl": 567890,
            "expired": True,
            "value": "some_value",
        }

        buf = serialise_ns10(**original_entry)
        entry = deserialise_ns10(buf)

        assert entry.key == original_entry["key"]
        assert entry.time_stamp == original_entry["time_stamp"]
        assert entry.ttl == original_entry["ttl"]
        assert entry.expired == original_entry["expired"]
        assert entry.value == original_entry["value"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "key": "some_key",
            "time_stamp": 123456,
            "ttl": 567890,
            "expired": True,
            "value": "some_value",
        }
        buf = serialise_ns10(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ns10(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ns10" in SERIALISERS
        assert "ns10" in DESERIALISERS
