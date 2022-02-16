import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.epics_connection_info_ep00 import (
    deserialise_ep00,
    serialise_ep00,
)
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.epics_connection_info_ep00 import EventType


class TestSerialisationEp00:
    original_entry = {
        "timestamp_ns": 1593620746000000000,
        "event_type": EventType.EventType.DISCONNECTED,
        "source_name": "test_source",
        "service_id": "test_service",
    }

    def test_serialises_and_deserialises_ep00_message_correctly(self):
        buf = serialise_ep00(**self.original_entry)
        deserialised_tuple = deserialise_ep00(buf)

        assert deserialised_tuple.timestamp == self.original_entry["timestamp_ns"]
        assert deserialised_tuple.type == self.original_entry["event_type"]
        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.service_id == self.original_entry["service_id"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_ep00(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ep00(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ep00" in SERIALISERS
        assert "ep00" in DESERIALISERS
