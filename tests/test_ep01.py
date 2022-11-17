import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.epics_connection_ep01 import (
    deserialise_ep01,
    serialise_ep01,
)
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.epics_connection_ep01 import ConnectionInfo


class TestSerialisationEp01:
    original_entry = {
        "timestamp_ns": 1593620746000000000,
        "status": ConnectionInfo.ConnectionInfo.DISCONNECTED,
        "source_name": "test_source",
        "service_id": "test_service",
    }

    def test_serialises_and_deserialises_ep01_message_correctly(self):
        buf = serialise_ep01(**self.original_entry)
        deserialised_tuple = deserialise_ep01(buf)

        assert deserialised_tuple.timestamp == self.original_entry["timestamp_ns"]
        assert deserialised_tuple.status == self.original_entry["status"]
        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.service_id == self.original_entry["service_id"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_ep01(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ep01(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ep01" in SERIALISERS
        assert "ep01" in DESERIALISERS
