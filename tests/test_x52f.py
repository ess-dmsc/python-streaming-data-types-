import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.status_x5f2 import deserialise_x5f2, serialise_x5f2

original_entry = {
    "software_name": "nicos/test",
    "software_version": "1.0.0",
    "service_id": "1a2b3c",
    "host_name": "localhost",
    "process_id": 1234,
    "update_interval": 0,
    "status_json": '{"content" : "log_or_status_message"}',
}


class TestSerialisationX52f:
    def test_serialises_and_deserialises_x5f2_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        buf = serialise_x5f2(**original_entry)
        entry = deserialise_x5f2(buf)

        assert entry.software_name == original_entry["software_name"]
        assert entry.software_version == original_entry["software_version"]
        assert entry.service_id == original_entry["service_id"]
        assert entry.host_name == original_entry["host_name"]
        assert entry.process_id == original_entry["process_id"]
        assert entry.update_interval == original_entry["update_interval"]
        assert entry.status_json == original_entry["status_json"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_x5f2(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_x5f2(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "x5f2" in SERIALISERS
        assert "x5f2" in DESERIALISERS
