import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.run_stop_6s4t import deserialise_6s4t, serialise_6s4t


class TestSerialisation6s4t:
    original_entry = {
        "job_id": "some_key",
        "stop_time": 578214,
        "run_name": "test_run",
        "service_id": "filewriter1",
        "command_id": "some command id",
    }

    def test_serialises_and_deserialises_6s4t_message_correctly(self):
        buf = serialise_6s4t(**self.original_entry)
        deserialised_tuple = deserialise_6s4t(buf)

        assert deserialised_tuple.job_id == self.original_entry["job_id"]
        assert deserialised_tuple.stop_time == self.original_entry["stop_time"]
        assert deserialised_tuple.run_name == self.original_entry["run_name"]
        assert deserialised_tuple.service_id == self.original_entry["service_id"]
        assert deserialised_tuple.command_id == self.original_entry["command_id"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_6s4t(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_6s4t(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "6s4t" in SERIALISERS
        assert "6s4t" in DESERIALISERS
