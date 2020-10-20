import pytest
from streaming_data_types.run_start_pl72 import serialise_pl72, deserialise_pl72
from streaming_data_types import SERIALISERS, DESERIALISERS


class TestSerialisationPl72:
    original_entry = {
        "job_id": "some_key",
        "filename": "test_file.nxs",
        "start_time": 567890,
        "stop_time": 578214,
        "run_name": "test_run",
        "nexus_structure": "{}",
        "service_id": "filewriter1",
        "instrument_name": "LOKI",
        "broker": "localhost:9092",
        "metadata": "{3:1}",
    }

    def test_serialises_and_deserialises_pl72_message_correctly(self):
        buf = serialise_pl72(**self.original_entry)
        deserialised_tuple = deserialise_pl72(buf)

        assert deserialised_tuple.job_id == self.original_entry["job_id"]
        assert deserialised_tuple.filename == self.original_entry["filename"]
        assert deserialised_tuple.start_time == self.original_entry["start_time"]
        assert deserialised_tuple.stop_time == self.original_entry["stop_time"]
        assert deserialised_tuple.run_name == self.original_entry["run_name"]
        assert (
            deserialised_tuple.nexus_structure == self.original_entry["nexus_structure"]
        )
        assert deserialised_tuple.service_id == self.original_entry["service_id"]
        assert (
            deserialised_tuple.instrument_name == self.original_entry["instrument_name"]
        )
        assert deserialised_tuple.broker == self.original_entry["broker"]
        assert deserialised_tuple.metadata == self.original_entry["metadata"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_pl72(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(RuntimeError):
            deserialise_pl72(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "pl72" in SERIALISERS
        assert "pl72" in DESERIALISERS
