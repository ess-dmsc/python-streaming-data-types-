import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.finished_writing_wrdn import deserialise_wrdn, serialise_wrdn


class TestEncoder(object):
    def test_serialise_and_deserialise_wrdn_message(self):
        """
        Round-trip to check what we serialise is what we get back.
        """

        original_entry = {
            "service_id": "some_service_id_1234",
            "job_id": "some_job_id_abcdef",
            "error_encountered": True,
            "file_name": "somefile.nxs",
            "metadata": '{"hello":4}',
            "message": "some random error message",
        }

        buf = serialise_wrdn(**original_entry)
        entry = deserialise_wrdn(buf)

        assert entry.service_id == original_entry["service_id"]
        assert entry.job_id == original_entry["job_id"]
        assert entry.error_encountered == original_entry["error_encountered"]
        assert entry.file_name == original_entry["file_name"]
        assert entry.metadata == original_entry["metadata"]
        assert entry.message == original_entry["message"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "service_id": "some_service_id_1234",
            "job_id": "some_job_id_abcdef",
            "error_encountered": True,
            "file_name": "somefile.nxs",
            "metadata": '{"hello":4}',
            "message": "some random error message",
        }

        buf = serialise_wrdn(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_wrdn(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "wrdn" in SERIALISERS
        assert "wrdn" in DESERIALISERS
