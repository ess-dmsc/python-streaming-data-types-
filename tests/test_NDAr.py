import pytest
from streaming_data_types.area_detector_NDAr import serialise_ndar, deserialise_ndar
from streaming_data_types import SERIALISERS, DESERIALISERS
import numpy as np


class TestSerialisationNDAr:
    def test_serialises_and_deserialises_NDAr_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "id": 754,
            "dims": [10, 10],
            "data_type": 1,
            "data": [0, 0, 100, 200, 250],
        }

        buf = serialise_ndar(**original_entry)
        entry = deserialise_ndar(buf)

        assert entry.id == original_entry["id"]
        assert np.array_equal(entry.dims, original_entry["dims"])
        assert entry.data_type == original_entry["data_type"]
        assert np.array_equal(entry.data, original_entry["data"])

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "id": 754,
            "dims": [10, 10],
            "data_type": 0,
            "data": [0, 0, 100, 200, 300],
        }

        buf = serialise_ndar(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(RuntimeError):
            deserialise_ndar(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "NDAr" in SERIALISERS
        assert "NDAr" in DESERIALISERS
