import pytest
from streaming_data_types.area_detector_ADAr import serialise_ADAr, deserialise_ADAr
from streaming_data_types.fbschemas.ADAr_ADArray_schema.DType import DType
from streaming_data_types import SERIALISERS, DESERIALISERS
import numpy as np
from datetime import datetime


class TestSerialisationNDAr:
    def test_serialises_and_deserialises_ADAr_message_correctly_array(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            "timestamp": datetime.now()
        }

        buf = serialise_ADAr(**original_entry)
        entry = deserialise_ADAr(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp == original_entry["timestamp"]
        assert np.array_equal(entry.data, original_entry["data"])
        assert entry.data.dtype == original_entry["data"].dtype

    def test_serialises_and_deserialises_ADAr_message_correctly_string(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": "hi, this is a string",
            "timestamp": datetime.now()
        }

        buf = serialise_ADAr(**original_entry)
        entry = deserialise_ADAr(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp == original_entry["timestamp"]
        assert entry.data == original_entry["data"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            "timestamp": datetime.now()
        }

        buf = serialise_ADAr(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(RuntimeError):
            deserialise_ADAr(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ADAr" in SERIALISERS
        assert "ADAr" in DESERIALISERS
