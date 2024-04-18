import time

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.area_detector_ad00 import (
    Attribute,
    deserialise_ad00,
    serialise_ad00,
)
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationAD00:
    def test_serialises_and_deserialises_ad00_int_array(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            "timestamp_ns": time.time_ns(),
            "attributes": [
                Attribute("name1", "desc1", "src1", "value"),
                Attribute("name2", "desc2", "src2", 11),
                Attribute("name3", "desc3", "src3", 3.14),
                Attribute("name4", "desc4", "src4", np.linspace(0, 10)),
            ],
        }

        buf = serialise_ad00(**original_entry)
        entry = deserialise_ad00(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp_ns == original_entry["timestamp_ns"]
        assert np.array_equal(entry.dimensions, original_entry["data"].shape)
        assert np.array_equal(entry.data.shape, entry.dimensions)  # Sanity check
        assert np.array_equal(entry.data, original_entry["data"])
        assert entry.data.dtype == original_entry["data"].dtype
        assert len(entry.attributes) == len(original_entry["attributes"])
        for i in range(len(entry.attributes)):
            assert entry.attributes[i] == original_entry["attributes"][i]

    def test_serialises_and_deserialises_ad00_float_array(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some other source name",
            "unique_id": 789679,
            "data": np.array([[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]], dtype=np.float32),
            "timestamp_ns": time.time_ns(),
        }

        buf = serialise_ad00(**original_entry)
        entry = deserialise_ad00(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp_ns == original_entry["timestamp_ns"]
        assert np.array_equal(entry.data, original_entry["data"])
        assert entry.data.dtype == original_entry["data"].dtype

    def test_serialises_and_deserialises_ad00_string(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": "hi, this is a string",
            "timestamp_ns": time.time_ns(),
        }

        buf = serialise_ad00(**original_entry)
        entry = deserialise_ad00(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp_ns == original_entry["timestamp_ns"]
        assert entry.data == original_entry["data"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            "timestamp_ns": time.time_ns(),
        }

        buf = serialise_ad00(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ad00(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ad00" in SERIALISERS
        assert "ad00" in DESERIALISERS
