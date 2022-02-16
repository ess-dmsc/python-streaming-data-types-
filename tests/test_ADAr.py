from datetime import datetime, timezone

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.area_detector_ADAr import (
    Attribute,
    deserialise_ADAr,
    serialise_ADAr,
)
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationNDAr:
    def test_serialises_and_deserialises_ADAr_int_array(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            "timestamp": datetime.now(tz=timezone.utc),
            "attributes": [
                Attribute("name1", "desc1", "src1", "value"),
                Attribute("name2", "desc2", "src2", 11),
                Attribute("name3", "desc3", "src3", 3.14),
                Attribute("name4", "desc4", "src4", np.linspace(0, 10)),
            ],
        }

        buf = serialise_ADAr(**original_entry)
        entry = deserialise_ADAr(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp == original_entry["timestamp"]
        assert np.array_equal(entry.data, original_entry["data"])
        assert entry.data.dtype == original_entry["data"].dtype
        assert len(entry.attributes) == len(original_entry["attributes"])
        for i in range(len(entry.attributes)):
            assert entry.attributes[i] == original_entry["attributes"][i]

    def test_serialises_and_deserialises_ADAr_float_array(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some other source name",
            "unique_id": 789679,
            "data": np.array([[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]], dtype=np.float32),
            "timestamp": datetime(
                year=1992,
                month=8,
                day=11,
                hour=3,
                minute=34,
                second=57,
                tzinfo=timezone.utc,
            ),
        }

        buf = serialise_ADAr(**original_entry)
        entry = deserialise_ADAr(buf)

        assert entry.unique_id == original_entry["unique_id"]
        assert entry.source_name == original_entry["source_name"]
        assert entry.timestamp == original_entry["timestamp"]
        assert np.array_equal(entry.data, original_entry["data"])
        assert entry.data.dtype == original_entry["data"].dtype

    def test_serialises_and_deserialises_ADAr_string(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some source name",
            "unique_id": 754,
            "data": "hi, this is a string",
            "timestamp": datetime.now(tz=timezone.utc),
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
            "timestamp": datetime.now(),
        }

        buf = serialise_ADAr(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ADAr(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ADAr" in SERIALISERS
        assert "ADAr" in DESERIALISERS
