import numpy as np
import pytest
from datetime import datetime, timezone
from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.arrays_wa00 import(
    deserialise_wav00,
    serialise_wa00,
)
from streaming_data_types.exceptions import WrongSchemaException

class TestSerialisationwa00:
    def test_serialise_and_deserialise_wa00_int_array(self):
        """
        Round-trip to check that we serialise is what we get back.
        """
        original_entry={
            "values_x_array": np.array([1,2,3,4,5,1],dtype=np.uint64),
            "values_y_array": np.array([6,7,8,9,10,6],dtype=np.uint64),
            "timestamp": datetime.now(tz=timezone.utc),
        }

        buf = serialise_wa00(**original_entry)
        entry = deserialise_wav00(buf)
        assert entry.timestamp == original_entry["timestamp"]
        assert np.array_equal(entry.values_x_array, original_entry["values_x_array"])
        assert np.array_equal(entry.values_y_array,original_entry["values_y_array"])