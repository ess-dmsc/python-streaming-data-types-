import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.readout_data_ar51 import deserialise_ar51, serialise_ar51


class TestSerialisationAR51:
    def test_serialises_and_deserialises_ar51_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "raw_data": bytearray(
                [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                ]
            ),
        }

        buf = serialise_ar51(**original_entry)
        entry = deserialise_ar51(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.raw_data, original_entry["raw_data"])

    def test_serialises_and_deserialises_ar51_message_correctly_for_numpy_arrays(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "raw_data": np.array([100, 200, 30, 40, 50, 60, 70, 80, 90]),
        }

        buf = serialise_ar51(**original_entry)
        entry = deserialise_ar51(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.raw_data, original_entry["raw_data"])

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "raw_data": np.array([100, 200, 300, 400, 500, 600, 700, 800, 900]),
        }

        buf = serialise_ar51(**original_entry)

        # Manually introduce error in id.
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ar51(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ar51" in SERIALISERS
        assert "ar51" in DESERIALISERS


if __name__ == "__main__":
    import unittest

    unittest.main()
