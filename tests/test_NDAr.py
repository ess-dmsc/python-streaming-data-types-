import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.area_detector_NDAr import deserialise_ndar, serialise_ndar
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.NDAr_NDArray_schema.DType import DType


class TestSerialisationNDAr:
    def test_serialises_and_deserialises_NDAr_message_correctly_float64_1_pixel(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "id": 754,
            "dims": [1, 1],
            "data_type": DType.Float64,
            "data": [54, 78, 100, 156, 43, 1, 23, 0],
        }

        buf = serialise_ndar(**original_entry)
        entry = deserialise_ndar(buf)

        assert entry.id == original_entry["id"]
        assert np.array_equal(entry.data, [[3.1991794446845865e-308]])

    def test_serialises_and_deserialises_NDAr_message_correctly_int32_3_pixel(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "id": 754,
            "dims": [1, 3],
            "data_type": DType.Int32,
            "data": [54, 78, 100, 200, 32, 19, 2, 156, 43, 1, 23, 0],
        }

        buf = serialise_ndar(**original_entry)
        entry = deserialise_ndar(buf)

        assert entry.id == original_entry["id"]
        assert np.array_equal(entry.data, [[-932950474, -1677585632, 1507627]])

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "id": 754,
            "dims": [10, 10],
            "data_type": 0,
            "data": [0, 0, 100, 200, 250],
        }

        buf = serialise_ndar(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ndar(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "NDAr" in SERIALISERS
        assert "NDAr" in DESERIALISERS
