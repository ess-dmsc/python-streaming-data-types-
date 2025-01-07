import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.eventdata_an44 import deserialise_an44, serialise_an44
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationEv44:
    def test_serialises_and_deserialises_an44_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "reference_time": [
                1618573589123781958,
                1618573590133830371,
                1618573593677164112,
                1618573594185190549,
                1618573596217316066,
                1618573596725363109,
                1618573601295720976,
                1618573601799761445,
                1618573607354064836,
            ],
            "reference_time_index": [2, 4, 5, 7],
            "time_of_flight": [100, 200, 300, 400, 500, 600, 700, 800, 900],
            "pixel_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
            "weight": [51, 52, 53, 54, 55, 56, 57, 58, 59],
        }

        buf = serialise_an44(**original_entry)
        entry = deserialise_an44(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.reference_time, original_entry["reference_time"])
        assert np.array_equal(
            entry.reference_time_index, original_entry["reference_time_index"]
        )
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.pixel_id, original_entry["pixel_id"])
        assert np.array_equal(entry.weight, original_entry["weight"])

    def test_serialises_and_deserialises_an44_message_correctly_for_numpy_arrays(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "reference_time": np.array(
                [
                    1618573589123781958,
                    1618573590133830371,
                    1618573593677164112,
                    1618573594185190549,
                    1618573596217316066,
                    1618573596725363109,
                    1618573601295720976,
                    1618573601799761445,
                    1618573607354064836,
                ]
            ),
            "reference_time_index": np.array([2, 4, 5, 7]),
            "time_of_flight": np.array([100, 200, 300, 400, 500, 600, 700, 800, 900]),
            "pixel_id": np.array([10, 20, 30, 40, 50, 60, 70, 80, 90]),
            "weight": np.array([51, 52, 53, 54, 55, 56, 57, 58, 59]),
        }

        buf = serialise_an44(**original_entry)
        entry = deserialise_an44(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.reference_time, original_entry["reference_time"])
        assert np.array_equal(
            entry.reference_time_index, original_entry["reference_time_index"]
        )
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.pixel_id, original_entry["pixel_id"])

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "reference_time": np.array(
                [
                    1618573589123781958,
                    1618573590133830371,
                    1618573593677164112,
                    1618573594185190549,
                    1618573596217316066,
                    1618573596725363109,
                    1618573601295720976,
                    1618573601799761445,
                    1618573607354064836,
                ]
            ),
            "reference_time_index": np.array([2, 4, 5, 7]),
            "time_of_flight": np.array([100, 200, 300, 400, 500, 600, 700, 800, 900]),
            "pixel_id": np.array([10, 20, 30, 40, 50, 60, 70, 80, 90]),
            "weight": np.array([51, 52, 53, 54, 55, 56, 57, 58, 59]),
        }
        buf = serialise_an44(**original_entry)

        # Manually introduce error in id.
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_an44(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "an44" in SERIALISERS
        assert "an44" in DESERIALISERS
