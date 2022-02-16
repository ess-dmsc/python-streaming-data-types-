import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.eventdata_ev43 import deserialise_ev43, serialise_ev43
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationEv42:
    def test_serialises_and_deserialises_ev43_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": [567890, 568890],
            "pulse_index": [0, 4],
            "time_of_flight": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "detector_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        }

        buf = serialise_ev43(**original_entry)
        entry = deserialise_ev43(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.pulse_time, original_entry["pulse_time"])
        assert np.array_equal(entry.pulse_index, original_entry["pulse_index"])
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.detector_id, original_entry["detector_id"])

    def test_serialises_and_deserialises_ev43_message_correctly_for_numpy_arrays(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": np.array([567890, 568890]),
            "pulse_index": np.array([0, 4]),
            "time_of_flight": np.array([1, 2, 3, 4, 5, 6, 7, 8, 9]),
            "detector_id": np.array([10, 20, 30, 40, 50, 60, 70, 80, 90]),
        }

        buf = serialise_ev43(**original_entry)
        entry = deserialise_ev43(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert np.array_equal(entry.pulse_time, original_entry["pulse_time"])
        assert np.array_equal(entry.pulse_index, original_entry["pulse_index"])
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.detector_id, original_entry["detector_id"])

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": [567890, 568890],
            "pulse_index": [0, 4],
            "time_of_flight": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "detector_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        }
        buf = serialise_ev43(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ev43(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ev43" in SERIALISERS
        assert "ev43" in DESERIALISERS
