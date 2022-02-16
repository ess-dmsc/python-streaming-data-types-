import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.eventdata_ev42 import deserialise_ev42, serialise_ev42
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationEv42:
    def test_serialises_and_deserialises_ev42_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": 567890,
            "time_of_flight": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "detector_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        }

        buf = serialise_ev42(**original_entry)
        entry = deserialise_ev42(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert entry.pulse_time == original_entry["pulse_time"]
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.detector_id, original_entry["detector_id"])

    def test_serialises_and_deserialises_ev42_message_correctly_for_numpy_arrays(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": 567890,
            "time_of_flight": np.array([1, 2, 3, 4, 5, 6, 7, 8, 9]),
            "detector_id": np.array([10, 20, 30, 40, 50, 60, 70, 80, 90]),
        }

        buf = serialise_ev42(**original_entry)
        entry = deserialise_ev42(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert entry.pulse_time == original_entry["pulse_time"]
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.detector_id, original_entry["detector_id"])

    def test_serialises_and_deserialises_ev42_message_correctly_with_isis_info(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        isis_data = {"period_number": 5, "run_state": 1, "proton_charge": 1.234}

        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": 567890,
            "time_of_flight": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "detector_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
            "isis_specific": isis_data,
        }

        buf = serialise_ev42(**original_entry)
        entry = deserialise_ev42(buf)

        assert entry.source_name == original_entry["source_name"]
        assert entry.message_id == original_entry["message_id"]
        assert entry.pulse_time == original_entry["pulse_time"]
        assert np.array_equal(entry.time_of_flight, original_entry["time_of_flight"])
        assert np.array_equal(entry.detector_id, original_entry["detector_id"])
        assert entry.specific_data["period_number"] == isis_data["period_number"]
        assert entry.specific_data["run_state"] == isis_data["run_state"]
        assert entry.specific_data["proton_charge"] == pytest.approx(
            isis_data["proton_charge"]
        )

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "source_name": "some_source",
            "message_id": 123456,
            "pulse_time": 567890,
            "time_of_flight": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "detector_id": [10, 20, 30, 40, 50, 60, 70, 80, 90],
        }
        buf = serialise_ev42(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_ev42(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "ev42" in SERIALISERS
        assert "ev42" in DESERIALISERS
