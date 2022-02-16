from datetime import datetime, timezone

import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.action_response_answ import (
    ActionOutcome,
    ActionType,
    deserialise_answ,
    serialise_answ,
)
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationAnsw:
    def test_serialise_and_deserialise_answ_message(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "service_id": "some_service_id_1234",
            "job_id": "some_job_id_abcdef",
            "command_id": "some command id",
            "action": ActionType.SetStopTime,
            "outcome": ActionOutcome.Failure,
            "message": "some random error message",
            "status_code": 123456789,
            "stop_time": datetime(
                year=2021,
                month=2,
                day=12,
                hour=2,
                minute=12,
                second=12,
                tzinfo=timezone.utc,
            ),
        }

        buf = serialise_answ(**original_entry)
        entry = deserialise_answ(buf)

        assert entry.service_id == original_entry["service_id"]
        assert entry.command_id == original_entry["command_id"]
        assert entry.job_id == original_entry["job_id"]
        assert entry.message == original_entry["message"]
        assert entry.action == original_entry["action"]
        assert entry.outcome == original_entry["outcome"]
        assert entry.status_code == original_entry["status_code"]
        assert entry.stop_time == original_entry["stop_time"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {
            "service_id": "some_service_id_1234",
            "job_id": "some_job_id_abcdef",
            "command_id": "some command id",
            "action": ActionType.SetStopTime,
            "outcome": ActionOutcome.Failure,
            "message": "some random error message",
            "status_code": 123456789,
            "stop_time": datetime(
                year=2021,
                month=2,
                day=12,
                hour=2,
                minute=12,
                second=12,
                tzinfo=timezone.utc,
            ),
        }

        buf = serialise_answ(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_answ(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "answ" in SERIALISERS
        assert "answ" in DESERIALISERS
