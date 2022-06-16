import pytest
from datetime import datetime, timezone
from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.epics_pv_alarm_status_pvAl import (
    deserialise_pvAl,
    serialise_pvAl,
)
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.pv_alarm_state_pvAl import AlarmState, CAAlarmState, AlarmSeverity


class TestSerialisation_pvAl:
    original_entry = {
        "source_name": "test_source",
        "timestamp": datetime(year=2021, month=3, day=26, hour = 12, minute=32, second=11, tzinfo=timezone.utc),
        "state": AlarmState.AlarmState.CLIENT,
        "ca_state": CAAlarmState.CAAlarmState.BAD_SUB,
        "severity": AlarmSeverity.AlarmSeverity.MAJOR,
        "message": "some_msg",
    }

    def test_serialises_and_deserialises_pvAl_message(self):
        buf = serialise_pvAl(**self.original_entry)
        deserialised_tuple = deserialise_pvAl(buf)

        assert deserialised_tuple.timestamp == self.original_entry["timestamp"]
        assert deserialised_tuple.state == self.original_entry["state"]
        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.ca_state == self.original_entry["ca_state"]
        assert deserialised_tuple.severity == self.original_entry["severity"]
        assert deserialised_tuple.message == self.original_entry["message"]


    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_pvAl(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_pvAl(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "pvAl" in SERIALISERS
        assert "pvAl" in DESERIALISERS
