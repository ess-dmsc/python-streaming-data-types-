import pytest
from datetime import datetime, timezone
from copy import deepcopy
from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.epics_pv_conn_status_pvCn import (
    deserialise_pvCn,
    serialise_pvCn,
)
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.epics_conn_status_pvCn import ConnectionInfo


class TestSerialisation_pvCn:
    original_entry = {
        "timestamp": datetime(year=2021, month=3, day=26, hour = 12, minute=32, second=11, tzinfo=timezone.utc),
        "status": ConnectionInfo.ConnectionInfo.DESTROYED,
        "source_name": "test_source",
        "service_id": "test_service",
    }

    def test_serialises_and_deserialises_pvCn_message_1(self):
        buf = serialise_pvCn(**self.original_entry)
        deserialised_tuple = deserialise_pvCn(buf)

        assert deserialised_tuple.timestamp == self.original_entry["timestamp"]
        assert deserialised_tuple.status == self.original_entry["status"]
        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.service_id == self.original_entry["service_id"]

    def test_serialises_and_deserialises_pvCn_message_1(self):
        modified_entry = deepcopy(self.original_entry)
        modified_entry.pop("service_id")
        buf = serialise_pvCn(**modified_entry)
        deserialised_tuple = deserialise_pvCn(buf)

        assert deserialised_tuple.timestamp == modified_entry["timestamp"]
        assert deserialised_tuple.status == modified_entry["status"]
        assert deserialised_tuple.source_name == modified_entry["source_name"]
        assert deserialised_tuple.service_id is None

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_pvCn(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_pvCn(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "pvCn" in SERIALISERS
        assert "pvCn" in DESERIALISERS
