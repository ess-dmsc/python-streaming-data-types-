import pytest
from datetime import datetime, timezone
from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.epics_pv_scalar_data_scal import (
    deserialise_scal,
    serialise_scal,
)
import numpy as np
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisation_scal:
    original_entry = {
        "source_name": "test_source",
        "timestamp": datetime(year=2021, month=3, day=26, hour=12, minute=32, second=11, tzinfo=timezone.utc),
    }

    @pytest.mark.parametrize("value", [1234, 3.14])
    def test_serialises_and_deserialises_scal_message(self, value):
        buf = serialise_scal(**self.original_entry, value=value)
        deserialised_tuple = deserialise_scal(buf)

        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.timestamp == self.original_entry["timestamp"]
        assert deserialised_tuple.value == value

    @pytest.mark.parametrize("value_type", [np.dtype("int8"), np.dtype("uint8"), np.dtype("int16"), np.dtype("uint16"), np.dtype("int32"), np.dtype("uint32"), np.dtype("int64"), np.dtype("uint64"), np.dtype("float32"), np.dtype("float64")])
    def test_serialises_and_deserialises_scal_message_numpy(self, value_type):
        value = np.arange(10, dtype=value_type)
        buf = serialise_scal(**self.original_entry, value=value)
        deserialised_tuple = deserialise_scal(buf)

        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.timestamp == self.original_entry["timestamp"]
        assert (deserialised_tuple.value == value).all()
        assert deserialised_tuple.value.dtype == value_type


    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_scal(**self.original_entry, value=123)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_scal(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "scal" in SERIALISERS
        assert "scal" in DESERIALISERS
