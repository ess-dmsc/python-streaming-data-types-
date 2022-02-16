from datetime import datetime, timezone

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.fbschemas.sample_environment_senv.Location import Location
from streaming_data_types.sample_environment_senv import (
    deserialise_senv,
    serialise_senv,
)

entry_1 = {
    "name": "some_name",
    "timestamp": datetime.now(tz=timezone.utc),
    "channel": 42,
    "message_counter": 123456,
    "sample_ts_delta": 0.005,
    "values": np.arange(100, dtype=np.uint16),
    "value_timestamps": np.arange(50) + 1111,
    "ts_location": Location.End,
}

entry_2 = {
    "name": "some_name_other_name",
    "timestamp": datetime.now(tz=timezone.utc),
    "channel": 11,
    "message_counter": 654321,
    "sample_ts_delta": 1.666,
    "values": np.arange(1000, dtype=np.int64),
    "value_timestamps": None,
    "ts_location": Location.Middle,
}


class TestSerialisationSenv:
    @pytest.mark.parametrize("input_entry", [entry_1, entry_2])
    def test_serialises_and_deserialises_senv(self, input_entry):
        original_entry = input_entry
        buf = serialise_senv(**original_entry)
        deserialised_tuple = deserialise_senv(buf)

        assert original_entry["name"] == deserialised_tuple.name
        assert original_entry["timestamp"] == deserialised_tuple.timestamp
        assert original_entry["channel"] == deserialised_tuple.channel
        assert original_entry["message_counter"] == deserialised_tuple.message_counter
        assert original_entry["sample_ts_delta"] == deserialised_tuple.sample_ts_delta
        assert np.array_equal(original_entry["values"], deserialised_tuple.values)
        assert np.array_equal(
            original_entry["value_timestamps"], deserialised_tuple.value_ts
        )
        assert original_entry["values"].dtype == deserialised_tuple.values.dtype
        assert original_entry["ts_location"] == deserialised_tuple.ts_location

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "senv" in SERIALISERS
        assert "senv" in DESERIALISERS
