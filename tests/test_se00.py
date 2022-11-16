from datetime import datetime, timezone

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.array_1d_se00 import deserialise_se00, serialise_se00
from streaming_data_types.fbschemas.array_1d_se00.Location import Location

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

entry_3 = {
    "name": "some_float_name",
    "timestamp": datetime.now(tz=timezone.utc),
    "channel": 11,
    "message_counter": 231465,
    "sample_ts_delta": 1.666,
    "values": np.arange(1000, dtype=np.float32),
    "value_timestamps": None,
    "ts_location": Location.Middle,
}

entry_4 = {
    "name": "some_double_name",
    "timestamp": datetime.now(tz=timezone.utc),
    "channel": 11,
    "message_counter": 324156,
    "sample_ts_delta": 1.666,
    "values": np.arange(1000, dtype=np.float64),
    "value_timestamps": None,
    "ts_location": Location.Middle,
}


class TestSerialisationSenv:
    @pytest.mark.parametrize("input_entry", [entry_1, entry_2, entry_3, entry_4])
    def test_serialises_and_deserialises_se00(self, input_entry):
        buf = serialise_se00(**input_entry)
        deserialised_tuple = deserialise_se00(buf)

        assert input_entry["name"] == deserialised_tuple.name
        assert input_entry["timestamp"] == deserialised_tuple.timestamp
        assert input_entry["channel"] == deserialised_tuple.channel
        assert input_entry["message_counter"] == deserialised_tuple.message_counter
        assert input_entry["sample_ts_delta"] == deserialised_tuple.sample_ts_delta
        assert np.array_equal(input_entry["values"], deserialised_tuple.values)
        assert np.array_equal(
            input_entry["value_timestamps"], deserialised_tuple.value_ts
        )
        assert input_entry["values"].dtype == deserialised_tuple.values.dtype
        assert input_entry["ts_location"] == deserialised_tuple.ts_location

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "se00" in SERIALISERS
        assert "se00" in DESERIALISERS
