import numpy as np
from streaming_data_types.sample_environment_senv import serialise_senv, deserialise_senv
from streaming_data_types import SERIALISERS, DESERIALISERS
from datetime import datetime
from streaming_data_types.fbschemas.sample_environment_senv.Location import Location


class TestSerialisationSenv:
    original_entry = {
        "name": "some_name",
        "timestamp": datetime.now(),
        "channel": 42,
        "message_counter": 123456,
        "sample_ts_delta": 0.005,
        "values": np.arange(100),
        "value_timestamps": None,
        "ts_location": Location.End
    }

    def test_serialises_and_deserialises_senv(self):
        buf = serialise_senv(**self.original_entry)
        deserialised_tuple = deserialise_senv(buf)

        assert self.original_entry["name"] == deserialised_tuple.name
        assert self.original_entry["timestamp"] == deserialised_tuple.timestamp
        assert self.original_entry["channel"] == deserialised_tuple.channel
        assert self.original_entry["message_counter"] == deserialised_tuple.message_counter
        assert self.original_entry["sample_ts_delta"] == deserialised_tuple.sample_ts_delta
        assert np.array_equal(self.original_entry["values"], deserialised_tuple.values)
        assert self.original_entry["value_timestamps"] == deserialised_tuple.value_ts
        assert self.original_entry["ts_location"] == deserialised_tuple.ts_location

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "senv" in SERIALISERS
        assert "senv" in DESERIALISERS
