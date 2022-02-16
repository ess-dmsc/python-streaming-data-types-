import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_rf5k import (
    Protocol,
    StreamInfo,
    deserialise_rf5k,
    serialise_rf5k,
)


class TestSerialisationRf5k:
    def test_serialises_and_deserialises_rf5k_message_with_streams_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        stream_1 = StreamInfo("channel1", "f142", "topic1", Protocol.Protocol.PVA)
        stream_2 = StreamInfo("channel2", "TdcTime", "topic2", Protocol.Protocol.CA)
        original_entry = {
            "config_change": UpdateType.ADD,
            "streams": [stream_1, stream_2],
        }

        buf = serialise_rf5k(**original_entry)
        entry = deserialise_rf5k(buf)

        assert entry.config_change == original_entry["config_change"]
        assert stream_1 in entry.streams
        assert stream_2 in entry.streams

    def test_serialises_and_deserialises_rf5k_message_without_streams_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {"config_change": UpdateType.REMOVEALL, "streams": []}

        buf = serialise_rf5k(**original_entry)
        entry = deserialise_rf5k(buf)

        assert entry.config_change == original_entry["config_change"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {"config_change": UpdateType.REMOVEALL, "streams": []}

        buf = serialise_rf5k(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_rf5k(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "rf5k" in SERIALISERS
        assert "rf5k" in DESERIALISERS
