import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import (
    UpdateType,
)
from streaming_data_types.forwarder_config_update_fc00 import (
    Protocol,
    StreamInfo,
    deserialise_fc00,
    serialise_fc00,
)


class TestSerialisationRf5k:
    def test_serialises_and_deserialises_fc00_message_with_streams_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        stream_1 = StreamInfo("channel1", "f144", "topic1", Protocol.Protocol.PVA, 0)
        stream_2 = StreamInfo("channel2", "TdcTime", "topic2", Protocol.Protocol.CA, 0)
        stream_3 = StreamInfo("channel3", "f144", "topic3", Protocol.Protocol.PVA, 1)
        original_entry = {
            "config_change": UpdateType.ADD,
            "streams": [stream_1, stream_2, stream_3],
        }

        buf = serialise_fc00(**original_entry)
        entry = deserialise_fc00(buf)

        assert entry.config_change == original_entry["config_change"]
        assert stream_1 in entry.streams
        assert stream_2 in entry.streams
        assert stream_3 in entry.streams

    def test_serialises_and_deserialises_fc00_message_without_streams_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {"config_change": UpdateType.REMOVEALL, "streams": []}

        buf = serialise_fc00(**original_entry)
        entry = deserialise_fc00(buf)

        assert entry.config_change == original_entry["config_change"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_entry = {"config_change": UpdateType.REMOVEALL, "streams": []}

        buf = serialise_fc00(**original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_fc00(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "fc00" in SERIALISERS
        assert "fc00" in DESERIALISERS
