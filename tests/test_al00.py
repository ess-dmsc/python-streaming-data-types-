import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.alarm_al00 import (Severity, deserialise_al00,
                                             serialise_al00)
from streaming_data_types.exceptions import WrongSchemaException


class TestSerialisationAl00:
    def test_serialises_and_deserialises_al00_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        buf = serialise_al00("some_source", 1234567890, Severity.MAJOR, "Some message")
        entry = deserialise_al00(buf)

        assert entry.source == "some_source"
        assert entry.timestamp_ns == 1234567890
        assert entry.severity == Severity.MAJOR
        assert entry.message == "Some message"

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_al00("some_source", 1234567890, Severity.MAJOR, "Some message")

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_al00(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "al00" in SERIALISERS
        assert "al00" in DESERIALISERS
