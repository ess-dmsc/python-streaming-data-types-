import json

import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.json_json import deserialise_json, serialise_json


class TestSerialisationJson:
    def test_serialises_and_deserialises_json_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        json_str = json.dumps(["foo", "bar"])
        buf = serialise_json(json_str)
        entry = deserialise_json(buf)

        assert entry == json_str

    def test_if_buffer_has_wrong_id_then_throws(self):
        json_str = json.dumps(["foo", "bar"])
        buf = serialise_json(json_str)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_json(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "json" in SERIALISERS
        assert "json" in DESERIALISERS
