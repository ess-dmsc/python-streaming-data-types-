import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.logdata_f142 import deserialise_f142, serialise_f142


class TestSerialisationF142:
    original_entry = {
        "source_name": "some_source",
        "value": 578214,
        "timestamp_unix_ns": 1585332414000000000,
    }

    def test_serialises_and_deserialises_integer_f142_message_correctly(self):
        buf = serialise_f142(**self.original_entry)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.value == self.original_entry["value"]
        assert (
            deserialised_tuple.timestamp_unix_ns
            == self.original_entry["timestamp_unix_ns"]
        )

    def test_serialises_and_deserialises_byte_f142_message_correctly(self):
        byte_log = {
            "source_name": "some_source",
            "value": 0x7F,
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**byte_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == byte_log["source_name"]
        assert deserialised_tuple.value == byte_log["value"]
        assert deserialised_tuple.timestamp_unix_ns == byte_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_float_f142_message_correctly(self):
        float_log = {
            "source_name": "some_source",
            "value": 1.234,
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**float_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == float_log["source_name"]
        assert deserialised_tuple.value == float_log["value"]
        assert deserialised_tuple.timestamp_unix_ns == float_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_scalar_ndarray_f142_message_correctly(self):
        numpy_log = {
            "source_name": "some_source",
            "value": np.array(42),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**numpy_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == numpy_log["source_name"]
        assert deserialised_tuple.value == np.array(numpy_log["value"])
        assert deserialised_tuple.timestamp_unix_ns == numpy_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_string_f142_message_correctly(self):
        string_log = {
            "source_name": "some_source",
            "value": "some_string",
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**string_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == string_log["source_name"]
        assert deserialised_tuple.value == string_log["value"]
        assert deserialised_tuple.timestamp_unix_ns == string_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_native_list_correctly(self):
        list_log = {
            "source_name": "some_source",
            "value": [1, 2, 3],
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**list_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == list_log["source_name"]
        # Array values are output as numpy array
        assert np.array_equal(deserialised_tuple.value, np.array(list_log["value"]))
        assert deserialised_tuple.timestamp_unix_ns == list_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_numpy_array_integers_correctly(self):
        array_log = {
            "source_name": "some_source",
            "value": np.array([1, 2, 3]),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**array_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == array_log["source_name"]
        assert np.array_equal(deserialised_tuple.value, array_log["value"])
        assert deserialised_tuple.timestamp_unix_ns == array_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_numpy_array_preserves_byte_type_correctly(
        self,
    ):
        array_log = {
            "source_name": "some_source",
            "value": np.array([1, 2, 3], dtype=np.uint8),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**array_log)
        deserialised_tuple = deserialise_f142(buf)

        assert np.array_equal(deserialised_tuple.value, array_log["value"])
        assert deserialised_tuple.value.dtype == array_log["value"].dtype

    def test_serialises_and_deserialises_numpy_array_preserves_integer_type_correctly(
        self,
    ):
        array_log = {
            "source_name": "some_source",
            "value": np.array([1, 2, 3], dtype=np.uint16),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**array_log)
        deserialised_tuple = deserialise_f142(buf)

        assert np.array_equal(deserialised_tuple.value, array_log["value"])
        assert deserialised_tuple.value.dtype == array_log["value"].dtype

    def test_serialises_and_deserialises_numpy_array_floats_correctly(self):
        array_log = {
            "source_name": "some_source",
            "value": np.array([1.1, 2.2, 3.3]),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**array_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == array_log["source_name"]
        assert np.allclose(deserialised_tuple.value, array_log["value"])
        assert deserialised_tuple.timestamp_unix_ns == array_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_numpy_array_strings_correctly(self):
        array_log = {
            "source_name": "some_source",
            "value": np.array(["1", "2", "3"]),
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**array_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == array_log["source_name"]
        assert np.array_equal(deserialised_tuple.value, array_log["value"])
        assert deserialised_tuple.timestamp_unix_ns == array_log["timestamp_unix_ns"]

    def test_serialises_and_deserialises_epics_alarms_correctly(self):
        float_log = {
            "source_name": "some_source",
            "value": 1.234,
            "timestamp_unix_ns": 1585332414000000000,
            "alarm_status": AlarmStatus.HIHI,
            "alarm_severity": AlarmSeverity.MAJOR,
        }
        buf = serialise_f142(**float_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.alarm_status == float_log["alarm_status"]
        assert deserialised_tuple.alarm_severity == float_log["alarm_severity"]

    def test_epics_alarms_default_to_no_change_when_not_provided_to_serialiser(self):
        float_log = {
            "source_name": "some_source",
            "value": 1.234,
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**float_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.alarm_status == AlarmStatus.NO_CHANGE
        assert deserialised_tuple.alarm_severity == AlarmSeverity.NO_CHANGE

    def test_raises_not_implemented_error_when_trying_to_serialise_numpy_complex_number_type(
        self,
    ):
        complex_log = {
            "source_name": "some_source",
            "value": np.complex(3, 4),
            "timestamp_unix_ns": 1585332414000000000,
        }
        with pytest.raises(NotImplementedError):
            serialise_f142(**complex_log)

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_f142(**self.original_entry)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_f142(buf)

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "f142" in SERIALISERS
        assert "f142" in DESERIALISERS
