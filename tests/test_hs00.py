import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.histogram_hs00 import deserialise_hs00, serialise_hs00


def create_test_data_with_type(numpy_type):
    return {
        "source": "some_source",
        "timestamp": 123456,
        "current_shape": [5],
        "dim_metadata": [
            {
                "length": 5,
                "unit": "m",
                "label": "some_label",
                "bin_boundaries": np.array([0, 1, 2, 3, 4, 5]).astype(numpy_type),
            }
        ],
        "last_metadata_timestamp": 123456,
        "data": np.array([1, 2, 3, 4, 5]).astype(numpy_type),
        "errors": np.array([5, 4, 3, 2, 1]).astype(numpy_type),
        "info": "info_string",
    }


class TestSerialisationHs00:
    def _check_metadata_for_one_dimension(self, data, original_data):
        assert np.array_equal(data["bin_boundaries"], original_data["bin_boundaries"])
        assert data["length"] == original_data["length"]
        assert data["unit"] == original_data["unit"]
        assert data["label"] == original_data["label"]

    def test_serialises_and_deserialises_hs00_message_correctly_for_full_1d_data(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
                }
            ],
            "last_metadata_timestamp": 123456,
            "data": np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
            "errors": np.array([5.0, 4.0, 3.0, 2.0, 1.0]),
            "info": "info_string",
        }

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_serialises_and_deserialises_hs00_message_correctly_for_minimal_1d_data(
        self,
    ):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
                }
            ],
            "data": np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == ""
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert len(hist["errors"]) == 0
        assert hist["info"] == ""

    def test_serialises_and_deserialises_hs00_message_correctly_for_full_2d_data(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [2, 5],
            "dim_metadata": [
                {
                    "length": 2,
                    "unit": "b",
                    "label": "y",
                    "bin_boundaries": np.array([10.0, 11.0, 12.0]),
                },
                {
                    "length": 5,
                    "unit": "m",
                    "label": "x",
                    "bin_boundaries": np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
                },
            ],
            "last_metadata_timestamp": 123456,
            "data": np.array([[1.0, 2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0, 10.0]]),
            "errors": np.array([[5.0, 4.0, 3.0, 2.0, 1.0], [10.0, 9.0, 8.0, 7.0, 6.0]]),
            "info": "info_string",
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][1], original_hist["dim_metadata"][1]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_if_buffer_has_wrong_id_then_throws(self):
        original_hist = {
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
                }
            ],
            "data": np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        }
        buf = serialise_hs00(original_hist)

        # Manually hack the id
        buf = bytearray(buf)
        buf[4:8] = b"1234"

        with pytest.raises(WrongSchemaException):
            deserialise_hs00(buf)

    def test_serialises_and_deserialises_hs00_message_correctly_for_int_array_data(
        self,
    ):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": np.array([0, 1, 2, 3, 4, 5]),
                }
            ],
            "last_metadata_timestamp": 123456,
            "data": np.array([1, 2, 3, 4, 5]),
            "errors": np.array([5, 4, 3, 2, 1]),
            "info": "info_string",
        }

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_serialise_and_deserialise_hs00_message_returns_uint32_type(self):
        original_hist = create_test_data_with_type(np.uint32)

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert np.issubdtype(
            hist["dim_metadata"][0]["bin_boundaries"].dtype,
            original_hist["dim_metadata"][0]["bin_boundaries"].dtype,
        )
        assert np.issubdtype(hist["data"].dtype, original_hist["data"].dtype)
        assert np.issubdtype(hist["errors"].dtype, original_hist["errors"].dtype)

    def test_serialise_and_deserialise_hs00_message_returns_uint64_type(self):
        original_hist = create_test_data_with_type(np.uint64)

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert np.issubdtype(
            hist["dim_metadata"][0]["bin_boundaries"].dtype,
            original_hist["dim_metadata"][0]["bin_boundaries"].dtype,
        )
        assert np.issubdtype(hist["data"].dtype, original_hist["data"].dtype)
        assert np.issubdtype(hist["errors"].dtype, original_hist["errors"].dtype)

    def test_serialise_and_deserialise_hs00_message_returns_float32_type(self):
        original_hist = create_test_data_with_type(np.float32)

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert np.issubdtype(
            hist["dim_metadata"][0]["bin_boundaries"].dtype,
            original_hist["dim_metadata"][0]["bin_boundaries"].dtype,
        )
        assert np.issubdtype(hist["data"].dtype, original_hist["data"].dtype)
        assert np.issubdtype(hist["errors"].dtype, original_hist["errors"].dtype)

    def test_serialise_and_deserialise_hs00_message_returns_float64_type(self):
        original_hist = create_test_data_with_type(np.float64)

        buf = serialise_hs00(original_hist)
        hist = deserialise_hs00(buf)

        assert np.issubdtype(
            hist["dim_metadata"][0]["bin_boundaries"].dtype,
            original_hist["dim_metadata"][0]["bin_boundaries"].dtype,
        )
        assert np.issubdtype(hist["data"].dtype, original_hist["data"].dtype)
        assert np.issubdtype(hist["errors"].dtype, original_hist["errors"].dtype)

    def test_serialises_and_deserialises_hs00_message_correctly_when_float_input_is_not_ndarray(
        self,
    ):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [2, 5],
            "dim_metadata": [
                {
                    "length": 2,
                    "unit": "b",
                    "label": "y",
                    "bin_boundaries": [10.0, 11.0, 12.0],
                },
                {
                    "length": 5,
                    "unit": "m",
                    "label": "x",
                    "bin_boundaries": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
                },
            ],
            "last_metadata_timestamp": 123456,
            "data": [[1.0, 2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0, 10.0]],
            "errors": [[5.0, 4.0, 3.0, 2.0, 1.0], [10.0, 9.0, 8.0, 7.0, 6.0]],
            "info": "info_string",
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][1], original_hist["dim_metadata"][1]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_serialises_and_deserialises_hs00_message_correctly_when_int_input_is_not_ndarray(
        self,
    ):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [2, 5],
            "dim_metadata": [
                {
                    "length": 2,
                    "unit": "b",
                    "label": "y",
                    "bin_boundaries": [10, 11, 12],
                },
                {
                    "length": 5,
                    "unit": "m",
                    "label": "x",
                    "bin_boundaries": [0, 1, 2, 3, 4, 5],
                },
            ],
            "last_metadata_timestamp": 123456,
            "data": [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]],
            "errors": [[5, 4, 3, 2, 1], [10, 9, 8, 7, 6]],
            "info": "info_string",
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["current_shape"] == original_hist["current_shape"]
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][0], original_hist["dim_metadata"][0]
        )
        self._check_metadata_for_one_dimension(
            hist["dim_metadata"][1], original_hist["dim_metadata"][1]
        )
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_schema_type_is_in_global_serialisers_list(self):
        assert "hs00" in SERIALISERS
        assert "hs00" in DESERIALISERS
