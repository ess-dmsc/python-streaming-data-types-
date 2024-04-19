import time

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.dataarray_da00 import (
    Variable,
    deserialise_da00,
    serialise_da00,
)
from streaming_data_types.exceptions import WrongSchemaException


def test_serialises_and_deserialises_da00_int_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "timestamp_ns": time.time_ns(),
        "data": [
            Variable(
                name="data",
                unit="counts",
                axes=["time", "x", "y"],
                data=np.array([[[1, 2, 3], [3, 4, 5]]], dtype=np.uint64),
            ),
            Variable(
                name="time",
                unit="hours",
                label="elapsed clock time",
                axes=["time"],
                data=np.array([13, 21], dtype=np.float32),
            ),
            Variable(
                name="x",
                unit="m",
                label="Position",
                axes=["x"],
                data=np.array([-1, 0, 1], dtype=np.float32),
            ),
            Variable(
                name="y",
                unit="m",
                label="Position",
                axes=["y"],
                data=np.array([0, 2, 4, 6], dtype=np.float32),
            ),
            Variable(name="name1", data="value", label="desc1", source="src1"),
            Variable(name="name2", data=11, label="desc2", source="src2"),
            Variable(name="name3", data=3.14, label="desc3", source="src3"),
            Variable(
                name="name4", data=np.linspace(0, 10), label="desc4", source="src4"
            ),
            Variable(
                name="name5",
                data=np.array([[1, 2], [3, 4]]),
                axes=["a", "b"],
                label="desc5",
                source="src5",
            ),
        ],
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp_ns == original_entry["timestamp_ns"]
    assert len(entry.data) == len(original_entry["data"])
    for a, b in zip(entry.data, original_entry["data"]):
        assert a == b


def test_serialises_and_deserialises_da00_float_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some other source name",
        "data": [
            Variable(
                name="data",
                axes=["x", "time", "y"],
                data=np.array([[[1.1, 2.2, 3.3]], [[4.4, 5.5, 6.6]]], dtype=np.float32),
            ),
            Variable(
                name="errors", axes=["y"], data=np.array([1, 2, 3], dtype=np.int8)
            ),
            Variable(
                name="y",
                unit="m",
                label="Position",
                axes=["y"],
                data=np.array([0, 2, 4, 6], dtype=np.float64),
            ),
            Variable(
                name="time",
                unit="hours",
                label="elapsed clock time",
                axes=["time"],
                data=np.array([13, 21], dtype=np.uint32),
            ),
            Variable(
                name="x",
                unit="m",
                label="Position",
                axes=["x"],
                data=np.array([-1, 0, 1], dtype=np.int8),
            ),
        ],
        "timestamp_ns": time.time_ns(),
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp_ns == original_entry["timestamp_ns"]
    assert len(entry.data) == len(original_entry["data"])
    for a, b in zip(entry.data, original_entry["data"]):
        assert a == b


def test_serialises_and_deserialises_da00_string():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "data": [Variable(data="hi, this is a string", axes=[], name="the_string")],
        "timestamp_ns": time.time_ns(),
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp_ns == original_entry["timestamp_ns"]
    assert len(entry.data) == len(original_entry["data"])
    for a, b in zip(entry.data, original_entry["data"]):
        assert a == b


def test_no_variables_throws():
    original_entry = {
        "source_name": "some source name",
        "data": [],
        "timestamp_ns": time.time_ns(),
    }

    with pytest.raises(RuntimeError):
        serialise_da00(**original_entry)


def test_if_buffer_has_wrong_id_then_throws():
    original_entry = {
        "source_name": "some source name",
        "data": [
            Variable(
                name="data",
                axes=["x", "y"],
                data=np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            )
        ],
        "timestamp_ns": time.time_ns(),
    }

    buf = serialise_da00(**original_entry)

    # Manually hack the id
    buf = bytearray(buf)
    buf[4:8] = b"1234"

    with pytest.raises(WrongSchemaException):
        deserialise_da00(buf)


def test_da00_schema_type_is_in_global_serialisers_list():
    assert "da00" in SERIALISERS
    assert "da00" in DESERIALISERS
