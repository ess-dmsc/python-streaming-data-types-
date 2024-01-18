from datetime import datetime, timezone

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.dataarray_da00 import (
    Attribute,
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
        "unique_id": 754,
        "timestamp": datetime.now(tz=timezone.utc),
        "data": [
            Variable(
                name="data",
                unit="counts",
                dims=["time", "x", "y"],
                data=np.array([[[1, 2, 3], [3, 4, 5]]], dtype=np.uint64),
            ),
            Variable(
                name="time",
                unit="hours",
                label="elapsed clock time",
                dims=["time"],
                data=np.array([13, 21], dtype=np.float32),
            ),
            Variable(
                name="x",
                unit="m",
                label="Position",
                dims=["x"],
                data=np.array([-1, 0, 1], dtype=np.float32),
            ),
            Variable(
                name="y",
                unit="m",
                label="Position",
                dims=["y"],
                data=np.array([0, 2, 4, 6], dtype=np.float32),
            ),
        ],
        "attributes": [
            Attribute("name1", "value", "desc1", "src1"),
            Attribute("name2", 11, "desc2", "src2"),
            Attribute("name3", 3.14, "desc3", "src3"),
            Attribute("name4", np.linspace(0, 10), "desc4", "src4"),
        ],
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.unique_id == original_entry["unique_id"]
    assert entry.timestamp == original_entry["timestamp"]
    assert len(entry.data) == len(original_entry["data"])
    for a, b in zip(entry.data, original_entry["data"]):
        assert a == b
    assert len(entry.attributes) == len(original_entry["attributes"])
    for a, b in zip(entry.attributes, original_entry["attributes"]):
        assert a == b


def test_serialises_and_deserialises_da00_float_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some other source name",
        "unique_id": 789679,
        "data": [
            Variable(
                name="data",
                dims=["x", "time", "y"],
                data=np.array([[[1.1, 2.2, 3.3]], [[4.4, 5.5, 6.6]]], dtype=np.float32),
            ),
            Variable(
                name="errors", dims=["y"], data=np.array([1, 2, 3], dtype=np.int8)
            ),
            Variable(
                name="y",
                unit="m",
                label="Position",
                dims=["y"],
                data=np.array([0, 2, 4, 6], dtype=np.float64),
            ),
            Variable(
                name="time",
                unit="hours",
                label="elapsed clock time",
                dims=["time"],
                data=np.array([13, 21], dtype=np.uint32),
            ),
            Variable(
                name="x",
                unit="m",
                label="Position",
                dims=["x"],
                data=np.array([-1, 0, 1], dtype=np.int8),
            ),
        ],
        "timestamp": datetime(
            year=1992,
            month=8,
            day=11,
            hour=3,
            minute=34,
            second=57,
            tzinfo=timezone.utc,
        ),
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.unique_id == original_entry["unique_id"]
    assert entry.timestamp == original_entry["timestamp"]
    assert len(entry.data) == len(original_entry["data"])
    for a, b in zip(entry.data, original_entry["data"]):
        assert a == b
    assert len(entry.attributes) == 0


def test_serialises_and_deserialises_da00_string():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "unique_id": 7001,
        "data": [Variable(data="hi, this is a string", dims=[""], name="the_string")],
        "timestamp": datetime.now(tz=timezone.utc),
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.unique_id == original_entry["unique_id"]
    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp == original_entry["timestamp"]
    assert entry.data == original_entry["data"]


def test_if_buffer_has_wrong_id_then_throws():
    original_entry = {
        "source_name": "some source name",
        "unique_id": 754,
        "data": [
            Variable(
                name="data",
                dims=["x", "y"],
                data=np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64),
            )
        ],
        "timestamp": datetime.now(),
    }

    buf = serialise_da00(**original_entry)

    # Manually hack the id
    buf = bytearray(buf)
    buf[4:8] = b"1234"

    with pytest.raises(WrongSchemaException):
        deserialise_da00(buf)


def test_hm00_schema_type_is_in_global_serialisers_list():
    assert "da00" in SERIALISERS
    assert "da00" in DESERIALISERS
