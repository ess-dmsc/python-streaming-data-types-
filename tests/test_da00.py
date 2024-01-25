from datetime import datetime, timezone

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
        "timestamp": datetime.now(tz=timezone.utc),
        "variables": [
            Variable(
                name="data",
                unit="counts",
                dims=["time", "x", "y"],
                data=np.array([[[1, 2, 3], [3, 4, 5]]], dtype=np.uint64),
            ),
        ],
        "constants": [
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
            Variable(name="name1", data="value", label="desc1", source="src1"),
            Variable(name="name2", data=11, label="desc2", source="src2"),
            Variable(name="name3", data=3.14, label="desc3", source="src3"),
            Variable(
                name="name4", data=np.linspace(0, 10), label="desc4", source="src4"
            ),
            Variable(
                name="name5",
                data=np.array([[1, 2], [3, 4]]),
                dims=["a", "b"],
                label="desc5",
                source="src5",
            ),
        ],
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp == original_entry["timestamp"]
    for group in ("variables", "constants", "attributes"):
        assert len(getattr(entry, group)) == len(original_entry[group])
        for a, b in zip(getattr(entry, group), original_entry[group]):
            assert a == b


def test_serialises_and_deserialises_da00_float_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some other source name",
        "variables": [
            Variable(
                name="data",
                dims=["x", "time", "y"],
                data=np.array([[[1.1, 2.2, 3.3]], [[4.4, 5.5, 6.6]]], dtype=np.float32),
            ),
            Variable(
                name="errors", dims=["y"], data=np.array([1, 2, 3], dtype=np.int8)
            ),
        ],
        "constants": [
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
    assert entry.timestamp == original_entry["timestamp"]
    for group in ("variables", "constants", "attributes"):
        assert len(getattr(entry, group)) == len(original_entry[group])
        for a, b in zip(getattr(entry, group), original_entry[group]):
            assert a == b


def test_serialises_and_deserialises_da00_string():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "data": [Variable(data="hi, this is a string", dims=[""], name="the_string")],
        "timestamp": datetime.now(tz=timezone.utc),
    }

    buf = serialise_da00(**original_entry)
    entry = deserialise_da00(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp == original_entry["timestamp"]
    for group in ("variables", "constants", "attributes"):
        assert len(getattr(entry, group)) == len(original_entry[group])
        for a, b in zip(getattr(entry, group), original_entry[group]):
            assert a == b


def test_if_buffer_has_wrong_id_then_throws():
    original_entry = {
        "source_name": "some source name",
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
