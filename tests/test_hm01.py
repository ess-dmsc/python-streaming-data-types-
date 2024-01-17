from datetime import datetime, timezone

import numpy as np
import pytest

from streaming_data_types import DESERIALISERS, SERIALISERS
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.histogram_hm01 import (
    Attribute,
    BinBoundaryData,
    HistogramData,
    deserialise_hm01,
    serialise_hm01,
)


def test_serialises_and_deserialises_hm01_int_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "unique_id": 754,
        "timestamp": datetime.now(tz=timezone.utc),
        "dimensions": [
            BinBoundaryData(
                "time", np.array([13, 21], dtype=np.float32), "hours", "clock time"
            ),
            BinBoundaryData(
                "x", np.array([-1, 0, 1], dtype=np.float32), "m", "Position"
            ),
            BinBoundaryData(
                "y", np.array([0, 2, 4, 6], dtype=np.float32), "m", "Position"
            ),
        ],
        "data": HistogramData(
            unit="counts", data=np.array([[[1, 2, 3], [3, 4, 5]]], dtype=np.uint64)
        ),
        "attributes": [
            Attribute("name1", "value", "desc1", "src1"),
            Attribute("name2", 11, "desc2", "src2"),
            Attribute("name3", 3.14, "desc3", "src3"),
            Attribute("name4", np.linspace(0, 10), "desc4", "src4"),
        ],
    }

    buf = serialise_hm01(**original_entry)
    entry = deserialise_hm01(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.unique_id == original_entry["unique_id"]
    assert entry.timestamp == original_entry["timestamp"]
    assert len(entry.dimensions) == len(original_entry["dimensions"])
    for a, b in zip(entry.dimensions, original_entry["dimensions"]):
        assert a == b
    assert entry.data == original_entry["data"]
    assert entry.errors is None
    assert len(entry.attributes) == len(original_entry["attributes"])
    for a, b in zip(entry.attributes, original_entry["attributes"]):
        assert a == b


def test_serialises_and_deserialises_hm01_float_array():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some other source name",
        "unique_id": 789679,
        "data": HistogramData(
            data=np.array([[[1.1, 2.2, 3.3]], [[4.4, 5.5, 6.6]]], dtype=np.float32)
        ),
        "errors": HistogramData(data=np.array([1, 2, 3], dtype=np.int8)),
        "timestamp": datetime(
            year=1992,
            month=8,
            day=11,
            hour=3,
            minute=34,
            second=57,
            tzinfo=timezone.utc,
        ),
        "dimensions": [
            BinBoundaryData(
                name="x", unit="m", data=np.array([-1, 0, 1], dtype=np.int8)
            ),
            BinBoundaryData(
                name="time",
                unit="hours",
                label="clock time",
                data=np.array([13, 21], dtype=np.uint32),
            ),
            BinBoundaryData(name="y", data=np.array([0, 2, 4, 6], dtype=np.float64)),
        ],
    }

    buf = serialise_hm01(**original_entry)
    entry = deserialise_hm01(buf)

    assert entry.source_name == original_entry["source_name"]
    assert entry.unique_id == original_entry["unique_id"]
    assert entry.timestamp == original_entry["timestamp"]
    assert len(entry.dimensions) == len(original_entry["dimensions"])
    for a, b in zip(entry.dimensions, original_entry["dimensions"]):
        assert a == b
    assert entry.data == original_entry["data"]
    assert entry.errors is not None
    assert entry.errors == original_entry["errors"]
    assert len(entry.attributes) == 0


def test_serialises_and_deserialises_hm01_string():
    """
    Round-trip to check what we serialise is what we get back.
    """
    original_entry = {
        "source_name": "some source name",
        "unique_id": 7001,
        "data": HistogramData(data="hi, this is a string"),
        "timestamp": datetime.now(tz=timezone.utc),
    }

    buf = serialise_hm01(**original_entry)
    entry = deserialise_hm01(buf)

    assert entry.unique_id == original_entry["unique_id"]
    assert entry.source_name == original_entry["source_name"]
    assert entry.timestamp == original_entry["timestamp"]
    assert entry.data == original_entry["data"]


def test_if_buffer_has_wrong_id_then_throws():
    original_entry = {
        "source_name": "some source name",
        "unique_id": 754,
        "data": HistogramData(data=np.array([[1, 2, 3], [3, 4, 5]], dtype=np.uint64)),
        "timestamp": datetime.now(),
    }

    buf = serialise_hm01(**original_entry)

    # Manually hack the id
    buf = bytearray(buf)
    buf[4:8] = b"1234"

    with pytest.raises(WrongSchemaException):
        deserialise_hm01(buf)


def test_hm01_schema_type_is_in_global_serialisers_list():
    assert "hm01" in SERIALISERS
    assert "hm01" in DESERIALISERS
