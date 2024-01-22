from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NamedTuple

import flatbuffers
import numpy

import streaming_data_types.fbschemas.dataarray_da00.Attribute as AttributeBuffer
import streaming_data_types.fbschemas.dataarray_da00.Variable as VariableBuffer
from streaming_data_types.fbschemas.dataarray_da00 import da00_DataArray
from streaming_data_types.fbschemas.dataarray_da00.DType import DType as DT
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"da00"


def get_dtype(data: numpy.ndarray | str | float | int):
    from numpy import ndarray

    if isinstance(data, ndarray):
        type_map = {
            numpy.dtype(x): d
            for x, d in (
                ("int8", DT.int8),
                ("int16", DT.int16),
                ("int32", DT.int32),
                ("int64", DT.int64),
                ("uint8", DT.uint8),
                ("uint16", DT.uint16),
                ("uint32", DT.uint32),
                ("uint64", DT.uint64),
                ("float32", DT.float32),
                ("float64", DT.float64),
            )
        }
        return type_map[data.dtype]
    if isinstance(data, str):
        return DT.c_string
    if isinstance(data, float):
        return DT.float64
    if isinstance(data, int):
        return DT.int64
    raise RuntimeError(f"Unsupported data type {type(data)} in get_dtype")


def to_buffer(data: numpy.ndarray | str | float | int):
    from struct import pack

    from numpy import frombuffer, ndarray, uint8

    if isinstance(data, ndarray):
        return data
    if isinstance(data, str):
        return frombuffer(data.encode(), uint8)
    if isinstance(data, int):
        return frombuffer(pack("q", data), uint8)
    if isinstance(data, float):
        return frombuffer(pack("d", data), uint8)
    raise RuntimeError(f"Unsupported data type {type(data)} in to_buffer")


def from_buffer(fb_array) -> numpy.ndarray:
    """Convert a flatbuffer array into the correct type"""
    raw_data = fb_array.DataAsNumpy()
    type_map = {
        d: numpy.dtype(x)
        for x, d in (
            ("int8", DT.int8),
            ("int16", DT.int16),
            ("int32", DT.int32),
            ("int64", DT.int64),
            ("uint8", DT.uint8),
            ("uint16", DT.uint16),
            ("uint32", DT.uint32),
            ("uint64", DT.uint64),
            ("float32", DT.float32),
            ("float64", DT.float64),
        )
    }
    return raw_data.view(type_map[fb_array.DataType()])


def create_optional_string(builder, string: str | None):
    return None if string is None else builder.CreateString(string)


@dataclass
class Attribute:
    name: str
    data: numpy.ndarray | str | int | float
    description: str | None = None
    source: str | None = None

    def __eq__(self, other):
        if not isinstance(other, Attribute):
            return False
        data_is_equal = type(self.data) == type(other.data)  # noqa: E721
        if type(self.data) is numpy.ndarray:
            data_is_equal = data_is_equal and numpy.array_equal(self.data, other.data)
        else:
            data_is_equal = data_is_equal and self.data == other.data
        return (
            data_is_equal
            and self.name == other.name
            and self.description == other.description
            and self.source == other.source
        )

    def pack(self, builder):
        from numpy import uint8

        import streaming_data_types.fbschemas.dataarray_da00.Attribute as Buffer

        name_offset = builder.CreateString(self.name)
        description_offset = create_optional_string(builder, self.description)
        source_offset = create_optional_string(builder, self.source)
        data_offset = builder.CreateNumpyVector(
            to_buffer(self.data).flatten().view(uint8)
        )
        Buffer.AttributeStart(builder)
        Buffer.AttributeAddName(builder, name_offset)
        if description_offset is not None:
            Buffer.AttributeAddDescription(builder, description_offset)
        if source_offset is not None:
            Buffer.AttributeAddSource(builder, source_offset)
        Buffer.AttributeAddDataType(builder, get_dtype(self.data))
        Buffer.AttributeAddData(builder, data_offset)
        return Buffer.AttributeEnd(builder)

    @classmethod
    def unpack(cls, b: AttributeBuffer):
        data = (
            b.DataAsNumpy().tobytes().decode()
            if b.DataType() == DT.c_string
            else from_buffer(b)
        )
        if isinstance(data, numpy.ndarray) and len(data) == 1:
            if numpy.issubdtype(data.dtype, numpy.floating):
                data = float(data[0])
            elif numpy.issubdtype(data.dtype, numpy.integer):
                data = int(data[0])
        source = None if b.Source() is None else b.Source().decode()
        description = None if b.Description() is None else b.Description().decode()
        name = b.Name().decode()
        return cls(name=name, description=description, source=source, data=data)


@dataclass
class Variable:
    name: str
    data: numpy.ndarray | str
    dims: list[str]
    shape: tuple[int, ...] | None = None
    unit: str | None = None
    label: str | None = None

    def __post_init__(self):
        # Calculate the shape when used, e.g., interactively
        # -- but allow to read it back from the buffered object too
        if self.shape is None:
            self.shape = to_buffer(self.data).shape

    def __eq__(self, other):
        if not isinstance(other, Variable):
            return False
        same_data = type(self.data) == type(other.data)  # noqa: E721
        if isinstance(self.data, numpy.ndarray):
            same_data &= numpy.array_equal(self.data, other.data)
        else:
            same_data &= self.data == other.data
        same_dims = len(self.dims) == len(other.dims) and all(
            a == b for a, b in zip(self.dims, other.dims)
        )
        return (
            same_data
            and same_dims
            and self.name == other.name
            and self.unit == other.unit
            and self.label == other.label
            and self.shape == other.shape
        )

    def pack(self, builder):
        from numpy import asarray, uint8

        import streaming_data_types.fbschemas.dataarray_da00.Variable as Buffer

        # insert pieces into the blob -- does this order matter?
        label_offset = create_optional_string(builder, self.label)
        unit_offset = create_optional_string(builder, self.unit)
        name_offset = builder.CreateString(self.name)
        buf = to_buffer(self.data)
        shape_offset = builder.CreateNumpyVector(asarray(buf.shape))
        data_offset = builder.CreateNumpyVector(buf.flatten().view(uint8))

        temp_dims = [builder.CreateString(x) for x in self.dims]
        Buffer.StartDimsVector(builder, len(temp_dims))
        for dim in reversed(temp_dims):
            builder.PrependUOffsetTRelative(dim)
        dims_offset = builder.EndVector()

        # Build the actual Variable buffer -- does this order matter?
        Buffer.Start(builder)
        Buffer.AddName(builder, name_offset)
        if unit_offset is not None:
            Buffer.AddUnit(builder, unit_offset)
        if label_offset is not None:
            Buffer.AddLabel(builder, label_offset)
        Buffer.AddDataType(builder, get_dtype(self.data))
        Buffer.AddDims(builder, dims_offset)
        Buffer.AddShape(builder, shape_offset)
        Buffer.AddData(builder, data_offset)
        return Buffer.End(builder)

    @classmethod
    def unpack(cls, b: VariableBuffer):
        data = (
            b.DataAsNumpy().tobytes().decode()
            if b.DataType() == DT.c_string
            else from_buffer(b).reshape(b.ShapeAsNumpy())
        )
        unit = None if b.Unit() is None else b.Unit().decode()
        label = None if b.Label() is None else b.Label().decode()
        name = b.Name().decode()
        dims = [b.Dims(i).decode() for i in range(b.DimsLength())]
        shape = tuple(b.ShapeAsNumpy())
        return cls(name=name, unit=unit, label=label, dims=dims, data=data, shape=shape)


def serialise_da00(
    source_name: str,
    unique_id: int,
    timestamp: datetime,
    data: list[Variable],
    attributes: list[Attribute] | None = None,
) -> bytes:
    import streaming_data_types.fbschemas.dataarray_da00.da00_DataArray as Buffer

    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    # Build data
    temp_data = [datum.pack(builder) for datum in data]
    Buffer.StartDataVector(builder, len(temp_data))
    for datum in reversed(temp_data):
        builder.PrependUOffsetTRelative(datum)
    data_offset = builder.EndVector()

    # Build attributes
    temp_attributes = (
        None if attributes is None else [item.pack(builder) for item in attributes]
    )
    if temp_attributes is not None:
        Buffer.StartAttributesVector(builder, len(attributes))
        for item in reversed(temp_attributes):
            builder.PrependUOffsetTRelative(item)
    attributes_offset = None if temp_attributes is None else builder.EndVector()

    source_name_offset = builder.CreateString(source_name)

    # Build the actual buffer
    Buffer.Start(builder)
    Buffer.AddSourceName(builder, source_name_offset)
    Buffer.AddId(builder, unique_id)
    Buffer.AddTimestamp(builder, int(timestamp.timestamp() * 1e9))
    Buffer.AddData(builder, data_offset)
    if attributes_offset is not None:
        Buffer.AddAttributes(builder, attributes_offset)
    array_message = Buffer.End(builder)

    builder.Finish(array_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


da00_DataArray_t = NamedTuple(
    "da00_DataArray",
    (
        ("source_name", str),
        ("unique_id", int),
        ("timestamp", datetime),
        ("data", list[Variable]),
        ("attributes", list[Attribute]),
    ),
)


def deserialise_da00(buffer: bytearray | bytes) -> da00_DataArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    da00 = da00_DataArray.da00_DataArray.GetRootAs(buffer, offset=0)
    unique_id = da00.Id()
    max_time = datetime(
        year=3001, month=1, day=1, hour=0, minute=0, second=0
    ).timestamp()
    used_timestamp = da00.Timestamp() / 1e9
    if used_timestamp > max_time:
        used_timestamp = max_time
    data = [Variable.unpack(da00.Data(j)) for j in range(da00.DataLength())]
    attributes = [
        Attribute.unpack(da00.Attributes(i)) for i in range(da00.AttributesLength())
    ]

    return da00_DataArray_t(
        source_name=da00.SourceName().decode(),
        unique_id=unique_id,
        timestamp=datetime.fromtimestamp(used_timestamp, tz=timezone.utc),
        data=data,
        attributes=attributes,
    )
