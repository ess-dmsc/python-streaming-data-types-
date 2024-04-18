from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NamedTuple

import flatbuffers
import numpy

import streaming_data_types.fbschemas.dataarray_da00.da00_Variable as VariableBuffer
from streaming_data_types.fbschemas.dataarray_da00 import da00_DataArray
from streaming_data_types.fbschemas.dataarray_da00.da00_dtype import da00_dtype
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"da00"


def get_dtype(data: numpy.ndarray | str | float | int):
    from numpy import ndarray

    if isinstance(data, ndarray):
        type_map = {
            numpy.dtype(x): d
            for x, d in (
                ("int8", da00_dtype.int8),
                ("int16", da00_dtype.int16),
                ("int32", da00_dtype.int32),
                ("int64", da00_dtype.int64),
                ("uint8", da00_dtype.uint8),
                ("uint16", da00_dtype.uint16),
                ("uint32", da00_dtype.uint32),
                ("uint64", da00_dtype.uint64),
                ("float32", da00_dtype.float32),
                ("float64", da00_dtype.float64),
            )
        }
        return type_map[data.dtype]
    if isinstance(data, str):
        return da00_dtype.c_string
    if isinstance(data, float):
        return da00_dtype.float64
    if isinstance(data, int):
        return da00_dtype.int64
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
            ("int8", da00_dtype.int8),
            ("int16", da00_dtype.int16),
            ("int32", da00_dtype.int32),
            ("int64", da00_dtype.int64),
            ("uint8", da00_dtype.uint8),
            ("uint16", da00_dtype.uint16),
            ("uint32", da00_dtype.uint32),
            ("uint64", da00_dtype.uint64),
            ("float32", da00_dtype.float32),
            ("float64", da00_dtype.float64),
        )
    }
    dtype = fb_array.DataType()
    if da00_dtype.c_string == dtype:
        return raw_data.tobytes().decode()
    return raw_data.view(type_map[fb_array.DataType()])


def create_optional_string(builder, string: str | None):
    return None if string is None else builder.CreateString(string)


@dataclass
class Variable:
    name: str
    data: numpy.ndarray | str
    axes: list[str] | None = None
    shape: tuple[int, ...] | None = None
    unit: str | None = None
    label: str | None = None
    source: str | None = None

    def __post_init__(self):
        # Calculate the shape when used, e.g., interactively
        # -- but allow to read it back from the buffered object too
        if self.axes is None:
            self.axes = []
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
        same_axes = len(self.axes) == len(other.axes) and all(
            a == b for a, b in zip(self.axes, other.axes)
        )
        return (
            same_data
            and same_axes
            and self.name == other.name
            and self.unit == other.unit
            and self.label == other.label
            and self.source == other.source
            and self.shape == other.shape
        )

    def pack(self, builder):
        from numpy import asarray, uint8

        import streaming_data_types.fbschemas.dataarray_da00.da00_Variable as Buffer

        source_offset = create_optional_string(builder, self.source)
        label_offset = create_optional_string(builder, self.label)
        unit_offset = create_optional_string(builder, self.unit)
        name_offset = builder.CreateString(self.name)
        buf = to_buffer(self.data)
        shape_offset = builder.CreateNumpyVector(asarray(buf.shape))
        data_offset = builder.CreateNumpyVector(buf.flatten().view(uint8))

        temp_axes = [builder.CreateString(x) for x in self.axes]
        Buffer.StartAxesVector(builder, len(temp_axes))
        for dim in reversed(temp_axes):
            builder.PrependUOffsetTRelative(dim)
        axes_offset = builder.EndVector()

        Buffer.Start(builder)
        Buffer.AddName(builder, name_offset)
        if unit_offset is not None:
            Buffer.AddUnit(builder, unit_offset)
        if label_offset is not None:
            Buffer.AddLabel(builder, label_offset)
        if source_offset is not None:
            Buffer.AddSource(builder, source_offset)
        Buffer.AddDataType(builder, get_dtype(self.data))
        Buffer.AddAxes(builder, axes_offset)
        Buffer.AddShape(builder, shape_offset)
        Buffer.AddData(builder, data_offset)
        return Buffer.End(builder)

    @classmethod
    def unpack(cls, b: VariableBuffer):
        data = from_buffer(b)
        axes = [b.Axes(i).decode() for i in range(b.AxesLength())]
        if len(axes):
            data = data.reshape(b.ShapeAsNumpy())
        elif b.DataType() != da00_dtype.c_string and numpy.prod(data.shape) == 1:
            data = data.item()

        unit = None if b.Unit() is None else b.Unit().decode()
        label = None if b.Label() is None else b.Label().decode()
        source = None if b.Source() is None else b.Source().decode()
        name = b.Name().decode()
        # the buffered shape is NOT the shape of the numpy array in all cases
        buffered_shape = tuple(b.ShapeAsNumpy())
        return cls(
            name=name,
            unit=unit,
            label=label,
            source=source,
            axes=axes,
            data=data,
            shape=buffered_shape,
        )


def insert_variable_list(starter, builder, objects: list[Variable] | None):
    if objects is None:
        return None
    temp = [obj.pack(builder) for obj in objects]
    starter(builder, len(temp))
    for obj in reversed(temp):
        builder.PrependUOffsetTRelative(obj)
    return builder.EndVector()


def serialise_da00(
    source_name: str,
    timestamp: datetime,
    data: list[Variable],
) -> bytes:
    import streaming_data_types.fbschemas.dataarray_da00.da00_DataArray as Buffer

    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    # build variables
    data_offset = insert_variable_list(Buffer.StartDataVector, builder, data)

    source_name_offset = builder.CreateString(source_name)

    # Build the actual buffer
    Buffer.Start(builder)
    Buffer.AddSourceName(builder, source_name_offset)
    if timestamp.tzinfo is None or timestamp.tzinfo != timezone.utc:
        timestamp = timestamp.astimezone(timezone.utc)
    Buffer.AddTimestamp(builder, int(timestamp.timestamp() * 1_000_000_000))
    if data_offset is not None:
        Buffer.AddData(builder, data_offset)
    array_message = Buffer.End(builder)

    builder.Finish(array_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


da00_DataArray_t = NamedTuple(
    "da00_DataArray",
    (
        ("source_name", str),
        ("timestamp", datetime),
        ("data", list[Variable]),
    ),
)


def deserialise_da00(buffer: bytearray | bytes) -> da00_DataArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    da00 = da00_DataArray.da00_DataArray.GetRootAs(buffer, offset=0)
    max_time = datetime(
        year=3001, month=1, day=1, hour=0, minute=0, second=0
    ).timestamp()
    used_timestamp = da00.Timestamp() / 1_000_000_000
    if used_timestamp > max_time:
        used_timestamp = max_time
    data = [Variable.unpack(da00.Data(j)) for j in range(da00.DataLength())]

    return da00_DataArray_t(
        source_name=da00.SourceName().decode(),
        timestamp=datetime.fromtimestamp(used_timestamp, tz=timezone.utc),
        data=data,
    )
