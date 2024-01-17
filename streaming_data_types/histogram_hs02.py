from typing import List, NamedTuple, Optional, Union

import flatbuffers
import numpy

import streaming_data_types.fbschemas.histogram_hs02.ArrayDouble as ArrayDouble
import streaming_data_types.fbschemas.histogram_hs02.ArrayFloat as ArrayFloat
import streaming_data_types.fbschemas.histogram_hs02.ArrayInt8 as ArrayInt8
import streaming_data_types.fbschemas.histogram_hs02.ArrayInt16 as ArrayInt16
import streaming_data_types.fbschemas.histogram_hs02.ArrayInt32 as ArrayInt32
import streaming_data_types.fbschemas.histogram_hs02.ArrayInt64 as ArrayInt64
import streaming_data_types.fbschemas.histogram_hs02.DimensionMetaData as hs02_DimensionMetaData
import streaming_data_types.fbschemas.histogram_hs02.hs02_EventHistogram as hs02_EventHistogram
from streaming_data_types.fbschemas.histogram_hs02.Array import Array
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"hs02"


_array_for_type = {
    Array.ArrayInt8: ArrayInt8.ArrayInt8(),
    Array.ArrayInt16: ArrayInt16.ArrayInt16(),
    Array.ArrayInt32: ArrayInt32.ArrayInt32(),
    Array.ArrayInt64: ArrayInt64.ArrayInt64(),
    Array.ArrayDouble: ArrayDouble.ArrayDouble(),
    Array.ArrayFloat: ArrayFloat.ArrayFloat(),
}


ArrayDatatypes = Union[
    ArrayInt8.ArrayInt8,
    ArrayInt16.ArrayInt16,
    ArrayInt32.ArrayInt32,
    ArrayInt64.ArrayInt64,
    ArrayDouble.ArrayDouble,
    ArrayFloat.ArrayFloat,
]


DimensionMetaData = NamedTuple(
    "DimensionMetaData",
    (
        ("label", str),
        ("length", int),
        ("unit", Optional[str]),
        ("bin_boundaries", Optional[ArrayDatatypes]),
    ),
)

EventHistogram = NamedTuple(
    "EventHistogram",
    (
        ("source_name", str),
        ("current_shape", [int]),
        ("dim_metadata", List[DimensionMetaData]),
        ("timestamp", int),
        ("data", Optional[numpy.ndarray]),
        ("errors", Optional[numpy.ndarray]),
        ("offset", Optional[numpy.ndarray]),
        ("last_metadata_timestamp", int),
        ("info", Optional[str]),
    ),
)


def _create_array_object_for_type(array_type):
    return _array_for_type.get(array_type, ArrayDouble.ArrayDouble())


def deserialise_hs02(buffer):
    """
    Deserialise flatbuffer into a histogram.

    :param buffer:
    :return: dict of histogram information
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    event_hist = hs02_EventHistogram.hs02_EventHistogram.GetRootAshs02_EventHistogram(
        buffer, 0
    )

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = _create_array_object_for_type(
            event_hist.DimMetadata(i).BinBoundariesType()
        )

        # Get bins
        bins_offset = event_hist.DimMetadata(i).BinBoundaries()
        bins_fb.Init(bins_offset.Bytes, bins_offset.Pos)
        bin_boundaries = None if bins_fb.ValueIsNone() else bins_fb.ValueAsNumpy()

        hist_info = {
            "label": event_hist.DimMetadata(i).Label().decode("utf-8"),
            "length": event_hist.DimMetadata(i).Length(),
            "unit": event_hist.DimMetadata(i).Unit().decode("utf-8")
            if event_hist.DimMetadata(i).Unit()
            else None,
            "bin_boundaries": bin_boundaries,
        }
        dims.append(hist_info)

    shape = event_hist.CurrentShapeAsNumpy().tolist()

    # Data
    data = None
    data_offset = event_hist.Data()
    if data_offset:
        data_fb = _create_array_object_for_type(event_hist.DataType())
        data_fb.Init(data_offset.Bytes, data_offset.Pos)
        data = None if data_fb.ValueIsNone() else data_fb.ValueAsNumpy().reshape(shape)

    # Errors
    errors = None
    errors_offset = event_hist.Errors()
    if errors_offset:
        errors_fb = _create_array_object_for_type(event_hist.ErrorsType())
        errors_fb.Init(errors_offset.Bytes, errors_offset.Pos)
        errors = (
            None if errors_fb.ValueIsNone() else errors_fb.ValueAsNumpy().reshape(shape)
        )

    # Offset
    offset = None if event_hist.OffsetIsNone() else event_hist.OffsetAsNumpy()

    return EventHistogram(
        source_name=event_hist.SourceName().decode("utf-8"),
        current_shape=shape,
        dim_metadata=dims,
        timestamp=event_hist.Timestamp(),
        data=data,
        errors=errors,
        offset=offset,
        last_metadata_timestamp=event_hist.LastMetadataTimestamp(),
        info=event_hist.Info().decode("utf-8") if event_hist.Info() else None,
    )


def _serialise_metadata(builder, length, edges, unit, label):
    unit_offset = builder.CreateString(unit)
    label_offset = builder.CreateString(label)

    bins_offset, bin_type = _serialise_array(builder, edges)

    hs02_DimensionMetaData.DimensionMetaDataStart(builder)
    hs02_DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    hs02_DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, bins_offset)
    hs02_DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, bin_type)
    hs02_DimensionMetaData.DimensionMetaDataAddLabel(builder, label_offset)
    hs02_DimensionMetaData.DimensionMetaDataAddUnit(builder, unit_offset)
    return hs02_DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs02(histogram):
    """
    Serialise a histogram as an hs02 FlatBuffers message.

    If arrays are provided as numpy arrays with type np.int32, np.int64, np.float32
    or np.float64 then type is preserved in output buffer.

    :param histogram: A dictionary containing the histogram to serialise.
    """
    source_name_offset = None
    info_offset = None

    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)
    if "source_name" in histogram:
        source_name_offset = builder.CreateString(histogram["source_name"])
    if "info" in histogram:
        info_offset = builder.CreateString(histogram["info"])

    # Build shape array
    shape_offset = builder.CreateNumpyVector(
        numpy.array(histogram["current_shape"]).astype(numpy.int32)
    )

    # Build dimensions metadata
    metadata = []
    for meta in histogram["dim_metadata"]:
        unit = "" if "unit" not in meta else meta["unit"]
        label = "" if "label" not in meta else meta["label"]
        metadata.append(
            _serialise_metadata(
                builder, meta["length"], meta["bin_boundaries"], unit, label
            )
        )

    rank = len(histogram["current_shape"])
    hs02_EventHistogram.hs02_EventHistogramStartDimMetadataVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector()

    # Build the data
    data_offset, data_type = _serialise_array(builder, histogram["data"])

    errors_offset = None
    if "errors" in histogram:
        errors_offset, error_type = _serialise_array(builder, histogram["errors"])

    # Build the actual buffer
    hs02_EventHistogram.hs02_EventHistogramStart(builder)
    if info_offset:
        hs02_EventHistogram.hs02_EventHistogramAddInfo(builder, info_offset)
    hs02_EventHistogram.hs02_EventHistogramAddData(builder, data_offset)
    hs02_EventHistogram.hs02_EventHistogramAddCurrentShape(builder, shape_offset)
    hs02_EventHistogram.hs02_EventHistogramAddDimMetadata(builder, metadata_vector)
    hs02_EventHistogram.hs02_EventHistogramAddTimestamp(builder, histogram["timestamp"])
    if source_name_offset:
        hs02_EventHistogram.hs02_EventHistogramAddSourceName(
            builder, source_name_offset
        )
    hs02_EventHistogram.hs02_EventHistogramAddDataType(builder, data_type)
    if errors_offset:
        hs02_EventHistogram.hs02_EventHistogramAddErrors(builder, errors_offset)
        hs02_EventHistogram.hs02_EventHistogramAddErrorsType(builder, error_type)
    if "last_metadata_timestamp" in histogram:
        hs02_EventHistogram.hs02_EventHistogramAddLastMetadataTimestamp(
            builder, histogram["last_metadata_timestamp"]
        )
    hist_message = hs02_EventHistogram.hs02_EventHistogramEnd(builder)

    builder.Finish(hist_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


def _serialise_array(builder, data):
    flattened_data = numpy.asarray(data).flatten()

    # Carefully preserve explicitly supported types
    if numpy.issubdtype(flattened_data.dtype, numpy.int8):
        return _serialise_int8(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.int16):
        return _serialise_int16(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.int32):
        return _serialise_int32(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.int64):
        return _serialise_int64(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.float32):
        return _serialise_float(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.float64):
        return _serialise_double(builder, flattened_data)

    # Otherwise use double as last resort
    return _serialise_double(builder, flattened_data)


def _serialise_float(builder, flattened_data):
    data_type = Array.ArrayFloat
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayFloat.ArrayFloatStart(builder)
    ArrayFloat.ArrayFloatAddValue(builder, data_vector)
    data_offset = ArrayFloat.ArrayFloatEnd(builder)
    return data_offset, data_type


def _serialise_double(builder, flattened_data):
    data_type = Array.ArrayDouble
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, data_vector)
    data_offset = ArrayDouble.ArrayDoubleEnd(builder)
    return data_offset, data_type


def _serialise_int8(builder, flattened_data):
    data_type = Array.ArrayInt8
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayInt8.ArrayInt8Start(builder)
    ArrayInt8.ArrayInt8AddValue(builder, data_vector)
    data_offset = ArrayInt8.ArrayInt8End(builder)
    return data_offset, data_type


def _serialise_int16(builder, flattened_data):
    data_type = Array.ArrayInt16
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayInt16.ArrayInt16Start(builder)
    ArrayInt16.ArrayInt16AddValue(builder, data_vector)
    data_offset = ArrayInt16.ArrayInt16End(builder)
    return data_offset, data_type


def _serialise_int32(builder, flattened_data):
    data_type = Array.ArrayInt32
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayInt32.ArrayInt32Start(builder)
    ArrayInt32.ArrayInt32AddValue(builder, data_vector)
    data_offset = ArrayInt32.ArrayInt32End(builder)
    return data_offset, data_type


def _serialise_int64(builder, flattened_data):
    data_type = Array.ArrayInt64
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayInt64.ArrayInt64Start(builder)
    ArrayInt64.ArrayInt64AddValue(builder, data_vector)
    data_offset = ArrayInt64.ArrayInt64End(builder)
    return data_offset, data_type
