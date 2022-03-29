import flatbuffers
import numpy

import streaming_data_types.fbschemas.histogram_hs01.ArrayDouble as ArrayDouble
import streaming_data_types.fbschemas.histogram_hs01.ArrayFloat as ArrayFloat
import streaming_data_types.fbschemas.histogram_hs01.ArrayInt as ArrayInt
import streaming_data_types.fbschemas.histogram_hs01.ArrayLong as ArrayLong
import streaming_data_types.fbschemas.histogram_hs01.DimensionMetaData as DimensionMetaData
import streaming_data_types.fbschemas.histogram_hs01.EventHistogram as EventHistogram
from streaming_data_types.fbschemas.histogram_hs01.Array import Array
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"hs01"


_array_for_type = {
    Array.ArrayInt: ArrayInt.ArrayInt(),
    Array.ArrayLong: ArrayLong.ArrayLong(),
    Array.ArrayFloat: ArrayFloat.ArrayFloat(),
}


def _create_array_object_for_type(array_type):
    return _array_for_type.get(array_type, ArrayDouble.ArrayDouble())


def deserialise_hs01(buffer):
    """
    Deserialise flatbuffer hs10 into a histogram.

    :param buffer:
    :return: dict of histogram information
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    event_hist = EventHistogram.EventHistogram.GetRootAsEventHistogram(buffer, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = _create_array_object_for_type(
            event_hist.DimMetadata(i).BinBoundariesType()
        )

        # Get bins
        bins_offset = event_hist.DimMetadata(i).BinBoundaries()
        bins_fb.Init(bins_offset.Bytes, bins_offset.Pos)
        bin_boundaries = bins_fb.ValueAsNumpy()

        hist_info = {
            "length": event_hist.DimMetadata(i).Length(),
            "bin_boundaries": bin_boundaries,
            "unit": event_hist.DimMetadata(i).Unit().decode("utf-8")
            if event_hist.DimMetadata(i).Unit()
            else "",
            "label": event_hist.DimMetadata(i).Label().decode("utf-8")
            if event_hist.DimMetadata(i).Label()
            else "",
        }
        dims.append(hist_info)

    metadata_timestamp = event_hist.LastMetadataTimestamp()

    data_fb = _create_array_object_for_type(event_hist.DataType())
    data_offset = event_hist.Data()
    data_fb.Init(data_offset.Bytes, data_offset.Pos)
    shape = event_hist.CurrentShapeAsNumpy().tolist()
    data = data_fb.ValueAsNumpy().reshape(shape)

    # Get the errors
    errors_offset = event_hist.Errors()
    if errors_offset:
        errors_fb = _create_array_object_for_type(event_hist.ErrorsType())
        errors_fb.Init(errors_offset.Bytes, errors_offset.Pos)
        errors = errors_fb.ValueAsNumpy().reshape(shape)
    else:
        errors = []

    hist = {
        "source": event_hist.Source().decode("utf-8") if event_hist.Source() else "",
        "timestamp": event_hist.Timestamp(),
        "current_shape": shape,
        "dim_metadata": dims,
        "data": data,
        "errors": errors,
        "last_metadata_timestamp": metadata_timestamp,
        "info": event_hist.Info().decode("utf-8") if event_hist.Info() else "",
    }
    return hist


def _serialise_metadata(builder, length, edges, unit, label):
    unit_offset = builder.CreateString(unit)
    label_offset = builder.CreateString(label)

    bins_offset, bin_type = _serialise_array(builder, edges)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, bins_offset)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, bin_type)
    DimensionMetaData.DimensionMetaDataAddLabel(builder, label_offset)
    DimensionMetaData.DimensionMetaDataAddUnit(builder, unit_offset)
    return DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs01(histogram):
    """
    Serialise a histogram as an hs01 FlatBuffers message.

    If arrays are provided as numpy arrays with type np.int32, np.int64, np.float32
    or np.float64 then type is preserved in output buffer.

    :param histogram: A dictionary containing the histogram to serialise.
    """
    source_offset = None
    info_offset = None

    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)
    if "source" in histogram:
        source_offset = builder.CreateString(histogram["source"])
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
    EventHistogram.EventHistogramStartDimMetadataVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector(rank)

    # Build the data
    data_offset, data_type = _serialise_array(builder, histogram["data"])

    errors_offset = None
    if "errors" in histogram:
        errors_offset, error_type = _serialise_array(builder, histogram["errors"])

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    if info_offset:
        EventHistogram.EventHistogramAddInfo(builder, info_offset)
    EventHistogram.EventHistogramAddData(builder, data_offset)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape_offset)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddTimestamp(builder, histogram["timestamp"])
    if source_offset:
        EventHistogram.EventHistogramAddSource(builder, source_offset)
    EventHistogram.EventHistogramAddDataType(builder, data_type)
    if errors_offset:
        EventHistogram.EventHistogramAddErrors(builder, errors_offset)
        EventHistogram.EventHistogramAddErrorsType(builder, error_type)
    if "last_metadata_timestamp" in histogram:
        EventHistogram.EventHistogramAddLastMetadataTimestamp(
            builder, histogram["last_metadata_timestamp"]
        )
    hist_message = EventHistogram.EventHistogramEnd(builder)

    builder.Finish(hist_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


def _serialise_array(builder, data):
    flattened_data = numpy.asarray(data).flatten()

    # Carefully preserve explicitly supported types
    if numpy.issubdtype(flattened_data.dtype, numpy.int32):
        return _serialise_int32(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.int64):
        return _serialise_int64(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.float32):
        return _serialise_float(builder, flattened_data)
    if numpy.issubdtype(flattened_data.dtype, numpy.float64):
        return _serialise_double(builder, flattened_data)

    # Otherwise if it looks like an int then use int64, or use double as last resort
    if numpy.issubdtype(flattened_data.dtype, numpy.int64):
        return _serialise_int64(builder, flattened_data)

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


def _serialise_int32(builder, flattened_data):
    data_type = Array.ArrayInt
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayInt.ArrayIntStart(builder)
    ArrayInt.ArrayIntAddValue(builder, data_vector)
    data_offset = ArrayInt.ArrayIntEnd(builder)
    return data_offset, data_type


def _serialise_int64(builder, flattened_data):
    data_type = Array.ArrayLong
    data_vector = builder.CreateNumpyVector(flattened_data)
    ArrayLong.ArrayLongStart(builder)
    ArrayLong.ArrayLongAddValue(builder, data_vector)
    data_offset = ArrayLong.ArrayLongEnd(builder)
    return data_offset, data_type
