from typing import Union, NamedTuple, List
import flatbuffers
from streaming_data_types.fbschemas.ADAr_ADArray_schema import ADArray
from streaming_data_types.fbschemas.ADAr_ADArray_schema.DType import DType
from streaming_data_types.utils import check_schema_identifier
import numpy as np
from datetime import datetime

FILE_IDENTIFIER = b"ADAr"

Attribute = NamedTuple(
    "Attribute", (
        ("name", str),
        ("description", str),
        ("source", str),
        ("data", Union[np.ndarray, str])
    )
)


def serialise_ADAr(
    source_name: str,
    unique_id: int,
    timestamp: datetime,
    data: Union[np.ndarray, str],
    attributes: List[Attribute] = []
) -> bytes:
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    type_map = {np.dtype("uint8"): DType.uint8,
                np.dtype("int8"): DType.int8,
                np.dtype("uint16"): DType.uint16,
                np.dtype("int16"): DType.int16,
                np.dtype("uint32"): DType.uint32,
                np.dtype("int32"): DType.int32,
                np.dtype("uint64"): DType.uint64,
                np.dtype("int64"): DType.int64,
                np.dtype("float32"): DType.float32,
                np.dtype("float64"): DType.float64,
                }

    if type(data) is str:
        data = np.frombuffer(data.encode(), np.uint8)
        data_type = DType.c_string
    else:
        data_type = type_map[data.dtype]

    # Build dims
    dims_offset = builder.CreateNumpyVector(np.array(data.shape))

    # Build data
    data_offset = builder.CreateNumpyVector(data.flatten().view(np.uint8))

    source_name_offset = builder.CreateString(source_name)

    # Build the actual buffer
    ADArray.ADArrayStart(builder)
    ADArray.ADArrayAddSourceName(builder, source_name_offset)
    ADArray.ADArrayAddDataType(builder, data_type)
    ADArray.ADArrayAddDimensions(builder, dims_offset)
    ADArray.ADArrayAddId(builder, unique_id)
    ADArray.ADArrayAddData(builder, data_offset)
    ADArray.ADArrayAddTimestamp(builder, int(timestamp.timestamp()*1e9))
    array_message = ADArray.ADArrayEnd(builder)

    builder.Finish(array_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


ADArray_t= NamedTuple(
    "ADArray",
    (
        ("source_name", str),
        ("unique_id", int),
        ("timestamp", datetime),
        ("data", np.ndarray),
        ("attributes", List[Attribute])
    ),
)


def get_data(fb_arr):
    """
    Converts the data array into the correct type.
    """
    raw_data = fb_arr.DataAsNumpy()
    type_map = {DType.uint8: np.uint8,
                DType.int8: np.int8,
                DType.uint16: np.uint16,
                DType.int16: np.int16,
                DType.uint32: np.uint32,
                DType.int32: np.int32,
                DType.uint64: np.uint64,
                DType.int64: np.int64,
                DType.float32: np.float32,
                DType.float64: np.float64,
                }
    return raw_data.view(type_map[fb_arr.DataType()]).reshape(fb_arr.DimensionsAsNumpy())


def deserialise_ADAr(buffer: Union[bytearray, bytes]) -> ADArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    ad_array = ADArray.ADArray.GetRootAsADArray(buffer, 0)
    unique_id = ad_array.Id()
    max_time = datetime(year=9000, month=1, day=1, hour=0, minute=0, second=0).timestamp()
    used_timestamp = ad_array.Timestamp() / 1e9
    if used_timestamp > max_time:
        used_timestamp = max_time
    if ad_array.DataType() == DType.c_string:
        data = ad_array.DataAsNumpy().tobytes().decode()
    else:
        data = get_data(ad_array)

    return ADArray_t(
        source_name=ad_array.SourceName().decode(),
        unique_id=unique_id,
        timestamp=datetime.fromtimestamp(used_timestamp),
        data=data,
        attributes=[]
    )
