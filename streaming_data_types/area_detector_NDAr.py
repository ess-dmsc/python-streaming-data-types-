import time
from collections import namedtuple
from typing import Union

import flatbuffers
import numpy as np

from streaming_data_types.fbschemas.NDAr_NDArray_schema import NDArray
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"NDAr"


def serialise_ndar(
    id: str,
    dims: list,
    data_type: int,
    data: list,
) -> bytes:
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    # Build dims
    NDArray.NDArrayStartDimsVector(builder, len(dims))
    # FlatBuffers builds arrays backwards
    for s in reversed(dims):
        builder.PrependUint64(s)
    dims_offset = builder.EndVector(len(dims))

    # Build data
    NDArray.NDArrayStartPDataVector(builder, len(data))
    # FlatBuffers builds arrays backwards
    for s in reversed(data):
        builder.PrependUint8(s)
    data_offset = builder.EndVector(len(data))

    # Build the actual buffer
    NDArray.NDArrayStart(builder)
    NDArray.NDArrayAddDataType(builder, data_type)
    NDArray.NDArrayAddDims(builder, dims_offset)
    NDArray.NDArrayAddId(builder, id)
    NDArray.NDArrayAddPData(builder, data_offset)
    NDArray.NDArrayAddTimeStamp(builder, int(time.time() * 1000))
    nd_array_message = NDArray.NDArrayEnd(builder)

    builder.Finish(nd_array_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


nd_Array = namedtuple(
    "NDArray",
    (
        "id",
        "timestamp",
        "data",
    ),
)


def get_data(fb_arr):
    """
    Converts the data array into the correct type.
    """
    raw_data = fb_arr.PDataAsNumpy()
    numpy_arr_type = [
        np.int8,
        np.uint8,
        np.int16,
        np.uint16,
        np.int32,
        np.uint32,
        np.int64,
        np.uint64,
        np.float32,
        np.float64,
    ]
    return raw_data.view(numpy_arr_type[fb_arr.DataType()]).reshape(
        fb_arr.DimsAsNumpy()
    )


def deserialise_ndar(buffer: Union[bytearray, bytes]) -> NDArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    nd_array = NDArray.NDArray.GetRootAsNDArray(buffer, 0)
    id = nd_array.Id()
    timestamp = nd_array.TimeStamp()
    data = get_data(nd_array)

    return nd_Array(
        id=id,
        timestamp=timestamp,
        data=data,
    )
