from typing import Union
import flatbuffers
from streaming_data_types.fbschemas.NDAr_NDArray_schema import NDArray
from streaming_data_types.utils import check_schema_identifier
from collections import namedtuple
import time

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
    builder.Finish(nd_array_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)


nd_Array = namedtuple(
    "NDArray",
    (
        "data_type",
        "id",
        "timestamp",
        "dims",
        "data",
    ),
)


def deserialise_ndar(buffer: Union[bytearray, bytes]) -> NDArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    nd_array = NDArray.NDArray.GetRootAsNDArray(buffer, 0)
    id = nd_array.Id() if nd_array.Id() else b""
    timestamp = nd_array.TimeStamp() if nd_array.TimeStamp() else b""
    data_type = nd_array.DataType() if nd_array.DataType() else b""
    dims = nd_array.DimsAsNumpy()
    data = nd_array.PDataAsNumpy()

    return nd_Array(
        data_type=data_type,
        id=id,
        timestamp=timestamp,
        dims=dims,
        data=data,
    )
