from typing import Union
from streaming_data_types.fbschemas.NDAr_NDArray_schema import NDArray, NDAttribute
from streaming_data_types.utils import check_schema_identifier
from collections import namedtuple

FILE_IDENTIFIER = b"NDAr"


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
