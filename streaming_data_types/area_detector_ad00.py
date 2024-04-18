from struct import pack
from typing import List, NamedTuple, Union

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.area_detector_ad00.Attribute as ADArAttribute
from streaming_data_types.fbschemas.area_detector_ad00 import ad00_ADArray
from streaming_data_types.fbschemas.area_detector_ad00.DType import DType
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ad00"


class Attribute:
    def __init__(
        self,
        name: str,
        description: str,
        source: str,
        data: Union[np.ndarray, str, int, float],
    ):
        self.name = name
        self.description = description
        self.source = source
        self.data = data

    def __eq__(self, other):
        data_is_equal = type(self.data) == type(other.data)  # noqa: E721
        if type(self.data) is np.ndarray:
            data_is_equal = data_is_equal and np.array_equal(self.data, other.data)
        else:
            data_is_equal = data_is_equal and self.data == other.data
        return (
            self.name == other.name
            and self.description == other.description
            and self.source == other.source
            and data_is_equal
        )


def serialise_ad00(
    source_name: str,
    unique_id: int,
    timestamp_ns: int,
    data: Union[np.ndarray, str],
    attributes: List[Attribute] = [],
) -> bytes:
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    type_map = {
        np.dtype("uint8"): DType.uint8,
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
    dims_offset = builder.CreateNumpyVector(np.asarray(data.shape))

    # Build data
    data_offset = builder.CreateNumpyVector(data.flatten().view(np.uint8))

    source_name_offset = builder.CreateString(source_name)

    temp_attributes = []
    for item in attributes:
        if type(item.data) is np.ndarray:
            attr_data_type = type_map[item.data.dtype]
            attr_data = item.data
        elif type(item.data) is str:
            attr_data_type = DType.c_string
            attr_data = np.frombuffer(item.data.encode(), np.uint8)
        elif type(item.data) is int:
            attr_data_type = DType.int64
            attr_data = np.frombuffer(pack("q", item.data), np.uint8)
        elif type(item.data) is float:
            attr_data_type = DType.float64
            attr_data = np.frombuffer(pack("d", item.data), np.uint8)
        attr_name_offset = builder.CreateString(item.name)
        attr_desc_offset = builder.CreateString(item.description)
        attr_src_offset = builder.CreateString(item.source)
        attr_data_offset = builder.CreateNumpyVector(attr_data.flatten().view(np.uint8))
        ADArAttribute.AttributeStart(builder)
        ADArAttribute.AttributeAddName(builder, attr_name_offset)
        ADArAttribute.AttributeAddDescription(builder, attr_desc_offset)
        ADArAttribute.AttributeAddSource(builder, attr_src_offset)
        ADArAttribute.AttributeAddDataType(builder, attr_data_type)
        ADArAttribute.AttributeAddData(builder, attr_data_offset)
        attr_offset = ADArAttribute.AttributeEnd(builder)
        temp_attributes.append(attr_offset)

    ad00_ADArray.ad00_ADArrayStartAttributesVector(builder, len(attributes))
    for item in reversed(temp_attributes):
        builder.PrependUOffsetTRelative(item)
    attributes_offset = builder.EndVector()

    # Build the actual buffer
    ad00_ADArray.ad00_ADArrayStart(builder)
    ad00_ADArray.ad00_ADArrayAddSourceName(builder, source_name_offset)
    ad00_ADArray.ad00_ADArrayAddDataType(builder, data_type)
    ad00_ADArray.ad00_ADArrayAddDimensions(builder, dims_offset)
    ad00_ADArray.ad00_ADArrayAddId(builder, unique_id)
    ad00_ADArray.ad00_ADArrayAddData(builder, data_offset)
    ad00_ADArray.ad00_ADArrayAddTimestamp(builder, timestamp_ns)
    ad00_ADArray.ad00_ADArrayAddAttributes(builder, attributes_offset)
    array_message = ad00_ADArray.ad00_ADArrayEnd(builder)

    builder.Finish(array_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


ADArray = NamedTuple(
    "ADArray",
    (
        ("source_name", str),
        ("unique_id", int),
        ("timestamp_ns", int),
        ("dimensions", np.ndarray),
        ("data", np.ndarray),
        ("attributes", List[Attribute]),
    ),
)


def get_payload_data(fb_arr) -> np.ndarray:
    return get_data(fb_arr).reshape(fb_arr.DimensionsAsNumpy())


def get_data(fb_arr) -> np.ndarray:
    """
    Converts the data array into the correct type.
    """
    raw_data = fb_arr.DataAsNumpy()
    type_map = {
        DType.uint8: np.uint8,
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
    return raw_data.view(type_map[fb_arr.DataType()])


def deserialise_ad00(buffer: Union[bytearray, bytes]) -> ADArray:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    ad_array = ad00_ADArray.ad00_ADArray.GetRootAsad00_ADArray(buffer, 0)
    unique_id = ad_array.Id()
    if ad_array.DataType() == DType.c_string:
        data = ad_array.DataAsNumpy().tobytes().decode()
    else:
        data = get_payload_data(ad_array)

    attributes_list = []
    for i in range(ad_array.AttributesLength()):
        attribute_ptr = ad_array.Attributes(i)
        if attribute_ptr.DataType() == DType.c_string:
            attr_data = attribute_ptr.DataAsNumpy().tobytes().decode()
        else:
            attr_data = get_data(attribute_ptr)
        temp_attribute = Attribute(
            name=attribute_ptr.Name().decode(),
            description=attribute_ptr.Description().decode(),
            source=attribute_ptr.Source().decode(),
            data=attr_data,
        )
        if type(temp_attribute.data) is np.ndarray and len(temp_attribute.data) == 1:
            if np.issubdtype(temp_attribute.data.dtype, np.floating):
                temp_attribute.data = float(temp_attribute.data[0])
            elif np.issubdtype(temp_attribute.data.dtype, np.integer):
                temp_attribute.data = int(temp_attribute.data[0])
        attributes_list.append(temp_attribute)

    return ADArray(
        source_name=ad_array.SourceName().decode(),
        unique_id=unique_id,
        timestamp_ns=ad_array.Timestamp(),
        dimensions=tuple(ad_array.DimensionsAsNumpy()),
        data=data,
        attributes=attributes_list,
    )
