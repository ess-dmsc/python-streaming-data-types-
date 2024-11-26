from collections import namedtuple

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.readout_data_ar51.RawReadoutMessage as RawReadoutMessage
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ar51"


RawReadoutData = namedtuple(
    "RawReadoutData",
    (
        "source_name",
        "message_id",
        "raw_data",
    ),
)


def deserialise_ar51(buffer):
    """
    Deserialize FlatBuffer ar51.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialized data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    event = RawReadoutMessage.RawReadoutMessage.GetRootAs(buffer, 0)

    return RawReadoutData(
        event.SourceName().decode("utf-8"),
        event.MessageId(),
        event.RawDataAsNumpy(),
    )


def serialise_ar51(
    source_name,
    message_id,
    raw_data,
):
    """
    Serialize data as an ar51 FlatBuffers message.

    :param source_name:
    :param message_id:
    :param raw_data:
    :return:
    """
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    source = builder.CreateString(source_name)
    raw_data_data = builder.CreateNumpyVector(np.asarray(raw_data).astype(np.ubyte))
    RawReadoutMessage.RawReadoutMessageStart(builder)
    RawReadoutMessage.RawReadoutMessageAddRawData(builder, raw_data_data)
    RawReadoutMessage.RawReadoutMessageAddMessageId(builder, message_id)
    RawReadoutMessage.RawReadoutMessageAddSourceName(builder, source)

    data = RawReadoutMessage.RawReadoutMessageEnd(builder)
    builder.Finish(data, file_identifier=FILE_IDENTIFIER)

    return bytes(builder.Output())
