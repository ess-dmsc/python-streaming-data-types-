from collections import namedtuple

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.eventdata_an44.AN44EventMessage as AN44EventMessage
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"an44"


EventData = namedtuple(
    "EventData",
    (
        "source_name",
        "message_id",
        "reference_time",
        "reference_time_index",
        "time_of_flight",
        "pixel_id",
        "weight",
    ),
)


def deserialise_an44(buffer):
    """
    Deserialise FlatBuffer an44.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    event = AN44EventMessage.AN44EventMessage.GetRootAs(buffer, 0)

    return EventData(
        event.SourceName().decode("utf-8"),
        event.MessageId(),
        event.ReferenceTimeAsNumpy(),
        event.ReferenceTimeIndexAsNumpy(),
        event.TimeOfFlightAsNumpy(),
        event.PixelIdAsNumpy(),
        event.WeightAsNumpy(),
    )


def serialise_an44(
    source_name,
    message_id,
    reference_time,
    reference_time_index,
    time_of_flight,
    pixel_id,
    weight,
):
    """
    Serialise event data as an an44 FlatBuffers message.

    :param source_name:
    :param message_id:
    :param reference_time:
    :param reference_time_index:
    :param time_of_flight:
    :param pixel_id:
    :param weight:
    :return:
    """
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    source = builder.CreateString(source_name)
    ref_time_data = builder.CreateNumpyVector(
        np.asarray(reference_time).astype(np.int64)
    )
    ref_time_index_data = builder.CreateNumpyVector(
        np.asarray(reference_time_index).astype(np.int32)
    )
    tof_data = builder.CreateNumpyVector(np.asarray(time_of_flight).astype(np.int32))
    pixel_id_data = builder.CreateNumpyVector(np.asarray(pixel_id).astype(np.int32))
    weight_data = builder.CreateNumpyVector(np.asarray(weight).astype(np.int16))

    AN44EventMessage.AN44EventMessageStart(builder)
    AN44EventMessage.AN44EventMessageAddReferenceTime(builder, ref_time_data)
    AN44EventMessage.AN44EventMessageAddReferenceTimeIndex(builder, ref_time_index_data)
    AN44EventMessage.AN44EventMessageAddTimeOfFlight(builder, tof_data)
    AN44EventMessage.AN44EventMessageAddPixelId(builder, pixel_id_data)
    AN44EventMessage.AN44EventMessageAddWeight(builder, weight_data)
    AN44EventMessage.AN44EventMessageAddMessageId(builder, message_id)
    AN44EventMessage.AN44EventMessageAddSourceName(builder, source)

    data = AN44EventMessage.AN44EventMessageEnd(builder)
    builder.Finish(data, file_identifier=FILE_IDENTIFIER)

    return bytes(builder.Output())
