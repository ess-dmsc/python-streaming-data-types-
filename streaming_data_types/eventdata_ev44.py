from collections import namedtuple

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.eventdata_ev44.Event44Message as Event44Message
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ev44"


EventData = namedtuple(
    "EventData",
    (
        "source_name",
        "message_id",
        "reference_time",
        "reference_time_index",
        "time_of_flight",
        "pixel_id",
    ),
)


def deserialise_ev44(buffer):
    """
    Deserialise FlatBuffer ev44.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    event = Event44Message.Event44Message.GetRootAs(buffer, 0)

    return EventData(
        event.SourceName().decode("utf-8"),
        event.MessageId(),
        event.ReferenceTimeAsNumpy(),
        event.ReferenceTimeIndexAsNumpy(),
        event.TimeOfFlightAsNumpy(),
        event.PixelIdAsNumpy(),
    )


def serialise_ev44(
    source_name,
    message_id,
    reference_time,
    reference_time_index,
    time_of_flight,
    pixel_id,
):
    """
    Serialise event data as an ev44 FlatBuffers message.

    :param source_name:
    :param message_id:
    :param reference_time:
    :param reference_time_index:
    :param time_of_flight:
    :param pixel_id:
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

    Event44Message.Event44MessageStart(builder)
    Event44Message.Event44MessageAddReferenceTime(builder, ref_time_data)
    Event44Message.Event44MessageAddReferenceTimeIndex(builder, ref_time_index_data)
    Event44Message.Event44MessageAddTimeOfFlight(builder, tof_data)
    Event44Message.Event44MessageAddPixelId(builder, pixel_id_data)
    Event44Message.Event44MessageAddMessageId(builder, message_id)
    Event44Message.Event44MessageAddSourceName(builder, source)

    data = Event44Message.Event44MessageEnd(builder)
    builder.Finish(data, file_identifier=FILE_IDENTIFIER)

    return bytes(builder.Output())
