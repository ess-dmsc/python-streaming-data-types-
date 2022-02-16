from collections import namedtuple

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.eventdata_ev43.Event43Message as Event43Message
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ev43"


EventData = namedtuple(
    "EventData",
    (
        "source_name",
        "message_id",
        "pulse_time",
        "pulse_index",
        "time_of_flight",
        "detector_id",
    ),
)


def deserialise_ev43(buffer):
    """
    Deserialise FlatBuffer ev43.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    event = Event43Message.Event43Message.GetRootAsEvent43Message(buffer, 0)

    return EventData(
        event.SourceName().decode("utf-8"),
        event.MessageId(),
        event.PulseTimeAsNumpy(),
        event.PulseIndexAsNumpy(),
        event.TimeOfFlightAsNumpy(),
        event.DetectorIdAsNumpy(),
    )


def serialise_ev43(
    source_name, message_id, pulse_time, pulse_index, time_of_flight, detector_id
):
    """
    Serialise event data as an ev43 FlatBuffers message.

    :param source_name:
    :param message_id:
    :param pulse_time:
    :param pulse_index:
    :param time_of_flight:
    :param detector_id:
    :return:
    """
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    source = builder.CreateString(source_name)

    pulse_ts_data = builder.CreateNumpyVector(np.asarray(pulse_time).astype(np.uint64))
    pulse_ix_data = builder.CreateNumpyVector(np.asarray(pulse_index).astype(np.uint32))
    tof_data = builder.CreateNumpyVector(np.asarray(time_of_flight).astype(np.uint32))
    det_data = builder.CreateNumpyVector(np.asarray(detector_id).astype(np.uint32))

    # Build the actual buffer
    Event43Message.Event43MessageStart(builder)
    Event43Message.Event43MessageAddPulseTime(builder, pulse_ts_data)
    Event43Message.Event43MessageAddPulseIndex(builder, pulse_ix_data)
    Event43Message.Event43MessageAddDetectorId(builder, det_data)
    Event43Message.Event43MessageAddTimeOfFlight(builder, tof_data)
    Event43Message.Event43MessageAddMessageId(builder, message_id)
    Event43Message.Event43MessageAddSourceName(builder, source)

    data = Event43Message.Event43MessageEnd(builder)

    builder.Finish(data, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())
