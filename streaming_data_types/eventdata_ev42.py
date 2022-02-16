from collections import namedtuple

import flatbuffers
import numpy as np

import streaming_data_types.fbschemas.eventdata_ev42.EventMessage as EventMessage
import streaming_data_types.fbschemas.eventdata_ev42.FacilityData as FacilityData
import streaming_data_types.fbschemas.isis_event_info_is84.ISISData as ISISData
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ev42"


EventData = namedtuple(
    "EventData",
    (
        "source_name",
        "message_id",
        "pulse_time",
        "time_of_flight",
        "detector_id",
        "specific_data",
    ),
)


def deserialise_ev42(buffer):
    """
    Deserialise FlatBuffer ev42.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    event = EventMessage.EventMessage.GetRootAsEventMessage(buffer, 0)

    specific_data = None
    if event.FacilitySpecificDataType() == FacilityData.FacilityData.ISISData:
        specific = event.FacilitySpecificData()
        isis_buf = ISISData.ISISData()
        isis_buf.Init(specific.Bytes, specific.Pos)
        specific_data = {
            "period_number": isis_buf.PeriodNumber(),
            "run_state": isis_buf.RunState(),
            "proton_charge": isis_buf.ProtonCharge(),
        }

    return EventData(
        event.SourceName().decode("utf-8"),
        event.MessageId(),
        event.PulseTime(),
        event.TimeOfFlightAsNumpy(),
        event.DetectorIdAsNumpy(),
        specific_data,
    )


def serialise_ev42(
    source_name, message_id, pulse_time, time_of_flight, detector_id, isis_specific=None
):
    """
    Serialise event data as an ev42 FlatBuffers message.

    :param source_name:
    :param message_id:
    :param pulse_time:
    :param time_of_flight:
    :param detector_id:
    :param isis_specific:
    :return:
    """
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    source = builder.CreateString(source_name)

    tof_data = builder.CreateNumpyVector(np.asarray(time_of_flight).astype(np.uint32))
    det_data = builder.CreateNumpyVector(np.asarray(detector_id).astype(np.uint32))

    isis_data = None
    if isis_specific:
        # isis_builder = flatbuffers.Builder(96)
        ISISData.ISISDataStart(builder)
        ISISData.ISISDataAddPeriodNumber(builder, isis_specific["period_number"])
        ISISData.ISISDataAddRunState(builder, isis_specific["run_state"])
        ISISData.ISISDataAddProtonCharge(builder, isis_specific["proton_charge"])
        isis_data = ISISData.ISISDataEnd(builder)

    # Build the actual buffer
    EventMessage.EventMessageStart(builder)
    EventMessage.EventMessageAddDetectorId(builder, det_data)
    EventMessage.EventMessageAddTimeOfFlight(builder, tof_data)
    EventMessage.EventMessageAddPulseTime(builder, pulse_time)
    EventMessage.EventMessageAddMessageId(builder, message_id)
    EventMessage.EventMessageAddSourceName(builder, source)

    if isis_specific:
        EventMessage.EventMessageAddFacilitySpecificDataType(
            builder, FacilityData.FacilityData.ISISData
        )
        EventMessage.EventMessageAddFacilitySpecificData(builder, isis_data)

    data = EventMessage.EventMessageEnd(builder)

    builder.Finish(data, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())
