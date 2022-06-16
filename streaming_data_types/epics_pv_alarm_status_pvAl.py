from typing import Union, NamedTuple
from datetime import datetime, timezone

import flatbuffers

from streaming_data_types.fbschemas.pv_alarm_state_pvAl import (
    AlarmState,
    AlarmSeverity,
    CAAlarmState,
    PV_AlarmState,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"pvAl"


def serialise_pvAl(
    source_name: str,
    timestamp: datetime,
    state: AlarmState.AlarmState,
    ca_state: CAAlarmState.CAAlarmState,
    severity: AlarmSeverity.AlarmSeverity,
    message: str = "",
) -> bytes:
    builder = flatbuffers.Builder(136)
    builder.ForceDefaults(True)

    source_name_offset = builder.CreateString(source_name)
    message_offset = builder.CreateString(message)

    PV_AlarmState.PV_AlarmStateStart(builder)
    PV_AlarmState.PV_AlarmStateAddSourceName(builder, source_name_offset)
    PV_AlarmState.PV_AlarmStateAddMessage(builder, message_offset)
    PV_AlarmState.PV_AlarmStateAddState(builder, state)
    PV_AlarmState.PV_AlarmStateAddCaState(builder, ca_state)
    PV_AlarmState.PV_AlarmStateAddSeverity(builder, severity)
    PV_AlarmState.PV_AlarmStateAddTimestamp(builder, int(timestamp.timestamp() * 1e6))

    end = PV_AlarmState.PV_AlarmStateEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


PV_AlarmStateInfo = NamedTuple(
    "PV_AlarmStateInfo",
    (
        ("source_name", str),
        ("timestamp", datetime),
        ("state", AlarmState.AlarmState),
        ("ca_state", CAAlarmState.CAAlarmState),
        ("severity", AlarmSeverity.AlarmSeverity),
        ("message", str),
    ),
)


def deserialise_pvAl(buffer: Union[bytearray, bytes]) -> PV_AlarmStateInfo:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    epics_alarm_state = PV_AlarmState.PV_AlarmState.GetRootAs(buffer, 0)

    source_name = (
        epics_alarm_state.SourceName() if epics_alarm_state.SourceName() else b""
    )
    message = epics_alarm_state.Message() if epics_alarm_state.Message() else b""

    return PV_AlarmStateInfo(
        source_name=source_name.decode("utf-8"),
        timestamp=datetime.fromtimestamp(
            epics_alarm_state.Timestamp() / 1e6, tz=timezone.utc
        ),
        state=epics_alarm_state.State(),
        ca_state=epics_alarm_state.CaState(),
        severity=epics_alarm_state.Severity(),
        message=message.decode("utf-8"),
    )
