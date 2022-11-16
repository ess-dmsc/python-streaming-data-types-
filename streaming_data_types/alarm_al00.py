from collections import namedtuple
from enum import Enum

import flatbuffers

import streaming_data_types.fbschemas.alarm_al00.Alarm as Alarm
import streaming_data_types.fbschemas.alarm_al00.Severity as FBSeverity
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"al00"

AlarmInfo = namedtuple("AlarmInfo", ("source", "timestamp", "severity", "message"))


class Severity(Enum):
    OK = 0,
    MINOR = 1,
    MAJOR = 2,
    INVALID = 3


_enum_to_severity = {
    Severity.OK: FBSeverity.Severity.OK,
    Severity.MINOR: FBSeverity.Severity.MINOR,
    Severity.MAJOR: FBSeverity.Severity.MAJOR,
    Severity.INVALID: FBSeverity.Severity.INVALID,
}

_severity_to_enum = {
    FBSeverity.Severity.OK: Severity.OK,
    FBSeverity.Severity.MINOR: Severity.MINOR,
    FBSeverity.Severity.MAJOR: Severity.MAJOR,
    FBSeverity.Severity.INVALID: Severity.INVALID,
}


def deserialise_al00(buffer):
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    alarm = Alarm.Alarm.GetRootAsAlarm(buffer, 0)

    return AlarmInfo(
        alarm.SourceName().decode("utf-8") if alarm.SourceName() else "",
        alarm.Timestamp(),
        _severity_to_enum[alarm.Severity()],
        alarm.Message().decode("utf-8") if alarm.Message() else ""
    )


def serialise_al00(source: str, timestamp: int, severity: Severity, message: str):
    builder = flatbuffers.Builder(128)

    message_offset = builder.CreateString(message)
    source_offset = builder.CreateString(source)

    Alarm.AlarmStart(builder)
    Alarm.AlarmAddSourceName(builder, source_offset)
    Alarm.AlarmAddTimestamp(builder, timestamp)
    Alarm.AlarmAddSeverity(builder, _enum_to_severity[severity])
    Alarm.AlarmAddMessage(builder, message_offset)
    alarm = Alarm.AlarmEnd(builder)

    builder.Finish(alarm, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())
