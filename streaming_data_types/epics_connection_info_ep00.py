from collections import namedtuple
from typing import Optional, Union

import flatbuffers

from streaming_data_types.fbschemas.epics_connection_info_ep00 import (
    EpicsConnectionInfo,
    EventType,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ep00"


def serialise_ep00(
    timestamp_ns: int,
    event_type: EventType,
    source_name: str,
    service_id: Optional[str] = None,
) -> bytes:
    builder = flatbuffers.Builder(136)
    builder.ForceDefaults(True)

    if service_id is not None:
        service_id_offset = builder.CreateString(service_id)
    source_name_offset = builder.CreateString(source_name)

    EpicsConnectionInfo.EpicsConnectionInfoStart(builder)
    if service_id is not None:
        EpicsConnectionInfo.EpicsConnectionInfoAddServiceId(builder, service_id_offset)
    EpicsConnectionInfo.EpicsConnectionInfoAddSourceName(builder, source_name_offset)
    EpicsConnectionInfo.EpicsConnectionInfoAddType(builder, event_type)
    EpicsConnectionInfo.EpicsConnectionInfoAddTimestamp(builder, timestamp_ns)

    end = EpicsConnectionInfo.EpicsConnectionInfoEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


EpicsConnection = namedtuple(
    "EpicsConnection", ("timestamp", "type", "source_name", "service_id")
)


def deserialise_ep00(buffer: Union[bytearray, bytes]) -> EpicsConnection:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    epics_connection = (
        EpicsConnectionInfo.EpicsConnectionInfo.GetRootAsEpicsConnectionInfo(buffer, 0)
    )

    source_name = (
        epics_connection.SourceName() if epics_connection.SourceName() else b""
    )
    service_id = epics_connection.ServiceId() if epics_connection.ServiceId() else b""

    return EpicsConnection(
        epics_connection.Timestamp(),
        epics_connection.Type(),
        source_name.decode(),
        service_id.decode(),
    )
