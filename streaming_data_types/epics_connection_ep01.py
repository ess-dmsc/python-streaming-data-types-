from collections import namedtuple
from enum import Enum
from typing import Optional, Union

import flatbuffers

from streaming_data_types.fbschemas.epics_connection_ep01 import EpicsPVConnectionInfo
from streaming_data_types.fbschemas.epics_connection_ep01.ConnectionInfo import (
    ConnectionInfo as FBConnectionInfo,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ep01"


class ConnectionInfo(Enum):
    UNKNOWN = 0
    NEVER_CONNECTED = 1
    CONNECTED = 2
    DISCONNECTED = 3
    DESTROYED = 4
    CANCELLED = 5
    FINISHED = 6
    REMOTE_ERROR = 7


_enum_to_status = {
    ConnectionInfo.UNKNOWN: FBConnectionInfo.UNKNOWN,
    ConnectionInfo.NEVER_CONNECTED: FBConnectionInfo.NEVER_CONNECTED,
    ConnectionInfo.CONNECTED: FBConnectionInfo.CONNECTED,
    ConnectionInfo.DISCONNECTED: FBConnectionInfo.DISCONNECTED,
    ConnectionInfo.DESTROYED: FBConnectionInfo.DESTROYED,
    ConnectionInfo.CANCELLED: FBConnectionInfo.CANCELLED,
    ConnectionInfo.FINISHED: FBConnectionInfo.FINISHED,
    ConnectionInfo.REMOTE_ERROR: FBConnectionInfo.REMOTE_ERROR,
}

_status_to_enum = {
    FBConnectionInfo.UNKNOWN: ConnectionInfo.UNKNOWN,
    FBConnectionInfo.NEVER_CONNECTED: ConnectionInfo.NEVER_CONNECTED,
    FBConnectionInfo.CONNECTED: ConnectionInfo.CONNECTED,
    FBConnectionInfo.DISCONNECTED: ConnectionInfo.DISCONNECTED,
    FBConnectionInfo.DESTROYED: ConnectionInfo.DESTROYED,
    FBConnectionInfo.CANCELLED: ConnectionInfo.CANCELLED,
    FBConnectionInfo.FINISHED: ConnectionInfo.FINISHED,
    FBConnectionInfo.REMOTE_ERROR: ConnectionInfo.REMOTE_ERROR,
}


def serialise_ep01(
    timestamp_ns: int,
    status: ConnectionInfo,
    source_name: str,
    service_id: Optional[str] = None,
) -> bytes:
    builder = flatbuffers.Builder(136)
    builder.ForceDefaults(True)

    if service_id is not None:
        service_id_offset = builder.CreateString(service_id)
    source_name_offset = builder.CreateString(source_name)

    EpicsPVConnectionInfo.EpicsPVConnectionInfoStart(builder)
    if service_id is not None:
        EpicsPVConnectionInfo.EpicsPVConnectionInfoAddServiceId(
            builder, service_id_offset
        )
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddSourceName(
        builder, source_name_offset
    )
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddStatus(
        builder, _enum_to_status[status]
    )
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddTimestamp(builder, timestamp_ns)

    end = EpicsPVConnectionInfo.EpicsPVConnectionInfoEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


EpicsPVConnection = namedtuple(
    "EpicsPVConnection", ("timestamp", "status", "source_name", "service_id")
)


def deserialise_ep01(buffer: Union[bytearray, bytes]) -> EpicsPVConnection:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    epics_connection = (
        EpicsPVConnectionInfo.EpicsPVConnectionInfo.GetRootAsEpicsPVConnectionInfo(
            buffer, 0
        )
    )

    source_name = (
        epics_connection.SourceName() if epics_connection.SourceName() else b""
    )
    service_id = epics_connection.ServiceId() if epics_connection.ServiceId() else b""

    return EpicsPVConnection(
        timestamp=epics_connection.Timestamp(),
        status=_status_to_enum[epics_connection.Status()],
        source_name=source_name.decode(),
        service_id=service_id.decode(),
    )
