from collections import namedtuple
from typing import Optional, Union

import flatbuffers

from streaming_data_types.fbschemas.epics_connection_ep01 import (
    EpicsPVConnectionInfo,
    ConnectionInfo,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"ep01"


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
        EpicsPVConnectionInfo.EpicsPVConnectionInfoAddServiceId(builder, service_id_offset)
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddSourceName(builder, source_name_offset)
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddStatus(builder, status)
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
        EpicsPVConnectionInfo.EpicsPVConnectionInfo.GetRootAsEpicsPVConnectionInfo(buffer, 0)
    )

    source_name = (
        epics_connection.SourceName() if epics_connection.SourceName() else b""
    )
    service_id = epics_connection.ServiceId() if epics_connection.ServiceId() else None

    return EpicsPVConnection(
        timestamp=epics_connection.Timestamp(),
        status=epics_connection.Status(),
        source_name=source_name.decode(),
        service_id=service_id.decode(),
    )
