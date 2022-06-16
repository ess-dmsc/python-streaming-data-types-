from typing import Optional, Union, NamedTuple
from datetime import datetime, timezone

import flatbuffers

from streaming_data_types.fbschemas.epics_conn_status_pvCn import (
    ConnectionInfo,
    EpicsPVConnectionInfo,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"pvCn"


def serialise_pvCn(
    timestamp: datetime,
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
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddStatus(builder, status)
    EpicsPVConnectionInfo.EpicsPVConnectionInfoAddTimestamp(
        builder, int(timestamp.timestamp() * 1e6)
    )

    end = EpicsPVConnectionInfo.EpicsPVConnectionInfoEnd(builder)
    builder.Finish(end, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


EpicsPVConnection = NamedTuple(
    "EpicsPVConnection",
    (
        ("timestamp", datetime),
        ("status", ConnectionInfo.ConnectionInfo),
        ("source_name", str),
        ("service_id", Optional[str]),
    ),
)


def deserialise_pvCn(buffer: Union[bytearray, bytes]) -> EpicsPVConnection:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    epics_connection = EpicsPVConnectionInfo.EpicsPVConnectionInfo.GetRootAs(buffer, 0)

    source_name = (
        epics_connection.SourceName() if epics_connection.SourceName() else b""
    )
    service_id = (
        epics_connection.ServiceId().decode() if epics_connection.ServiceId() else None
    )

    return EpicsPVConnection(
        timestamp=datetime.fromtimestamp(
            epics_connection.Timestamp() / 1e6, tz=timezone.utc
        ),
        status=epics_connection.Status(),
        source_name=source_name.decode(),
        service_id=service_id,
    )
