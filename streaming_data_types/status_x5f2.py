from collections import namedtuple

import flatbuffers

from streaming_data_types.fbschemas.status_x5f2 import Status
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"x5f2"

StatusMessage = namedtuple(
    "StatusMessage",
    (
        "software_name",
        "software_version",
        "service_id",
        "host_name",
        "process_id",
        "update_interval",
        "status_json",
    ),
)


def deserialise_x5f2(buffer):
    """
    Deserialise FlatBuffer x5f2.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    log_message = Status.Status.GetRootAsStatus(buffer, 0)

    return StatusMessage(
        log_message.SoftwareName().decode("utf-8"),
        log_message.SoftwareVersion().decode("utf-8"),
        log_message.ServiceId().decode("utf-8"),
        log_message.HostName().decode("utf-8"),
        log_message.ProcessId(),
        log_message.UpdateInterval(),
        log_message.StatusJson().decode("utf-8"),
    )


def serialise_x5f2(
    software_name: str,
    software_version: str,
    service_id: str,
    host_name: str,
    process_id: int,
    update_interval: int,
    status_json: str,
) -> bytes:
    """
    Serialise status message as an x5f2 FlatBuffers message.

    :param software_name:
    :param software_version:
    :param service_id:
    :param host_name:
    :param process_id:
    :param update_interval:
    :param status_json:
    :return:
    """

    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    software_name = builder.CreateString(software_name)
    software_version = builder.CreateString(software_version)
    service_id = builder.CreateString(service_id)
    host_name = builder.CreateString(host_name)
    status_json = builder.CreateString(status_json)

    # Build the actual buffer
    Status.StatusStart(builder)

    Status.StatusAddSoftwareName(builder, software_name)
    Status.StatusAddSoftwareVersion(builder, software_version)
    Status.StatusAddServiceId(builder, service_id)
    Status.StatusAddHostName(builder, host_name)
    Status.StatusAddProcessId(builder, process_id)
    Status.StatusAddUpdateInterval(builder, update_interval)
    Status.StatusAddStatusJson(builder, status_json)

    data = Status.StatusEnd(builder)
    builder.Finish(data, file_identifier=FILE_IDENTIFIER)

    return bytes(builder.Output())
