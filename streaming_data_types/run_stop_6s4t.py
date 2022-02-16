from datetime import datetime
from typing import NamedTuple, Union

import flatbuffers

from streaming_data_types.fbschemas.run_stop_6s4t import RunStop
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"6s4t"


def serialise_6s4t(
    job_id: str,
    run_name: str = "test_run",
    service_id: str = "",
    command_id: str = "",
    stop_time: Union[int, datetime, None] = None,
) -> bytes:
    builder = flatbuffers.Builder(500)
    builder.ForceDefaults(True)

    if service_id is None:
        service_id = ""
    if type(stop_time) is datetime:
        stop_time = int(stop_time.timestamp() * 1000)
    elif stop_time is None:
        stop_time = 0

    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    run_name_offset = builder.CreateString(run_name)
    command_id_offset = builder.CreateString(command_id)

    # Build the actual buffer
    RunStop.RunStopStart(builder)
    RunStop.RunStopAddServiceId(builder, service_id_offset)
    RunStop.RunStopAddJobId(builder, job_id_offset)
    RunStop.RunStopAddRunName(builder, run_name_offset)
    RunStop.RunStopAddStopTime(builder, stop_time)
    RunStop.RunStopAddCommandId(builder, command_id_offset)

    run_stop_message = RunStop.RunStopEnd(builder)
    builder.Finish(run_stop_message, file_identifier=FILE_IDENTIFIER)

    return bytes(builder.Output())


RunStopInfo = NamedTuple(
    "RunStopInfo",
    (
        ("stop_time", int),
        ("run_name", str),
        ("job_id", str),
        ("service_id", str),
        ("command_id", str),
    ),
)


def deserialise_6s4t(buffer: Union[bytearray, bytes]) -> RunStopInfo:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    run_stop = RunStop.RunStop.GetRootAsRunStop(buffer, 0)
    service_id = run_stop.ServiceId() if run_stop.ServiceId() else b""
    job_id = run_stop.JobId() if run_stop.JobId() else b""
    run_name = run_stop.RunName() if run_stop.RunName() else b""
    stop_time = run_stop.StopTime()
    command_id = run_stop.CommandId()

    return RunStopInfo(
        stop_time=stop_time,
        run_name=run_name.decode(),
        job_id=job_id.decode(),
        service_id=service_id.decode(),
        command_id=command_id.decode(),
    )
