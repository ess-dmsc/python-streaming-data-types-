from datetime import datetime, timezone
from typing import NamedTuple, Union

import flatbuffers

import streaming_data_types.fbschemas.action_response_answ.ActionResponse as ActionResponse
from streaming_data_types.fbschemas.action_response_answ.ActionOutcome import (
    ActionOutcome,
)
from streaming_data_types.fbschemas.action_response_answ.ActionType import ActionType
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"answ"


def serialise_answ(
    service_id: str,
    job_id: str,
    command_id: str,
    action: ActionType,
    outcome: ActionOutcome,
    message: str,
    status_code: int,
    stop_time: datetime,
) -> bytes:
    builder = flatbuffers.Builder(500)
    builder.ForceDefaults(True)
    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    message_offset = builder.CreateString(message)
    command_id_offset = builder.CreateString(command_id)

    ActionResponse.ActionResponseStart(builder)
    ActionResponse.ActionResponseAddServiceId(builder, service_id_offset)
    ActionResponse.ActionResponseAddJobId(builder, job_id_offset)
    ActionResponse.ActionResponseAddAction(builder, action)
    ActionResponse.ActionResponseAddOutcome(builder, outcome)
    ActionResponse.ActionResponseAddMessage(builder, message_offset)
    ActionResponse.ActionResponseAddCommandId(builder, command_id_offset)
    ActionResponse.ActionResponseAddStatusCode(builder, status_code)
    ActionResponse.ActionResponseAddStopTime(builder, int(stop_time.timestamp() * 1000))

    out_message = ActionResponse.ActionResponseEnd(builder)
    builder.Finish(out_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


Response = NamedTuple(
    "Response",
    (
        ("service_id", str),
        ("job_id", str),
        ("command_id", str),
        ("action", ActionType),
        ("outcome", ActionOutcome),
        ("message", str),
        ("status_code", int),
        ("stop_time", datetime),
    ),
)


def deserialise_answ(buffer: Union[bytearray, bytes]):
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    answ_message = ActionResponse.ActionResponse.GetRootAsActionResponse(buffer, 0)
    max_time = datetime(
        year=9000, month=1, day=1, hour=0, minute=0, second=0
    ).timestamp()
    used_timestamp = answ_message.StopTime() / 1000
    if used_timestamp > max_time:
        used_timestamp = max_time
    return Response(
        service_id=answ_message.ServiceId().decode("utf-8"),
        job_id=answ_message.JobId().decode("utf-8"),
        command_id=answ_message.CommandId().decode("utf-8"),
        action=answ_message.Action(),
        outcome=answ_message.Outcome(),
        message=answ_message.Message().decode("utf-8"),
        status_code=answ_message.StatusCode(),
        stop_time=datetime.fromtimestamp(used_timestamp, tz=timezone.utc),
    )
