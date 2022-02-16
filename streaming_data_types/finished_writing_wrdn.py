from typing import NamedTuple, Optional, Union

import flatbuffers

from streaming_data_types.fbschemas.finished_writing_wrdn import FinishedWriting
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"wrdn"


def serialise_wrdn(
    service_id: str,
    job_id: str,
    error_encountered: bool,
    file_name: str,
    metadata: Optional[str] = None,
    message: Optional[str] = None,
) -> bytes:
    builder = flatbuffers.Builder(500)
    builder.ForceDefaults(True)

    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    file_name_offset = builder.CreateString(file_name)
    if metadata is not None:
        metadata_offset = builder.CreateString(metadata)
    if message is not None:
        message_offset = builder.CreateString(message)

    # Build the actual buffer
    FinishedWriting.FinishedWritingStart(builder)
    FinishedWriting.FinishedWritingAddServiceId(builder, service_id_offset)
    FinishedWriting.FinishedWritingAddJobId(builder, job_id_offset)
    FinishedWriting.FinishedWritingAddErrorEncountered(builder, error_encountered)
    FinishedWriting.FinishedWritingAddFileName(builder, file_name_offset)
    if metadata:
        FinishedWriting.FinishedWritingAddMetadata(builder, metadata_offset)
    if message:
        FinishedWriting.FinishedWritingAddMessage(builder, message_offset)

    finished_writing_message = FinishedWriting.FinishedWritingEnd(builder)

    builder.Finish(finished_writing_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


WritingFinished = NamedTuple(
    "FinishedWriting",
    (
        ("service_id", str),
        ("job_id", str),
        ("error_encountered", bool),
        ("file_name", str),
        ("metadata", Optional[str]),
        ("message", Optional[str]),
    ),
)


def deserialise_wrdn(buffer: Union[bytearray, bytes]) -> FinishedWriting:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    finished_writing = FinishedWriting.FinishedWriting.GetRootAsFinishedWriting(
        buffer, 0
    )
    service_id = finished_writing.ServiceId()
    job_id = finished_writing.JobId()
    has_error = finished_writing.ErrorEncountered()
    file_name = finished_writing.FileName() if finished_writing.FileName() else b""
    metadata = (
        finished_writing.Metadata().decode() if finished_writing.Metadata() else None
    )
    message = (
        finished_writing.Message().decode() if finished_writing.Message() else None
    )

    return WritingFinished(
        service_id=service_id.decode(),
        job_id=job_id.decode(),
        error_encountered=has_error,
        file_name=file_name.decode(),
        metadata=metadata,
        message=message,
    )
