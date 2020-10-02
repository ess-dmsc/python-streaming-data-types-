from typing import Optional, Union
import flatbuffers
from streaming_data_types.fbschemas.finished_writing_wrdn import FinishedWriting
from streaming_data_types.utils import check_schema_identifier
from collections import namedtuple

FILE_IDENTIFIER = b"wrdn"


def serialise_wrdn(
    service_id: str,
    job_id: str,
    error_encountered: bool,
    file_name: str,
    metadata: str = "",
    message: str = ""
) -> bytes:
    builder = flatbuffers.Builder(500)

    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    file_name_offset = builder.CreateString(file_name)
    if metadata:
        metadata_offset = builder.CreateString(metadata)
    if message:
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
    builder.Finish(finished_writing_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)


WritingFinished = namedtuple(
    "FinishedWriting", ("service_id", "job_id", "error_encountered", "file_name", "metadata", "message")
)


def deserialise_wrdn(buffer: Union[bytearray, bytes]) -> FinishedWriting:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    finished_writing = FinishedWriting.FinishedWriting.GetRootAsFinishedWriting(buffer, 0)
    service_id = finished_writing.ServiceId()
    job_id = finished_writing.JobId()
    has_error = finished_writing.ErrorEncountered()
    file_name = finished_writing.FileName() if finished_writing.FileName() else b""
    metadata = finished_writing.Metadata() if finished_writing.Metadata() else b""
    message = finished_writing.Message() if finished_writing.Message() else b""

    return WritingFinished(
        service_id.decode(), job_id.decode(), has_error, file_name.decode(), metadata.decode(), message.decode()
    )
