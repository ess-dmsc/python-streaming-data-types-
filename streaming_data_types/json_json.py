import flatbuffers

import streaming_data_types.fbschemas.json_json.JsonData as JsonData
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"json"


def deserialise_json(buffer) -> str:
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    return JsonData.JsonData.GetRootAsJsonData(buffer, 0).Json().decode("utf-8")


def serialise_json(json_str) -> bytes:
    builder = flatbuffers.Builder(128)

    offset = builder.CreateString(json_str)

    JsonData.JsonDataStart(builder)
    JsonData.AddJson(builder, offset)
    result = JsonData.JsonDataEnd(builder)

    builder.Finish(result, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())
