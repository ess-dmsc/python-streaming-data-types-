"""Helper functions for data types"""
from streaming_data_types.exceptions import ShortBufferException, WrongSchemaException


def get_schema(buffer) -> str:
    """
    Extract the schema code embedded in the buffer

    :param buffer: The raw buffer of the FlatBuffers message.
    :return: The schema identifier
    """
    if len(buffer) < 8:
        raise ShortBufferException("Could not retrieve schema as buffer too short")
    return buffer[4:8].decode("utf-8")


def check_schema_identifier(buffer, expected_identifer: bytes):
    """
    Check the schema code embedded in the buffer matches an expected identifier

    :param buffer: The raw buffer of the FlatBuffers message
    :param expected_identifer: The expected flatbuffer identifier
    """
    if get_schema(buffer) != expected_identifer.decode():
        raise WrongSchemaException(
            f"Incorrect schema: expected {expected_identifer} but got {get_schema(buffer)}"
        )


def latest_schema(schema_type: str):
    """
    Returns the latest schema identifier for that type of schema and a deprecated flag if relevant
    """
    return all_schemas()[schema_type]


def all_schemas(return_deprecated: bool = False):
    """
    Returns list of all schemas, with deprecated schemas returning a list rather than a single value
    """
    DEPRECATED = True
    fbs_identifier = {
        "alarm": "al00",
        "area_detector": "ad00",
        "array_1d": ["se00", DEPRECATED],  # deprecated for sample_environment
        "dataarray": "da00",
        "epics_connection": "ep01",
        "eventdata": "ev44",
        "finished_writing": "wrdn",
        "forwader": "fc00",
        "histogram": "hs01",  # used by just-bin-it and at non-ESS facilities
        "json": ["json", DEPRECATED],  # only debugging
        "logdata": "f144",
        "nicos_cache": "ns10",
        "run_start": "pl72",
        "run_stop": "6s4t",
        "sample_environment": "senv",
        "status": "x5f2",
        "timestamps": "tdct",
    }

    return {k: v for k, v in fbs_identifier.items() if not isinstance(v, list) or return_deprecated}
