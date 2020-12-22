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
