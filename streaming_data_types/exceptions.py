class StreamingDataTypesException(Exception):
    pass


class WrongSchemaException(StreamingDataTypesException):
    pass


class ShortBufferException(StreamingDataTypesException):
    pass
