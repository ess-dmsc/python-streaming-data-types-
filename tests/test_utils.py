import pytest

from streaming_data_types.exceptions import ShortBufferException
from streaming_data_types.utils import check_schema_identifier


def test_schema_check_throws_if_buffer_too_short():
    short_buffer = b"1234567"
    with pytest.raises(ShortBufferException):
        check_schema_identifier(short_buffer, b"1234")
