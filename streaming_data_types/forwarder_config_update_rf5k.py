from collections import namedtuple
from typing import List, Union

import flatbuffers
from flatbuffers.packer import struct as flatbuffer_struct

from streaming_data_types.fbschemas.forwarder_config_update_rf5k import (
    ConfigUpdate,
    Protocol,
    Stream,
    UpdateType,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"rf5k"

ConfigurationUpdate = namedtuple("ConfigurationUpdate", ("config_change", "streams"))

StreamInfo = namedtuple("StreamInfo", ("channel", "schema", "topic", "protocol"))


def deserialise_rf5k(buffer: Union[bytearray, bytes]) -> ConfigurationUpdate:
    """
    Deserialise FlatBuffer rf5k.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    config_message = ConfigUpdate.ConfigUpdate.GetRootAsConfigUpdate(buffer, 0)

    streams = []
    try:
        for i in range(config_message.StreamsLength()):
            stream_message = config_message.Streams(i)
            streams.append(
                StreamInfo(
                    stream_message.Channel().decode("utf-8")
                    if stream_message.Channel()
                    else "",
                    stream_message.Schema().decode("utf-8")
                    if stream_message.Schema()
                    else "",
                    stream_message.Topic().decode("utf-8")
                    if stream_message.Topic()
                    else "",
                    stream_message.Protocol(),
                )
            )
    except flatbuffer_struct.error:
        pass  # No streams in buffer

    return ConfigurationUpdate(config_message.ConfigChange(), streams)


def serialise_stream(
    builder: flatbuffers.Builder,
    protocol: Protocol,
    channel_offset: int,
    schema_offset: int,
    topic_offset: int,
) -> int:
    Stream.StreamStart(builder)
    Stream.StreamAddProtocol(builder, protocol)
    Stream.StreamAddTopic(builder, topic_offset)
    Stream.StreamAddSchema(builder, schema_offset)
    Stream.StreamAddChannel(builder, channel_offset)
    return Stream.StreamEnd(builder)


def serialise_rf5k(config_change: UpdateType, streams: List[StreamInfo]) -> bytes:
    """
    Serialise config update message as an rf5k FlatBuffers message.

    :param config_change:
    :param streams: channel, schema and output topic configurations
    :return:
    """
    builder = flatbuffers.Builder(1024)
    builder.ForceDefaults(True)

    if streams:
        # We have to use multiple loops/list comprehensions here because we cannot create strings after we have
        # called StreamStart and cannot create streams after we have called StartVector
        stream_field_offsets = [
            (
                builder.CreateString(stream.channel),
                builder.CreateString(stream.schema),
                builder.CreateString(stream.topic),
            )
            for stream in streams
        ]
        stream_offsets = [
            serialise_stream(builder, stream.protocol, *stream_fields)
            for stream, stream_fields in zip(streams, stream_field_offsets)
        ]

        ConfigUpdate.ConfigUpdateStartStreamsVector(builder, len(streams))
        for stream_offset in stream_offsets:
            builder.PrependUOffsetTRelative(stream_offset)
        streams_offset = builder.EndVector(len(streams))

    # Build the actual buffer
    ConfigUpdate.ConfigUpdateStart(builder)
    if streams:
        ConfigUpdate.ConfigUpdateAddStreams(builder, streams_offset)
    ConfigUpdate.ConfigUpdateAddConfigChange(builder, config_change)
    data = ConfigUpdate.ConfigUpdateEnd(builder)

    builder.Finish(data, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())
