#pragma once

#include "f142_logdata_generated.h"
#include <chrono>
#include <span>
#include <string>

template <typename ValueType>
void serialiseValue(flatbuffers::FlatBufferBuilder &, LogDataBuilder &,
                    const ValueType &);

template <typename ValueBuilderType, typename ValueType>
void serialiseScalarValueBase(flatbuffers::FlatBufferBuilder &Builder,
                              LogDataBuilder &LogData, const ValueType &Value,
                              const enum Value &ValueTypeId) {
  ValueBuilderType ValueBuilder(Builder);
  ValueBuilder.add_value(Value);
  LogData.add_value(ValueBuilder.Finish().Union());
  LogData.add_value_type(ValueTypeId);
}

template <>
void serialiseValue<double>(flatbuffers::FlatBufferBuilder &Builder,
                            LogDataBuilder &LogData, const double &Value) {
  serialiseScalarValueBase<DoubleBuilder, double>(Builder, LogData, Value,
                                                  Value::Value_Double);
}

template <>
void serialiseValue<int32_t>(flatbuffers::FlatBufferBuilder &Builder,
                             LogDataBuilder &LogData, const int32_t &Value) {
  serialiseScalarValueBase<IntBuilder, int32_t>(Builder, LogData, Value,
                                                Value::Value_Int);
}

template <>
void serialiseValue<int64_t>(flatbuffers::FlatBufferBuilder &Builder,
                             LogDataBuilder &LogData, const int64_t &Value) {
  serialiseScalarValueBase<LongBuilder, int64_t>(Builder, LogData, Value,
                                                 Value::Value_Long);
}

template <typename ValueBuilderType, typename ValueType>
void serialiseArrayValueBase(flatbuffers::FlatBufferBuilder &Builder,
                             LogDataBuilder &LogData,
                             const std::span<const ValueType> &ValueArray,
                             const enum Value &ValueTypeId) {
  auto VectorOffset =
      Builder.CreateVector(ValueArray.data(), ValueArray.size());
  ValueBuilderType ValueBuilder(Builder);
  ValueBuilder.add_value(VectorOffset);
  LogData.add_value(ValueBuilder.Finish().Union());
  LogData.add_value_type(ValueTypeId);
}

template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const int8_t> &ValueArray) {
  serialiseArrayValueBase<ArrayByteBuilder, int8_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayByte);
}
template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const uint8_t> &ValueArray) {
  serialiseArrayValueBase<ArrayUByteBuilder, uint8_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayUByte);
}

template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const int16_t> &ValueArray) {
  serialiseArrayValueBase<ArrayShortBuilder, int16_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayShort);
}
template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const uint16_t> &ValueArray) {
  serialiseArrayValueBase<ArrayUShortBuilder, uint16_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayUShort);
}

template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const int32_t> &ValueArray) {
  serialiseArrayValueBase<ArrayIntBuilder, int32_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayInt);
}
template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const uint32_t> &ValueArray) {
  serialiseArrayValueBase<ArrayUIntBuilder, uint32_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayUInt);
}

template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const int64_t> &ValueArray) {
  serialiseArrayValueBase<ArrayLongBuilder, int64_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayLong);
}
template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const uint64_t> &ValueArray) {
  serialiseArrayValueBase<ArrayULongBuilder, uint64_t>(
      Builder, LogData, ValueArray, Value::Value_ArrayULong);
}

template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const float> &ValueArray) {
  serialiseArrayValueBase<ArrayFloatBuilder, float>(
      Builder, LogData, ValueArray, Value::Value_ArrayFloat);
}
template <>
void serialiseValue<>(flatbuffers::FlatBufferBuilder &Builder,
                      LogDataBuilder &LogData,
                      const std::span<const double> &ValueArray) {
  serialiseArrayValueBase<ArrayDoubleBuilder, double>(
      Builder, LogData, ValueArray, Value::Value_ArrayDouble);
}

class f142_serialiser {
public:
  template <typename ValueType>
  std::string
  serialise_message(std::string SourceName, ValueType Value,
                    std::chrono::system_clock::time_point Timestamp) {
    Builder.Clear();
    auto SourceNameOffset = Builder.CreateString(SourceName);
    LogDataBuilder LogDataBuilder(Builder);
    serialiseValue(Builder, LogDataBuilder, Value);
    LogDataBuilder.add_timestamp(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Timestamp.time_since_epoch())
            .count());
    LogDataBuilder.add_source_name(SourceNameOffset);

    LogDataBuilder.add_status(AlarmStatus::AlarmStatus_NO_ALARM);
    LogDataBuilder.add_severity(AlarmSeverity::AlarmSeverity_NO_ALARM);

    FinishLogDataBuffer(Builder, LogDataBuilder.Finish());
    return {reinterpret_cast<char *>(Builder.GetBufferPointer()),
            Builder.GetSize()};
  }

private:
  flatbuffers::FlatBufferBuilder Builder{};
};
