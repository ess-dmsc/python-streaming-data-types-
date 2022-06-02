#include "f142_serialiser.h"

#include <pybind11/chrono.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <span>

namespace py = pybind11;

PYBIND11_MODULE(fast_f142_serialiser, m) {
  m.doc() = "A module for a C++ based f142 serialiser that is significantly "
            "more performant than the Python version.";
  py::class_<f142_serialiser>(m, "f142_serialiser")
      .def(py::init<>())
      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName, double Value,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(
                 o.serialise_message<double>(SourceName, Value, Timestamp));
           })
      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName, int32_t Value,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(
                 o.serialise_message<int32_t>(SourceName, Value, Timestamp));
           })
      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName, int64_t Value,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(
                 o.serialise_message<int64_t>(SourceName, Value, Timestamp));
           })

      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<int8_t, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const int8_t>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })
      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<uint8_t, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const uint8_t>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })

      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<int16_t, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const int16_t>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })
      .def(
          "serialise_message",
          [](f142_serialiser &o, std::string SourceName,
             py::array_t<uint16_t, py::array::c_style | py::array::forcecast>
                 ValueArray,
             std::chrono::system_clock::time_point Timestamp) {
            return py::bytes(o.serialise_message(
                SourceName,
                std::span<const uint16_t>(ValueArray.data(), ValueArray.size()),
                Timestamp));
          })

      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<int32_t, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const int32_t>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })
      .def(
          "serialise_message",
          [](f142_serialiser &o, std::string SourceName,
             py::array_t<uint32_t, py::array::c_style | py::array::forcecast>
                 ValueArray,
             std::chrono::system_clock::time_point Timestamp) {
            return py::bytes(o.serialise_message(
                SourceName,
                std::span<const uint32_t>(ValueArray.data(), ValueArray.size()),
                Timestamp));
          })

      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<int64_t, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const int64_t>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })
      .def(
          "serialise_message",
          [](f142_serialiser &o, std::string SourceName,
             py::array_t<uint64_t, py::array::c_style | py::array::forcecast>
                 ValueArray,
             std::chrono::system_clock::time_point Timestamp) {
            return py::bytes(o.serialise_message(
                SourceName,
                std::span<const uint64_t>(ValueArray.data(), ValueArray.size()),
                Timestamp));
          })

      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<float, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const float>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           })
      .def("serialise_message",
           [](f142_serialiser &o, std::string SourceName,
              py::array_t<double, py::array::c_style | py::array::forcecast>
                  ValueArray,
              std::chrono::system_clock::time_point Timestamp) {
             return py::bytes(o.serialise_message(
                 SourceName,
                 std::span<const double>(ValueArray.data(), ValueArray.size()),
                 Timestamp));
           });
}
