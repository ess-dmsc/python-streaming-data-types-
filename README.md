# Python Streaming Data Types
Utilities for working with the FlatBuffers schemas used at the European
Spallation Source ERIC for data transport.

https://github.com/ess-dmsc/streaming-data-types

To install, run:

```pip install ess_streaming_data_types```

To install with the f142 C++ serialiser, run the following command:

```pip install ess_streaming_data_types[cpp_f142]```

## FlatBuffer Schemas

|name|description|
|----|-----------|
|hs00|Histogram schema (deprecated in favour of hs01)|
|hs01|Histogram schema|
|ns10|NICOS cache entry schema|
|pl72|Run start|
|6s4t|Run stop|
|f142|Log data|
|ev42|Event data|
|ev43|Event data from multiple pulses|
|x5f2|Status messages|
|tdct|Timestamps|
|ep00|EPICS connection info|
|rf5k|Forwarder configuration update|
|answ|File-writer command response|
|wrdn|File-writer finished writing|
|NDAr|**Deprecated**|
|ADAr|For storing EPICS areaDetector data|

### hs00 and hs01
Schema for histogram data. It is one of the more complicated to use schemas.
It takes a Python dictionary as its input; this dictionary needs to have correctly
named fields.

The input histogram data for serialisation and the output deserialisation data
have the same dictionary "layout".
Example for a 2-D histogram:
```json
hist = {
    "source": "some_source",
    "timestamp": 123456,
    "current_shape": [2, 5],
    "dim_metadata": [
        {
            "length": 2,
            "unit": "a",
            "label": "x",
            "bin_boundaries": np.array([10, 11, 12]),
        },
        {
            "length": 5,
            "unit": "b",
            "label": "y",
            "bin_boundaries": np.array([0, 1, 2, 3, 4, 5]),
        },
    ],
    "last_metadata_timestamp": 123456,
    "data": np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]),
    "errors": np.array([[5, 4, 3, 2, 1], [10, 9, 8, 7, 6]]),
    "info": "info_string",
}
```
The arrays passed in for `data`, `errors` and `bin_boundaries` can be NumPy arrays
or regular lists, but on deserialisation they will be NumPy arrays.

### *f142* C++ serialiser

A C++ implementation of a f142 serialiser with Python bindings implemented using pybind11. C++ code will only be compiled if the pybind11 Python package is available. Note that the C++ code requires ```std::span``` and hence C++20 in its current form.

#### Usage

```Python
from fast_f142_serialiser import f142_serialiser
from datetime import datetime
serialiser = f142_serialiser()
msg = serialiser.serialise_message("source_name", 3.14, datetime.now())
print(f"Message is of type \"{type(msg)}\" with a length of {len(msg)} bytes.")
# Message is of type "<class 'bytes'>" with a length of 96 bytes.
```

To maximise performance of the C++ f142 serialiser, an object is used in order to preserve resources and re-use memory allocated to the serialiser. Note that the C++ f142 serialiser does not implement serialisation of alarm and alarm severity information. Also, instead of providing the timestamp in nanoseconds since Unix epoch, it must be provided in the form of a ```datetime``` object.
