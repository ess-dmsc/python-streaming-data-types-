# Python Streaming Data Types
Utilities for working with the FlatBuffers schemas used at the European
Spallation Source ERIC for data transport.

https://github.com/ess-dmsc/streaming-data-types

## FlatBuffer Schemas

|name|description|verifiable*|
|----|-----------|----------|
|hs00|Histogram schema|Y|
|ns10|NICOS cache entry schema|Y|
|pl72|Run start|N|
|6s4t|Run stop|N|
|f142|Log data|Y|
|ev42|Event data|Y|
|x5f2|Status messages|N|
|tdct|Timestamps|Y|
|ep00|EPICS connection info|Y|
|rf5k|Forwarder configuration update|Y|
|answ|File-writer command response|?|

\* whether it passes verification via the C++ FlatBuffers library.

### hs00
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
