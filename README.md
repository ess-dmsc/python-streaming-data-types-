# Python Streaming Data Types
Utilities for working with the FlatBuffers schemas used at the European
Spallation Source ERIC for data transport.

https://github.com/ess-dmsc/streaming-data-types

## FlatBuffer Schemas

| name | description                                                                  |
|------|------------------------------------------------------------------------------|
| hs00 | **Deprecated** Histogram schema (deprecated in favour of hs01)               |
| hs01 | **Deprecated** Histogram schema                                              |
| hs02 | Histogram schema                                                             |
| ns10 | NICOS cache entry schema                                                     |
| pl72 | Run start                                                                    |
| 6s4t | Run stop                                                                     |
| f142 | Log data (deprecated in favour of f144)                                      |
| f144 | Log data                                                                     |
| ev42 | Event data (deprecated in favour of ev44)                                    |
| ev43 | Event data from multiple pulses                                              |
| ev44 | Event data with signed data types                                            |
| x5f2 | Status messages                                                              |
| tdct | Timestamps                                                                   |
| ep00 | EPICS connection info (deprecated in favour of ep01)                         |
| ep01 | EPICS connection info                                                        |
| rf5k | Forwarder configuration update                                               |
| answ | File-writer command response                                                 |
| wrdn | File-writer finished writing                                                 |
| NDAr | **Deprecated**                                                               |
| ADAr | EPICS areaDetector data                                                      |
| al00 | Alarm/status messages used by the Forwarder and NICOS                        |
| senv | **Deprecated**                                                               |
| json | Generic JSON data                                                            |
| se00 | Arrays with optional timestamps, for example waveform data. Replaces _senv_. |

### hs00, hs01, hs02
Schema for histogram data. It is one of the more complicated to use schemas.
It takes a Python dictionary as its input; this dictionary needs to have correctly
named fields.

The input histogram data for serialisation and the output deserialisation data
have the same dictionary "layout".

Note that hs02 uses 'source_name' rather than 'source'.

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


## Developer documentation

See [README_DEV.md](README_DEV.md)
