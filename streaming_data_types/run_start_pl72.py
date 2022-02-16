import time
from collections import namedtuple
from datetime import datetime
from typing import NamedTuple, Optional, Union

import flatbuffers
import numpy as np

from streaming_data_types.fbschemas.run_start_pl72 import (
    RunStart,
    SpectraDetectorMapping,
)
from streaming_data_types.utils import check_schema_identifier

FILE_IDENTIFIER = b"pl72"


DetectorSpectrumMap = namedtuple(
    "DetectorSpectrumMap",
    (
        "spectrum_numbers",  # numpy ndarray of int
        "detector_ids",  # numpy ndarray of int
        "n_spectra",  # int
    ),
)


def serialise_pl72(
    job_id: str,
    filename: str,
    start_time: Union[int, datetime, None] = None,
    stop_time: Union[int, datetime, None] = None,
    run_name: str = "test_run",
    nexus_structure: str = "{}",
    service_id: str = "",
    instrument_name: str = "TEST",
    broker: str = "localhost:9092",
    metadata: str = "{}",
    detector_spectrum_map: Optional[DetectorSpectrumMap] = None,
    control_topic: str = "",
) -> bytes:
    builder = flatbuffers.Builder(512)
    builder.ForceDefaults(True)

    if type(start_time) is datetime:
        start_time = int(start_time.timestamp() * 1000)
    elif start_time is None:
        start_time = int(time.time() * 1000)
    if service_id is None:
        service_id = ""
    if type(stop_time) is datetime:
        stop_time = int(stop_time.timestamp() * 1000)
    elif stop_time is None:
        stop_time = 0

    service_id_offset = builder.CreateString(service_id)
    broker_offset = builder.CreateString(broker)
    job_id_offset = builder.CreateString(job_id)
    nexus_structure_offset = builder.CreateString(nexus_structure)
    instrument_name_offset = builder.CreateString(instrument_name)
    run_name_offset = builder.CreateString(run_name)
    filename_offset = builder.CreateString(filename)
    metadata_offset = builder.CreateString(metadata)
    control_topic_offset = builder.CreateString(control_topic)

    # Build detector-spectrum map
    if detector_spectrum_map is not None:
        spectrum_map_offset = builder.CreateNumpyVector(
            np.asarray(detector_spectrum_map.spectrum_numbers).astype(np.int32)
        )
        det_id_map_offset = builder.CreateNumpyVector(
            np.asarray(detector_spectrum_map.detector_ids).astype(np.int32)
        )
        SpectraDetectorMapping.SpectraDetectorMappingStart(builder)
        SpectraDetectorMapping.SpectraDetectorMappingAddSpectrum(
            builder, spectrum_map_offset
        )
        SpectraDetectorMapping.SpectraDetectorMappingAddDetectorId(
            builder, det_id_map_offset
        )
        SpectraDetectorMapping.SpectraDetectorMappingAddNSpectra(
            builder, detector_spectrum_map.n_spectra
        )
        detector_spectrum_map_offset = SpectraDetectorMapping.SpectraDetectorMappingEnd(
            builder
        )

    # Build the actual buffer
    RunStart.RunStartStart(builder)
    RunStart.RunStartAddServiceId(builder, service_id_offset)
    RunStart.RunStartAddBroker(builder, broker_offset)
    RunStart.RunStartAddJobId(builder, job_id_offset)
    RunStart.RunStartAddNexusStructure(builder, nexus_structure_offset)
    RunStart.RunStartAddInstrumentName(builder, instrument_name_offset)
    RunStart.RunStartAddRunName(builder, run_name_offset)
    RunStart.RunStartAddStopTime(builder, stop_time)
    RunStart.RunStartAddStartTime(builder, start_time)
    RunStart.RunStartAddFilename(builder, filename_offset)
    RunStart.RunStartAddNPeriods(builder, 1)
    RunStart.RunStartAddMetadata(builder, metadata_offset)
    if detector_spectrum_map is not None:
        RunStart.RunStartAddDetectorSpectrumMap(builder, detector_spectrum_map_offset)
    RunStart.RunStartAddControlTopic(builder, control_topic_offset)

    run_start_message = RunStart.RunStartEnd(builder)

    builder.Finish(run_start_message, file_identifier=FILE_IDENTIFIER)
    return bytes(builder.Output())


class RunStartInfo(NamedTuple):
    job_id: str
    filename: str
    start_time: int
    stop_time: int
    nexus_structure: str
    service_id: str
    broker: str
    run_name: str = ""
    instrument_name: str = ""
    metadata: str = ""
    detector_spectrum_map: Optional[DetectorSpectrumMap] = None
    control_topic: str = ""


def deserialise_pl72(buffer: Union[bytearray, bytes]) -> RunStartInfo:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    run_start = RunStart.RunStart.GetRootAsRunStart(buffer, 0)
    service_id = run_start.ServiceId() if run_start.ServiceId() else b""
    broker = run_start.Broker() if run_start.Broker() else b""
    job_id = run_start.JobId() if run_start.JobId() else b""
    filename = run_start.Filename() if run_start.Filename() else b""
    nexus_structure = run_start.NexusStructure() if run_start.NexusStructure() else b""
    instrument_name = run_start.InstrumentName() if run_start.InstrumentName() else b""
    run_name = run_start.RunName() if run_start.RunName() else b""
    metadata = run_start.Metadata() if run_start.Metadata() else b""
    control_topic = run_start.ControlTopic() if run_start.ControlTopic() else b""

    detector_spectrum_map = None
    det_spec_map_buf = run_start.DetectorSpectrumMap()
    if det_spec_map_buf is not None:
        detector_spectrum_map = DetectorSpectrumMap(
            det_spec_map_buf.SpectrumAsNumpy(),
            det_spec_map_buf.DetectorIdAsNumpy(),
            det_spec_map_buf.NSpectra(),
        )

    return RunStartInfo(
        job_id=job_id.decode(),
        filename=filename.decode(),
        start_time=run_start.StartTime(),
        stop_time=run_start.StopTime(),
        run_name=run_name.decode(),
        nexus_structure=nexus_structure.decode(),
        service_id=service_id.decode(),
        instrument_name=instrument_name.decode(),
        broker=broker.decode(),
        metadata=metadata.decode(),
        detector_spectrum_map=detector_spectrum_map,
        control_topic=control_topic.decode(),
    )
