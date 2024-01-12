# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers
from flatbuffers.compat import import_numpy

np = import_numpy()


class hs02_EventHistogram(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = hs02_EventHistogram()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAshs02_EventHistogram(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)

    @classmethod
    def hs02_EventHistogramBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(
            buf, offset, b"\x68\x73\x30\x32", size_prefixed=size_prefixed
        )

    # hs02_EventHistogram
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # hs02_EventHistogram
    def SourceName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # hs02_EventHistogram
    def Timestamp(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # hs02_EventHistogram
    def DimMetadata(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from .DimensionMetaData import DimensionMetaData

            obj = DimensionMetaData()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # hs02_EventHistogram
    def DimMetadataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # hs02_EventHistogram
    def DimMetadataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # hs02_EventHistogram
    def LastMetadataTimestamp(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # hs02_EventHistogram
    def CurrentShape(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int32Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4),
            )
        return 0

    # hs02_EventHistogram
    def CurrentShapeAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # hs02_EventHistogram
    def CurrentShapeLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # hs02_EventHistogram
    def CurrentShapeIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        return o == 0

    # hs02_EventHistogram
    def Offset(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int32Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4),
            )
        return 0

    # hs02_EventHistogram
    def OffsetAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # hs02_EventHistogram
    def OffsetLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # hs02_EventHistogram
    def OffsetIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        return o == 0

    # hs02_EventHistogram
    def DataType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # hs02_EventHistogram
    def Data(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(18))
        if o != 0:
            from flatbuffers.table import Table

            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # hs02_EventHistogram
    def ErrorsType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(20))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # hs02_EventHistogram
    def Errors(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(22))
        if o != 0:
            from flatbuffers.table import Table

            obj = Table(bytearray(), 0)
            self._tab.Union(obj, o)
            return obj
        return None

    # hs02_EventHistogram
    def Info(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(24))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None


def hs02_EventHistogramStart(builder):
    builder.StartObject(11)


def Start(builder):
    hs02_EventHistogramStart(builder)


def hs02_EventHistogramAddSourceName(builder, sourceName):
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(sourceName), 0
    )


def AddSourceName(builder, sourceName):
    hs02_EventHistogramAddSourceName(builder, sourceName)


def hs02_EventHistogramAddTimestamp(builder, timestamp):
    builder.PrependInt64Slot(1, timestamp, 0)


def AddTimestamp(builder, timestamp):
    hs02_EventHistogramAddTimestamp(builder, timestamp)


def hs02_EventHistogramAddDimMetadata(builder, dimMetadata):
    builder.PrependUOffsetTRelativeSlot(
        2, flatbuffers.number_types.UOffsetTFlags.py_type(dimMetadata), 0
    )


def AddDimMetadata(builder, dimMetadata):
    hs02_EventHistogramAddDimMetadata(builder, dimMetadata)


def hs02_EventHistogramStartDimMetadataVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartDimMetadataVector(builder, numElems: int) -> int:
    return hs02_EventHistogramStartDimMetadataVector(builder, numElems)


def hs02_EventHistogramAddLastMetadataTimestamp(builder, lastMetadataTimestamp):
    builder.PrependInt64Slot(3, lastMetadataTimestamp, 0)


def AddLastMetadataTimestamp(builder, lastMetadataTimestamp):
    hs02_EventHistogramAddLastMetadataTimestamp(builder, lastMetadataTimestamp)


def hs02_EventHistogramAddCurrentShape(builder, currentShape):
    builder.PrependUOffsetTRelativeSlot(
        4, flatbuffers.number_types.UOffsetTFlags.py_type(currentShape), 0
    )


def AddCurrentShape(builder, currentShape):
    hs02_EventHistogramAddCurrentShape(builder, currentShape)


def hs02_EventHistogramStartCurrentShapeVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartCurrentShapeVector(builder, numElems: int) -> int:
    return hs02_EventHistogramStartCurrentShapeVector(builder, numElems)


def hs02_EventHistogramAddOffset(builder, offset):
    builder.PrependUOffsetTRelativeSlot(
        5, flatbuffers.number_types.UOffsetTFlags.py_type(offset), 0
    )


def AddOffset(builder, offset):
    hs02_EventHistogramAddOffset(builder, offset)


def hs02_EventHistogramStartOffsetVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartOffsetVector(builder, numElems: int) -> int:
    return hs02_EventHistogramStartOffsetVector(builder, numElems)


def hs02_EventHistogramAddDataType(builder, dataType):
    builder.PrependUint8Slot(6, dataType, 0)


def AddDataType(builder, dataType):
    hs02_EventHistogramAddDataType(builder, dataType)


def hs02_EventHistogramAddData(builder, data):
    builder.PrependUOffsetTRelativeSlot(
        7, flatbuffers.number_types.UOffsetTFlags.py_type(data), 0
    )


def AddData(builder, data):
    hs02_EventHistogramAddData(builder, data)


def hs02_EventHistogramAddErrorsType(builder, errorsType):
    builder.PrependUint8Slot(8, errorsType, 0)


def AddErrorsType(builder, errorsType):
    hs02_EventHistogramAddErrorsType(builder, errorsType)


def hs02_EventHistogramAddErrors(builder, errors):
    builder.PrependUOffsetTRelativeSlot(
        9, flatbuffers.number_types.UOffsetTFlags.py_type(errors), 0
    )


def AddErrors(builder, errors):
    hs02_EventHistogramAddErrors(builder, errors)


def hs02_EventHistogramAddInfo(builder, info):
    builder.PrependUOffsetTRelativeSlot(
        10, flatbuffers.number_types.UOffsetTFlags.py_type(info), 0
    )


def AddInfo(builder, info):
    hs02_EventHistogramAddInfo(builder, info)


def hs02_EventHistogramEnd(builder):
    return builder.EndObject()


def End(builder):
    return hs02_EventHistogramEnd(builder)
