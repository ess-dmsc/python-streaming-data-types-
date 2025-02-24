# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers
from flatbuffers.compat import import_numpy

np = import_numpy()


class AN44EventMessage(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = AN44EventMessage()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsAN44EventMessage(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)

    @classmethod
    def AN44EventMessageBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(
            buf, offset, b"\x61\x6E\x34\x34", size_prefixed=size_prefixed
        )

    # AN44EventMessage
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # AN44EventMessage
    def SourceName(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # AN44EventMessage
    def MessageId(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int64Flags, o + self._tab.Pos)
        return 0

    # AN44EventMessage
    def ReferenceTime(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int64Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 8),
            )
        return 0

    # AN44EventMessage
    def ReferenceTimeAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int64Flags, o)
        return 0

    # AN44EventMessage
    def ReferenceTimeLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # AN44EventMessage
    def ReferenceTimeIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # AN44EventMessage
    def ReferenceTimeIndex(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int32Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4),
            )
        return 0

    # AN44EventMessage
    def ReferenceTimeIndexAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # AN44EventMessage
    def ReferenceTimeIndexLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # AN44EventMessage
    def ReferenceTimeIndexIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

    # AN44EventMessage
    def TimeOfFlight(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int32Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4),
            )
        return 0

    # AN44EventMessage
    def TimeOfFlightAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # AN44EventMessage
    def TimeOfFlightLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # AN44EventMessage
    def TimeOfFlightIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        return o == 0

    # AN44EventMessage
    def PixelId(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int32Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4),
            )
        return 0

    # AN44EventMessage
    def PixelIdAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # AN44EventMessage
    def PixelIdLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # AN44EventMessage
    def PixelIdIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(14))
        return o == 0

    # AN44EventMessage
    def Weight(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Int16Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 2),
            )
        return 0

    # AN44EventMessage
    def WeightAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int16Flags, o)
        return 0

    # AN44EventMessage
    def WeightLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # AN44EventMessage
    def WeightIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(16))
        return o == 0


def AN44EventMessageStart(builder):
    builder.StartObject(7)


def Start(builder):
    AN44EventMessageStart(builder)


def AN44EventMessageAddSourceName(builder, sourceName):
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(sourceName), 0
    )


def AddSourceName(builder, sourceName):
    AN44EventMessageAddSourceName(builder, sourceName)


def AN44EventMessageAddMessageId(builder, messageId):
    builder.PrependInt64Slot(1, messageId, 0)


def AddMessageId(builder, messageId):
    AN44EventMessageAddMessageId(builder, messageId)


def AN44EventMessageAddReferenceTime(builder, referenceTime):
    builder.PrependUOffsetTRelativeSlot(
        2, flatbuffers.number_types.UOffsetTFlags.py_type(referenceTime), 0
    )


def AddReferenceTime(builder, referenceTime):
    AN44EventMessageAddReferenceTime(builder, referenceTime)


def AN44EventMessageStartReferenceTimeVector(builder, numElems):
    return builder.StartVector(8, numElems, 8)


def StartReferenceTimeVector(builder, numElems):
    return AN44EventMessageStartReferenceTimeVector(builder, numElems)


def AN44EventMessageAddReferenceTimeIndex(builder, referenceTimeIndex):
    builder.PrependUOffsetTRelativeSlot(
        3, flatbuffers.number_types.UOffsetTFlags.py_type(referenceTimeIndex), 0
    )


def AddReferenceTimeIndex(builder, referenceTimeIndex):
    AN44EventMessageAddReferenceTimeIndex(builder, referenceTimeIndex)


def AN44EventMessageStartReferenceTimeIndexVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartReferenceTimeIndexVector(builder, numElems):
    return AN44EventMessageStartReferenceTimeIndexVector(builder, numElems)


def AN44EventMessageAddTimeOfFlight(builder, timeOfFlight):
    builder.PrependUOffsetTRelativeSlot(
        4, flatbuffers.number_types.UOffsetTFlags.py_type(timeOfFlight), 0
    )


def AddTimeOfFlight(builder, timeOfFlight):
    AN44EventMessageAddTimeOfFlight(builder, timeOfFlight)


def AN44EventMessageStartTimeOfFlightVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartTimeOfFlightVector(builder, numElems):
    return AN44EventMessageStartTimeOfFlightVector(builder, numElems)


def AN44EventMessageAddPixelId(builder, pixelId):
    builder.PrependUOffsetTRelativeSlot(
        5, flatbuffers.number_types.UOffsetTFlags.py_type(pixelId), 0
    )


def AddPixelId(builder, pixelId):
    AN44EventMessageAddPixelId(builder, pixelId)


def AN44EventMessageStartPixelIdVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)


def StartPixelIdVector(builder, numElems):
    return AN44EventMessageStartPixelIdVector(builder, numElems)


def AN44EventMessageAddWeight(builder, weight):
    builder.PrependUOffsetTRelativeSlot(
        6, flatbuffers.number_types.UOffsetTFlags.py_type(weight), 0
    )


def AddWeight(builder, weight):
    AN44EventMessageAddWeight(builder, weight)


def AN44EventMessageStartWeightVector(builder, numElems):
    return builder.StartVector(2, numElems, 2)


def StartWeightVector(builder, numElems):
    return AN44EventMessageStartWeightVector(builder, numElems)


def AN44EventMessageEnd(builder):
    return builder.EndObject()


def End(builder):
    return AN44EventMessageEnd(builder)
