# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers
from flatbuffers.compat import import_numpy

np = import_numpy()


class ArrayUInt8(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = ArrayUInt8()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsArrayUInt8(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)

    @classmethod
    def ArrayUInt8BufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(
            buf, offset, b"\x73\x63\x61\x6C", size_prefixed=size_prefixed
        )

    # ArrayUInt8
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # ArrayUInt8
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(
                flatbuffers.number_types.Uint8Flags,
                a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 1),
            )
        return 0

    # ArrayUInt8
    def ValueAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Uint8Flags, o)
        return 0

    # ArrayUInt8
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # ArrayUInt8
    def ValueIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0


def ArrayUInt8Start(builder):
    builder.StartObject(1)


def Start(builder):
    return ArrayUInt8Start(builder)


def ArrayUInt8AddValue(builder, value):
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0
    )


def AddValue(builder, value):
    return ArrayUInt8AddValue(builder, value)


def ArrayUInt8StartValueVector(builder, numElems):
    return builder.StartVector(1, numElems, 1)


def StartValueVector(builder, numElems):
    return ArrayUInt8StartValueVector(builder, numElems)


def ArrayUInt8End(builder):
    return builder.EndObject()


def End(builder):
    return ArrayUInt8End(builder)
