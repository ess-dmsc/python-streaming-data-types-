# automatically generated by the FlatBuffers compiler, do not modify

# namespace: 

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()


class Array(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsArray(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Array()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def ArrayBufferHasIdentifier(cls, buf, offset, size_prefixed=False):
        return flatbuffers.util.BufferHasIdentifier(buf, offset, b"\x6E\x73\x31\x31", size_prefixed=size_prefixed)

    # Array
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Array
    def Value(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * 4
            x = self._tab.Indirect(x)
            from ArrayElement import ArrayElement
            obj = ArrayElement()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None

    # Array
    def ValueLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Array
    def ValueIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

    # Array
    def ArrayType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int8Flags, o + self._tab.Pos)
        return 0

def ArrayStart(builder): builder.StartObject(2)
def ArrayAddValue(builder, value): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0)
def ArrayStartValueVector(builder, numElems): return builder.StartVector(4, numElems, 4)
def ArrayAddArrayType(builder, arrayType): builder.PrependInt8Slot(1, arrayType, 0)
def ArrayEnd(builder): return builder.EndObject()
