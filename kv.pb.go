// Code generated by protoc-gen-go-lite. DO NOT EDIT.
// protoc-gen-go-lite version: v0.4.9
// source: github.com/aperturerobotics/go-kvfile/kv.proto

package kvfile

import (
	io "io"

	protobuf_go_lite "github.com/aperturerobotics/protobuf-go-lite"
	json "github.com/aperturerobotics/protobuf-go-lite/json"
	errors "github.com/pkg/errors"
)

// IndexEntry is an entry in the index.
// The index is sorted by key.
type IndexEntry struct {
	unknownFields []byte
	// Key is the key of the entry.
	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// Offset is the position of the value in bytes.
	Offset uint64 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	// Size is the size of the value in bytes.
	Size uint64 `protobuf:"varint,3,opt,name=size,proto3" json:"size,omitempty"`
}

func (x *IndexEntry) Reset() {
	*x = IndexEntry{}
}

func (*IndexEntry) ProtoMessage() {}

func (x *IndexEntry) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *IndexEntry) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *IndexEntry) GetSize() uint64 {
	if x != nil {
		return x.Size
	}
	return 0
}

func (m *IndexEntry) CloneVT() *IndexEntry {
	if m == nil {
		return (*IndexEntry)(nil)
	}
	r := new(IndexEntry)
	r.Offset = m.Offset
	r.Size = m.Size
	if rhs := m.Key; rhs != nil {
		tmpBytes := make([]byte, len(rhs))
		copy(tmpBytes, rhs)
		r.Key = tmpBytes
	}
	if len(m.unknownFields) > 0 {
		r.unknownFields = make([]byte, len(m.unknownFields))
		copy(r.unknownFields, m.unknownFields)
	}
	return r
}

func (m *IndexEntry) CloneMessageVT() protobuf_go_lite.CloneMessage {
	return m.CloneVT()
}

func (this *IndexEntry) EqualVT(that *IndexEntry) bool {
	if this == that {
		return true
	} else if this == nil || that == nil {
		return false
	}
	if string(this.Key) != string(that.Key) {
		return false
	}
	if this.Offset != that.Offset {
		return false
	}
	if this.Size != that.Size {
		return false
	}
	return string(this.unknownFields) == string(that.unknownFields)
}

func (this *IndexEntry) EqualMessageVT(thatMsg any) bool {
	that, ok := thatMsg.(*IndexEntry)
	if !ok {
		return false
	}
	return this.EqualVT(that)
}

// MarshalProtoJSON marshals the IndexEntry message to JSON.
func (x *IndexEntry) MarshalProtoJSON(s *json.MarshalState) {
	if x == nil {
		s.WriteNil()
		return
	}
	s.WriteObjectStart()
	var wroteField bool
	if len(x.Key) > 0 || s.HasField("key") {
		s.WriteMoreIf(&wroteField)
		s.WriteObjectField("key")
		s.WriteBytes(x.Key)
	}
	if x.Offset != 0 || s.HasField("offset") {
		s.WriteMoreIf(&wroteField)
		s.WriteObjectField("offset")
		s.WriteUint64(x.Offset)
	}
	if x.Size != 0 || s.HasField("size") {
		s.WriteMoreIf(&wroteField)
		s.WriteObjectField("size")
		s.WriteUint64(x.Size)
	}
	s.WriteObjectEnd()
}

// MarshalJSON marshals the IndexEntry to JSON.
func (x *IndexEntry) MarshalJSON() ([]byte, error) {
	return json.DefaultMarshalerConfig.Marshal(x)
}

// UnmarshalProtoJSON unmarshals the IndexEntry message from JSON.
func (x *IndexEntry) UnmarshalProtoJSON(s *json.UnmarshalState) {
	if s.ReadNil() {
		return
	}
	s.ReadObject(func(key string) {
		switch key {
		default:
			s.ReadAny() // ignore unknown field
		case "key":
			s.AddField("key")
			x.Key = s.ReadBytes()
		case "offset":
			s.AddField("offset")
			x.Offset = s.ReadUint64()
		case "size":
			s.AddField("size")
			x.Size = s.ReadUint64()
		}
	})
}

// UnmarshalJSON unmarshals the IndexEntry from JSON.
func (x *IndexEntry) UnmarshalJSON(b []byte) error {
	return json.DefaultUnmarshalerConfig.Unmarshal(b, x)
}

func (m *IndexEntry) MarshalVT() (dAtA []byte, err error) {
	if m == nil {
		return nil, nil
	}
	size := m.SizeVT()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBufferVT(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *IndexEntry) MarshalToVT(dAtA []byte) (int, error) {
	size := m.SizeVT()
	return m.MarshalToSizedBufferVT(dAtA[:size])
}

func (m *IndexEntry) MarshalToSizedBufferVT(dAtA []byte) (int, error) {
	if m == nil {
		return 0, nil
	}
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.unknownFields != nil {
		i -= len(m.unknownFields)
		copy(dAtA[i:], m.unknownFields)
	}
	if m.Size != 0 {
		i = protobuf_go_lite.EncodeVarint(dAtA, i, uint64(m.Size))
		i--
		dAtA[i] = 0x18
	}
	if m.Offset != 0 {
		i = protobuf_go_lite.EncodeVarint(dAtA, i, uint64(m.Offset))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Key) > 0 {
		i -= len(m.Key)
		copy(dAtA[i:], m.Key)
		i = protobuf_go_lite.EncodeVarint(dAtA, i, uint64(len(m.Key)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *IndexEntry) SizeVT() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Key)
	if l > 0 {
		n += 1 + l + protobuf_go_lite.SizeOfVarint(uint64(l))
	}
	if m.Offset != 0 {
		n += 1 + protobuf_go_lite.SizeOfVarint(uint64(m.Offset))
	}
	if m.Size != 0 {
		n += 1 + protobuf_go_lite.SizeOfVarint(uint64(m.Size))
	}
	n += len(m.unknownFields)
	return n
}

func (m *IndexEntry) UnmarshalVT(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return protobuf_go_lite.ErrIntOverflow
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return errors.Errorf("proto: IndexEntry: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return errors.Errorf("proto: IndexEntry: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return errors.Errorf("proto: wrong wireType = %d for field Key", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protobuf_go_lite.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return protobuf_go_lite.ErrInvalidLength
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return protobuf_go_lite.ErrInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Key = append(m.Key[:0], dAtA[iNdEx:postIndex]...)
			if m.Key == nil {
				m.Key = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return errors.Errorf("proto: wrong wireType = %d for field Offset", wireType)
			}
			m.Offset = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protobuf_go_lite.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Offset |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return errors.Errorf("proto: wrong wireType = %d for field Size", wireType)
			}
			m.Size = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return protobuf_go_lite.ErrIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Size |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := protobuf_go_lite.Skip(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return protobuf_go_lite.ErrInvalidLength
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.unknownFields = append(m.unknownFields, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
