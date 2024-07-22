package common

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Column) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			z.Name, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "type":
			z.Type, err = dc.ReadByte()
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "charset":
			z.Charset, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Charset")
				return
			}
		case "collation":
			z.Collation, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Collation")
				return
			}
		case "flag":
			err = z.Flag.DecodeMsg(dc)
			if err != nil {
				err = msgp.WrapError(err, "Flag")
				return
			}
		case "column":
			z.Value, err = dc.ReadIntf()
			if err != nil {
				err = msgp.WrapError(err, "Value")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Column) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "name"
	err = en.Append(0x86, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Name)
	if err != nil {
		err = msgp.WrapError(err, "Name")
		return
	}
	// write "type"
	err = en.Append(0xa4, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteByte(z.Type)
	if err != nil {
		err = msgp.WrapError(err, "Type")
		return
	}
	// write "charset"
	err = en.Append(0xa7, 0x63, 0x68, 0x61, 0x72, 0x73, 0x65, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.Charset)
	if err != nil {
		err = msgp.WrapError(err, "Charset")
		return
	}
	// write "collation"
	err = en.Append(0xa9, 0x63, 0x6f, 0x6c, 0x6c, 0x61, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteString(z.Collation)
	if err != nil {
		err = msgp.WrapError(err, "Collation")
		return
	}
	// write "flag"
	err = en.Append(0xa4, 0x66, 0x6c, 0x61, 0x67)
	if err != nil {
		return
	}
	err = z.Flag.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "Flag")
		return
	}
	// write "column"
	err = en.Append(0xa6, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteIntf(z.Value)
	if err != nil {
		err = msgp.WrapError(err, "Value")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Column) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "name"
	o = append(o, 0x86, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "type"
	o = append(o, 0xa4, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendByte(o, z.Type)
	// string "charset"
	o = append(o, 0xa7, 0x63, 0x68, 0x61, 0x72, 0x73, 0x65, 0x74)
	o = msgp.AppendString(o, z.Charset)
	// string "collation"
	o = append(o, 0xa9, 0x63, 0x6f, 0x6c, 0x6c, 0x61, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendString(o, z.Collation)
	// string "flag"
	o = append(o, 0xa4, 0x66, 0x6c, 0x61, 0x67)
	o, err = z.Flag.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "Flag")
		return
	}
	// string "column"
	o = append(o, 0xa6, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e)
	o, err = msgp.AppendIntf(o, z.Value)
	if err != nil {
		err = msgp.WrapError(err, "Value")
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Column) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "type":
			z.Type, bts, err = msgp.ReadByteBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "charset":
			z.Charset, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Charset")
				return
			}
		case "collation":
			z.Collation, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Collation")
				return
			}
		case "flag":
			bts, err = z.Flag.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Flag")
				return
			}
		case "column":
			z.Value, bts, err = msgp.ReadIntfBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Value")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Column) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 5 + msgp.ByteSize + 8 + msgp.StringPrefixSize + len(z.Charset) + 10 + msgp.StringPrefixSize + len(z.Collation) + 5 + z.Flag.Msgsize() + 7 + msgp.GuessSize(z.Value)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RowChangedEvent) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "PhysicalTableID":
			z.PhysicalTableID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "PhysicalTableID")
				return
			}
		case "StartTs":
			z.StartTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "CommitTs":
			z.CommitTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "CommitTs")
				return
			}
		case "columns":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Columns")
				return
			}
			if cap(z.Columns) >= int(zb0002) {
				z.Columns = (z.Columns)[:zb0002]
			} else {
				z.Columns = make([]*Column, zb0002)
			}
			for za0001 := range z.Columns {
				if dc.IsNil() {
					err = dc.ReadNil()
					if err != nil {
						err = msgp.WrapError(err, "Columns", za0001)
						return
					}
					z.Columns[za0001] = nil
				} else {
					if z.Columns[za0001] == nil {
						z.Columns[za0001] = new(Column)
					}
					err = z.Columns[za0001].DecodeMsg(dc)
					if err != nil {
						err = msgp.WrapError(err, "Columns", za0001)
						return
					}
				}
			}
		case "pre-columns":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "PreColumns")
				return
			}
			if cap(z.PreColumns) >= int(zb0003) {
				z.PreColumns = (z.PreColumns)[:zb0003]
			} else {
				z.PreColumns = make([]*Column, zb0003)
			}
			for za0002 := range z.PreColumns {
				if dc.IsNil() {
					err = dc.ReadNil()
					if err != nil {
						err = msgp.WrapError(err, "PreColumns", za0002)
						return
					}
					z.PreColumns[za0002] = nil
				} else {
					if z.PreColumns[za0002] == nil {
						z.PreColumns[za0002] = new(Column)
					}
					err = z.PreColumns[za0002].DecodeMsg(dc)
					if err != nil {
						err = msgp.WrapError(err, "PreColumns", za0002)
						return
					}
				}
			}
		case "replicating-ts":
			z.ReplicatingTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ReplicatingTs")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *RowChangedEvent) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "PhysicalTableID"
	err = en.Append(0x86, 0xaf, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x44)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.PhysicalTableID)
	if err != nil {
		err = msgp.WrapError(err, "PhysicalTableID")
		return
	}
	// write "StartTs"
	err = en.Append(0xa7, 0x53, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.StartTs)
	if err != nil {
		err = msgp.WrapError(err, "StartTs")
		return
	}
	// write "CommitTs"
	err = en.Append(0xa8, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.CommitTs)
	if err != nil {
		err = msgp.WrapError(err, "CommitTs")
		return
	}
	// write "columns"
	err = en.Append(0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Columns)))
	if err != nil {
		err = msgp.WrapError(err, "Columns")
		return
	}
	for za0001 := range z.Columns {
		if z.Columns[za0001] == nil {
			err = en.WriteNil()
			if err != nil {
				return
			}
		} else {
			err = z.Columns[za0001].EncodeMsg(en)
			if err != nil {
				err = msgp.WrapError(err, "Columns", za0001)
				return
			}
		}
	}
	// write "pre-columns"
	err = en.Append(0xab, 0x70, 0x72, 0x65, 0x2d, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.PreColumns)))
	if err != nil {
		err = msgp.WrapError(err, "PreColumns")
		return
	}
	for za0002 := range z.PreColumns {
		if z.PreColumns[za0002] == nil {
			err = en.WriteNil()
			if err != nil {
				return
			}
		} else {
			err = z.PreColumns[za0002].EncodeMsg(en)
			if err != nil {
				err = msgp.WrapError(err, "PreColumns", za0002)
				return
			}
		}
	}
	// write "replicating-ts"
	err = en.Append(0xae, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6e, 0x67, 0x2d, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ReplicatingTs)
	if err != nil {
		err = msgp.WrapError(err, "ReplicatingTs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *RowChangedEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "PhysicalTableID"
	o = append(o, 0x86, 0xaf, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x44)
	o = msgp.AppendInt64(o, z.PhysicalTableID)
	// string "StartTs"
	o = append(o, 0xa7, 0x53, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73)
	o = msgp.AppendUint64(o, z.StartTs)
	// string "CommitTs"
	o = append(o, 0xa8, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x54, 0x73)
	o = msgp.AppendUint64(o, z.CommitTs)
	// string "columns"
	o = append(o, 0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Columns)))
	for za0001 := range z.Columns {
		if z.Columns[za0001] == nil {
			o = msgp.AppendNil(o)
		} else {
			o, err = z.Columns[za0001].MarshalMsg(o)
			if err != nil {
				err = msgp.WrapError(err, "Columns", za0001)
				return
			}
		}
	}
	// string "pre-columns"
	o = append(o, 0xab, 0x70, 0x72, 0x65, 0x2d, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.PreColumns)))
	for za0002 := range z.PreColumns {
		if z.PreColumns[za0002] == nil {
			o = msgp.AppendNil(o)
		} else {
			o, err = z.PreColumns[za0002].MarshalMsg(o)
			if err != nil {
				err = msgp.WrapError(err, "PreColumns", za0002)
				return
			}
		}
	}
	// string "replicating-ts"
	o = append(o, 0xae, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6e, 0x67, 0x2d, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.ReplicatingTs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *RowChangedEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "PhysicalTableID":
			z.PhysicalTableID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PhysicalTableID")
				return
			}
		case "StartTs":
			z.StartTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "CommitTs":
			z.CommitTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CommitTs")
				return
			}
		case "columns":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Columns")
				return
			}
			if cap(z.Columns) >= int(zb0002) {
				z.Columns = (z.Columns)[:zb0002]
			} else {
				z.Columns = make([]*Column, zb0002)
			}
			for za0001 := range z.Columns {
				if msgp.IsNil(bts) {
					bts, err = msgp.ReadNilBytes(bts)
					if err != nil {
						return
					}
					z.Columns[za0001] = nil
				} else {
					if z.Columns[za0001] == nil {
						z.Columns[za0001] = new(Column)
					}
					bts, err = z.Columns[za0001].UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "Columns", za0001)
						return
					}
				}
			}
		case "pre-columns":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PreColumns")
				return
			}
			if cap(z.PreColumns) >= int(zb0003) {
				z.PreColumns = (z.PreColumns)[:zb0003]
			} else {
				z.PreColumns = make([]*Column, zb0003)
			}
			for za0002 := range z.PreColumns {
				if msgp.IsNil(bts) {
					bts, err = msgp.ReadNilBytes(bts)
					if err != nil {
						return
					}
					z.PreColumns[za0002] = nil
				} else {
					if z.PreColumns[za0002] == nil {
						z.PreColumns[za0002] = new(Column)
					}
					bts, err = z.PreColumns[za0002].UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "PreColumns", za0002)
						return
					}
				}
			}
		case "replicating-ts":
			z.ReplicatingTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ReplicatingTs")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *RowChangedEvent) Msgsize() (s int) {
	s = 1 + 16 + msgp.Int64Size + 8 + msgp.Uint64Size + 9 + msgp.Uint64Size + 8 + msgp.ArrayHeaderSize
	for za0001 := range z.Columns {
		if z.Columns[za0001] == nil {
			s += msgp.NilSize
		} else {
			s += z.Columns[za0001].Msgsize()
		}
	}
	s += 12 + msgp.ArrayHeaderSize
	for za0002 := range z.PreColumns {
		if z.PreColumns[za0002] == nil {
			s += msgp.NilSize
		} else {
			s += z.PreColumns[za0002].Msgsize()
		}
	}
	s += 15 + msgp.Uint64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *TxnEvent) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "cluster-id":
			z.ClusterID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ClusterID")
				return
			}
		case "dispatcher-id":
			z.DispatcherID, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "DispatcherID")
				return
			}
		case "rows":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Rows")
				return
			}
			if cap(z.Rows) >= int(zb0002) {
				z.Rows = (z.Rows)[:zb0002]
			} else {
				z.Rows = make([]*RowChangedEvent, zb0002)
			}
			for za0001 := range z.Rows {
				if dc.IsNil() {
					err = dc.ReadNil()
					if err != nil {
						err = msgp.WrapError(err, "Rows", za0001)
						return
					}
					z.Rows[za0001] = nil
				} else {
					if z.Rows[za0001] == nil {
						z.Rows[za0001] = new(RowChangedEvent)
					}
					err = z.Rows[za0001].DecodeMsg(dc)
					if err != nil {
						err = msgp.WrapError(err, "Rows", za0001)
						return
					}
				}
			}
		case "resolved-ts":
			z.ResolvedTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		case "start-ts":
			z.StartTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "commit-ts":
			z.CommitTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "CommitTs")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *TxnEvent) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "cluster-id"
	err = en.Append(0x86, 0xaa, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2d, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ClusterID)
	if err != nil {
		err = msgp.WrapError(err, "ClusterID")
		return
	}
	// write "dispatcher-id"
	err = en.Append(0xad, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x65, 0x72, 0x2d, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteString(z.DispatcherID)
	if err != nil {
		err = msgp.WrapError(err, "DispatcherID")
		return
	}
	// write "rows"
	err = en.Append(0xa4, 0x72, 0x6f, 0x77, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Rows)))
	if err != nil {
		err = msgp.WrapError(err, "Rows")
		return
	}
	for za0001 := range z.Rows {
		if z.Rows[za0001] == nil {
			err = en.WriteNil()
			if err != nil {
				return
			}
		} else {
			err = z.Rows[za0001].EncodeMsg(en)
			if err != nil {
				err = msgp.WrapError(err, "Rows", za0001)
				return
			}
		}
	}
	// write "resolved-ts"
	err = en.Append(0xab, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x2d, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ResolvedTs)
	if err != nil {
		err = msgp.WrapError(err, "ResolvedTs")
		return
	}
	// write "start-ts"
	err = en.Append(0xa8, 0x73, 0x74, 0x61, 0x72, 0x74, 0x2d, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.StartTs)
	if err != nil {
		err = msgp.WrapError(err, "StartTs")
		return
	}
	// write "commit-ts"
	err = en.Append(0xa9, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x2d, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.CommitTs)
	if err != nil {
		err = msgp.WrapError(err, "CommitTs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TxnEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "cluster-id"
	o = append(o, 0x86, 0xaa, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2d, 0x69, 0x64)
	o = msgp.AppendUint64(o, z.ClusterID)
	// string "dispatcher-id"
	o = append(o, 0xad, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x65, 0x72, 0x2d, 0x69, 0x64)
	o = msgp.AppendString(o, z.DispatcherID)
	// string "rows"
	o = append(o, 0xa4, 0x72, 0x6f, 0x77, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Rows)))
	for za0001 := range z.Rows {
		if z.Rows[za0001] == nil {
			o = msgp.AppendNil(o)
		} else {
			o, err = z.Rows[za0001].MarshalMsg(o)
			if err != nil {
				err = msgp.WrapError(err, "Rows", za0001)
				return
			}
		}
	}
	// string "resolved-ts"
	o = append(o, 0xab, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x2d, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.ResolvedTs)
	// string "start-ts"
	o = append(o, 0xa8, 0x73, 0x74, 0x61, 0x72, 0x74, 0x2d, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.StartTs)
	// string "commit-ts"
	o = append(o, 0xa9, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x2d, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.CommitTs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TxnEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "cluster-id":
			z.ClusterID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ClusterID")
				return
			}
		case "dispatcher-id":
			z.DispatcherID, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DispatcherID")
				return
			}
		case "rows":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Rows")
				return
			}
			if cap(z.Rows) >= int(zb0002) {
				z.Rows = (z.Rows)[:zb0002]
			} else {
				z.Rows = make([]*RowChangedEvent, zb0002)
			}
			for za0001 := range z.Rows {
				if msgp.IsNil(bts) {
					bts, err = msgp.ReadNilBytes(bts)
					if err != nil {
						return
					}
					z.Rows[za0001] = nil
				} else {
					if z.Rows[za0001] == nil {
						z.Rows[za0001] = new(RowChangedEvent)
					}
					bts, err = z.Rows[za0001].UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "Rows", za0001)
						return
					}
				}
			}
		case "resolved-ts":
			z.ResolvedTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		case "start-ts":
			z.StartTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "commit-ts":
			z.CommitTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CommitTs")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *TxnEvent) Msgsize() (s int) {
	s = 1 + 11 + msgp.Uint64Size + 14 + msgp.StringPrefixSize + len(z.DispatcherID) + 5 + msgp.ArrayHeaderSize
	for za0001 := range z.Rows {
		if z.Rows[za0001] == nil {
			s += msgp.NilSize
		} else {
			s += z.Rows[za0001].Msgsize()
		}
	}
	s += 12 + msgp.Uint64Size + 9 + msgp.Uint64Size + 10 + msgp.Uint64Size
	return
}