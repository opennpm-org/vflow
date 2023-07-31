//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    marshal_proto.go
//: details: encoding of each decoded IPFIX data sets in protobuf
//: author:  Stephen Pendleton
//: date:    07/21/2023
//:
//: Licensed under the Apache License, Version 2.0 (the "License");
//: you may not use this file except in compliance with the License.
//: You may obtain a copy of the License at
//:
//:     http://www.apache.org/licenses/LICENSE-2.0
//:
//: Unless required by applicable law or agreed to in writing, software
//: distributed under the License is distributed on an "AS IS" BASIS,
//: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//: See the License for the specific language governing permissions and
//: limitations under the License.
//: ----------------------------------------------------------------------------

package netflow9

import (
	"bytes"
	"encoding/hex"
	"net"
	"strconv"

	"github.com/EdgeCast/vflow/producer"
	proto "google.golang.org/protobuf/proto"
)

// ProtobufMarshal encodes NetflowV9 message in protobuf format
func (m *Message) ProtobufMarshal(b *bytes.Buffer) ([]byte, error) {
	var vm producer.VFlowMessage
	err := m.encodeDataSetFlatProto(&vm)
	if err != nil {
		return nil, err
	}
	m.encodeHeaderProto(&vm)
	m.encodeAgentProto(&vm)

	buf, err := proto.Marshal(&vm)

	if err != nil {
		return buf, err
	}

	return buf, nil
}

func (m *Message) encodeDataSetFlatProto(vm *producer.VFlowMessage) error {
	var (
		err error
	)

	for i := range m.DataSets {

		for j, d := range m.DataSets[i] {
			var ds producer.DataSet
			ds.I = uint32(d.ID)
			var b bytes.Buffer
			m.writeValueProto(&b, i, j)
			ds.V = b.String()
			vm.DataSets = append(vm.DataSets, &ds)
		}

	}
	return err
}

func (m *Message) encodeHeaderProto(vm *producer.VFlowMessage) {
	vm.Version = uint32(m.Header.Version)
	vm.Length = uint32(m.Header.Count)
	vm.ExportTime = uint32(m.Header.UNIXSecs)
	vm.SequenceNo = uint32(m.Header.SeqNum)
	vm.DomainID = uint32(m.Header.SrcID)
}

func (m *Message) encodeAgentProto(vm *producer.VFlowMessage) {
	vm.AgentID = m.AgentID
}

func (m *Message) writeValueProto(b *bytes.Buffer, i, j int) error {
	switch m.DataSets[i][j].Value.(type) {
	case uint:
		b.WriteString(strconv.FormatUint(uint64(m.DataSets[i][j].Value.(uint)), 10))
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(m.DataSets[i][j].Value.(uint8)), 10))
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(m.DataSets[i][j].Value.(uint16)), 10))
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(m.DataSets[i][j].Value.(uint32)), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(m.DataSets[i][j].Value.(uint64), 10))
	case int:
		b.WriteString(strconv.FormatInt(int64(m.DataSets[i][j].Value.(int)), 10))
	case int8:
		b.WriteString(strconv.FormatInt(int64(m.DataSets[i][j].Value.(int8)), 10))
	case int16:
		b.WriteString(strconv.FormatInt(int64(m.DataSets[i][j].Value.(int16)), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(m.DataSets[i][j].Value.(int32)), 10))
	case int64:
		b.WriteString(strconv.FormatInt(m.DataSets[i][j].Value.(int64), 10))
	case float32:
		b.WriteString(strconv.FormatFloat(float64(m.DataSets[i][j].Value.(float32)), 'E', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(m.DataSets[i][j].Value.(float64), 'E', -1, 64))
	case string:
		b.WriteString(m.DataSets[i][j].Value.(string))
	case net.IP:
		b.WriteString(m.DataSets[i][j].Value.(net.IP).String())
	case net.HardwareAddr:
		b.WriteString(m.DataSets[i][j].Value.(net.HardwareAddr).String())
	case []uint8:
		b.WriteString("0x" + hex.EncodeToString(m.DataSets[i][j].Value.([]uint8)))
	default:
		return errUknownMarshalDataType
	}

	return nil
}
