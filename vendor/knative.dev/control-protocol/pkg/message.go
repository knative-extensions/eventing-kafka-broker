/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package control

import (
	"encoding/binary"
	"io"

	"github.com/google/uuid"
)

const (
	// ActualProtocolVersion is the supported protocol version, and the default one used to send outbound messages
	ActualProtocolVersion   uint8 = 0
	maximumSupportedVersion uint8 = ActualProtocolVersion
	outboundMessageVersion        = maximumSupportedVersion
)

type MessageFlag uint8

/*
	MessageHeader represents a message header

	+---------+---------+-------+----------+--------+
	|         |   0-8   |  8-16 |   16-24  |  24-32 |
	+---------+---------+-------+----------+--------+
	| 0-32    | version | flags | <unused> | opcode |
	+---------+---------+-------+----------+--------+
	| 32-64   |              uuid[0:4]              |
	+---------+-------------------------------------+
	| 64-96   |              uuid[4:8]              |
	+---------+-------------------------------------+
	| 96-128  |              uuid[8:12]             |
	+---------+-------------------------------------+
	| 128-160 |             uuid[12:16]             |
	+---------+-------------------------------------+
	| 160-192 |                length               |
	+---------+-------------------------------------+
*/
type MessageHeader struct {
	version uint8
	flags   uint8
	opcode  uint8
	uuid    [16]byte
	// In bytes
	length uint32
}

// Version returns the protocol version
func (m MessageHeader) Version() uint8 {
	return m.version
}

// Check returns true if the flag is 1, false otherwise
func (m MessageHeader) Check(flag MessageFlag) bool {
	return (m.flags & uint8(flag)) == uint8(flag)
}

// OpCode returns the opcode of the message
func (m MessageHeader) OpCode() uint8 {
	return m.opcode
}

// UUID returns the message uuid
func (m MessageHeader) UUID() uuid.UUID {
	return m.uuid
}

// UUID returns the payload length
func (m MessageHeader) Length() uint32 {
	return m.length
}

func (m MessageHeader) WriteTo(w io.Writer) (int64, error) {
	var b [4]byte
	var n int64
	b[0] = m.version
	b[1] = m.flags
	b[3] = m.opcode
	n1, err := w.Write(b[0:4])
	n = n + int64(n1)
	if err != nil {
		return n, err
	}

	n1, err = w.Write(m.uuid[0:16])
	n = n + int64(n1)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(b[0:4], m.length)
	n1, err = w.Write(b[0:4])
	n = n + int64(n1)
	return n, err
}

func messageHeaderFromBytes(b [24]byte) MessageHeader {
	m := MessageHeader{}
	m.version = b[0]
	m.flags = b[1]
	m.opcode = b[3]
	for i := 0; i < 16; i++ {
		m.uuid[i] = b[4+i]
	}
	m.length = binary.BigEndian.Uint32(b[20:24])
	return m
}

// Message is a MessageHeader and a payload
type Message struct {
	MessageHeader
	payload []byte
}

// MessageOpt is an additional option for NewMessage
type MessageOpt func(*Message)

// WithVersion specifies the version to use when creating a new Message
func WithVersion(version uint8) MessageOpt {
	return func(message *Message) {
		message.version = version
	}
}

// WithVersion specifies the flags to use when creating a new Message
func WithFlags(flags uint8) MessageOpt {
	return func(message *Message) {
		message.flags = flags
	}
}

// NewMessage creates a new message.
func NewMessage(uuid [16]byte, opcode uint8, payload []byte, opts ...MessageOpt) Message {
	length := uint32(0)
	if payload != nil {
		length = uint32(len(payload))
	}
	msg := Message{
		MessageHeader: MessageHeader{
			version: outboundMessageVersion,
			flags:   0,
			opcode:  opcode,
			uuid:    uuid,
			length:  length,
		},
		payload: payload,
	}

	for _, f := range opts {
		f(&msg)
	}

	return msg
}

func (m Message) Payload() []byte {
	return m.payload
}

func (m *Message) ReadFrom(r io.Reader) (count int64, err error) {
	var b [24]byte
	var n int
	n, err = io.ReadAtLeast(io.LimitReader(r, 24), b[0:24], 24)
	count = count + int64(n)
	if err != nil {
		return count, err
	}

	m.MessageHeader = messageHeaderFromBytes(b)
	if m.Length() != 0 {
		// We need to read the payload
		m.payload = make([]byte, m.Length())
		n, err = io.ReadAtLeast(io.LimitReader(r, int64(m.Length())), m.payload, int(m.Length()))
		count = count + int64(n)
	}
	return count, err
}

func (m *Message) WriteTo(w io.Writer) (count int64, err error) {
	n, err := m.MessageHeader.WriteTo(w)
	count = count + n
	if err != nil {
		return count, err
	}

	if m.payload != nil {
		var n1 int
		n1, err = w.Write(m.payload)
		count = count + int64(n1)
	}
	return count, err
}
