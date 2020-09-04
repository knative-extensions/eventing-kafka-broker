// Code generated by protoc-gen-go. DO NOT EDIT.
// source: proto/def/triggers.proto

package config

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ContentMode int32

const (
	ContentMode_BINARY     ContentMode = 0
	ContentMode_STRUCTURED ContentMode = 1
)

var ContentMode_name = map[int32]string{
	0: "BINARY",
	1: "STRUCTURED",
}

var ContentMode_value = map[string]int32{
	"BINARY":     0,
	"STRUCTURED": 1,
}

func (x ContentMode) String() string {
	return proto.EnumName(ContentMode_name, int32(x))
}

func (ContentMode) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_3cd32e421bcc2dd3, []int{0}
}

type Trigger struct {
	// attributes filters events by exact match on event context attributes.
	// Each key in the map is compared with the equivalent key in the event
	// context. An event passes the filter if all values are equal to the
	// specified values.
	//
	// Nested context attributes are not supported as keys. Only string values are supported.
	Attributes map[string]string `protobuf:"bytes,1,rep,name=attributes,proto3" json:"attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// destination is the address that receives events from the Broker that pass the Filter.
	Destination string `protobuf:"bytes,2,opt,name=destination,proto3" json:"destination,omitempty"`
	// trigger identifier
	Id                   string   `protobuf:"bytes,3,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Trigger) Reset()         { *m = Trigger{} }
func (m *Trigger) String() string { return proto.CompactTextString(m) }
func (*Trigger) ProtoMessage()    {}
func (*Trigger) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cd32e421bcc2dd3, []int{0}
}

func (m *Trigger) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Trigger.Unmarshal(m, b)
}
func (m *Trigger) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Trigger.Marshal(b, m, deterministic)
}
func (m *Trigger) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Trigger.Merge(m, src)
}
func (m *Trigger) XXX_Size() int {
	return xxx_messageInfo_Trigger.Size(m)
}
func (m *Trigger) XXX_DiscardUnknown() {
	xxx_messageInfo_Trigger.DiscardUnknown(m)
}

var xxx_messageInfo_Trigger proto.InternalMessageInfo

func (m *Trigger) GetAttributes() map[string]string {
	if m != nil {
		return m.Attributes
	}
	return nil
}

func (m *Trigger) GetDestination() string {
	if m != nil {
		return m.Destination
	}
	return ""
}

func (m *Trigger) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type Broker struct {
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// the Kafka topic to consume.
	Topic string `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
	// dead letter sink URI
	DeadLetterSink string `protobuf:"bytes,3,opt,name=deadLetterSink,proto3" json:"deadLetterSink,omitempty"`
	// triggers associated with the broker
	Triggers []*Trigger `protobuf:"bytes,4,rep,name=triggers,proto3" json:"triggers,omitempty"`
	// path to listen for incoming events.
	Path string `protobuf:"bytes,5,opt,name=path,proto3" json:"path,omitempty"`
	// A comma separated list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
	BootstrapServers     string      `protobuf:"bytes,6,opt,name=bootstrapServers,proto3" json:"bootstrapServers,omitempty"`
	ContentMode          ContentMode `protobuf:"varint,7,opt,name=contentMode,proto3,enum=ContentMode" json:"contentMode,omitempty"`
	XXX_NoUnkeyedLiteral struct{}    `json:"-"`
	XXX_unrecognized     []byte      `json:"-"`
	XXX_sizecache        int32       `json:"-"`
}

func (m *Broker) Reset()         { *m = Broker{} }
func (m *Broker) String() string { return proto.CompactTextString(m) }
func (*Broker) ProtoMessage()    {}
func (*Broker) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cd32e421bcc2dd3, []int{1}
}

func (m *Broker) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Broker.Unmarshal(m, b)
}
func (m *Broker) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Broker.Marshal(b, m, deterministic)
}
func (m *Broker) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Broker.Merge(m, src)
}
func (m *Broker) XXX_Size() int {
	return xxx_messageInfo_Broker.Size(m)
}
func (m *Broker) XXX_DiscardUnknown() {
	xxx_messageInfo_Broker.DiscardUnknown(m)
}

var xxx_messageInfo_Broker proto.InternalMessageInfo

func (m *Broker) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *Broker) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *Broker) GetDeadLetterSink() string {
	if m != nil {
		return m.DeadLetterSink
	}
	return ""
}

func (m *Broker) GetTriggers() []*Trigger {
	if m != nil {
		return m.Triggers
	}
	return nil
}

func (m *Broker) GetPath() string {
	if m != nil {
		return m.Path
	}
	return ""
}

func (m *Broker) GetBootstrapServers() string {
	if m != nil {
		return m.BootstrapServers
	}
	return ""
}

func (m *Broker) GetContentMode() ContentMode {
	if m != nil {
		return m.ContentMode
	}
	return ContentMode_BINARY
}

type Brokers struct {
	Brokers []*Broker `protobuf:"bytes,1,rep,name=brokers,proto3" json:"brokers,omitempty"`
	// Count each config map update.
	// Make sure each data plane pod has the same volume generation number.
	VolumeGeneration     uint64   `protobuf:"varint,2,opt,name=volumeGeneration,proto3" json:"volumeGeneration,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Brokers) Reset()         { *m = Brokers{} }
func (m *Brokers) String() string { return proto.CompactTextString(m) }
func (*Brokers) ProtoMessage()    {}
func (*Brokers) Descriptor() ([]byte, []int) {
	return fileDescriptor_3cd32e421bcc2dd3, []int{2}
}

func (m *Brokers) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Brokers.Unmarshal(m, b)
}
func (m *Brokers) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Brokers.Marshal(b, m, deterministic)
}
func (m *Brokers) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Brokers.Merge(m, src)
}
func (m *Brokers) XXX_Size() int {
	return xxx_messageInfo_Brokers.Size(m)
}
func (m *Brokers) XXX_DiscardUnknown() {
	xxx_messageInfo_Brokers.DiscardUnknown(m)
}

var xxx_messageInfo_Brokers proto.InternalMessageInfo

func (m *Brokers) GetBrokers() []*Broker {
	if m != nil {
		return m.Brokers
	}
	return nil
}

func (m *Brokers) GetVolumeGeneration() uint64 {
	if m != nil {
		return m.VolumeGeneration
	}
	return 0
}

func init() {
	proto.RegisterEnum("ContentMode", ContentMode_name, ContentMode_value)
	proto.RegisterType((*Trigger)(nil), "Trigger")
	proto.RegisterMapType((map[string]string)(nil), "Trigger.AttributesEntry")
	proto.RegisterType((*Broker)(nil), "Broker")
	proto.RegisterType((*Brokers)(nil), "Brokers")
}

func init() { proto.RegisterFile("proto/def/triggers.proto", fileDescriptor_3cd32e421bcc2dd3) }

var fileDescriptor_3cd32e421bcc2dd3 = []byte{
	// 418 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0x52, 0x5d, 0x6b, 0x13, 0x41,
	0x14, 0x75, 0x92, 0x34, 0x5b, 0x6f, 0x34, 0x86, 0xc1, 0x87, 0x41, 0x10, 0x62, 0x10, 0x89, 0x85,
	0xce, 0x42, 0x7d, 0x29, 0x82, 0x0f, 0x4d, 0x2c, 0x22, 0xa8, 0x0f, 0x93, 0x14, 0x54, 0xf0, 0x61,
	0x92, 0xbd, 0x59, 0x87, 0x8d, 0x33, 0xcb, 0xec, 0xcd, 0x42, 0xff, 0x97, 0x3f, 0xcb, 0x1f, 0x21,
	0x3b, 0xbb, 0xdb, 0x2e, 0xcd, 0xdb, 0xbd, 0xe7, 0x9c, 0x39, 0xf7, 0x6b, 0x40, 0xe4, 0xde, 0x91,
	0x8b, 0x13, 0xdc, 0xc5, 0xe4, 0x4d, 0x9a, 0xa2, 0x2f, 0x64, 0x80, 0x66, 0x7f, 0x19, 0x44, 0xeb,
	0x1a, 0xe2, 0x97, 0x00, 0x9a, 0xc8, 0x9b, 0xcd, 0x81, 0xb0, 0x10, 0x6c, 0xda, 0x9f, 0x8f, 0x2e,
	0x84, 0x6c, 0x58, 0x79, 0x75, 0x47, 0x5d, 0x5b, 0xf2, 0xb7, 0xaa, 0xa3, 0xe5, 0x53, 0x18, 0x25,
	0x58, 0x90, 0xb1, 0x9a, 0x8c, 0xb3, 0xa2, 0x37, 0x65, 0xf3, 0xc7, 0xaa, 0x0b, 0xf1, 0x31, 0xf4,
	0x4c, 0x22, 0xfa, 0x81, 0xe8, 0x99, 0xe4, 0xc5, 0x07, 0x78, 0xf6, 0xc0, 0x90, 0x4f, 0xa0, 0x9f,
	0xe1, 0xad, 0x60, 0x41, 0x53, 0x85, 0xfc, 0x39, 0x9c, 0x94, 0x7a, 0x7f, 0xc0, 0xc6, 0xb0, 0x4e,
	0xde, 0xf7, 0x2e, 0xd9, 0xec, 0x1f, 0x83, 0xe1, 0xc2, 0xbb, 0x0c, 0x7d, 0xe3, 0xcc, 0x5a, 0xe7,
	0xea, 0x11, 0xb9, 0xdc, 0x6c, 0xdb, 0x47, 0x21, 0xe1, 0x6f, 0x60, 0x9c, 0xa0, 0x4e, 0xbe, 0x20,
	0x11, 0xfa, 0x95, 0xb1, 0x59, 0xd3, 0xcb, 0x03, 0x94, 0xbf, 0x86, 0xd3, 0x76, 0x43, 0x62, 0x10,
	0x36, 0x70, 0xda, 0x6e, 0x40, 0xdd, 0x31, 0x9c, 0xc3, 0x20, 0xd7, 0xf4, 0x5b, 0x9c, 0x04, 0x8f,
	0x10, 0xf3, 0x33, 0x98, 0x6c, 0x9c, 0xa3, 0x82, 0xbc, 0xce, 0x57, 0xe8, 0xcb, 0xca, 0x61, 0x18,
	0xf8, 0x23, 0x9c, 0x4b, 0x18, 0x6d, 0x9d, 0x25, 0xb4, 0xf4, 0xd5, 0x25, 0x28, 0xa2, 0x29, 0x9b,
	0x8f, 0x2f, 0x9e, 0xc8, 0xe5, 0x3d, 0xa6, 0xba, 0x82, 0xd9, 0x77, 0x88, 0xea, 0x69, 0x0b, 0xfe,
	0x0a, 0xa2, 0x4d, 0x1d, 0x36, 0x17, 0x8a, 0x64, 0x4d, 0xa9, 0x16, 0xaf, 0x3a, 0x29, 0xdd, 0xfe,
	0xf0, 0x07, 0x3f, 0xa1, 0x45, 0x7f, 0x7f, 0x92, 0x81, 0x3a, 0xc2, 0xcf, 0xde, 0xc2, 0xa8, 0x53,
	0x95, 0x03, 0x0c, 0x17, 0x9f, 0xbf, 0x5d, 0xa9, 0x1f, 0x93, 0x47, 0x7c, 0x0c, 0xb0, 0x5a, 0xab,
	0x9b, 0xe5, 0xfa, 0x46, 0x5d, 0x7f, 0x9c, 0xb0, 0xc5, 0x2f, 0x38, 0x4f, 0xb0, 0x94, 0x59, 0x75,
	0xd1, 0x12, 0x25, 0x96, 0x68, 0xc9, 0xd8, 0x54, 0x66, 0x7a, 0x97, 0x69, 0x59, 0x17, 0x97, 0x5b,
	0xe7, 0x51, 0x6e, 0x9d, 0xdd, 0x99, 0x74, 0xf1, 0xb4, 0xe9, 0x79, 0x19, 0xd2, 0x9f, 0x2f, 0xab,
	0x89, 0xbc, 0xdb, 0x9f, 0xe7, 0x7b, 0x6d, 0x31, 0xce, 0xb3, 0x34, 0xae, 0xd4, 0x71, 0xad, 0xde,
	0x0c, 0xc3, 0x87, 0x7c, 0xf7, 0x3f, 0x00, 0x00, 0xff, 0xff, 0x50, 0xff, 0x2d, 0x2f, 0xac, 0x02,
	0x00, 0x00,
}
