package avronats

import (
	"reflect"
	"testing"

	avro "github.com/elodina/go-avro"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats"
	"github.com/nats-io/nats/encoders/protobuf"
)

const (
	schemaRepositoryURL = "http://localhost:8081"
	rawMetricsSchema    = `{"namespace": "metrics","type": "record","name": "Timings","fields": [{"name": "id", "type": "long"},{"name": "timings",  "type": {"type":"array", "items": "long"} }]}`
)

var parsedSchema, _ = avro.ParseSchema(rawMetricsSchema)

type Metric struct {
	Id      int64   `protobuf:"varint,1,opt,name=id" json:"id,omitempty"`
	Timings []int64 `protobuf:"varint,2,rep,name=timings" json:"timings,omitempty"`
}

func (*Metric) Schema() avro.Schema {
	return parsedSchema
}

func TestEncoderDecoder(t *testing.T) {
	encoder := NewAvroEncoder(schemaRepositoryURL)

	record := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	bytes, err := encoder.Encode("test", record)
	assert(t, err, nil)

	decoded := &Metric{}
	err = encoder.Decode("test", bytes, decoded)
	assert(t, err, nil)

	assert(t, decoded.Id, record.Id)
	assert(t, decoded.Timings, []int64{123456, 654321})
}

func TestNatsIntegration(t *testing.T) {
	nc, _ := nats.Connect(nats.DefaultURL)
	encoder := NewAvroEncoder(schemaRepositoryURL)
	nats.RegisterEncoder("avro", encoder)
	c, err := nats.NewEncodedConn(nc, "avro")
	assert(t, err, nil)
	metric := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	done := make(chan struct{})
	_, err = c.Subscribe("foo", func(m *Metric) {
		assert(t, m.Id, metric.Id)
		assert(t, m.Timings, metric.Timings)
		done <- struct{}{}
	})
	assert(t, err, nil)

	err = c.Publish("foo", metric)
	assert(t, err, nil)
	<-done
}

func BenchmarkAvroEncode(b *testing.B) {
	encoder := NewAvroEncoder(schemaRepositoryURL)

	metric := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	for i := 0; i < b.N; i++ {
		encoder.Encode("test", metric)
	}
}

func BenchmarkProtoEncode(b *testing.B) {
	encoder := &protobuf.ProtobufEncoder{}

	metric := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	for i := 0; i < b.N; i++ {
		encoder.Encode("test", metric)
	}
}

func BenchmarkAvroDecode(b *testing.B) {
	encoder := NewAvroEncoder(schemaRepositoryURL)

	metric := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	newMetric := Metric{}
	encoded, _ := encoder.Encode("test", metric)

	for i := 0; i < b.N; i++ {
		encoder.Decode("test", encoded, &newMetric)
	}
}

func BenchmarkProtoDecode(b *testing.B) {
	encoder := &protobuf.ProtobufEncoder{}

	metric := &Metric{
		Id:      int64(3),
		Timings: []int64{123456, 654321},
	}

	newMetric := Metric{}
	encoded, _ := encoder.Encode("test", metric)

	for i := 0; i < b.N; i++ {
		encoder.Decode("test", encoded, &newMetric)
	}
}

func assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func assertNot(t *testing.T, actual interface{}, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("%v should not be %v", actual, expected)
	}
}

func (m *Metric) Reset()                    { *m = Metric{} }
func (m *Metric) String() string            { return proto.CompactTextString(m) }
func (*Metric) ProtoMessage()               {}
func (*Metric) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

var fileDescriptor0 = []byte{
	// 87 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0xc9, 0x4d, 0x2d, 0x29,
	0xca, 0x4c, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x05, 0x53, 0x4a, 0x46, 0x5c, 0x6c,
	0xbe, 0x60, 0x61, 0x21, 0x3e, 0x2e, 0xa6, 0xcc, 0x14, 0x09, 0x46, 0x05, 0x46, 0x0d, 0xe6, 0x20,
	0x20, 0x4b, 0x48, 0x82, 0x8b, 0xbd, 0x24, 0x33, 0x37, 0x33, 0x2f, 0xbd, 0x58, 0x82, 0x49, 0x81,
	0x19, 0x28, 0x08, 0xe3, 0x26, 0xb1, 0x81, 0xb5, 0x1a, 0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x28,
	0x05, 0xd1, 0xa7, 0x51, 0x00, 0x00, 0x00,
}
