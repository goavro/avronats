package avronats

import (
	"reflect"
	"testing"

	avro "github.com/elodina/go-avro"
	"github.com/nats-io/nats"
)

const (
	schemaRepositoryUrl = "http://localhost:8081"
	rawMetricsSchema    = `{"namespace": "metrics","type": "record","name": "Timings","fields": [{"name": "id", "type": "long"},{"name": "timings",  "type": {"type":"array", "items": "long"} }]}`
)

type Metric struct {
	Id      int64
	Timings []int64
}

func (t *Metric) Schema() avro.Schema {
	schema, _ := avro.ParseSchema(rawMetricsSchema)
	return schema
}

func TestEncoderDecoder(t *testing.T) {
	encoder := NewAvroEncoder(schemaRepositoryUrl)

	_, err := avro.ParseSchema(rawMetricsSchema)
	assert(t, err, nil)

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
	encoder := NewAvroEncoder(schemaRepositoryUrl)
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
