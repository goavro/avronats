package avronats

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	avro "github.com/elodina/go-avro"
	"github.com/goavro/schemaclient"
)

var magicBytes = []byte{0}

// AvroEncoder is avro encoder for NATS messaging system
type AvroEncoder struct {
	primitiveSchemas map[string]avro.Schema
	schemaRegistry   schemaclient.SchemaRegistryClient
	buffers          *sync.Pool
}

// NewAvroEncoder creates new encoder
func NewAvroEncoder(schemaURL string) *AvroEncoder {
	primitiveSchemas := make(map[string]avro.Schema)
	primitiveSchemas["Null"] = createPrimitiveSchema("null")
	primitiveSchemas["Boolean"] = createPrimitiveSchema("boolean")
	primitiveSchemas["Int"] = createPrimitiveSchema("int")
	primitiveSchemas["Long"] = createPrimitiveSchema("long")
	primitiveSchemas["Float"] = createPrimitiveSchema("float")
	primitiveSchemas["Double"] = createPrimitiveSchema("double")
	primitiveSchemas["String"] = createPrimitiveSchema("string")
	primitiveSchemas["Bytes"] = createPrimitiveSchema("bytes")
	buffers := &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}

	return &AvroEncoder{
		schemaRegistry:   schemaclient.NewCachedSchemaRegistryClient(schemaURL),
		primitiveSchemas: primitiveSchemas,
		buffers:          buffers,
	}
}

// Encode implements Encoder interface
func (ae *AvroEncoder) Encode(subject string, obj interface{}) ([]byte, error) {
	if obj == nil {
		return nil, nil
	}

	schema := ae.getSchema(obj)
	id, err := ae.schemaRegistry.Register(subject, schema)
	if err != nil {
		return nil, err
	}

	buffer := ae.buffers.Get().(*bytes.Buffer)
	_, err = buffer.Write(magicBytes)
	if err != nil {
		return nil, err
	}
	idSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(idSlice, uint32(id))
	_, err = buffer.Write(idSlice)
	if err != nil {
		return nil, err
	}

	enc := avro.NewBinaryEncoder(buffer)
	var writer = avro.NewSpecificDatumWriter()
	writer.SetSchema(schema)
	err = writer.Write(obj, enc)
	if err != nil {
		return nil, err
	}
	res := buffer.Bytes()
	buffer.Reset()
	ae.buffers.Put(buffer)
	return res, nil
}

// Decode implements Encoder interface
func (ae *AvroEncoder) Decode(subject string, data []byte, vPtr interface{}) error {
	if data == nil {
		return nil
	}
	if data[0] != 0 {
		return errors.New("Unknown magic byte!")
	}
	id := int32(binary.BigEndian.Uint32(data[1:]))
	schema, err := ae.schemaRegistry.GetByID(id)
	if err != nil {
		return err
	}

	reader := avro.NewSpecificDatumReader()
	reader.SetSchema(schema)
	return reader.Read(vPtr, avro.NewBinaryDecoder(data[5:]))
}

func (ae *AvroEncoder) getSchema(obj interface{}) avro.Schema {
	if obj == nil {
		return ae.primitiveSchemas["Null"]
	}

	switch t := obj.(type) {
	case bool:
		return ae.primitiveSchemas["Boolean"]
	case int32:
		return ae.primitiveSchemas["Int"]
	case int64:
		return ae.primitiveSchemas["Long"]
	case float32:
		return ae.primitiveSchemas["Float"]
	case float64:
		return ae.primitiveSchemas["Double"]
	case string:
		return ae.primitiveSchemas["String"]
	case []byte:
		return ae.primitiveSchemas["Bytes"]
	case avro.AvroRecord:
		return t.Schema()
	default:
		panic("Unsupported Avro type. Supported types are nil, bool, int32, int64, float32, float64, string, []byte and AvroRecord")
	}
}

func createPrimitiveSchema(schemaType string) avro.Schema {
	schema, err := avro.ParseSchema(fmt.Sprintf(`{"type" : "%s" }`, schemaType))
	if err != nil {
		panic(err)
	}

	return schema
}
