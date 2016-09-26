[![Build Status](https://travis-ci.org/goavro/avronats.svg?branch=master)](https://travis-ci.org/goavro/avronats)

# avronats
Go Avro Encoder for NATS messaging system.

## How to use
```
  nc, _ := nats.Connect(nats.DefaultURL)
  encoder := avronats.NewAvroEncoder(schemaRegistryURL)
  nats.RegisterEncoder("avro", encoder)
  c, err := nats.NewEncodedConn(nc, "avro")
  //...
```
