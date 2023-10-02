package kafka

import (
	"bytes"
	"log"
	"os"

	"github.com/linkedin/goavro"
)

type AvroEncoder interface {
	Encode(message interface{}) ([]byte, error)
}

type AvroEncoderImpl struct {
	codec *goavro.Codec
}

func NewAvroEncoder(schemaPath string) (AvroEncoder, error) {
	avroSchemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}
	log.Printf("Avro schema: %s", string(avroSchemaBytes))
	codec, err := goavro.NewCodec(string(avroSchemaBytes))
	if err != nil {
		return nil, err
	}

	return &AvroEncoderImpl{codec: codec}, nil
}

// Encode implements AvroEncoder.
func (a *AvroEncoderImpl) Encode(message interface{}) ([]byte, error) {
	binaryBuffer := new(bytes.Buffer)

	// Initialize Avro Writer with the binary buffer and codec
	avroWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     binaryBuffer,
		Codec: a.codec,
	})

	if err != nil {
		return nil, err
	}
	if err := avroWriter.Append([]interface{}{message}); err != nil {
		return nil, err
	}

	return binaryBuffer.Bytes(), nil
}
