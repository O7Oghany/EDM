package kafka

import (
	"bytes"
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
	// Append the native Go form of the data into Avro binary buffer
	if err := avroWriter.Append([]interface{}{message}); err != nil {
		return nil, err
	}

	return binaryBuffer.Bytes(), nil
}
