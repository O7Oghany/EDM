package kafka

import (
	"bytes"
	"os"

	"github.com/linkedin/goavro"
)

type AvroDecoder interface {
	Decode(message []byte) (interface{}, error)
}

type AvroDecoderImpl struct {
	codec *goavro.Codec
}

func NewAvroDecoder(schemaPath string) (AvroDecoder, error) {
	avroSchemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(string(avroSchemaBytes))
	if err != nil {
		return nil, err
	}

	return &AvroDecoderImpl{codec: codec}, nil
}

func (a *AvroDecoderImpl) Decode(message []byte) (interface{}, error) {
	binaryBuffer := bytes.NewBuffer(message)

	// Initialize Avro Reader with binary buffer and codec
	avroReader, err := goavro.NewOCFReader(binaryBuffer)
	if err != nil {
		return nil, err
	}

	var decodedMessage interface{}

	for avroReader.Scan() {
		decodedMessage, err = avroReader.Read()
		if err != nil {
			return nil, err
		}
	}

	return decodedMessage, nil
}
