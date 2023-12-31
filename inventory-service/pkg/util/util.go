package util

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"gopkg.in/yaml.v2"
)

func LoadConfigs() (models.ProducerConfig, models.ConsumerConfig, error) {
	var kafkaProducerConfig models.ProducerConfig
	var kafkaConsumerConfig models.ConsumerConfig

	if err := readConfig("../kafka-configs/producer-config.yml", &kafkaProducerConfig); err != nil {
		return kafkaProducerConfig, kafkaConsumerConfig, fmt.Errorf("read producer config: %w", err)
	}
	if err := readConfig("../kafka-configs/consumer-config.yml", &kafkaConsumerConfig); err != nil {
		return kafkaProducerConfig, kafkaConsumerConfig, fmt.Errorf("read consumer config: %w", err)
	}

	return kafkaProducerConfig, kafkaConsumerConfig, nil
}

func readConfig(filename string, out interface{}) error {
	file, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("Could not read file: %s", err)
		return err
	}

	err = yaml.Unmarshal(file, out)
	if err != nil {
		log.Printf("Could not unmarshal YAML: %s", err)
		return err
	}
	return nil
}

func PopulateItemUpdated(updated models.CPU, original models.CPU) models.ItemUpdated {
	event := models.ItemUpdated{
		ItemID:    updated.ID,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}
	var cores32 *int32
	if updated.Cores != 0 {
		cores32 = new(int32)
		*cores32 = int32(updated.Cores)
	}
	var quantity32 *int32
	if updated.InStock != 0 {
		quantity32 = new(int32)
		*quantity32 = int32(updated.InStock)
	}
	if updated.Name != original.Name {
		event.Name = &updated.Name
	}
	if updated.Brand != original.Brand {
		event.Brand = &updated.Brand
	}
	if updated.ClockSpeed != original.ClockSpeed {
		event.ClockSpeed = &updated.ClockSpeed
	}
	if updated.Cores != original.Cores {
		event.Cores = cores32
	}
	if updated.InStock != original.InStock {
		event.Quantity = quantity32
	}
	if updated.Price != original.Price {
		event.Price = &updated.Price
	}

	return event
}

func StructToMapGeneric(item interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	valueOf := reflect.ValueOf(item)
	typeOf := valueOf.Type()

	for i := 0; i < valueOf.NumField(); i++ {
		field := valueOf.Field(i)
		fieldType := typeOf.Field(i)

		avroType := fieldType.Tag.Get("avro")
		jsonKey := fieldType.Tag.Get("json")
		// Convert the json key to the format you need for Avro
		avroKey := strings.Split(jsonKey, ",")[0]

		// If avroType is empty, this is a non-union field
		if avroType == "" {
			result[avroKey] = field.Interface()
			continue // skip to next iteration
		}

		// If it's a pointer and not nil, this is a union field
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			result[avroKey] = goavro.Union(avroType, field.Elem().Interface())
		}
	}
	return result
}

func PopulateStockDepleted(cpu models.CPU, remainingStock int32) *models.StockDepleted {
	return &models.StockDepleted{
		ItemID:         cpu.ID,
		RemainingStock: remainingStock,
		Timestamp:      time.Now().UnixNano() / 1e6, // converting to milliseconds
	}
}

func PopulateStockReplenished(cpu models.CPU, addedStock int32, newTotalStock int32) *models.StockReplenished {
	return &models.StockReplenished{
		ItemID:        cpu.ID,
		AddedStock:    addedStock,
		NewTotalStock: newTotalStock,
		Timestamp:     time.Now().UnixNano() / 1e6, // converting to milliseconds
	}
}

func GenerateBatchID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func GenerateBatchUUID() string {
	return uuid.New().String()
}
