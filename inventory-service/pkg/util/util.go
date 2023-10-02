package util

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
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

/*func GetChangedAndPopulate(original, updated *models.ItemUpdated) (map[string]interface{}, error) {
	var updatedFields map[string]interface{}
	var err error

	if original.Original != nil && updated.Updated != nil {
		// Map fields between the original and updated CPU models
		updatedFields, err = MapUpdatedFields(original.Original, updated.Updated)
		if err != nil {
			return nil, err
		}
		// Populate the ItemUpdated model with these fields
		PopulateItemUpdated(updated, updatedFields)
	} else {
		// This will execute if Original and Updated within ItemUpdated aren't set
		updatedFields, err = MapUpdatedFields(original, updated)
		if err != nil {
			return nil, err
		}
		PopulateItemUpdated(updated, updatedFields)
	}

	// Convert to Avro-compatible map
	var goFieldToAvroField = map[string]string{
		"ItemID":    "item_id",
		"Name":      "name",
		"InStock":   "quantity",
		"Price":     "price",
		"Timestamp": "timestamp",
	}
	avroReadyMap := RemapKeys(updatedFields, goFieldToAvroField)

	return avroReadyMap, nil
}*/

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

func populateItemUpdatedReflect(itemUpdated *models.ItemUpdated, updatedFields map[string]interface{}) {
	val := reflect.ValueOf(itemUpdated).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.CanSet() {
			continue
		}

		fieldName := typ.Field(i).Name
		if newVal, ok := updatedFields[fieldName]; ok {
			fmt.Printf("Before: Field: %s, Value: %v\n", fieldName, field.Interface()) // Log before

			newValValue := reflect.ValueOf(newVal)
			if newValValue.Kind() != reflect.String && newValValue.Kind() != reflect.Invalid {
				fmt.Printf("Debug: newValValue.Type().Name() = %s, newValValue.Kind() = %s\n", newValValue.Type().Name(), newValValue.Kind())
				fieldType := newValValue.Type().Name()
				field.Set(reflect.ValueOf(map[string]interface{}{fieldType: newVal}))
			} else {
				if newValValue.Type().AssignableTo(field.Type()) {
					field.Set(newValValue)
				}
			}
			fmt.Printf("After: Field: %s, Value: %v\n", fieldName, field.Interface()) // Log after
		}
	}
}

func mapUpdatedFields(original, updated interface{}) (map[string]interface{}, error) {
	originalValue := reflect.ValueOf(original)
	updatedValue := reflect.ValueOf(updated)

	if originalValue.Kind() == reflect.Ptr || originalValue.Kind() == reflect.Interface {
		originalValue = originalValue.Elem()
	}
	if updatedValue.Kind() == reflect.Ptr || updatedValue.Kind() == reflect.Interface {
		updatedValue = updatedValue.Elem()
	}

	if originalValue.Kind() != reflect.Struct || updatedValue.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expecting a struct, got %v and %v", originalValue.Kind(), updatedValue.Kind())
	}

	updatedFields := make(map[string]interface{})

	for i := 0; i < originalValue.NumField(); i++ {
		originalField := originalValue.Field(i)
		updatedField := updatedValue.Field(i)

		if !reflect.DeepEqual(originalField.Interface(), updatedField.Interface()) {
			updatedFields[originalValue.Type().Field(i).Name] = updatedField.Interface()
		}
	}
	return updatedFields, nil
}

func remapKeys(originalMap map[string]interface{}, keyMap map[string]string) map[string]interface{} {
	remapped := make(map[string]interface{})
	for oldKey, value := range originalMap {
		newKey, exists := keyMap[oldKey]
		if exists {
			remapped[newKey] = value
		} else {
			remapped[oldKey] = value
		}
	}
	return remapped
}

func StructToMapGeneric(item interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	valueOf := reflect.ValueOf(item)
	typeOf := valueOf.Type()

	for i := 0; i < valueOf.NumField(); i++ {
		field := valueOf.Field(i)
		fieldType := typeOf.Field(i)

		avroType := fieldType.Tag.Get("avro")
		if avroType == "" {
			continue
		}

		key := fieldType.Name
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			result[key] = goavro.Union(avroType, field.Elem().Interface())
		}
	}

	return result
}
