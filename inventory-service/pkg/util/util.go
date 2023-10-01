package util

import (
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
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

func GetChangedFieldsMap(original, updated interface{}) (map[string]interface{}, error) {
	updatedFields, err := MapUpdatedFields(original, updated)
	if err != nil {
		return nil, err
	}

	changedFieldsMap := make(map[string]interface{})
	val := reflect.ValueOf(updated).Elem()

	for fieldName, newValue := range updatedFields {
		field := val.FieldByName(fieldName)
		if field.IsValid() {
			changedFieldsMap[fieldName] = newValue
		}
	}

	return changedFieldsMap, nil
}

func PopulateItemUpdated(itemUpdated *models.ItemUpdated, updatedFields map[string]interface{}) {
	val := reflect.ValueOf(itemUpdated).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.CanSet() {
			continue
		}

		fieldName := typ.Field(i).Name
		if newVal, ok := updatedFields[fieldName]; ok {
			newValValue := reflect.ValueOf(newVal)
			if newValValue.Type().AssignableTo(field.Type()) {
				field.Set(newValValue)
			}
		}
	}
}

func MapUpdatedFields(original, updated interface{}) (map[string]interface{}, error) {
	originalValue := reflect.ValueOf(original)
	updatedValue := reflect.ValueOf(updated)

	if originalValue.Kind() == reflect.Ptr || originalValue.Kind() == reflect.Interface {
		originalValue = originalValue.Elem()
	}
	if updatedValue.Kind() == reflect.Ptr || updatedValue.Kind() == reflect.Interface {
		updatedValue = updatedValue.Elem()
	}

	if originalValue.Kind() != reflect.Struct || updatedValue.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Expecting a struct, got %v and %v", originalValue.Kind(), updatedValue.Kind())
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

func RemapKeys(originalMap map[string]interface{}, keyMap map[string]string) map[string]interface{} {
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
