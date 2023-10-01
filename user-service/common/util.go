package common

import (
	"fmt"

	"github.com/O7Oghany/EDM/user-service/models"
)

func LoadConfigs() (models.ProducerConfig, models.ConsumerConfig, error) {
	var kafkaProducerConfig models.ProducerConfig
	var kafkaConsumerConfig models.ConsumerConfig

	if err := models.ReadConfig("../kafka-configs/producer-config.yml", &kafkaProducerConfig); err != nil {
		return kafkaProducerConfig, kafkaConsumerConfig, fmt.Errorf("read producer config: %w", err)
	}
	if err := models.ReadConfig("../kafka-configs/consumer-config.yml", &kafkaConsumerConfig); err != nil {
		return kafkaProducerConfig, kafkaConsumerConfig, fmt.Errorf("read consumer config: %w", err)
	}

	return kafkaProducerConfig, kafkaConsumerConfig, nil
}
