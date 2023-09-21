package main

import (
	"fmt"
	"log"

	"github.com/O7Oghany/EDM/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	var kafkaProducerConfig models.ProducerConfig

	models.ReadConfig("../kafka-configs/producer-config.yml", &kafkaProducerConfig)

	config := &kafka.ConfigMap{
		"debug":             "security",
		"bootstrap.servers": kafkaProducerConfig.BootstrapServers,
		"security.protocol": kafkaProducerConfig.SecurityProtocol,
		"ssl.ca.location":          kafkaProducerConfig.SSLCALocation,
		"acks":       kafkaProducerConfig.Acks,
		"retries":    kafkaProducerConfig.Retries,
		"batch.size": kafkaProducerConfig.BatchSize,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v\nConfig: %+v", err, config)
	}

	defer producer.Close()

	userID := "123"
	userName := "AegisAnkh"
	event := "user_created"
	message := []byte(fmt.Sprintf("UserCreated,%s,%s", userID, userName))

	sendMessage(producer, event, message)
}

func sendMessage(producer *kafka.Producer, topic string, message []byte) {
	deliveryChan := make(chan kafka.Event)
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, deliveryChan)

	if err != nil {
		log.Printf("Failed sending message: %v", err)
		return
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(deliveryChan)
}
