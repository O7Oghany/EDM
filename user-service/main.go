package main

import (
	"context"
	"log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.178.25:9092",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	userID := "123"
	userName := "AegisAnkh"

	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: "user_created", Partition: kafka.PartitionAny},
		Value:          []byte("UserCreated," + userID + "," + userName),
	}

	producer.Produce(&message, nil)

	// Wait for delivery report
	e := <-producer.Events()
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}
