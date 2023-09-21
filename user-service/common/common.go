package common

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
}

// MessageSender interface
type MessageSender interface {
	SendMessage(topic string, message []byte)
}

func (m *Message) SendMessage(producer *kafka.Producer, topic string, message []byte) {
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
	deliveryMessage := e.(*kafka.Message)

	if deliveryMessage.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", deliveryMessage.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v",
			*deliveryMessage.TopicPartition.Topic, deliveryMessage.TopicPartition.Partition, deliveryMessage.TopicPartition.Offset)
	}
	close(deliveryChan)
}
