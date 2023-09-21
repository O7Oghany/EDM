package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

	http.HandleFunc("/user/create", func(w http.ResponseWriter, r *http.Request) {
        userID := r.URL.Query().Get("userID")
        userName := r.URL.Query().Get("userName")
        message := []byte(fmt.Sprintf("UserCreated,%s,%s", userID, userName))
        sendMessage(producer, "UserEvents", message)
    })
	http.HandleFunc("/user/update", func(w http.ResponseWriter, r *http.Request) {
        userID := r.URL.Query().Get("userID")
        userName := r.URL.Query().Get("userName")
        message := []byte(fmt.Sprintf("UserUpdated,%s,%s", userID, userName))
        sendMessage(producer, "UserEvents", message)
    })
	http.HandleFunc("/user/delete", func(w http.ResponseWriter, r *http.Request) {
        userID := r.URL.Query().Get("userID")
        message := []byte(fmt.Sprintf("UserDeleted,%s,", userID))
        sendMessage(producer, "UserEvents", message)
    })

    http.HandleFunc("/user/login", func(w http.ResponseWriter, r *http.Request) {
        userID := r.URL.Query().Get("userID")
        userName := r.URL.Query().Get("userName")
        message := []byte(fmt.Sprintf("UserLoggedIn,%s,%s", userID, userName))
        sendMessage(producer, "UserEvents", message)
    })

	
	
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
