package main

import (
	"log"
	"net/http"

	"github.com/O7Oghany/EDM/events"
	"github.com/O7Oghany/EDM/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

func main() {

	var kafkaProducerConfig models.ProducerConfig

	models.ReadConfig("../kafka-configs/producer-config.yml", &kafkaProducerConfig)

	config := &kafka.ConfigMap{
		"debug":             "security",
		"bootstrap.servers": kafkaProducerConfig.BootstrapServers,
		"security.protocol": kafkaProducerConfig.SecurityProtocol,
		"ssl.ca.location":   kafkaProducerConfig.SSLCALocation,
		"acks":              kafkaProducerConfig.Acks,
		"retries":           kafkaProducerConfig.Retries,
		"batch.size":        kafkaProducerConfig.BatchSize,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v\nConfig: %+v", err, config)
	}
	defer producer.Close()
	events := events.NewEvent(producer)
	r := mux.NewRouter()

	events.InitEventHandlers(r)
	log.Println("Server started at :8080")
	log.Fatal(http.ListenAndServe(":8080", r))

}
