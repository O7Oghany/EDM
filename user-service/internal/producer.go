package internal

import (
	"fmt"
	"log"
	"net/http"

	"github.com/O7Oghany/EDM/events"
	"github.com/O7Oghany/EDM/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
)

type Producer struct {
}

func NewProducer() *Producer {
	return &Producer{}
}

func (p *Producer) StartProducerServer(producerCfg models.ProducerConfig) error {
	producer, err := initProducer(producerCfg)
	if err != nil {
		log.Printf("Failed to create producer: %v\nConfig: %+v", err, producerCfg)
		return err
	}
	defer producer.Close()
	events := events.NewEvent(producer)
	r := mux.NewRouter()

	events.InitEventHandlers(r)
	log.Println("Server started at :9090")
	log.Fatal(http.ListenAndServe(":9090", r))
	return nil
}

func initProducer(cfg models.ProducerConfig) (*kafka.Producer, error) {
	configMap := &kafka.ConfigMap{
		"debug":             "security",
		"bootstrap.servers": cfg.BootstrapServers,
		"security.protocol": cfg.SecurityProtocol,
		"ssl.ca.location":   cfg.SSLCALocation,
		"acks":              cfg.Acks,
		"retries":           cfg.Retries,
		"batch.size":        cfg.BatchSize,
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("Failed to create producer: %w\nConfig: %+v", err, configMap)
	}
	return producer, nil
}
