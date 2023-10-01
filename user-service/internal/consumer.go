package internal

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/O7Oghany/EDM/user-service/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

type Consumer struct {
}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (c *Consumer) ConsumeEvents(consumerCfg models.ConsumerConfig, ctx context.Context, cancel context.CancelFunc) {
	consumer, err := initConsumer(consumerCfg)
	if err != nil {
		log.Printf("Failed to create consumer: %v\nConfig: %+v", err, consumer)
		cancel()
		return
	}
	defer consumer.Close()

	topics := []string{"payment_created_topic", "payment_updated_topic", "payment_deleted_topic"}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Printf("Failed to subscribe to topics: %v", err)
		cancel()
		return
	}
	avroSchemaBytes, err := os.ReadFile("./avro/PaymentEvent.avsc")
	if err != nil {
		log.Printf("Failed to read Avro schema file: %v", err)
		cancel()
		return
	}

	codec, err := goavro.NewCodec(string(avroSchemaBytes))
	if err != nil {
		log.Printf("Failed to create Avro codec: %v", err)
		cancel()
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down consumer")
			return
		default:
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue // transient error, just continue
				}
				log.Printf("Failed to read message: %v", err)
				cancel()
				return
			}
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			native, _, err := codec.NativeFromBinary(msg.Value)
			if err != nil {
				log.Printf("Failed to decode Avro: %v", err)
				cancel()
				return
			}
			log.Printf("Decoded Avro: %+v", native)
			mpVal, ok := native.(map[string]interface{})
			if !ok {
				log.Printf("Failed to cast Avro to map: %+v", native)
				cancel()
				return
			}
			log.Printf("Decoded Avro as map: %+v", mpVal)
		}
	}
}
func initConsumer(cfg models.ConsumerConfig) (*kafka.Consumer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":       cfg.BootstrapServers,
		"group.id":                cfg.GroupID,
		"auto.offset.reset":       cfg.AutoOffsetReset,
		"enable.auto.commit":      cfg.EnableAutoCommit,
		"auto.commit.interval.ms": cfg.AutoCommitInterval,
		"session.timeout.ms":      cfg.SessionTimeout,
		"security.protocol":       cfg.SecurityProtocol,
		"ssl.ca.location":         cfg.SSLCALocation,
	}

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("Failed to create consumer: %w\nConfig: %+v", err, configMap)
	}
	return consumer, nil
}
