package kafka

import (
	"context"

	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageProducer interface {
	SendMessageWithAvro(ctx context.Context, topic string, message interface{}) error
	UpdateAvroEncoder(avroEncoder AvroEncoder)
}

type KafkaProducer struct {
	producer    *kafka.Producer
	logger      logger.Logger
	avroEncoder AvroEncoder
}

func NewKafkaProducer(ctx context.Context, cfg models.ProducerConfig, logger logger.Logger) (*KafkaProducer, error) {
	producer, err := initProducer(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{producer: producer, logger: logger}, nil
}

func initProducer(ctx context.Context, cfg models.ProducerConfig, logger logger.Logger) (*kafka.Producer, error) {
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
		logger.Error(ctx, "Failed to create producer", "error", err, "config", configMap)
		return nil, err
	}
	return producer, nil
}

func (kp *KafkaProducer) UpdateAvroEncoder(avroEncoder AvroEncoder) {
	kp.avroEncoder = avroEncoder
}

func (kp *KafkaProducer) SendMessageWithAvro(ctx context.Context, topic string, message interface{}) error {
	avroBytes, err := kp.avroEncoder.Encode(message)
	if err != nil {
		kp.logger.Error(ctx, "Failed to Avro encode message: %v", err)
		return err
	}
	return kp.sendMessage(ctx, topic, avroBytes)
}

func (kp *KafkaProducer) sendMessage(ctx context.Context, topic string, message []byte) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, deliveryChan)

	if err != nil {
		kp.logger.Info(ctx, "Failed sending message: %v", err)
		return err
	}

	e := <-deliveryChan
	deliveryMessage := e.(*kafka.Message)

	if deliveryMessage.TopicPartition.Error != nil {
		kp.logger.Error(ctx, "Delivery failed: %v", deliveryMessage.TopicPartition.Error)
		return deliveryMessage.TopicPartition.Error
	} else {
		kp.logger.Info(ctx, "Delivered the message successfully",
			"topic", *deliveryMessage.TopicPartition.Topic,
			"partition", deliveryMessage.TopicPartition.Partition,
			"offset", deliveryMessage.TopicPartition.Offset)

	}
	return nil
}
