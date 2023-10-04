package kafka

import (
	"context"

	"github.com/O7Oghany/EDM/inventory-service/pkg/consts"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/util"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessageProducer interface {
	SendMessageWithAvro(ctx context.Context, topic string, message interface{}) error
	updateAvroEncoder(avroEncoder AvroEncoder)
	PublishItemAdded(ctx context.Context, data models.ItemAdded) error
	PublishItemUpdated(ctx context.Context, data models.ItemUpdated) error
	PublishItemRemoved(ctx context.Context, data models.ItemRemoved) error
	PublishStockReplenished(ctx context.Context, data models.StockReplenished) error
	PublishStockDepleted(ctx context.Context, data models.StockDepleted) error
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

func (kp *KafkaProducer) PublishItemAdded(ctx context.Context, data models.ItemAdded) error {
	if err := kp.updateAvroSchema(consts.ItemCreatedEvent); err != nil {
		kp.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"name":      data.Name,
		"quantity":  data.Quantity,
		"price":     data.Price,
		"timestamp": data.Timestamp,
	}
	err := kp.SendMessageWithAvro(ctx, consts.ItemCreatedEvent, item)
	if err != nil {
		kp.logger.Error(ctx, "Failed to publish ItemAdded event: %v", err)
		return err
	}
	return nil
}

func (kp *KafkaProducer) PublishItemUpdated(ctx context.Context, data models.ItemUpdated) error {
	if err := kp.updateAvroSchema(consts.ItemUpdatedEvent); err != nil {
		kp.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	avroData := util.StructToMapGeneric(data)
	err := kp.SendMessageWithAvro(ctx, consts.ItemUpdatedEvent, avroData)
	if err != nil {
		kp.logger.Error(ctx, "Failed to publish ItemUpdated event: %v", err)
		return err
	}
	kp.logger.Info(ctx, "published ItemUpdated event", "item", avroData)
	return nil
}

func (kp *KafkaProducer) PublishItemRemoved(ctx context.Context, data models.ItemRemoved) error {
	topic := consts.ItemDeletedEvent
	if err := kp.updateAvroSchema(consts.ItemDeletedEvent); err != nil {
		kp.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"timestamp": data.Timestamp,
	}
	kp.logger.Info(ctx, "publishing item removed event", "item", item)
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Failed to publish ItemRemoved event: %v", err)
		return err
	}
	return nil
}

func (kp *KafkaProducer) PublishStockReplenished(ctx context.Context, data models.StockReplenished) error {
	topic := consts.StockReplenishedEvent
	kp.logger.Info(ctx, "publishing stock depleted event", "item", data)
	kp.logger.Info(ctx, "publishing stock depleted event", "item", data.ItemID)
	if err := kp.updateAvroSchema(consts.StockReplenishedEvent); err != nil {
		kp.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	item := map[string]interface{}{
		"item_id":         data.ItemID,
		"added_stock":     data.AddedStock,
		"new_total_stock": data.NewTotalStock,
		"timestamp":       data.Timestamp,
	}
	kp.logger.Info(ctx, "publishing stock depleted event", "item", item)
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Failed to publish StockReplenished event: %v", err)
		return err
	}
	return nil
}

func (kp *KafkaProducer) PublishStockDepleted(ctx context.Context, data models.StockDepleted) error {
	topic := consts.StockDepletedEvent
	if err := kp.updateAvroSchema(consts.StockDepletedEvent); err != nil {
		kp.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	item := map[string]interface{}{
		"item_id":         data.ItemID,
		"timestamp":       data.Timestamp,
		"remaining_stock": data.RemainingStock,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Failed to publish StockDepleted event: %v", err)
		return err
	}
	return nil
}

func (kp *KafkaProducer) updateAvroSchema(event string) error {
	schemaPath := "./schemas/" + event + ".avsc" // or however you determine your schema path
	avroEncoder, err := NewAvroEncoder(schemaPath)
	if err != nil {
		kp.logger.Error(context.TODO(), "Failed to create Avro encoder: %v", err)
		return err
	}
	kp.updateAvroEncoder(avroEncoder)
	return nil
}
func (kp *KafkaProducer) updateAvroEncoder(avroEncoder AvroEncoder) {
	kp.avroEncoder = avroEncoder
}

func (kp *KafkaProducer) SendMessageWithAvro(ctx context.Context, topic string, message interface{}) error {
	avroBytes, err := kp.avroEncoder.Encode(message)
	kp.logger.Info(ctx, "Sending message to Kafka", "topic", topic, "message", message)
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
