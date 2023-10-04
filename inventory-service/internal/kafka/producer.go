package kafka

import (
	"context"
	"fmt"

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
	PublishItemsBatchAdded(ctx context.Context, data models.ItemsBatchAdded) error
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
	topic := consts.ItemCreatedEvent
	if err := kp.updateAvroSchema(topic); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"name":      data.Name,
		"quantity":  data.Quantity,
		"price":     data.Price,
		"timestamp": data.Timestamp,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Item_Added_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish item added send failure: %w", err)
	}
	return nil
}
func (kp *KafkaProducer) PublishItemsBatchAdded(ctx context.Context, data models.ItemsBatchAdded) error {
	topic := consts.ItemsBatchAdded
	if err := kp.updateAvroSchema(topic); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	item := map[string]interface{}{
		"batch_id":  data.BatchID,
		"items":     data.Items,
		"timestamp": data.Timestamp,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Items_Batch_Added_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish items batch added send failure: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) PublishItemUpdated(ctx context.Context, data models.ItemUpdated) error {
	topic := consts.ItemUpdatedEvent
	if err := kp.updateAvroSchema(topic); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	avroData := util.StructToMapGeneric(data)
	err := kp.SendMessageWithAvro(ctx, topic, avroData)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Item_Updated_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish item updated send failure: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) PublishItemRemoved(ctx context.Context, data models.ItemRemoved) error {
	topic := consts.ItemDeletedEvent
	if err := kp.updateAvroSchema(consts.ItemDeletedEvent); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"timestamp": data.Timestamp,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Item_Removed_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish item removed send failure: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) PublishStockReplenished(ctx context.Context, data models.StockReplenished) error {
	topic := consts.StockReplenishedEvent
	if err := kp.updateAvroSchema(topic); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	item := map[string]interface{}{
		"item_id":         data.ItemID,
		"added_stock":     data.AddedStock,
		"new_total_stock": data.NewTotalStock,
		"timestamp":       data.Timestamp,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Stock_Replenished_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish stock replenished send failure: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) PublishStockDepleted(ctx context.Context, data models.StockDepleted) error {
	topic := consts.StockDepletedEvent
	if err := kp.updateAvroSchema(consts.StockDepletedEvent); err != nil {
		kp.logger.Error(ctx, "Update_Avro_Schema_Failure", "error", err)
		return fmt.Errorf("update avro schema failure: %w", err)
	}
	item := map[string]interface{}{
		"item_id":         data.ItemID,
		"timestamp":       data.Timestamp,
		"remaining_stock": data.RemainingStock,
	}
	err := kp.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		kp.logger.Error(ctx, "Publish_Stock_Depleted_Send_Failure", "topic", topic, "error", err)
		return fmt.Errorf("publish stock depleted send failure: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) updateAvroSchema(event string) error {
	schemaPath := "./schemas/" + event + ".avsc" // or however you determine your schema path
	avroEncoder, err := NewAvroEncoder(schemaPath)
	if err != nil {
		kp.logger.Error(context.TODO(), "Avro_Encoder_Failure", "error", err)
		return fmt.Errorf("avro encoder failure: %w", err)
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
		kp.logger.Error(ctx, "Kafka_Send_Failure", "error", err)
		return fmt.Errorf("kafka send failure: %w", err)
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
		kp.logger.Info(ctx, "Kafka_Send_Failure", "error", err)
		return fmt.Errorf("kafka send failure: %w", err)
	}

	select {
	case e := <-deliveryChan:
		deliveryMessage := e.(*kafka.Message)

		if deliveryMessage.TopicPartition.Error != nil {
			kp.logger.Error(ctx, "Kafka_Delivery_Failure", "error", deliveryMessage.TopicPartition.Error)
			return fmt.Errorf("kafka delivery failure: %w", deliveryMessage.TopicPartition.Error)
		} else {
			kp.logger.Info(ctx, "Kafka_Delivery_Success",
				"topic", *deliveryMessage.TopicPartition.Topic,
				"partition", deliveryMessage.TopicPartition.Partition,
				"offset", deliveryMessage.TopicPartition.Offset)

		}
	case <-ctx.Done():
		kp.logger.Info(ctx, "Context_Cancelled", "warning", "message delivery may not be guaranteed")
		return fmt.Errorf("context cancelled, message delivery may not be guaranteed: %w", ctx.Err())
	}
	return nil
}
