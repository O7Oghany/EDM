package service

import (
	"context"
	"fmt"

	"github.com/O7Oghany/EDM/inventory-service/internal/kafka"
	"github.com/O7Oghany/EDM/inventory-service/pkg/consts"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/util"
)

func (s *inventoryServiceImpl) PublishEvent(ctx context.Context, eventType string, payload interface{}) error {
	switch eventType {
	case consts.ItemCreatedEvent:
		data, ok := payload.(models.ItemAdded)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishItemAdded(ctx, data)
	case consts.ItemUpdatedEvent:
		data, ok := payload.(models.ItemUpdated)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishItemUpdated(ctx, data)
	case consts.ItemDeletedEvent:
		data, ok := payload.(models.ItemRemoved)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishItemRemoved(ctx, data)
	case consts.StockDepletedEvent:
		data, ok := payload.(models.StockDepleted)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishStockDepleted(ctx, data)
	case consts.StockReplenishedEvent:
		data, ok := payload.(models.StockReplenished)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishStockReplenished(ctx, data)
	default:
		s.logger.Error(ctx, "unknown event type %s", eventType)
		return fmt.Errorf("unknown event type %s", eventType)
	}
}

func (s *inventoryServiceImpl) publishItemAdded(ctx context.Context, data models.ItemAdded) error {
	if err := s.updateAvroSchema(consts.ItemCreatedEvent); err != nil {
		s.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"name":      data.Name,
		"quantity":  data.Quantity,
		"price":     data.Price,
		"timestamp": data.Timestamp,
	}
	err := s.producer.SendMessageWithAvro(ctx, consts.ItemCreatedEvent, item)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemAdded event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishItemUpdated(ctx context.Context, data models.ItemUpdated) error {
	if err := s.updateAvroSchema(consts.ItemUpdatedEvent); err != nil {
		s.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
	}
	avroData := util.StructToMapGeneric(data)
	err := s.producer.SendMessageWithAvro(ctx, consts.ItemUpdatedEvent, avroData)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemUpdated event: %v", err)
		return err
	}
	s.logger.Info(ctx, "published ItemUpdated event", "item", avroData)
	return nil
}

func (s *inventoryServiceImpl) publishItemRemoved(ctx context.Context, data models.ItemRemoved) error {
	topic := consts.ItemDeletedEvent
	item := map[string]interface{}{
		"item_id":   data.ItemID,
		"timestamp": data.Timestamp,
	}
	s.logger.Info(ctx, "publishing item removed event", "item", item)
	err := s.producer.SendMessageWithAvro(ctx, topic, item)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemRemoved event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishStockReplenished(ctx context.Context, data models.StockReplenished) error {
	topic := consts.StockReplenishedEvent
	err := s.producer.SendMessageWithAvro(ctx, topic, data)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish StockReplenished event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishStockDepleted(ctx context.Context, data models.StockDepleted) error {
	topic := consts.StockDepletedEvent
	err := s.producer.SendMessageWithAvro(ctx, topic, data)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish StockDepleted event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) updateAvroSchema(event string) error {
	schemaPath := "./schemas/" + event + ".avsc" // or however you determine your schema path

	avroEncoder, err := kafka.NewAvroEncoder(schemaPath)
	if err != nil {
		s.logger.Error(context.TODO(), "Failed to create Avro encoder: %v", err)
		return err
	}
	s.producer.UpdateAvroEncoder(avroEncoder)
	return nil
}
