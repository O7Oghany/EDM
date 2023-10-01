package service

import (
	"context"
	"fmt"

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
	case consts.PriceUpdatedEvent:
		data, ok := payload.(models.PriceUpdated)
		if !ok {
			s.logger.Error(ctx, "invalid payload for event type %s", eventType)
			return fmt.Errorf("invalid payload for event type %s", eventType)
		}
		return s.publishPriceUpdated(ctx, data)
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
	topic := consts.ItemCreatedEvent
	itemMap := map[string]interface{}{
		"item_id":   data.ItemID,
		"name":      data.Name,
		"quantity":  data.Quantity,
		"price":     data.Price,
		"timestamp": data.Timestamp,
	}
	err := s.producer.SendMessageWithAvro(ctx, topic, itemMap)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemAdded event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishItemUpdated(ctx context.Context, data models.ItemUpdated) error {
	var goFieldToAvroField = map[string]string{
		"ItemID":      "item_id",
		"Name":        "name",
		"Brand":       "brand",
		"ClockSpeed":  "clock_speed",
		"Cores":       "cores",
		"Price":       "price",
		"SKU":         "sku",
		"Quantity":    "quantity",
		"IsAvailable": "is_available",
		"Timestamp":   "timestamp",
	}

	originalData := models.ItemUpdated{}
	changedFields, err := util.GetChangedFieldsMap(&originalData, &data)
	if err != nil {
		s.logger.Error(ctx, "Failed to map updated fields: %v", err)
		return err
	}

	avroReadyMap := util.RemapKeys(changedFields, goFieldToAvroField)

	err = s.producer.SendMessageWithAvro(ctx, consts.ItemUpdatedEvent, avroReadyMap)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemUpdated event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishItemRemoved(ctx context.Context, data models.ItemRemoved) error {
	topic := consts.ItemDeletedEvent
	err := s.producer.SendMessageWithAvro(ctx, topic, data)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish ItemRemoved event: %v", err)
		return err
	}
	return nil
}

func (s *inventoryServiceImpl) publishPriceUpdated(ctx context.Context, data models.PriceUpdated) error {
	topic := consts.PriceUpdatedEvent
	err := s.producer.SendMessageWithAvro(ctx, topic, data)
	if err != nil {
		s.logger.Error(ctx, "Failed to publish PriceUpdated event: %v", err)
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
