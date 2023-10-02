package service

import (
	"context"
	"reflect"
	"time"

	"github.com/O7Oghany/EDM/inventory-service/internal/db"
	"github.com/O7Oghany/EDM/inventory-service/internal/kafka"
	"github.com/O7Oghany/EDM/inventory-service/pkg/consts"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"github.com/O7Oghany/EDM/inventory-service/pkg/util"
)

type InventoryService interface {
	AddItem(ctx context.Context, cpu models.CPU) error
	UpdateItem(ctx context.Context, originalCPU models.CPU, updatedCPU models.CPU) error
	DeleteItem(ctx context.Context, id string) error
	GetItem(ctx context.Context, id string) (*models.CPU, error)
	ListItems(ctx context.Context) ([]models.CPU, error)
	PublishEvent(ctx context.Context, eventType string, payload interface{}) error
	UpdateAvroEncoder(avroEncoder kafka.AvroEncoder) error
}

type inventoryServiceImpl struct {
	db          db.InventoryDB
	logger      logger.Logger
	producer    kafka.MessageProducer
	avroEncoder kafka.AvroEncoder
}

func NewInventoryService(db db.InventoryDB, logger logger.Logger, producer kafka.MessageProducer) InventoryService {
	return &inventoryServiceImpl{
		db:       db,
		logger:   logger,
		producer: producer,
	}
}

func (s *inventoryServiceImpl) UpdateAvroEncoder(avroEncoder kafka.AvroEncoder) error {
	s.avroEncoder = avroEncoder
	return nil
}

func (s *inventoryServiceImpl) AddItem(ctx context.Context, cpu models.CPU) error {
	if err := s.db.CreateItem(ctx, cpu); err != nil {
		s.logger.Error(ctx, "failed to create item", "error: ", err)
		return err
	}
	itemAdded := models.ItemAdded{
		ItemID:    cpu.ID,
		Name:      cpu.Name,
		Quantity:  cpu.InStock,
		Price:     cpu.Price,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	if err := s.PublishEvent(ctx, consts.ItemCreatedEvent, itemAdded); err != nil {
		s.logger.Error(ctx, "Failed to publish events", "Error: ", err)
		return err
	}
	if cpu.InStock <= 0 {
		if err := s.handleStockDepleted(ctx, cpu); err != nil {
			s.logger.Error(ctx, "Failed to handle Stock Depleted", "Error: ", err)
			return err
		}
	} else {
		if err := s.handleStockReplenished(ctx, cpu, int32(cpu.InStock)); err != nil {
			s.logger.Error(ctx, "Failed to handle Stock Replenished", "Error: ", err)
			return err
		}
	}
	s.logger.Info(ctx, "item created successfully", "id", cpu.ID)
	return nil
}

func (s *inventoryServiceImpl) UpdateItem(ctx context.Context, originalCPU models.CPU, updatedCPU models.CPU) error {
	if reflect.DeepEqual(originalCPU, updatedCPU) {
		s.logger.Info(ctx, "no change in item, skipping update", "id", updatedCPU.ID)
		return nil
	}
	updatedCPU.ID = originalCPU.ID
	if err := s.db.UpdateItem(ctx, updatedCPU); err != nil {
		s.logger.Error(ctx, "failed to update item", "error", err)
		return err
	}
	addedStock := int32(updatedCPU.InStock - originalCPU.InStock)
	if updatedCPU.InStock <= 0 && originalCPU.InStock > 0 {
		if err := s.handleStockDepleted(ctx, updatedCPU); err != nil {
			s.logger.Error(ctx, "Failed", "Error", err)
			return err
		}
	} else if updatedCPU.InStock > 0 && originalCPU.InStock <= 0 {
		if err := s.handleStockReplenished(ctx, updatedCPU, addedStock); err != nil {
			s.logger.Error(ctx, "Failed", "Error", err)
			return err
		}
	}

	kafkaEvent := util.PopulateItemUpdated(updatedCPU, originalCPU)

	if err := s.PublishEvent(ctx, consts.ItemUpdatedEvent, kafkaEvent); err != nil {
		s.logger.Error(ctx, "Failed", err)
		return err
	}

	s.logger.Info(ctx, "item updated successfully", "id", updatedCPU.ID)
	return nil
}

func (s *inventoryServiceImpl) DeleteItem(ctx context.Context, id string) error {

	if err := s.db.DeleteItem(ctx, id); err != nil {
		s.logger.Error(ctx, "failed to delete item", "error", err)
		return err
	}

	itemRemoved := models.ItemRemoved{
		ItemID:    id,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	if err := s.PublishEvent(ctx, consts.ItemDeletedEvent, itemRemoved); err != nil {
		s.logger.Error(ctx, "Failed: %v", err)
		return err
	}

	s.logger.Info(ctx, "item deleted successfully", "id", id)
	return nil
}

func (s *inventoryServiceImpl) ListItems(ctx context.Context) ([]models.CPU, error) {
	if cpus, err := s.db.ListItems(ctx); err != nil {
		s.logger.Error(ctx, "failed to list items", "error", err)
		return nil, err
	} else {
		s.logger.Info(ctx, "items listed successfully", "count", len(cpus))
		return cpus, nil
	}
}

func (s *inventoryServiceImpl) GetItem(ctx context.Context, id string) (*models.CPU, error) {
	if cpu, err := s.db.GetItem(ctx, id); err != nil {
		s.logger.Error(ctx, "failed to get item", "error", err)
		return nil, err
	} else {
		s.logger.Info(ctx, "item retrieved successfully", "id", id)
		return cpu, nil
	}
}

func (s *inventoryServiceImpl) handleStockDepleted(ctx context.Context, cpu models.CPU) error {
	depletedEvent := util.PopulateStockDepleted(cpu, 0)
	if err := s.PublishEvent(ctx, consts.StockDepletedEvent, depletedEvent); err != nil {
		s.logger.Error(ctx, "Failed to publish StockDepleted event", "Error", err)
		return err
	}
	return nil
}
func (s *inventoryServiceImpl) handleStockReplenished(ctx context.Context, cpu models.CPU, addedStock int32) error {
	replenishedEvent := util.PopulateStockReplenished(cpu, addedStock, int32(cpu.InStock))
	if err := s.PublishEvent(ctx, consts.StockReplenishedEvent, replenishedEvent); err != nil {
		s.logger.Error(ctx, "Failed to publish StockReplenished event", "Error", err)
		return err
	}
	return nil
}
