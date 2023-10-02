package service

import (
	"context"
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
		s.logger.Error(ctx, "failed to create item", "error", err)
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
		s.logger.Error(ctx, "Failed: %v", err)
		return err
	}

	s.logger.Info(ctx, "item created successfully", "id", cpu.ID)
	return nil
}

func (s *inventoryServiceImpl) UpdateItem(ctx context.Context, originalCPU models.CPU, updatedCPU models.CPU) error {
	// Update item in the database
	if err := s.db.UpdateItem(ctx, updatedCPU); err != nil {
		s.logger.Error(ctx, "failed to update item", "error", err)
		return err
	}

	// Populate only changed fields into models.ItemUpdated
	kafkaEvent := util.PopulateItemUpdated(updatedCPU, originalCPU)

	// Publish the changes
	if err := s.PublishEvent(ctx, consts.ItemUpdatedEvent, kafkaEvent); err != nil {
		s.logger.Error(ctx, "Failed: %v", err)
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

	if err := s.updateAvroSchema(consts.ItemDeletedEvent); err != nil {
		s.logger.Error(ctx, "Failed to update Avro schema: %v", err)
		return err
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
