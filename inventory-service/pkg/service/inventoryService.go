package service

import (
	"context"

	"github.com/O7Oghany/EDM/inventory-service/internal/db"
	"github.com/O7Oghany/EDM/inventory-service/internal/kafka"
	"github.com/O7Oghany/EDM/inventory-service/pkg/logger"
	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
)

type InventoryService interface {
	AddItem(ctx context.Context, cpu models.CPU) error
	UpdateItem(ctx context.Context, cpu models.CPU) error
	DeleteItem(ctx context.Context, id string) error
	GetItem(ctx context.Context, id string) (*models.CPU, error)
	ListItems(ctx context.Context) ([]models.CPU, error)
	PublishEvent(ctx context.Context, eventType string, payload interface{}) error
	UpdateAvroEncoder(avroEncoder kafka.AvroEncoder) error
}

type inventoryServiceImpl struct {
	db       db.InventoryDB
	logger   logger.Logger
	producer kafka.MessageProducer
}

func NewInventoryService(db db.InventoryDB, logger logger.Logger, producer kafka.MessageProducer) InventoryService {
	return &inventoryServiceImpl{
		db:       db,
		logger:   logger,
		producer: producer,
	}
}

func (s *inventoryServiceImpl) UpdateAvroEncoder(avroEncoder kafka.AvroEncoder) error {
	s.producer.UpdateAvroEncoder(avroEncoder)
	return nil
}

func (s *inventoryServiceImpl) AddItem(ctx context.Context, cpu models.CPU) error {
	if err := s.db.CreateItem(ctx, cpu); err != nil {
		s.logger.Error(ctx, "failed to create item", "error", err)
		return err
	}
	s.logger.Info(ctx, "item created successfully", "id", cpu.ID)
	return nil
}

// UpdateItem implements InventoryService.
func (s *inventoryServiceImpl) UpdateItem(ctx context.Context, cpu models.CPU) error {
	if err := s.db.UpdateItem(ctx, cpu); err != nil {

		s.logger.Error(ctx, "failed to update item", "error", err)
		return err
	}
	s.logger.Info(ctx, "item updated successfully", "id", cpu.ID)
	return nil

}

// DeleteItem implements InventoryService.
func (s *inventoryServiceImpl) DeleteItem(ctx context.Context, id string) error {
	if err := s.db.DeleteItem(ctx, id); err != nil {

		s.logger.Error(ctx, "failed to delete item", "error", err)
		return err
	}
	s.logger.Info(ctx, "item deleted successfully", "id", id)
	return nil
}

// ListItems implements InventoryService.
func (s *inventoryServiceImpl) ListItems(ctx context.Context) ([]models.CPU, error) {
	if cpus, err := s.db.ListItems(ctx); err != nil {
		s.logger.Error(ctx, "failed to list items", "error", err)
		return nil, err
	} else {
		s.logger.Info(ctx, "items listed successfully", "count", len(cpus))
		return cpus, nil
	}
}

// GetItem implements InventoryService.
func (s *inventoryServiceImpl) GetItem(ctx context.Context, id string) (*models.CPU, error) {
	if cpu, err := s.db.GetItem(ctx, id); err != nil {
		s.logger.Error(ctx, "failed to get item", "error", err)
		return nil, err
	} else {
		s.logger.Info(ctx, "item retrieved successfully", "id", id)
		return cpu, nil
	}
}
