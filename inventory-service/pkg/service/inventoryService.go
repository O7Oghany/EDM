package service

import (
	"context"
	"fmt"
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
	AddItemsBatch(ctx context.Context, cpus []models.CPU) error
	UpdateItem(ctx context.Context, originalCPU models.CPU, updatedCPU models.CPU) error
	DeleteItem(ctx context.Context, id string) error
	GetItem(ctx context.Context, id string) (*models.CPU, error)
	ListItems(ctx context.Context) ([]models.CPU, error)
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
func (s *inventoryServiceImpl) AddItemsBatch(ctx context.Context, cpus []models.CPU) error {
	var itemAddedEvents []models.ItemAdded
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()
	for _, cpu := range cpus {
		if err := s.db.CreateItem(dbCtx, cpu); err != nil {
			s.logger.Error(ctx, "DB_CreateItem_Failure", "error", err)
			return fmt.Errorf("failed to create item in DB: %w", err)
		}

		itemAdded := models.ItemAdded{
			ItemID:    cpu.ID,
			Name:      cpu.Name,
			Quantity:  cpu.InStock,
			Price:     cpu.Price,
			Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		}
		itemAddedEvents = append(itemAddedEvents, itemAdded)
	}
	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()
	batchEvent := models.ItemsBatchAdded{
		BatchID:   util.GenerateBatchUUID(), // Implement this function to generate a unique batch ID
		Items:     itemAddedEvents,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	if err := s.producer.PublishItemsBatchAdded(kafkaCtx, batchEvent); err != nil {
		s.logger.Error(ctx, "Kafka_Publish_Failure", "error", err)
		return fmt.Errorf("failed to publish events to Kafka: %w", err)
	}

	return nil
}

func (s *inventoryServiceImpl) AddItem(ctx context.Context, cpu models.CPU) error {
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()

	if err := s.db.CreateItem(dbCtx, cpu); err != nil {
		s.logger.Error(ctx, "DB_CreateItem_Failure", "error", err)
		return fmt.Errorf("failed to create item in DB: %w", err)
	}
	itemAdded := models.ItemAdded{
		ItemID:    cpu.ID,
		Name:      cpu.Name,
		Quantity:  cpu.InStock,
		Price:     cpu.Price,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}
	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()
	if err := s.producer.PublishItemAdded(kafkaCtx, itemAdded); err != nil {
		s.logger.Error(ctx, "Kafka_Publish_Failure", "error", err)
		return fmt.Errorf("failed to publish events to Kafka: %w", err)
	}
	if cpu.InStock <= 0 {
		if err := s.handleStockDepleted(ctx, cpu); err != nil {
			s.logger.Error(ctx, "Failed to handle Stock Depleted", "Error", err)
			return err
		}
	} else {
		if err := s.handleStockReplenished(ctx, cpu, int32(cpu.InStock)); err != nil {
			s.logger.Error(ctx, "Failed to handle Stock Replenished", "Error", err)
			return err
		}
	}
	s.logger.Info(ctx, "item created successfully", "id", cpu.ID)
	return nil
}

func (s *inventoryServiceImpl) UpdateItem(ctx context.Context, originalCPU models.CPU, updatedCPU models.CPU) error {
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()

	if reflect.DeepEqual(originalCPU, updatedCPU) {
		s.logger.Info(ctx, "no change in item, skipping update", "id", updatedCPU.ID)
		return nil
	}
	updatedCPU.ID = originalCPU.ID
	if err := s.db.UpdateItem(dbCtx, updatedCPU); err != nil {
		s.logger.Error(ctx, "DB_UpdateItem_Failure", "error", err)
		return fmt.Errorf("failed to update item in DB: %w", err)
	}
	addedStock := int32(updatedCPU.InStock - originalCPU.InStock)
	if updatedCPU.InStock <= 0 && originalCPU.InStock > 0 {
		if err := s.handleStockDepleted(ctx, updatedCPU); err != nil {
			s.logger.Error(ctx, "HandleStockDepleted_Failed", "Error", err)
			return fmt.Errorf("failed to handle StockDepleted: %w", err)
		}
	} else if updatedCPU.InStock > 0 && originalCPU.InStock <= 0 {
		if err := s.handleStockReplenished(ctx, updatedCPU, addedStock); err != nil {
			s.logger.Error(ctx, "HandleStockReplenished_Failed", "Error", err)
			return fmt.Errorf("failed to handle StockReplenished: %w", err)
		}
	}

	kafkaEvent := util.PopulateItemUpdated(updatedCPU, originalCPU)
	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()
	if err := s.producer.PublishItemUpdated(kafkaCtx, kafkaEvent); err != nil {
		s.logger.Error(ctx, "Kafka_Publish_Failure", "error", err)
		return fmt.Errorf("failed to publish events to Kafka: %w", err)
	}

	s.logger.Info(ctx, "item updated successfully", "id", updatedCPU.ID)
	return nil
}

func (s *inventoryServiceImpl) DeleteItem(ctx context.Context, id string) error {
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()

	if err := s.db.DeleteItem(dbCtx, id); err != nil {
		s.logger.Error(ctx, "DB_DeleteItem_Failure", "error", err)
		return fmt.Errorf("failed to delete item in DB: %w", err)
	}

	itemRemoved := models.ItemRemoved{
		ItemID:    id,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
	}

	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()
	if err := s.producer.PublishItemRemoved(kafkaCtx, itemRemoved); err != nil {
		s.logger.Error(ctx, "Kafka_Publish_Failure", "error", err)
		return fmt.Errorf("failed to publish events to Kafka: %w", err)
	}

	s.logger.Info(ctx, "item deleted successfully", "id", id)
	return nil
}

func (s *inventoryServiceImpl) ListItems(ctx context.Context) ([]models.CPU, error) {
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()
	if cpus, err := s.db.ListItems(dbCtx); err != nil {
		s.logger.Error(ctx, "DB_ListItems_Failure", "error", err)
		return nil, fmt.Errorf("failed to lits items in DB: %w", err)
	} else {
		s.logger.Info(ctx, "items listed successfully", "count", len(cpus))
		return cpus, nil
	}
}

func (s *inventoryServiceImpl) GetItem(ctx context.Context, id string) (*models.CPU, error) {
	dbCtx, cancel := context.WithTimeout(ctx, consts.DBTimeout)
	defer cancel()

	if cpu, err := s.db.GetItem(dbCtx, id); err != nil {
		s.logger.Error(ctx, "DB_GetItem_Failure", "error", err)
		return nil, fmt.Errorf("failed to get item in DB: %w", err)
	} else {
		s.logger.Info(ctx, "item retrieved successfully", "id", id)
		return cpu, nil
	}
}

func (s *inventoryServiceImpl) handleStockDepleted(ctx context.Context, cpu models.CPU) error {
	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()
	depletedEvent := util.PopulateStockDepleted(cpu, 0)
	if err := s.producer.PublishStockDepleted(kafkaCtx, *depletedEvent); err != nil {
		s.logger.Error(ctx, "Populate_Stock_Depleted_Failure", "Error", err)
		return fmt.Errorf("failed to publish StockDepleted event: %w", err)
	}
	return nil
}

func (s *inventoryServiceImpl) handleStockReplenished(ctx context.Context, cpu models.CPU, addedStock int32) error {
	kafkaCtx, cancel := context.WithTimeout(ctx, consts.KafkaTimeout)
	defer cancel()

	replenishedEvent := util.PopulateStockReplenished(cpu, addedStock, int32(cpu.InStock))
	if err := s.producer.PublishStockReplenished(kafkaCtx, *replenishedEvent); err != nil {
		s.logger.Error(ctx, "Populate_Stock_Replenished_Failure", "Error", err)
		return fmt.Errorf("failed to publish StockReplenished event: %w", err)
	}
	return nil
}
