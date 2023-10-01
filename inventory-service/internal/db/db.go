package db

import (
	"context"

	"github.com/O7Oghany/EDM/inventory-service/pkg/models"
	"gorm.io/gorm"
)

type InventoryDBImpl struct {
	DB *gorm.DB
}

func NewInventoryDB(db *gorm.DB) InventoryDB {
	return &InventoryDBImpl{
		DB: db,
	}
}

type InventoryDB interface {
	CreateItem(ctx context.Context, cpu models.CPU) error
	GetItem(ctx context.Context, id string) (*models.CPU, error)
	UpdateItem(ctx context.Context, cpu models.CPU) error
	DeleteItem(ctx context.Context, id string) error
	ListItems(ctx context.Context) ([]models.CPU, error)
}

func (i *InventoryDBImpl) CreateItem(ctx context.Context, cpu models.CPU) error {
	if err := i.DB.Create(&cpu).Error; err != nil {
		return err
	}
	return nil
}

func (i *InventoryDBImpl) GetItem(ctx context.Context, id string) (*models.CPU, error) {
	var cpu models.CPU
	err := i.DB.Where("id = ?", id).First(&cpu).Error
	if err != nil {
		return nil, err
	}
	return &cpu, nil
}

func (i *InventoryDBImpl) UpdateItem(ctx context.Context, cpu models.CPU) error {
	return i.DB.Save(&cpu).Error
}

func (i *InventoryDBImpl) DeleteItem(ctx context.Context, id string) error {
	return i.DB.Where("id = ?", id).Delete(&models.CPU{}).Error
}

func (i *InventoryDBImpl) ListItems(ctx context.Context) ([]models.CPU, error) {
	var cpus []models.CPU
	err := i.DB.Find(&cpus).Error
	if err != nil {
		return nil, err
	}
	return cpus, nil
}
