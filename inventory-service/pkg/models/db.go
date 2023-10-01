package models

import (
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewDatabaseConnection(dsn string) (*gorm.DB, error) {
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
		return nil, err
	}
	gormDB.AutoMigrate(&CPU{})
	return gormDB, nil
}
