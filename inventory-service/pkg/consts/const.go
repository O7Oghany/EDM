package consts

import "time"

const (
	ItemCreatedEvent      = "item_created_topic"
	ItemUpdatedEvent      = "item_updated_topic"
	ItemDeletedEvent      = "item_deleted_topic"
	PriceUpdatedEvent     = "price_updated_topic"
	StockReplenishedEvent = "stock_replenished_topic"
	StockDepletedEvent    = "stock_depleted_topic"
	ItemsBatchAdded       = "items_batch_added_topic"
	DBTimeout             = 2 * time.Second
	KafkaTimeout          = 5 * time.Second
)
