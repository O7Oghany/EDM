package models

type CPU struct {
	ID          string  `json:"id" gorm:"primaryKey"`
	Name        string  `json:"name,omitempty"`
	Brand       string  `json:"brand,omitempty"`
	ClockSpeed  float64 `json:"clock_speed,omitempty"` // in GHz
	Cores       int     `json:"cores,omitempty"`
	Price       float64 `json:"price,omitempty"` // in EUR
	SKU         string  `json:"sku,omitempty"`
	InStock     int     `json:"in_stock,omitempty"`
	IsAvailable bool    `json:"is_available,omitempty"`
}

type ProducerConfig struct {
	BootstrapServers        string `yaml:"bootstrap.servers"`
	SecurityProtocol        string `yaml:"security.protocol"`
	SSLKeyLocation          string `yaml:"ssl.key.location"`
	SSLKeyPassword          string `yaml:"ssl.key.password"`
	SSLKeyStoreLocation     string `yaml:"ssl.keystore.location"`
	SSLKeyStorePassword     string `yaml:"ssl.keystore.password"`
	SSLCALocation           string `yaml:"ssl.ca.location"`
	KeySerializer           string `yaml:"key.serializer"`
	ValueSerializer         string `yaml:"value.serializer"`
	Acks                    string `yaml:"acks"`
	Retries                 int    `yaml:"retries"`
	BatchSize               int    `yaml:"batch.size"`
	LingerMs                int    `yaml:"linger.ms"`
	BufferMemory            int    `yaml:"buffer.memory"`
	IdentificationAlgorithm string `yaml:"ssl.endpoint.identification.algorithm"`
}

type ConsumerConfig struct {
	BootstrapServers   string `yaml:"bootstrap.servers"`
	GroupID            string `yaml:"group.id"`
	AutoOffsetReset    string `yaml:"auto.offset.reset"`
	EnableAutoCommit   string `yaml:"enable.auto.commit"`
	AutoCommitInterval string `yaml:"auto.commit.interval.ms"`
	SessionTimeout     string `yaml:"session.timeout.ms"`
	SecurityProtocol   string `yaml:"security.protocol"`
	SSLCALocation      string `yaml:"ssl.ca.location"`
}

type ItemAdded struct {
	ItemID    string  `json:"item_id"`
	Name      string  `json:"name"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

type ItemRemoved struct {
	ItemID    string `json:"item_id"`
	Timestamp int64  `json:"timestamp"`
}

type ItemUpdated struct {
	ItemID      string   `json:"item_id" avro:""`
	Name        *string  `json:"name,omitempty" avro:"string"`
	Brand       *string  `json:"brand,omitempty" avro:"string"`
	ClockSpeed  *float64 `json:"clock_speed,omitempty" avro:"double"`
	Cores       *int32   `json:"cores,omitempty" avro:"int"`
	Price       *float64 `json:"price,omitempty" avro:"double"`
	SKU         *string  `json:"sku,omitempty" avro:"string"`
	Quantity    *int32   `json:"quantity,omitempty" avro:"int"`
	IsAvailable *bool    `json:"is_available,omitempty" avro:"boolean"`
	Timestamp   int64    `json:"timestamp" avro:""`
}

type StockDepleted struct {
	ItemID         string `json:"item_id"`
	RemainingStock int32    `json:"remaining_stock"`
	Timestamp      int64  `json:"timestamp"`
}

type StockReplenished struct {
	ItemID        string `json:"item_id"`
	AddedStock    int32    `json:"added_stock"`
	NewTotalStock int32    `json:"new_total_stock"`
	Timestamp     int64  `json:"timestamp"`
}
