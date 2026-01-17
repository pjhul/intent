package inserter

import (
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/pjhul/intent/internal/config"
)

// Config holds configuration for the inserter service
type Config struct {
	BatchSize                   int                     `envconfig:"BATCH_SIZE" default:"1000"`
	FlushInterval               time.Duration           `envconfig:"FLUSH_INTERVAL_MS" default:"5000ms"`
	KafkaBrokers                []string                `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	EventsTopic                 string                  `envconfig:"KAFKA_EVENTS_TOPIC" default:"events.raw"`
	MembershipTopic             string                  `envconfig:"KAFKA_MEMBERSHIP_TOPIC" default:"cohort.membership"`
	EventsConsumerGroup         string                  `envconfig:"KAFKA_EVENTS_CONSUMER_GROUP" default:"inserter-events"`
	MembershipConsumerGroup     string                  `envconfig:"KAFKA_MEMBERSHIP_CONSUMER_GROUP" default:"inserter-membership"`
	ClickHouse                  config.ClickHouseConfig `envconfig:"CLICKHOUSE"`
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
