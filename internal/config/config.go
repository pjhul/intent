package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration for the service
type Config struct {
	Server     ServerConfig
	PostgreSQL PostgreSQLConfig
	ClickHouse ClickHouseConfig
	Kafka      KafkaConfig
	Redis      RedisConfig
	Flink      FlinkConfig
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Host         string        `envconfig:"SERVER_HOST" default:"0.0.0.0"`
	Port         int           `envconfig:"SERVER_PORT" default:"8080"`
	ReadTimeout  time.Duration `envconfig:"SERVER_READ_TIMEOUT" default:"30s"`
	WriteTimeout time.Duration `envconfig:"SERVER_WRITE_TIMEOUT" default:"30s"`
}

// PostgreSQLConfig holds PostgreSQL configuration
type PostgreSQLConfig struct {
	Host         string        `envconfig:"POSTGRES_HOST" default:"localhost"`
	Port         int           `envconfig:"POSTGRES_PORT" default:"5432"`
	User         string        `envconfig:"POSTGRES_USER" default:"cohort"`
	Password     string        `envconfig:"POSTGRES_PASSWORD" default:"cohort"`
	Database     string        `envconfig:"POSTGRES_DATABASE" default:"cohort"`
	SSLMode      string        `envconfig:"POSTGRES_SSL_MODE" default:"disable"`
	MaxOpenConns int           `envconfig:"POSTGRES_MAX_OPEN_CONNS" default:"25"`
	MaxIdleConns int           `envconfig:"POSTGRES_MAX_IDLE_CONNS" default:"5"`
	MaxIdleTime  time.Duration `envconfig:"POSTGRES_MAX_IDLE_TIME" default:"5m"`
}

// DSN returns the PostgreSQL connection string
func (c PostgreSQLConfig) DSN() string {
	return "host=" + c.Host +
		" port=" + intToStr(c.Port) +
		" user=" + c.User +
		" password=" + c.Password +
		" dbname=" + c.Database +
		" sslmode=" + c.SSLMode
}

// ClickHouseConfig holds ClickHouse configuration
type ClickHouseConfig struct {
	Host         string        `envconfig:"CLICKHOUSE_HOST" default:"localhost"`
	Port         int           `envconfig:"CLICKHOUSE_PORT" default:"9000"`
	User         string        `envconfig:"CLICKHOUSE_USER" default:"default"`
	Password     string        `envconfig:"CLICKHOUSE_PASSWORD" default:""`
	Database     string        `envconfig:"CLICKHOUSE_DATABASE" default:"cohort"`
	MaxOpenConns int           `envconfig:"CLICKHOUSE_MAX_OPEN_CONNS" default:"10"`
	MaxIdleConns int           `envconfig:"CLICKHOUSE_MAX_IDLE_CONNS" default:"5"`
	DialTimeout  time.Duration `envconfig:"CLICKHOUSE_DIAL_TIMEOUT" default:"10s"`
}

// KafkaConfig holds Kafka configuration
type KafkaConfig struct {
	Brokers          []string      `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	EventsTopic      string        `envconfig:"KAFKA_EVENTS_TOPIC" default:"events.raw"`
	CohortsTopic     string        `envconfig:"KAFKA_COHORTS_TOPIC" default:"cohort.definitions"`
	ChangesTopic     string        `envconfig:"KAFKA_CHANGES_TOPIC" default:"cohort.changes"`
	ConsumerGroup    string        `envconfig:"KAFKA_CONSUMER_GROUP" default:"cohort-service"`
	SessionTimeout   time.Duration `envconfig:"KAFKA_SESSION_TIMEOUT" default:"30s"`
	HeartbeatTimeout time.Duration `envconfig:"KAFKA_HEARTBEAT_TIMEOUT" default:"3s"`
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host         string        `envconfig:"REDIS_HOST" default:"localhost"`
	Port         int           `envconfig:"REDIS_PORT" default:"6379"`
	Password     string        `envconfig:"REDIS_PASSWORD" default:""`
	DB           int           `envconfig:"REDIS_DB" default:"0"`
	PoolSize     int           `envconfig:"REDIS_POOL_SIZE" default:"10"`
	MinIdleConns int           `envconfig:"REDIS_MIN_IDLE_CONNS" default:"5"`
	CacheTTL     time.Duration `envconfig:"REDIS_CACHE_TTL" default:"5m"`
}

// FlinkConfig holds Flink REST API configuration
type FlinkConfig struct {
	Host string `envconfig:"FLINK_HOST" default:"localhost"`
	Port int    `envconfig:"FLINK_PORT" default:"8081"`
}

// URL returns the Flink REST API URL
func (c FlinkConfig) URL() string {
	return "http://" + c.Host + ":" + intToStr(c.Port)
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
