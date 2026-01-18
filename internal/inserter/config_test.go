package inserter_test

import (
	"os"
	"testing"
	"time"

	"github.com/pjhul/intent/internal/inserter"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any environment variables that might interfere
	envVars := []string{
		"BATCH_SIZE",
		"FLUSH_INTERVAL_MS",
		"KAFKA_BROKERS",
		"KAFKA_EVENTS_TOPIC",
		"KAFKA_MEMBERSHIP_TOPIC",
		"KAFKA_EVENTS_CONSUMER_GROUP",
		"KAFKA_MEMBERSHIP_CONSUMER_GROUP",
	}
	for _, env := range envVars {
		os.Unsetenv(env)
	}

	cfg, err := inserter.Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, expected 1000", cfg.BatchSize)
	}

	if cfg.FlushInterval != 5000*time.Millisecond {
		t.Errorf("FlushInterval = %v, expected 5000ms", cfg.FlushInterval)
	}

	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("KafkaBrokers = %v, expected [localhost:9092]", cfg.KafkaBrokers)
	}

	if cfg.EventsTopic != "events.raw" {
		t.Errorf("EventsTopic = %q, expected %q", cfg.EventsTopic, "events.raw")
	}

	if cfg.MembershipTopic != "cohort.membership" {
		t.Errorf("MembershipTopic = %q, expected %q", cfg.MembershipTopic, "cohort.membership")
	}

	if cfg.EventsConsumerGroup != "inserter-events" {
		t.Errorf("EventsConsumerGroup = %q, expected %q", cfg.EventsConsumerGroup, "inserter-events")
	}

	if cfg.MembershipConsumerGroup != "inserter-membership" {
		t.Errorf("MembershipConsumerGroup = %q, expected %q", cfg.MembershipConsumerGroup, "inserter-membership")
	}
}

func TestLoad_CustomValues(t *testing.T) {
	// Set custom environment variables
	os.Setenv("BATCH_SIZE", "500")
	os.Setenv("FLUSH_INTERVAL_MS", "10000ms")
	os.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
	os.Setenv("KAFKA_EVENTS_TOPIC", "custom.events")
	os.Setenv("KAFKA_MEMBERSHIP_TOPIC", "custom.membership")
	os.Setenv("KAFKA_EVENTS_CONSUMER_GROUP", "custom-events-group")
	os.Setenv("KAFKA_MEMBERSHIP_CONSUMER_GROUP", "custom-membership-group")

	// Cleanup after test
	defer func() {
		os.Unsetenv("BATCH_SIZE")
		os.Unsetenv("FLUSH_INTERVAL_MS")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("KAFKA_EVENTS_TOPIC")
		os.Unsetenv("KAFKA_MEMBERSHIP_TOPIC")
		os.Unsetenv("KAFKA_EVENTS_CONSUMER_GROUP")
		os.Unsetenv("KAFKA_MEMBERSHIP_CONSUMER_GROUP")
	}()

	cfg, err := inserter.Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.BatchSize != 500 {
		t.Errorf("BatchSize = %d, expected 500", cfg.BatchSize)
	}

	if cfg.FlushInterval != 10000*time.Millisecond {
		t.Errorf("FlushInterval = %v, expected 10000ms", cfg.FlushInterval)
	}

	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka1:9092" || cfg.KafkaBrokers[1] != "kafka2:9092" {
		t.Errorf("KafkaBrokers = %v, expected [kafka1:9092 kafka2:9092]", cfg.KafkaBrokers)
	}

	if cfg.EventsTopic != "custom.events" {
		t.Errorf("EventsTopic = %q, expected %q", cfg.EventsTopic, "custom.events")
	}

	if cfg.MembershipTopic != "custom.membership" {
		t.Errorf("MembershipTopic = %q, expected %q", cfg.MembershipTopic, "custom.membership")
	}

	if cfg.EventsConsumerGroup != "custom-events-group" {
		t.Errorf("EventsConsumerGroup = %q, expected %q", cfg.EventsConsumerGroup, "custom-events-group")
	}

	if cfg.MembershipConsumerGroup != "custom-membership-group" {
		t.Errorf("MembershipConsumerGroup = %q, expected %q", cfg.MembershipConsumerGroup, "custom-membership-group")
	}
}

func TestConfig_FlushIntervalParsing(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected time.Duration
	}{
		{"milliseconds", "100ms", 100 * time.Millisecond},
		{"seconds", "5s", 5 * time.Second},
		{"minutes", "1m", 1 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("FLUSH_INTERVAL_MS", tt.envValue)
			defer os.Unsetenv("FLUSH_INTERVAL_MS")

			cfg, err := inserter.Load()
			if err != nil {
				t.Fatalf("Load() returned error: %v", err)
			}

			if cfg.FlushInterval != tt.expected {
				t.Errorf("FlushInterval = %v, expected %v", cfg.FlushInterval, tt.expected)
			}
		})
	}
}
