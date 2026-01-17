package inserter

import (
	"context"
	"encoding/json"
	"log"

	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
)

// EventsInserter handles batch insertion of events into ClickHouse
type EventsInserter struct {
	client *clickhouse.Client
}

// NewEventsInserter creates a new events inserter
func NewEventsInserter(client *clickhouse.Client) *EventsInserter {
	return &EventsInserter{client: client}
}

// InsertBatch inserts a batch of events into ClickHouse
func (i *EventsInserter) InsertBatch(ctx context.Context, events []RawEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := i.client.PrepareBatch(ctx, `
		INSERT INTO events_raw (id, user_id, event_name, properties, timestamp, received_at)
	`)
	if err != nil {
		return err
	}

	for _, e := range events {
		props, err := json.Marshal(e.Properties)
		if err != nil {
			log.Printf("error marshaling properties: %v", err)
			props = []byte("{}")
		}

		if err := batch.Append(e.ID, e.UserID, e.EventName, string(props), e.Timestamp, e.ReceivedAt); err != nil {
			return err
		}
	}

	return batch.Send()
}
