package inserter

import (
	"context"

	"github.com/pjhul/intent/internal/infrastructure/clickhouse"
)

// BatchPreparer prepares a batch for inserting data into ClickHouse
type BatchPreparer interface {
	PrepareBatch(ctx context.Context, query string) (InserterBatch, error)
}

// InserterBatch represents a ClickHouse batch for inserting data
type InserterBatch interface {
	Append(args ...any) error
	Send() error
}

// clickhouseBatchPreparer wraps the ClickHouse client to implement BatchPreparer
type clickhouseBatchPreparer struct {
	client *clickhouse.Client
}

// PrepareBatch implements BatchPreparer
func (c *clickhouseBatchPreparer) PrepareBatch(ctx context.Context, query string) (InserterBatch, error) {
	return c.client.PrepareBatch(ctx, query)
}
