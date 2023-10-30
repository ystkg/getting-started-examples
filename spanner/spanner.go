package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
)

func NewClient(db string) (*spanner.Client, error) {
	return spanner.NewClient(context.Background(), db)
}
