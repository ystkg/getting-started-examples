package gcs

import (
	"context"

	"cloud.google.com/go/storage"
)

func NewClient() (*storage.Client, error) {
	return storage.NewClient(context.Background())
}
