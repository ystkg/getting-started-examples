package gcs

import (
	"context"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type Gcs struct {
	client *storage.Client
}

func NewGcs(ctx context.Context) (*Gcs, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &Gcs{client}, nil
}

func (s *Gcs) Close() error {
	return s.client.Close()
}

func (s *Gcs) CreateBucket(ctx context.Context, projectID, name string) error {
	return s.client.Bucket(name).Create(ctx, projectID, nil)
}

func (s *Gcs) DeleteBucket(ctx context.Context, name string) error {
	return s.client.Bucket(name).Delete(ctx)
}

func (s *Gcs) ExistsBucket(ctx context.Context, projectID, name string) (bool, error) {
	it := s.client.Buckets(ctx, projectID)
	for {
		attr, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				return false, nil
			}
			return false, err
		}
		if attr.Name == name {
			return true, nil
		}
	}
}

func (s *Gcs) Buckets(ctx context.Context, projectID string) ([]string, error) {
	buckets := []string{}

	it := s.client.Buckets(ctx, projectID)
	for {
		attr, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		buckets = append(buckets, attr.Name)
	}

	return buckets, nil
}
