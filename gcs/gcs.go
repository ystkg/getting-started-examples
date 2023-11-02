package gcs

import (
	"context"
	"io"

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

func (s *Gcs) ListBuckets(ctx context.Context, projectID string) ([]string, error) {
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

func (s *Gcs) Write(ctx context.Context, bucket, name, contentType string, bytes []byte) error {
	w := s.client.Bucket(bucket).Object(name).NewWriter(ctx)
	defer w.Close()

	w.ContentType = contentType
	if _, err := w.Write(bytes); err != nil {
		return err
	}

	return nil
}

func (s *Gcs) Read(ctx context.Context, bucket, name string) ([]byte, error) {
	r, err := s.client.Bucket(bucket).Object(name).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

func (s *Gcs) Delete(ctx context.Context, bucket, name string) error {
	return s.client.Bucket(bucket).Object(name).Delete(ctx)
}

func (s *Gcs) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	query := &storage.Query{Prefix: prefix}

	names := []string{}

	it := s.client.Bucket(bucket).Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		names = append(names, obj.Name)
	}

	return names, nil
}

func (s *Gcs) Exists(ctx context.Context, bucket, name string) (bool, error) {
	query := &storage.Query{Prefix: name}

	it := s.client.Bucket(bucket).Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return false, err
		}
		if name == obj.Name { // not prefix match
			return true, nil
		}
	}

	return false, nil
}
