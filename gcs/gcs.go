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

// バケット作成
// if not exists
func (s *Gcs) CreateBucket(ctx context.Context, projectID, name string) error {
	_, err := s.client.Bucket(name).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		return s.client.Bucket(name).Create(ctx, projectID, nil)
	}
	return err
}

// バケット削除
// if exists
func (s *Gcs) DeleteBucket(ctx context.Context, name string) error {
	err := s.client.Bucket(name).Delete(ctx)
	if err == storage.ErrBucketNotExist {
		return nil
	}
	return err
}

// バケット存在確認
func (s *Gcs) ExistsBucket(ctx context.Context, projectID, name string) (bool, error) {
	_, err := s.client.Bucket(name).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// バケット一覧
func (s *Gcs) Buckets(ctx context.Context, projectID string) ([]string, error) {
	buckets := []string{}

	it := s.client.Buckets(ctx, projectID)
	for {
		attr, err := it.Next()
		if err == iterator.Done {
			return buckets, nil
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, attr.Name)
	}
}

// オブジェクト書き込み
// create or replace
func (s *Gcs) Write(ctx context.Context, bucket, name, contentType string, bytes []byte) error {
	w := s.client.Bucket(bucket).Object(name).NewWriter(ctx)
	defer w.Close()

	w.ContentType = contentType
	if _, err := w.Write(bytes); err != nil {
		return err
	}

	return nil
}

// オブジェクト読み込み
func (s *Gcs) Read(ctx context.Context, bucket, name string) ([]byte, error) {
	r, err := s.client.Bucket(bucket).Object(name).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// オブジェクト削除
// if exists
func (s *Gcs) Delete(ctx context.Context, bucket, name string) error {
	err := s.client.Bucket(bucket).Object(name).Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

// オブジェクト一覧
func (s *Gcs) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	query := &storage.Query{Prefix: prefix}

	names := []string{}

	it := s.client.Bucket(bucket).Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			return names, nil
		}
		if err != nil {
			return nil, err
		}
		names = append(names, obj.Name)
	}
}

// オブジェクト存在確認
func (s *Gcs) Exists(ctx context.Context, bucket, name string) (bool, error) {
	_, err := s.client.Bucket(bucket).Object(name).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
