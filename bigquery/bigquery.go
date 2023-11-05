package bigquery

import (
	"context"
	"net/http"

	bigqueryapi "cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQuery struct {
	client *bigqueryapi.Client
}

func NewBigQuery(ctx context.Context, projectID, url string) (*BigQuery, error) {
	client, err := bigqueryapi.NewClient(
		ctx,
		projectID,
		option.WithEndpoint(url),
		option.WithoutAuthentication(),
	)
	if err != nil {
		return nil, err
	}
	return &BigQuery{client}, nil
}

func (bq *BigQuery) Close() error {
	return bq.client.Close()
}

// データセット作成
// if not exists
func (bq *BigQuery) CreateDataset(ctx context.Context, datasetID string) error {
	md := &bigqueryapi.DatasetMetadata{}
	_, err := bq.client.Dataset(datasetID).Metadata(ctx)
	if err == nil {
		return nil
	}
	if e, ok := err.(*googleapi.Error); ok {
		if e.Code != http.StatusNotFound {
			return err
		}
	}
	return bq.client.Dataset(datasetID).Create(ctx, md)
}

// データセット削除
// if exists
func (bq *BigQuery) DeleteDataset(ctx context.Context, datasetID string) error {
	_, err := bq.client.Dataset(datasetID).Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				return nil
			}
		}
		return err
	}
	return bq.client.Dataset(datasetID).Delete(ctx)
}

// データセット一覧
func (bq *BigQuery) Datasets(ctx context.Context) ([]string, error) {
	datasets := []string{}

	it := bq.client.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			return datasets, nil
		}
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, dataset.DatasetID)
	}
}

// テーブル作成
// if not exists
func (bq *BigQuery) CreateTable(ctx context.Context, datasetID, tableID string, schema bigqueryapi.Schema) error {
	_, err := bq.client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err == nil {
		return nil
	}
	if e, ok := err.(*googleapi.Error); ok {
		if e.Code != http.StatusNotFound {
			return err
		}
	}
	return bq.client.Dataset(datasetID).Table(tableID).Create(ctx, &bigqueryapi.TableMetadata{
		Schema: schema,
	})
}

// テーブル削除
// if exists
func (bq *BigQuery) DeleteTable(ctx context.Context, datasetID, tableID string) error {
	_, err := bq.client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				return nil
			}
		}
		return err
	}
	return bq.client.Dataset(datasetID).Table(tableID).Delete(ctx)
}

// テーブル一覧
func (bq *BigQuery) Tables(ctx context.Context, datasetID string) ([]string, error) {
	dataset := bq.client.Dataset(datasetID)
	if dataset == nil {
		return nil, nil
	}

	tables := []string{}

	it := dataset.Tables(ctx)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			return tables, nil
		}
		if err != nil {
			return nil, err
		}
		tables = append(tables, table.TableID)
	}
}

func (bq *BigQuery) Insert(ctx context.Context, datasetID, tableID string, items interface{}) error {
	return bq.client.Dataset(datasetID).Table(tableID).Inserter().Put(ctx, items)
}

func (bq *BigQuery) Query(ctx context.Context, sql string) ([][]bigqueryapi.Value, error) {
	it, err := bq.client.Query(sql).Read(ctx)
	if err != nil {
		return nil, err
	}

	rows := [][]bigqueryapi.Value{}

	for {
		var values []bigqueryapi.Value
		err = it.Next(&values)
		if err == iterator.Done {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, values)
	}
}
