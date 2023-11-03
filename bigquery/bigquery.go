package bigquery

import (
	"context"

	bigqueryapi "cloud.google.com/go/bigquery"
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

func (bq *BigQuery) CreateDataset(ctx context.Context, datasetID string) error {
	md := &bigqueryapi.DatasetMetadata{}
	return bq.client.Dataset(datasetID).Create(ctx, md)
}

func (bq *BigQuery) DeleteDataset(ctx context.Context, datasetID string) error {
	return bq.client.Dataset(datasetID).Delete(ctx)
}

func (bq *BigQuery) Datasets(ctx context.Context) ([]string, error) {
	datasets := []string{}

	it := bq.client.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		datasets = append(datasets, dataset.DatasetID)
	}

	return datasets, nil
}

func (bq *BigQuery) CreateTable(ctx context.Context, datasetID, tableID string, schema bigqueryapi.Schema) error {
	tm := &bigqueryapi.TableMetadata{
		Schema: schema,
	}
	return bq.client.Dataset(datasetID).Table(tableID).Create(ctx, tm)
}

func (bq *BigQuery) DeleteTable(ctx context.Context, datasetID, tableID string) error {
	return bq.client.Dataset(datasetID).Table(tableID).Delete(ctx)
}

func (bq *BigQuery) Tables(ctx context.Context, datasetID string) ([]string, error) {
	dataset := bq.client.Dataset(datasetID)
	if dataset == nil {
		return nil, nil
	}

	tables := []string{}

	it := dataset.Tables(ctx)
	for {
		table, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		tables = append(tables, table.TableID)
	}

	return tables, nil
}
