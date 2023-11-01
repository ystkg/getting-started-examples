package bigquery

import (
	"context"

	bquery "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQuery struct {
	client *bquery.Client
}

func NewBigQuery(ctx context.Context, projectID, url string) (*BigQuery, error) {
	client, err := bquery.NewClient(
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
