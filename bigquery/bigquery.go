package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func NewClient(projectID, url string) (*bigquery.Client, error) {
	return bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithEndpoint(url),
		option.WithoutAuthentication(),
	)
}
