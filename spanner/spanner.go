package spanner

import (
	"context"
	"fmt"

	spnr "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Spanner struct {
	client *spnr.Client
}

func NewSpanner(ctx context.Context, db string) (*Spanner, error) {
	client, err := spnr.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	return &Spanner{client}, nil
}

func (s *Spanner) Close() {
	s.client.Close()
}

func (s *Spanner) SingleQuery(ctx context.Context, sql string) *spnr.RowIterator {
	stmt := spnr.Statement{SQL: sql}
	return s.client.Single().Query(ctx, stmt)
}

func CreateInstance(ctx context.Context, projectID, instanceID string) error {
	client, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
	})
	if err == nil {
		if _, err = op.Wait(ctx); err != nil {
			return err
		}
	} else if status.Code(err) != codes.AlreadyExists {
		return err
	}

	return nil
}

func CreateDatabase(ctx context.Context, projectID, instanceID, databaseID string) error {
	client, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	op, err := client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err == nil {
		if _, err = op.Wait(ctx); err != nil {
			return err
		}
	} else if status.Code(err) != codes.AlreadyExists {
		return err
	}

	return nil
}
