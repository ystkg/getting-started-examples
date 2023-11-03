package spanner

import (
	"context"
	"fmt"

	spannerapi "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InstanceAdmin struct {
	client *instance.InstanceAdminClient
}

type DatabaseAdmin struct {
	client *database.DatabaseAdminClient
}

type Spanner struct {
	client *spannerapi.Client
}

func NewInstanceAdmin(ctx context.Context) (*InstanceAdmin, error) {
	client, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	return &InstanceAdmin{client}, nil
}

func (i *InstanceAdmin) Close() {
	i.client.Close()
}

func (i *InstanceAdmin) CreateInstance(ctx context.Context, projectID, instanceID string) error {
	op, err := i.client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
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

func (i *InstanceAdmin) DeleteInstance(ctx context.Context, projectID, instanceID string) error {
	return i.client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
	})
}

func (i *InstanceAdmin) ListInstances(ctx context.Context, projectID string) ([]string, error) {
	instances := []string{}

	it := i.client.ListInstances(ctx, &instancepb.ListInstancesRequest{
		Parent: fmt.Sprintf("projects/%s", projectID),
	})

	for {
		ins, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		instances = append(instances, ins.Name)
	}

	return instances, nil
}

func NewDatabaseAdmin(ctx context.Context) (*DatabaseAdmin, error) {
	client, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	return &DatabaseAdmin{client}, nil
}

func (d *DatabaseAdmin) Close() {
	d.client.Close()
}

func (d *DatabaseAdmin) CreateDatabase(ctx context.Context, projectID, instanceID, databaseID string) error {
	op, err := d.client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
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

func (d *DatabaseAdmin) DropDatabase(ctx context.Context, projectID, instanceID, databaseID string) error {
	return d.client.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
	})
}

func (d *DatabaseAdmin) ListDatabases(ctx context.Context, projectID, instanceID string) ([]string, error) {
	databases := []string{}

	it := d.client.ListDatabases(ctx, &databasepb.ListDatabasesRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
	})

	for {
		db, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		databases = append(databases, db.Name)
	}

	return databases, nil
}

func NewSpanner(ctx context.Context, db string) (*Spanner, error) {
	client, err := spannerapi.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	return &Spanner{client}, nil
}

func (s *Spanner) Close() {
	s.client.Close()
}

func (s *Spanner) SingleQuery(ctx context.Context, sql string) *spannerapi.RowIterator {
	stmt := spannerapi.Statement{SQL: sql}
	return s.client.Single().Query(ctx, stmt)
}
