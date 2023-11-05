package spanner

import (
	"context"
	"fmt"
	"strings"

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

// インスタンス作成
// if not exists
func (i *InstanceAdmin) Create(ctx context.Context, projectID, instanceID string) error {
	op, err := i.client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
	})
	code := status.Code(err)
	if code == codes.AlreadyExists {
		return nil
	}
	if code == codes.OK {
		_, err = op.Wait(ctx)
	}
	return err
}

// インスタンス削除
// if exists
func (i *InstanceAdmin) Delete(ctx context.Context, projectID, instanceID string) error {
	err := i.client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// インスタンス一覧
func (i *InstanceAdmin) Instances(ctx context.Context, projectID string) ([]string, error) {
	instanceIDs := []string{}
	parent := fmt.Sprintf("projects/%s", projectID)
	prefix := parent + "/instances/"

	it := i.client.ListInstances(ctx, &instancepb.ListInstancesRequest{
		Parent: parent,
	})

	for {
		ins, err := it.Next()
		if err == iterator.Done {
			return instanceIDs, nil
		}
		if err != nil {
			return nil, err
		}
		if after, found := strings.CutPrefix(ins.Name, prefix); found {
			instanceIDs = append(instanceIDs, after)
		}
	}
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

// データベース作成
// if not exists
func (d *DatabaseAdmin) CreateDatabase(ctx context.Context, projectID, instanceID, databaseID string) error {
	op, err := d.client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	code := status.Code(err)
	if code == codes.AlreadyExists {
		return nil
	}
	if err == nil {
		_, err = op.Wait(ctx)
	}
	return err
}

// インスタンス削除
// if exists
func (d *DatabaseAdmin) DropDatabase(ctx context.Context, projectID, instanceID, databaseID string) error {
	err := d.client.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// インスタンス一覧
func (d *DatabaseAdmin) Databases(ctx context.Context, projectID, instanceID string) ([]string, error) {
	databaseIDs := []string{}
	parent := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)
	prefix := parent + "/databases/"

	it := d.client.ListDatabases(ctx, &databasepb.ListDatabasesRequest{
		Parent: parent,
	})

	for {
		db, err := it.Next()
		if err == iterator.Done {
			return databaseIDs, nil
		}
		if err != nil {
			return nil, err
		}
		if after, found := strings.CutPrefix(db.Name, prefix); found {
			databaseIDs = append(databaseIDs, after)
		}
	}
}

// テーブル作成
func (d *DatabaseAdmin) CreateTable(ctx context.Context, projectID, instanceID, databaseID, ddl string) error {
	op, err := d.client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
		Statements: []string{ddl},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

// テーブル削除
// if not exists
func (d *DatabaseAdmin) DropTable(ctx context.Context, projectID, instanceID, databaseID, name string) error {
	op, err := d.client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
		Statements: []string{fmt.Sprintf("DROP TABLE IF EXISTS `%s`", name)},
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
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

// テーブル一覧
func (s *Spanner) Tables(ctx context.Context) ([]string, error) {
	tables := []string{}

	it := s.client.Single().Query(ctx, spannerapi.Statement{
		SQL: "SELECT table_name FROM information_schema.tables WHERE table_schema = ''",
	})
	defer it.Stop()

	for {
		row, err := it.Next()
		if err == iterator.Done {
			return tables, nil
		}
		if err != nil {
			return nil, err
		}
		var table string
		if err := row.Columns(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
}

func (s *Spanner) UpdateSQL(ctx context.Context, sql string) error {
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spannerapi.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spannerapi.Statement{SQL: sql})
		return err
	})
	return err
}

func (s *Spanner) UpdateMutation(ctx context.Context, ms []*spannerapi.Mutation) error {
	_, err := s.client.Apply(ctx, ms)
	return err
}

func (s *Spanner) Query(ctx context.Context, sql string) ([]*spannerapi.Row, error) {
	stmt := spannerapi.Statement{SQL: sql}
	it := s.client.Single().Query(ctx, stmt)
	defer it.Stop()

	rows := []*spannerapi.Row{}

	for {
		row, err := it.Next()
		if err == iterator.Done {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
}
