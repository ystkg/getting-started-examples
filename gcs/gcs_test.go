package gcs_test

import (
	"bytes"
	"context"
	_ "embed"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/gcs"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Gcs struct {
			Ports []string
		}
	}
}

//go:embed testdata/file1.txt
var file1 []byte

//go:embed testdata/file2.txt
var file2 []byte

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatal(err)
	}
	m.Run()
}

func setup() error {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		return err
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		return err
	}

	port, _, _ := strings.Cut(conf.Services.Gcs.Ports[0], ":")
	if err = os.Setenv("STORAGE_EMULATOR_HOST", "localhost:"+port); err != nil {
		return err
	}

	return nil
}

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gcs, err := gcs.NewGcs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gcs.Close()

	const projectID = "local-gcs-20231029"
	const bucketName = "bucket1"

	_, err = gcs.ExistsBucket(ctx, projectID, bucketName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gcs, err := gcs.NewGcs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gcs.Close()

	const projectID = "local-gcs-20231029"
	const bucketName = "bucket2"

	if err = gcs.CreateBucket(ctx, projectID, bucketName); err != nil {
		t.Fatal(err)
	}

	exists, err := gcs.ExistsBucket(ctx, projectID, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error(bucketName + " not exists")
	}

	buckets, err := gcs.Buckets(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) < 1 {
		t.Error("bucket is empty")
	}

	if err = gcs.Write(ctx, bucketName, "file1", "text/plain", file1); err != nil {
		t.Fatal(err)
	}

	exists, err = gcs.Exists(ctx, bucketName, "file1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("file1 not exists")
	}

	got, err := gcs.Read(ctx, bucketName, "file1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(file1, got) {
		t.Error("file1 not equal")
	}

	if err = gcs.Write(ctx, bucketName, "file2", "text/plain", file2); err != nil {
		t.Fatal(err)
	}

	exists, err = gcs.Exists(ctx, bucketName, "file2")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("file2 not exists")
	}

	got, err = gcs.Read(ctx, bucketName, "file2")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(file2, got) {
		t.Error("file2 not equal")
	}

	files, err := gcs.List(ctx, bucketName, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range files {
		if err = gcs.Delete(ctx, bucketName, v); err != nil {
			t.Error(err)
		}
	}

	exists, err = gcs.Exists(ctx, bucketName, "file1")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("file1 exists")
	}

	exists, err = gcs.Exists(ctx, bucketName, "file2")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("file2 exists")
	}

	if err := gcs.DeleteBucket(ctx, bucketName); err != nil {
		t.Fatal(err)
	}

	exists, err = gcs.ExistsBucket(ctx, projectID, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error(bucketName + " exists")
	}
}
