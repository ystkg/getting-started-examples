package redis_test

import (
	"context"
	"github.com/ystkg/getting-started-examples/redis"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Redis struct {
			Ports []string
		}
	}
}

func TestNewClient(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port := strings.SplitN(conf.Services.Redis.Ports[0], ":", 2)[0]
	addr := "localhost:" + port
	rdb := redis.NewClient(addr, "", "")
	defer rdb.Close()

	ctx := context.Background()

	const want = "val1"
	if err = rdb.Set(ctx, "key1", want, 0).Err(); err != nil {
		t.Fatal(err)
	}

	got, err := rdb.Get(ctx, "key1").Result()
	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("%s, want %s", got, want)
	}
}
