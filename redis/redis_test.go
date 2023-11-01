package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/redis"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Redis struct {
			Ports []string
		}
	}
}

func TestConnect(t *testing.T) {
	buf, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	var conf DockerCompose
	if err = yaml.Unmarshal(buf, &conf); err != nil {
		t.Fatal(err)
	}

	port, _, _ := strings.Cut(conf.Services.Redis.Ports[0], ":")
	addr := "localhost:" + port
	client := redis.NewRedis(addr, "", "")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const want = "val1"
	if err = client.Set(ctx, "key1", want, 0); err != nil {
		t.Fatal(err)
	}

	got, err := client.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("%s, want %s", got, want)
	}
}
