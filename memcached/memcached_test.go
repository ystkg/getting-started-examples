package memcached_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ystkg/getting-started-examples/memcached"

	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Memcached struct {
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

	port, _, _ := strings.Cut(conf.Services.Memcached.Ports[0], ":")
	server := "localhost:" + port

	client := memcached.NewMemcached(server, 5*time.Second)
	defer client.Close()

	const want = "val1"
	if err = client.SetString("key1", want, 0); err != nil {
		t.Fatal(err)
	}

	val, err := client.Get("key1")
	if err != nil {
		t.Fatal(err)
	}

	got := string(val)
	if got != want {
		t.Errorf("%s, want %s", got, want)
	}
}
