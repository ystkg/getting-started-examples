package memcached_test

import (
	"os"
	"strings"
	"testing"

	"github.com/ystkg/getting-started-examples/memcached"

	"github.com/bradfitz/gomemcache/memcache"
	"gopkg.in/yaml.v3"
)

type DockerCompose struct {
	Services struct {
		Memcached struct {
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

	port := strings.SplitN(conf.Services.Memcached.Ports[0], ":", 2)[0]
	server := "localhost:" + port

	client := memcached.NewClient(server)

	const want = "val1"
	if err = client.Set(&memcache.Item{Key: "key1", Value: []byte(want)}); err != nil {
		t.Fatal(err)
	}

	item, err := client.Get("key1")
	if err != nil {
		t.Fatal(err)
	}

	got := string(item.Value)
	if got != want {
		t.Errorf("%s, want %s", got, want)
	}
}
