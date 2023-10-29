package memcached

import (
	"github.com/bradfitz/gomemcache/memcache"
)

func NewClient(server string) *memcache.Client {
	return memcache.New(server)
}
