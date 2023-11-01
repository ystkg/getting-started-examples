package memcached

import (
	"time"

	mc "github.com/bradfitz/gomemcache/memcache"
)

type Memcached struct {
	client *mc.Client
}

func NewMemcached(server string, timeout time.Duration) *Memcached {
	m := &Memcached{mc.New(server)}
	m.client.Timeout = timeout
	return m
}

func (m *Memcached) Close() error {
	return m.client.Close()
}

func (m *Memcached) SetString(key string, value string, expiration int32) error {
	return m.client.Set(&mc.Item{
		Key:        key,
		Value:      []byte(value),
		Expiration: expiration,
	})
}

func (m *Memcached) Set(key string, value []byte, expiration int32) error {
	return m.client.Set(&mc.Item{
		Key:        key,
		Value:      value,
		Expiration: expiration,
	})
}

func (m *Memcached) Get(key string) ([]byte, error) {
	item, err := m.client.Get(key)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}
