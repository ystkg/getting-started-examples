package redis

import (
	"github.com/redis/go-redis/v9"
)

func NewClient(addr, username, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: username,
		Password: password,
	})
}
