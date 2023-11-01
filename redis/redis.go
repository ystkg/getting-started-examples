package redis

import (
	"context"
	"time"

	rds "github.com/redis/go-redis/v9"
)

type Redis struct {
	client *rds.Client
}

func NewRedis(addr, username, password string) *Redis {
	return &Redis{rds.NewClient(&rds.Options{
		Addr:     addr,
		Username: username,
		Password: password,
	})}
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}
