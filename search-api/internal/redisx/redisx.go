package redisx

import (
    "context"
    "time"

    "github.com/redis/go-redis/v9"
)

type Client struct { Rdb *redis.Client }

func New(addr string, password string, db int) *Client {
    rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
    return &Client{Rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
    return c.Rdb.Ping(ctx).Err()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
    return c.Rdb.Get(ctx, key).Result()
}

func (c *Client) Set(ctx context.Context, key string, val string, ttl time.Duration) error {
    return c.Rdb.Set(ctx, key, val, ttl).Err()
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
    n, err := c.Rdb.Exists(ctx, key).Result()
    return n == 1, err
}

func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
    return c.Rdb.TTL(ctx, key).Result()
}

func (c *Client) SetNX(ctx context.Context, key string, val string, ttl time.Duration) (bool, error) {
    return c.Rdb.SetNX(ctx, key, val, ttl).Result()
}
