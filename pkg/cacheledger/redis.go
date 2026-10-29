/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacheledger

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisKeyPrefix namespaces ledger keys in a shared Redis.
const redisKeyPrefix = "llm-d:cache-ledger:"

// matchAndTouchScript runs the match and the writes in one script, so
// concurrent requests sharing a prefix are serialized by Redis. A write only
// ever extends a key's TTL: a request asking for a shorter retention must not
// cut short a longer one already promised.
// KEYS are the block keys in prompt order; ARGV[1] is the TTL in milliseconds.
//
//nolint:dupword // Each Lua "end" closes its own nested block.
var matchAndTouchScript = redis.NewScript(`
local ttl = tonumber(ARGV[1])
local matched = 0
local contiguous = true
for _, key in ipairs(KEYS) do
	local remaining = redis.call("PTTL", key)
	if contiguous then
		if remaining > 0 then
			matched = matched + 1
		else
			contiguous = false
		end
	end
	if remaining < ttl then
		redis.call("SET", key, "1", "PX", ttl)
	end
end
return matched
`)

// touchScript extends each key's TTL to at least the matching ARGV entry, in
// milliseconds, creating the key if it is gone.
//
//nolint:dupword // Each Lua "end" closes its own nested block.
var touchScript = redis.NewScript(`
for i, key in ipairs(KEYS) do
	local ttl = tonumber(ARGV[i])
	if redis.call("PTTL", key) < ttl then
		redis.call("SET", key, "1", "PX", ttl)
	end
end
return 0
`)

// RedisStore is a Store backed by Redis, shared by every ledger replica that
// points at the same Redis. Entry expiry uses Redis key TTLs.
type RedisStore struct {
	rdb *redis.Client
}

// NewRedisStore connects to the Redis at url, for example redis://redis:6379/0.
func NewRedisStore(url string) (*RedisStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}
	return &RedisStore{rdb: redis.NewClient(opts)}, nil
}

// Ping checks that Redis is reachable.
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// Close releases the Redis connections.
func (s *RedisStore) Close() error {
	return s.rdb.Close()
}

// MatchAndTouch implements Store.
func (s *RedisStore) MatchAndTouch(ctx context.Context, tenant string, hashes []uint64, ttl time.Duration) (int, error) {
	if len(hashes) == 0 {
		return 0, nil
	}
	keys := make([]string, len(hashes))
	for i, h := range hashes {
		keys[i] = redisKey(tenant, h)
	}
	matched, err := matchAndTouchScript.Run(ctx, s.rdb, keys, ttl.Milliseconds()).Int()
	if err != nil {
		return 0, fmt.Errorf("cache ledger match-and-touch: %w", err)
	}
	return matched, nil
}

// Touch extends the TTL of each hash to at least the given duration, without
// matching. Non-positive durations are skipped.
func (s *RedisStore) Touch(ctx context.Context, tenant string, ttls map[uint64]time.Duration) error {
	keys := make([]string, 0, len(ttls))
	args := make([]any, 0, len(ttls))
	for h, ttl := range ttls {
		if ttl.Milliseconds() <= 0 {
			continue
		}
		keys = append(keys, redisKey(tenant, h))
		args = append(args, ttl.Milliseconds())
	}
	if len(keys) == 0 {
		return nil
	}
	if err := touchScript.Run(ctx, s.rdb, keys, args...).Err(); err != nil {
		return fmt.Errorf("cache ledger touch: %w", err)
	}
	return nil
}

// redisKey places the tenant in a hash tag so every key of one tenant lands in
// one Redis Cluster slot, which a multi-key script requires. The hash has a
// fixed width, so distinct tenants cannot produce the same key.
func redisKey(tenant string, hash uint64) string {
	return redisKeyPrefix + "{" + tenant + "}:" + fmt.Sprintf("%016x", hash)
}
