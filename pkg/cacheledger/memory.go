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
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

// memoryShards spreads tenants over independent locks so one busy tenant does
// not serialize the others.
const memoryShards = 64

// MemoryStore is a Store held in process memory. Its state is lost on restart
// and is not shared between replicas, so it suits a single replica or tests.
type MemoryStore struct {
	shards [memoryShards]memoryShard
	now    func() time.Time
}

type memoryShard struct {
	mu sync.Mutex
	// tenants maps tenant to block hash to expiry.
	tenants map[string]map[uint64]time.Time
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{now: time.Now}
	for i := range s.shards {
		s.shards[i].tenants = make(map[string]map[uint64]time.Time)
	}
	return s
}

func (s *MemoryStore) shard(tenant string) *memoryShard {
	return &s.shards[xxhash.Sum64String(tenant)%memoryShards]
}

// MatchAndTouch implements Store.
func (s *MemoryStore) MatchAndTouch(_ context.Context, tenant string, hashes []uint64, ttl time.Duration) (int, error) {
	sh := s.shard(tenant)
	now := s.now()

	sh.mu.Lock()
	defer sh.mu.Unlock()
	matched := sh.leading(tenant, hashes, now)
	sh.extend(tenant, hashes, now.Add(ttl))
	return matched, nil
}

// leading returns how many leading hashes the tenant holds unexpired.
func (s *MemoryStore) leading(tenant string, hashes []uint64) int {
	sh := s.shard(tenant)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.leading(tenant, hashes, s.now())
}

// extend sets each hash's expiry to at least expiry.
func (s *MemoryStore) extend(tenant string, hashes []uint64, expiry time.Time) {
	sh := s.shard(tenant)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.extend(tenant, hashes, expiry)
}

func (sh *memoryShard) leading(tenant string, hashes []uint64, now time.Time) int {
	entries := sh.tenants[tenant]
	for i, h := range hashes {
		if exp, found := entries[h]; !found || !exp.After(now) {
			return i
		}
	}
	return len(hashes)
}

// extend only ever lengthens an entry's life: a request asking for a shorter
// retention must not cut short a longer one already promised.
func (sh *memoryShard) extend(tenant string, hashes []uint64, expiry time.Time) {
	entries, ok := sh.tenants[tenant]
	if !ok {
		entries = make(map[uint64]time.Time, len(hashes))
		sh.tenants[tenant] = entries
	}
	for _, h := range hashes {
		if exp, found := entries[h]; !found || exp.Before(expiry) {
			entries[h] = expiry
		}
	}
}

// Run removes expired entries every interval until ctx is done. Lookups
// already ignore expired entries; this only bounds memory.
func (s *MemoryStore) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *MemoryStore) sweep() {
	now := s.now()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for tenant, entries := range sh.tenants {
			for h, exp := range entries {
				if !exp.After(now) {
					delete(entries, h)
				}
			}
			if len(entries) == 0 {
				delete(sh.tenants, tenant)
			}
		}
		sh.mu.Unlock()
	}
}
