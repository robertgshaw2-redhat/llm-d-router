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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// storeUnderTest pairs a Store with a way to move its clock forward.
type storeUnderTest struct {
	store   Store
	advance func(time.Duration)
}

func storesUnderTest(t *testing.T) map[string]func() storeUnderTest {
	t.Helper()
	return map[string]func() storeUnderTest{
		"memory": func() storeUnderTest {
			now := time.Unix(1_000_000, 0)
			var mu sync.Mutex
			s := NewMemoryStore()
			s.now = func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				return now
			}
			return storeUnderTest{store: s, advance: func(d time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				now = now.Add(d)
			}}
		},
		"redis": func() storeUnderTest {
			mr := miniredis.RunT(t)
			s, err := NewRedisStore("redis://" + mr.Addr())
			if err != nil {
				t.Fatalf("NewRedisStore: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return storeUnderTest{store: s, advance: mr.FastForward}
		},
	}
}

func matchAndTouch(t *testing.T, s Store, tenant string, hashes []uint64, ttl time.Duration) int {
	t.Helper()
	n, err := s.MatchAndTouch(context.Background(), tenant, hashes, ttl)
	if err != nil {
		t.Fatalf("MatchAndTouch: %v", err)
	}
	return n
}

func TestStoreMatchesLeadingRetainedPrefix(t *testing.T) {
	for name, newStore := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			s := newStore().store
			if got := matchAndTouch(t, s, "a", []uint64{1, 2, 3}, time.Minute); got != 0 {
				t.Fatalf("first request matched %d, want 0", got)
			}
			if got := matchAndTouch(t, s, "a", []uint64{1, 2, 3, 4}, time.Minute); got != 3 {
				t.Fatalf("extended prompt matched %d, want 3", got)
			}
			if got := matchAndTouch(t, s, "a", []uint64{1, 2, 9, 4}, time.Minute); got != 2 {
				t.Fatalf("divergent prompt matched %d, want 2: a retained hash after the first miss must not count", got)
			}
			if got := matchAndTouch(t, s, "a", []uint64{7, 1, 2}, time.Minute); got != 0 {
				t.Fatalf("prompt missing its first block matched %d, want 0", got)
			}
		})
	}
}

func TestStoreIsolatesTenants(t *testing.T) {
	for name, newStore := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			s := newStore().store
			matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute)
			if got := matchAndTouch(t, s, "b", []uint64{1, 2}, time.Minute); got != 0 {
				t.Fatalf("tenant b matched %d of tenant a's blocks, want 0", got)
			}
		})
	}
}

func TestStoreExpiresAndRefreshes(t *testing.T) {
	for name, newStore := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			sut := newStore()
			s := sut.store
			matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute)

			sut.advance(50 * time.Second)
			// A hit refreshes the TTL, so the prefix outlives its first minute.
			if got := matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute); got != 2 {
				t.Fatalf("matched %d before expiry, want 2", got)
			}
			sut.advance(50 * time.Second)
			if got := matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute); got != 2 {
				t.Fatalf("matched %d after a refresh, want 2", got)
			}

			sut.advance(61 * time.Second)
			if got := matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute); got != 0 {
				t.Fatalf("matched %d after expiry, want 0", got)
			}
		})
	}
}

func TestStoreNeverShortensRetention(t *testing.T) {
	for name, newStore := range storesUnderTest(t) {
		t.Run(name, func(t *testing.T) {
			sut := newStore()
			matchAndTouch(t, sut.store, "a", []uint64{1}, time.Hour)
			matchAndTouch(t, sut.store, "a", []uint64{1}, time.Minute)
			sut.advance(2 * time.Minute)
			if got := matchAndTouch(t, sut.store, "a", []uint64{1}, time.Minute); got != 1 {
				t.Fatalf("matched %d, want 1: a 1m request cut short a 1h retention", got)
			}
		})
	}
}

func TestMemoryStoreSweepDropsExpiredEntries(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewMemoryStore()
	s.now = func() time.Time { return now }
	matchAndTouch(t, s, "a", []uint64{1, 2}, time.Minute)
	matchAndTouch(t, s, "b", []uint64{3}, time.Hour)

	now = now.Add(2 * time.Minute)
	s.sweep()

	if sh := s.shard("a"); len(sh.tenants["a"]) != 0 {
		t.Errorf("tenant a kept %d expired entries", len(sh.tenants["a"]))
	}
	if sh := s.shard("b"); len(sh.tenants["b"]) != 1 {
		t.Errorf("tenant b has %d entries, want its 1 live entry", len(sh.tenants["b"]))
	}
}

func TestRedisKeyDoesNotCollideAcrossTenants(t *testing.T) {
	if redisKey("a", 1) == redisKey("a}:0000000000000000", 1) {
		t.Fatal("distinct tenants produced the same key")
	}
}
