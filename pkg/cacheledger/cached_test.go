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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// cachedReplica is one ledger replica: a CachedStore with its own local clock
// over a Redis shared with the other replicas.
type cachedReplica struct {
	*CachedStore
	now time.Time
}

func newCachedReplica(t *testing.T, mr *miniredis.Miniredis, localTTL time.Duration) *cachedReplica {
	t.Helper()
	rs, err := NewRedisStore("redis://" + mr.Addr())
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = rs.Close() })
	r := &cachedReplica{CachedStore: NewCachedStore(rs, localTTL, prometheus.NewRegistry()), now: time.Unix(1_000_000, 0)}
	r.local.now = func() time.Time { return r.now }
	return r
}

func TestCachedStoreAnswersLocalHitsWithoutRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	r := newCachedReplica(t, mr, time.Minute)
	matchAndTouch(t, r, "a", []uint64{1, 2, 3}, time.Hour)

	mr.Close()
	if got := matchAndTouch(t, r, "a", []uint64{1, 2, 3}, time.Hour); got != 3 {
		t.Fatalf("matched %d with Redis down, want all 3 from the local cache", got)
	}
	if _, err := r.MatchAndTouch(context.Background(), "a", []uint64{1, 2, 3, 4}, time.Hour); err == nil {
		t.Fatal("a block missing locally was answered without Redis")
	}
}

func TestCachedStoreSendsOnlyTheTailToRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	r := newCachedReplica(t, mr, time.Minute)
	matchAndTouch(t, r, "a", []uint64{1, 2, 3}, time.Hour)
	if got := matchAndTouch(t, r, "a", []uint64{1, 2, 3, 4, 5}, time.Hour); got != 3 {
		t.Fatalf("matched %d, want 3", got)
	}
	if got := testutil.ToFloat64(r.localBlocks); got != 3 {
		t.Errorf("local blocks = %v, want 3", got)
	}
	if got := testutil.ToFloat64(r.remoteBlocks); got != 5 {
		t.Errorf("remote blocks = %v, want 5 (3 on the first call, 2 on the second)", got)
	}
}

func TestCachedStoreSeesOtherReplicasThroughRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newCachedReplica(t, mr, time.Minute)
	b := newCachedReplica(t, mr, time.Minute)
	matchAndTouch(t, a, "t", []uint64{1, 2}, time.Hour)
	if got := matchAndTouch(t, b, "t", []uint64{1, 2, 3}, time.Hour); got != 2 {
		t.Fatalf("replica b matched %d of replica a's blocks, want 2", got)
	}
}

func TestCachedStoreFlushPropagatesLocalRefreshes(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newCachedReplica(t, mr, time.Hour)
	matchAndTouch(t, a, "t", []uint64{1, 2}, time.Minute)

	// A local hit 50s later extends the retention to 110s, but only in a's
	// local copy until the flush.
	a.now = a.now.Add(50 * time.Second)
	mr.FastForward(50 * time.Second)
	matchAndTouch(t, a, "t", []uint64{1, 2}, time.Minute)
	a.Flush(context.Background(), logr.Discard())

	mr.FastForward(30 * time.Second)
	b := newCachedReplica(t, mr, time.Hour)
	if got := matchAndTouch(t, b, "t", []uint64{1, 2}, time.Minute); got != 2 {
		t.Fatalf("replica b matched %d at 80s, want 2: the refresh at 50s did not reach Redis", got)
	}
}

func TestCachedStoreLocalCopyExpiresBeforeRetention(t *testing.T) {
	mr := miniredis.RunT(t)
	r := newCachedReplica(t, mr, time.Minute)
	matchAndTouch(t, r, "a", []uint64{1}, time.Hour)

	r.now = r.now.Add(2 * time.Minute)
	mr.FastForward(2 * time.Minute)
	before := testutil.ToFloat64(r.remoteBlocks)
	if got := matchAndTouch(t, r, "a", []uint64{1}, time.Hour); got != 1 {
		t.Fatalf("matched %d after the local copy expired, want 1 from Redis", got)
	}
	if got := testutil.ToFloat64(r.remoteBlocks) - before; got != 1 {
		t.Errorf("remote blocks grew by %v, want 1", got)
	}
}
