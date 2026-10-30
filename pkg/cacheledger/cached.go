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

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
)

// touchBatchSize bounds the keys one touch script writes, so a large backlog
// does not block Redis for one long script.
const touchBatchSize = 4096

// CachedStore keeps a local copy of the entries this replica has seen in front
// of a RedisStore. Every entry this replica recorded is a hit it can answer
// without Redis: an entry is only ever extended, so one known to be live stays
// live until its local expiry. Only the blocks after the local prefix, which
// another replica may have recorded, go to Redis synchronously. Refreshing
// the TTL of the locally matched prefix goes to Redis in batches every flush
// interval, coalesced across requests.
//
// Other replicas see a refresh up to one flush interval late, and a refresh
// queued when this replica stops is lost. Either can only cause another
// replica to under-report a hit near the end of a TTL.
type CachedStore struct {
	local  *MemoryStore
	remote *RedisStore
	// localTTL caps how long an entry stays in the local copy, which bounds
	// its memory. A shorter local life only sends more lookups to Redis.
	localTTL time.Duration

	mu sync.Mutex
	// pending maps tenant to hash to the expiry Redis must be extended to.
	pending map[string]map[uint64]time.Time

	localBlocks  prometheus.Counter
	remoteBlocks prometheus.Counter
}

// NewCachedStore returns a CachedStore over remote whose metrics are
// registered on reg.
func NewCachedStore(remote *RedisStore, localTTL time.Duration, reg prometheus.Registerer) *CachedStore {
	s := &CachedStore{
		local:    NewMemoryStore(),
		remote:   remote,
		localTTL: localTTL,
		pending:  make(map[string]map[uint64]time.Time),
		localBlocks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llm_d_cache_ledger_local_blocks_total",
			Help: "Block hashes matched from the local cache without a Redis lookup.",
		}),
		remoteBlocks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llm_d_cache_ledger_remote_blocks_total",
			Help: "Block hashes looked up in Redis.",
		}),
	}
	reg.MustRegister(s.localBlocks, s.remoteBlocks)
	return s
}

// MatchAndTouch implements Store.
func (s *CachedStore) MatchAndTouch(ctx context.Context, tenant string, hashes []uint64, ttl time.Duration) (int, error) {
	now := s.local.now()
	local := s.local.leading(tenant, hashes)
	s.localBlocks.Add(float64(local))

	matched := local
	if local < len(hashes) {
		tail := hashes[local:]
		s.remoteBlocks.Add(float64(len(tail)))
		remote, err := s.remote.MatchAndTouch(ctx, tenant, tail, ttl)
		if err != nil {
			return 0, err
		}
		matched += remote
	}

	expiry := now.Add(ttl)
	s.local.extend(tenant, hashes, now.Add(min(ttl, s.localTTL)))
	// The remote call already refreshed the tail; only the local prefix is
	// behind in Redis.
	if local > 0 {
		s.queueTouch(tenant, hashes[:local], expiry)
	}
	return matched, nil
}

func (s *CachedStore) queueTouch(tenant string, hashes []uint64, expiry time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, ok := s.pending[tenant]
	if !ok {
		entries = make(map[uint64]time.Time, len(hashes))
		s.pending[tenant] = entries
	}
	for _, h := range hashes {
		if exp, found := entries[h]; !found || exp.Before(expiry) {
			entries[h] = expiry
		}
	}
}

// Run flushes queued refreshes to Redis every flushInterval and drops expired
// local entries every sweepInterval, until ctx is done. Call Flush after the
// last MatchAndTouch to write what is still queued.
func (s *CachedStore) Run(ctx context.Context, logger logr.Logger, flushInterval, sweepInterval time.Duration) {
	go s.local.Run(ctx, sweepInterval)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Flush(ctx, logger)
		}
	}
}

// Flush writes every queued refresh to Redis.
func (s *CachedStore) Flush(ctx context.Context, logger logr.Logger) {
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[string]map[uint64]time.Time, len(pending))
	s.mu.Unlock()

	now := s.local.now()
	for tenant, entries := range pending {
		batch := make(map[uint64]time.Duration, min(len(entries), touchBatchSize))
		for h, exp := range entries {
			batch[h] = exp.Sub(now)
			if len(batch) == touchBatchSize {
				s.touch(ctx, logger, tenant, batch)
				batch = make(map[uint64]time.Duration, touchBatchSize)
			}
		}
		if len(batch) > 0 {
			s.touch(ctx, logger, tenant, batch)
		}
	}
}

func (s *CachedStore) touch(ctx context.Context, logger logr.Logger, tenant string, batch map[uint64]time.Duration) {
	if err := s.remote.Touch(ctx, tenant, batch); err != nil {
		logger.Error(err, "refreshing ledger entries in redis failed", "tenant", tenant, "entries", len(batch))
	}
}
