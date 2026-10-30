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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/llm-d/llm-d-router/pkg/cacheledger/api/gen"
)

// ServerConfig bounds what a client may ask of the ledger.
type ServerConfig struct {
	// DefaultTTL applies when a request names no TTL.
	DefaultTTL time.Duration
	// MaxTTL caps a requested TTL.
	MaxTTL time.Duration
	// MaxBlocks rejects requests carrying more hashes, bounding the work one
	// request can cause.
	MaxBlocks int
}

// Server implements the CacheLedger gRPC service over a Store.
type Server struct {
	gen.UnimplementedCacheLedgerServer
	store   Store
	cfg     ServerConfig
	metrics *serverMetrics
}

type serverMetrics struct {
	requests *prometheus.CounterVec
	blocks   prometheus.Counter
	matched  prometheus.Counter
	latency  prometheus.Histogram
}

const (
	resultOK    = "ok"
	resultError = "error"
)

// NewServer returns a Server over store whose metrics are registered on reg.
func NewServer(store Store, cfg ServerConfig, reg prometheus.Registerer) *Server {
	m := &serverMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_d_cache_ledger_requests_total",
			Help: "MatchAndTouch calls by result.",
		}, []string{"result"}),
		blocks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llm_d_cache_ledger_blocks_total",
			Help: "Block hashes received by MatchAndTouch.",
		}),
		matched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "llm_d_cache_ledger_matched_blocks_total",
			Help: "Block hashes MatchAndTouch reported as retained.",
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "llm_d_cache_ledger_store_duration_seconds",
			Help:    "Store latency of MatchAndTouch.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
		}),
	}
	reg.MustRegister(m.requests, m.blocks, m.matched, m.latency)
	return &Server{store: store, cfg: cfg, metrics: m}
}

// MatchAndTouch implements gen.CacheLedgerServer.
func (s *Server) MatchAndTouch(ctx context.Context, req *gen.MatchAndTouchRequest) (*gen.MatchAndTouchResponse, error) {
	if req.GetTenant() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}
	hashes := req.GetBlockHashes()
	if len(hashes) > s.cfg.MaxBlocks {
		return nil, status.Errorf(codes.InvalidArgument, "%d block hashes exceed the limit of %d", len(hashes), s.cfg.MaxBlocks)
	}

	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl == 0 {
		ttl = s.cfg.DefaultTTL
	}
	ttl = min(ttl, s.cfg.MaxTTL)

	start := time.Now()
	matched, err := s.store.MatchAndTouch(ctx, req.GetTenant(), hashes, ttl)
	s.metrics.latency.Observe(time.Since(start).Seconds())
	if err != nil {
		s.metrics.requests.WithLabelValues(resultError).Inc()
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	s.metrics.requests.WithLabelValues(resultOK).Inc()
	s.metrics.blocks.Add(float64(len(hashes)))
	s.metrics.matched.Add(float64(matched))
	return &gen.MatchAndTouchResponse{MatchedBlocks: uint32(matched)}, nil //nolint:gosec // matched <= len(hashes) <= MaxBlocks
}
