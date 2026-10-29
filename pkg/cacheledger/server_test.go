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
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/llm-d/llm-d-router/pkg/cacheledger/api/gen"
)

type recordingStore struct {
	gotTTL  time.Duration
	matched int
	err     error
}

func (s *recordingStore) MatchAndTouch(_ context.Context, _ string, _ []uint64, ttl time.Duration) (int, error) {
	s.gotTTL = ttl
	return s.matched, s.err
}

var testServerConfig = ServerConfig{DefaultTTL: 5 * time.Minute, MaxTTL: time.Hour, MaxBlocks: 4}

func TestServerResolvesTTL(t *testing.T) {
	tests := []struct {
		name       string
		ttlSeconds uint32
		want       time.Duration
	}{
		{name: "unset uses the default", ttlSeconds: 0, want: 5 * time.Minute},
		{name: "within bounds is kept", ttlSeconds: 600, want: 10 * time.Minute},
		{name: "above the maximum is clamped", ttlSeconds: 7200, want: time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingStore{}
			srv := NewServer(store, testServerConfig, prometheus.NewRegistry())
			if _, err := srv.MatchAndTouch(context.Background(), &gen.MatchAndTouchRequest{
				Tenant: "a", BlockHashes: []uint64{1}, TtlSeconds: tt.ttlSeconds,
			}); err != nil {
				t.Fatalf("MatchAndTouch: %v", err)
			}
			if store.gotTTL != tt.want {
				t.Errorf("store TTL = %s, want %s", store.gotTTL, tt.want)
			}
		})
	}
}

func TestServerRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		req  *gen.MatchAndTouchRequest
	}{
		{name: "no tenant", req: &gen.MatchAndTouchRequest{BlockHashes: []uint64{1}}},
		{name: "too many blocks", req: &gen.MatchAndTouchRequest{Tenant: "a", BlockHashes: []uint64{1, 2, 3, 4, 5}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer(&recordingStore{}, testServerConfig, prometheus.NewRegistry())
			_, err := srv.MatchAndTouch(context.Background(), tt.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestServerReportsStoreResults(t *testing.T) {
	store := &recordingStore{matched: 2}
	srv := NewServer(store, testServerConfig, prometheus.NewRegistry())
	resp, err := srv.MatchAndTouch(context.Background(), &gen.MatchAndTouchRequest{Tenant: "a", BlockHashes: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatalf("MatchAndTouch: %v", err)
	}
	if resp.GetMatchedBlocks() != 2 {
		t.Errorf("matched blocks = %d, want 2", resp.GetMatchedBlocks())
	}
	if got := testutil.ToFloat64(srv.metrics.blocks); got != 3 {
		t.Errorf("blocks metric = %v, want 3", got)
	}
	if got := testutil.ToFloat64(srv.metrics.matched); got != 2 {
		t.Errorf("matched metric = %v, want 2", got)
	}

	store.err = errors.New("redis down")
	if _, err := srv.MatchAndTouch(context.Background(), &gen.MatchAndTouchRequest{Tenant: "a", BlockHashes: []uint64{1}}); status.Code(err) != codes.Unavailable {
		t.Errorf("store failure returned %v, want Unavailable", err)
	}
	if got := testutil.ToFloat64(srv.metrics.requests.WithLabelValues(resultError)); got != 1 {
		t.Errorf("error requests metric = %v, want 1", got)
	}
}
