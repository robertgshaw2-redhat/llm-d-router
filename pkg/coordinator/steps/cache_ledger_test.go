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

package steps

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"google.golang.org/grpc"

	"github.com/llm-d/llm-d-router/pkg/cacheledger"
	"github.com/llm-d/llm-d-router/pkg/cacheledger/api/gen"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

type fakeLedgerClient struct {
	got     *gen.MatchAndTouchRequest
	matched uint32
	err     error
}

func (c *fakeLedgerClient) MatchAndTouch(_ context.Context, in *gen.MatchAndTouchRequest, _ ...grpc.CallOption) (*gen.MatchAndTouchResponse, error) {
	c.got = in
	if c.err != nil {
		return nil, c.err
	}
	return &gen.MatchAndTouchResponse{MatchedBlocks: c.matched}, nil
}

func ledgerRequestContext(tenant string, tokens int) *pipeline.RequestContext {
	headers := http.Header{}
	if tenant != "" {
		headers.Set(reqcommon.TenantHeaderKey, tenant)
	}
	ids := make([]int, tokens)
	for i := range ids {
		ids[i] = i + 1
	}
	return &pipeline.RequestContext{
		Model:           "m",
		OriginalHeaders: headers,
		Body:            map[string]any{reqcommon.FieldCacheSalt: "salt"},
		TokenIDs:        ids,
	}
}

func TestCacheLedgerStepReportsMatchedTokens(t *testing.T) {
	client := &fakeLedgerClient{matched: 2}
	step, err := newCacheLedgerStep(client, map[string]any{"block_size": 4, "ttl": "1h"})
	if err != nil {
		t.Fatalf("newCacheLedgerStep: %v", err)
	}
	reqCtx := ledgerRequestContext("team-a", 13)

	if err := step.Execute(context.Background(), reqCtx); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if reqCtx.LedgerCachedTokens == nil || *reqCtx.LedgerCachedTokens != 8 {
		t.Fatalf("LedgerCachedTokens = %v, want 8 (2 blocks of 4)", reqCtx.LedgerCachedTokens)
	}
	if client.got.GetTenant() != "team-a" {
		t.Errorf("tenant = %q, want team-a", client.got.GetTenant())
	}
	if client.got.GetTtlSeconds() != 3600 {
		t.Errorf("ttl = %d, want 3600", client.got.GetTtlSeconds())
	}
	want := cacheledger.BlockHashes("m", "salt", reqCtx.TokenIDs, 4, nil)
	if !slices.Equal(client.got.GetBlockHashes(), want) {
		t.Errorf("hashes = %v, want %v", client.got.GetBlockHashes(), want)
	}
}

func TestCacheLedgerStepLeavesTheEngineCountWhenItCannotAnswer(t *testing.T) {
	tests := []struct {
		name   string
		tenant string
		tokens int
		err    error
	}{
		{name: "no tenant", tokens: 32},
		{name: "no token IDs", tenant: "team-a"},
		{name: "ledger unavailable", tenant: "team-a", tokens: 32, err: errors.New("unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step, err := newCacheLedgerStep(&fakeLedgerClient{err: tt.err}, nil)
			if err != nil {
				t.Fatalf("newCacheLedgerStep: %v", err)
			}
			reqCtx := ledgerRequestContext(tt.tenant, tt.tokens)
			if err := step.Execute(context.Background(), reqCtx); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if reqCtx.LedgerCachedTokens != nil {
				t.Errorf("LedgerCachedTokens = %d, want nil", *reqCtx.LedgerCachedTokens)
			}
		})
	}
}

func TestCacheLedgerStepReportsZeroBelowOneBlock(t *testing.T) {
	client := &fakeLedgerClient{}
	step, err := newCacheLedgerStep(client, nil)
	if err != nil {
		t.Fatalf("newCacheLedgerStep: %v", err)
	}
	reqCtx := ledgerRequestContext("team-a", defaultCacheLedgerBlockSize-1)
	if err := step.Execute(context.Background(), reqCtx); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if reqCtx.LedgerCachedTokens == nil || *reqCtx.LedgerCachedTokens != 0 {
		t.Errorf("LedgerCachedTokens = %v, want 0", reqCtx.LedgerCachedTokens)
	}
	if client.got != nil {
		t.Error("ledger was called with no full block")
	}
}

func TestCacheLedgerStepRejectsInvalidParams(t *testing.T) {
	for name, params := range map[string]map[string]any{
		"zero block size":  {"block_size": 0},
		"sub-second ttl":   {"ttl": "500ms"},
		"zero timeout":     {"timeout": "0s"},
		"malformed ttl":    {"ttl": "1"},
		"non-numeric size": {"block_size": "16"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newCacheLedgerStep(&fakeLedgerClient{}, params); err == nil {
				t.Error("expected an error")
			}
		})
	}
	if _, err := NewCacheLedgerStep(nil, nil); err == nil {
		t.Error("expected an error without an address")
	}
}
