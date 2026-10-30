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
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/cacheledger"
	"github.com/llm-d/llm-d-router/pkg/cacheledger/api/gen"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const CacheLedgerStepName = "cache-ledger"

const (
	defaultCacheLedgerBlockSize = 16
	defaultCacheLedgerTimeout   = 100 * time.Millisecond
)

func init() {
	pipeline.Register(CacheLedgerStepName, NewCacheLedgerStep)
}

// CacheLedgerStep asks the cache ledger how much of the prompt the tenant sent
// within the retention window and records the answer for the decode step to
// report as the prompt-cache hit count.
type CacheLedgerStep struct {
	client    gen.CacheLedgerClient
	blockSize int
	// ttlSeconds is zero when unset, which selects the ledger's default.
	ttlSeconds uint32
	timeout    time.Duration
}

func NewCacheLedgerStep(_ *gateway.Client, params map[string]any) (pipeline.Step, error) {
	address, err := paramString(params, "address")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", CacheLedgerStepName, err)
	}
	if address == "" {
		return nil, fmt.Errorf("%s: address is required", CacheLedgerStepName)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", CacheLedgerStepName, err)
	}
	return newCacheLedgerStep(gen.NewCacheLedgerClient(conn), params)
}

func newCacheLedgerStep(client gen.CacheLedgerClient, params map[string]any) (*CacheLedgerStep, error) {
	s := &CacheLedgerStep{client: client, blockSize: defaultCacheLedgerBlockSize, timeout: defaultCacheLedgerTimeout}

	if v, ok, err := paramInt(params, "block_size"); err != nil {
		return nil, fmt.Errorf("%s: %w", CacheLedgerStepName, err)
	} else if ok {
		if v <= 0 {
			return nil, fmt.Errorf("%s: block_size must be positive, got %d", CacheLedgerStepName, v)
		}
		s.blockSize = v
	}
	if v, ok, err := paramDuration(params, "ttl"); err != nil {
		return nil, fmt.Errorf("%s: %w", CacheLedgerStepName, err)
	} else if ok {
		if v < time.Second || v.Seconds() > math.MaxUint32 {
			return nil, fmt.Errorf("%s: ttl must be between 1s and %ds, got %s", CacheLedgerStepName, uint32(math.MaxUint32), v)
		}
		s.ttlSeconds = uint32(v.Seconds())
	}
	if v, ok, err := paramDuration(params, "timeout"); err != nil {
		return nil, fmt.Errorf("%s: %w", CacheLedgerStepName, err)
	} else if ok {
		if v <= 0 {
			return nil, fmt.Errorf("%s: timeout must be positive, got %s", CacheLedgerStepName, v)
		}
		s.timeout = v
	}
	return s, nil
}

func (s *CacheLedgerStep) Name() string { return CacheLedgerStepName }

// Execute fails only when the client has gone. Without a tenant, tokens, or a
// ledger answer it leaves LedgerCachedTokens nil, and the client sees the hit
// count the engine realized.
func (s *CacheLedgerStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(CacheLedgerStepName)

	tenant := reqCtx.OriginalHeaders.Get(reqcommon.TenantHeaderKey)
	if tenant == "" {
		logger.V(logutil.DEBUG).Info("no tenant header, skipping")
		return nil
	}
	if len(reqCtx.TokenIDs) == 0 {
		logger.V(logutil.DEBUG).Info("no token IDs, skipping")
		return nil
	}

	salt, _ := reqCtx.Body[reqcommon.FieldCacheSalt].(string)
	media := make([]cacheledger.Media, 0, len(reqCtx.MultimodalEntries))
	for _, e := range reqCtx.MultimodalEntries {
		media = append(media, cacheledger.Media{Hash: e.Hash, Offset: e.Placeholder.Offset, Length: e.Placeholder.Length})
	}
	hashes := cacheledger.BlockHashes(reqCtx.Model, salt, reqCtx.TokenIDs, s.blockSize, media)
	if len(hashes) == 0 {
		cached := 0
		reqCtx.LedgerCachedTokens = &cached
		return nil
	}

	callCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	resp, err := s.client.MatchAndTouch(callCtx, &gen.MatchAndTouchRequest{
		Tenant:      tenant,
		BlockHashes: hashes,
		TtlSeconds:  s.ttlSeconds,
	})
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return fmt.Errorf("%s: %w", CacheLedgerStepName, ctx.Err())
		}
		logger.Error(err, "cache ledger call failed, reporting the engine's cache hit count")
		return nil
	}

	cached := int(resp.GetMatchedBlocks()) * s.blockSize
	reqCtx.LedgerCachedTokens = &cached
	logger.V(logutil.DEBUG).Info("ledger answered", "blocks", len(hashes), "matchedBlocks", resp.GetMatchedBlocks(), "cachedTokens", cached)
	return nil
}
