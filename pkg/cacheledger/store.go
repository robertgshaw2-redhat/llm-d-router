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

// Package cacheledger records the prompt prefixes each tenant sent within a
// retention window, so prompt-cache hits can be billed against the retention
// the provider promised rather than the hits an engine realized.
package cacheledger

import (
	"context"
	"time"
)

// Store holds per-tenant block hashes with an expiry.
type Store interface {
	// MatchAndTouch returns the number of leading hashes the tenant recorded
	// that have not expired, then records every hash with expiry now+ttl. The
	// count reflects the state before this call's writes. Concurrent calls for
	// one tenant must not interleave between the match and the writes.
	MatchAndTouch(ctx context.Context, tenant string, hashes []uint64, ttl time.Duration) (int, error)
}
