/*
Copyright 2025 The Kubernetes Authors.

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

package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

// endpointWithRole builds an endpoint carrying the given disaggregation role. An empty role yields a
// non-nil but roleless Labels map; nilLabels yields no Labels map at all.
func endpointWithRole(role string, nilLabels bool) fwkdl.Endpoint {
	meta := &fwkdl.EndpointMetadata{}
	if !nilLabels {
		meta.Labels = map[string]string{}
		if role != "" {
			meta.Labels[bylabel.RoleLabel] = role
		}
	}
	return fwkdl.NewEndpoint(meta, nil)
}

func TestPartitionEndpoints(t *testing.T) {
	testCases := []struct {
		name      string
		role      string
		nilLabels bool
		bucket    string // "prefill", "decode", or "interleaved"
	}{
		{name: "prefill", role: bylabel.RolePrefill, bucket: "prefill"},
		{name: "encode-prefill", role: bylabel.RoleEncodePrefill, bucket: "prefill"},
		{name: "decode", role: bylabel.RoleDecode, bucket: "decode"},
		{name: "prefill-decode is interleaved", role: bylabel.RolePrefillDecode, bucket: "interleaved"},
		{name: "both is interleaved", role: bylabel.RoleBoth, bucket: "interleaved"},
		{name: "encode-prefill-decode is interleaved", role: bylabel.RoleEncodePrefillDecode, bucket: "interleaved"},
		{name: "unknown role is interleaved", role: "sidecar", bucket: "interleaved"},
		{name: "labeled but roleless defaults to decode", role: "", bucket: "decode"},
		{name: "nil labels is interleaved", nilLabels: true, bucket: "interleaved"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ep := endpointWithRole(tc.role, tc.nilLabels)
			prefill, decode, interleaved := partitionEndpoints([]fwkdl.Endpoint{ep})

			counts := map[string]int{"prefill": len(prefill), "decode": len(decode), "interleaved": len(interleaved)}
			assert.Equal(t, 1, counts[tc.bucket], "endpoint should land in the %s bucket", tc.bucket)
			// The other two buckets must be empty.
			for bucket, n := range counts {
				if bucket != tc.bucket {
					assert.Zero(t, n, "%s bucket should be empty", bucket)
				}
			}
		})
	}
}

func TestPartitionEndpointsNilMetadata(t *testing.T) {
	// An endpoint with nil metadata must not panic and lands in the interleaved bucket.
	ep := fwkdl.NewEndpoint(nil, nil)
	ep.UpdateMetadata(nil)
	prefill, decode, interleaved := partitionEndpoints([]fwkdl.Endpoint{ep})
	assert.Empty(t, prefill)
	assert.Empty(t, decode)
	assert.Len(t, interleaved, 1)
}

// roleKeyedSaturation returns a detector func that scores each partition by the role of its first
// endpoint, so a test can assert which tier drove the gate.
func roleKeyedSaturation(byRole map[string]float64, emptyScore float64) func(context.Context, []fwkdl.Endpoint) float64 {
	return func(_ context.Context, eps []fwkdl.Endpoint) float64 {
		if len(eps) == 0 {
			return emptyScore
		}
		return byRole[eps[0].GetMetadata().Labels[bylabel.RoleLabel]]
	}
}

func TestPoolSaturationMaxGate(t *testing.T) {
	h := newTestHarness(t, testTTL)

	t.Run("gates on max across active tiers, excluding empty partitions", func(t *testing.T) {
		// Prefill is hotter than decode; the interleaved tier is empty. If the empty partition were
		// scored (1.0, fail-closed) it would dominate; the gate must ignore it and return 0.9.
		h.saturationDetector.SaturationFunc = roleKeyedSaturation(
			map[string]float64{bylabel.RolePrefill: 0.9, bylabel.RoleDecode: 0.3}, 1.0)
		pool := []fwkdl.Endpoint{
			endpointWithRole(bylabel.RolePrefill, false),
			endpointWithRole(bylabel.RoleDecode, false),
		}
		assert.Equal(t, 0.9, h.processor.poolSaturation(context.Background(), pool))
	})

	t.Run("monolithic pool gates on its single decode tier", func(t *testing.T) {
		// Unlabeled endpoints default to decode; prefill and interleaved are empty and excluded.
		h.saturationDetector.SaturationFunc = roleKeyedSaturation(
			map[string]float64{"": 0.7}, 1.0)
		pool := []fwkdl.Endpoint{endpointWithRole("", false), endpointWithRole("", false)}
		assert.Equal(t, 0.7, h.processor.poolSaturation(context.Background(), pool))
	})

	t.Run("empty pool falls back to the detector's empty-pool signal", func(t *testing.T) {
		h.saturationDetector.SaturationFunc = roleKeyedSaturation(nil, 1.0)
		assert.Equal(t, 1.0, h.processor.poolSaturation(context.Background(), nil))
	})
}
