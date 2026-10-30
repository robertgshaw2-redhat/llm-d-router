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
	"slices"
	"testing"
)

func tokenRange(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i + 100
	}
	return out
}

func TestBlockHashesFullBlocksOnly(t *testing.T) {
	if got := BlockHashes("m", "", tokenRange(3), 4, nil); len(got) != 0 {
		t.Errorf("prompt shorter than a block produced %d hashes", len(got))
	}
	if got := BlockHashes("m", "", tokenRange(11), 4, nil); len(got) != 2 {
		t.Errorf("11 tokens in blocks of 4 produced %d hashes, want 2", len(got))
	}
	if got := BlockHashes("m", "", tokenRange(8), 0, nil); got != nil {
		t.Errorf("block size 0 produced %v", got)
	}
}

func TestBlockHashesSharedPrefixSharesHashes(t *testing.T) {
	short := BlockHashes("m", "s", tokenRange(8), 4, nil)
	long := BlockHashes("m", "s", tokenRange(16), 4, nil)
	if !slices.Equal(short, long[:2]) {
		t.Errorf("prefix hashes %v differ from the longer prompt's %v", short, long[:2])
	}

	changed := tokenRange(16)
	changed[5] = 1
	diverged := BlockHashes("m", "s", changed, 4, nil)
	if diverged[0] != long[0] {
		t.Error("a change in block 1 altered block 0's hash")
	}
	for i := 1; i < 4; i++ {
		if diverged[i] == long[i] {
			t.Errorf("block %d hash survived a change in block 1; hashes must chain", i)
		}
	}
}

func TestBlockHashesSeparateModelsAndSalts(t *testing.T) {
	base := BlockHashes("m", "s", tokenRange(4), 4, nil)[0]
	for name, got := range map[string]uint64{
		"model": BlockHashes("other", "s", tokenRange(4), 4, nil)[0],
		"salt":  BlockHashes("m", "other", tokenRange(4), 4, nil)[0],
		// Length prefixes keep ("ms", "") and ("m", "s") apart.
		"boundary": BlockHashes("ms", "", tokenRange(4), 4, nil)[0],
	} {
		if got == base {
			t.Errorf("changing the %s left the hash unchanged", name)
		}
	}
}

func TestBlockHashesMixMediaIntoOverlappingBlocks(t *testing.T) {
	tokens := tokenRange(12)
	imageA := []Media{{Hash: "a", Offset: 5, Length: 2}}
	imageB := []Media{{Hash: "b", Offset: 5, Length: 2}}

	withA := BlockHashes("m", "", tokens, 4, imageA)
	withB := BlockHashes("m", "", tokens, 4, imageB)
	if withA[0] != withB[0] {
		t.Error("media placed in block 1 changed block 0's hash")
	}
	if withA[1] == withB[1] {
		t.Error("different media with identical placeholder tokens produced the same block hash")
	}
}
