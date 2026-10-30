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
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// Media is one multimodal item in a tokenized prompt. Its placeholder tokens
// are identical for every item of a modality, so the item's content hash is
// mixed into each block its placeholder span overlaps.
type Media struct {
	Hash   string
	Offset int
	Length int
}

// BlockHashes returns one chained hash per full block of blockSize tokens.
// Block 0 chains from a seed over the model and cache salt, and block i hashes
// its own content together with block i-1's hash, so a block's hash covers the
// whole prefix ending at it. A trailing partial block is not hashed, because
// engines cache only full blocks.
//
// Every client of one ledger must hash identically, or their hits are counted
// against different keys:
//
//	seed     = xxh64(le64(len(model)) model le64(len(salt)) salt)
//	content  = xxh64(le32(token)... [le64(len(h)) h for each overlapping media hash h])
//	block[i] = xxh64(le64(content) le64(block[i-1] or seed))
func BlockHashes(model, cacheSalt string, tokens []int, blockSize int, media []Media) []uint64 {
	if blockSize <= 0 {
		return nil
	}
	count := len(tokens) / blockSize
	if count == 0 {
		return nil
	}

	var lenBuf [8]byte
	var d xxhash.Digest
	d.Reset()
	writeLengthPrefixed(&d, lenBuf[:], model)
	writeLengthPrefixed(&d, lenBuf[:], cacheSalt)
	prev := d.Sum64()

	hashes := make([]uint64, 0, count)
	tokenBuf := make([]byte, 4*blockSize)
	var chainBuf [16]byte
	for i := range count {
		start, end := i*blockSize, (i+1)*blockSize
		for j, tok := range tokens[start:end] {
			binary.LittleEndian.PutUint32(tokenBuf[4*j:], uint32(tok)) //nolint:gosec // token IDs fit in uint32
		}
		d.Reset()
		_, _ = d.Write(tokenBuf)
		for _, m := range media {
			if m.Offset < end && m.Offset+m.Length > start {
				writeLengthPrefixed(&d, lenBuf[:], m.Hash)
			}
		}
		binary.LittleEndian.PutUint64(chainBuf[:8], d.Sum64())
		binary.LittleEndian.PutUint64(chainBuf[8:], prev)
		prev = xxhash.Sum64(chainBuf[:])
		hashes = append(hashes, prev)
	}
	return hashes
}

func writeLengthPrefixed(d *xxhash.Digest, lenBuf []byte, s string) {
	binary.LittleEndian.PutUint64(lenBuf, uint64(len(s)))
	_, _ = d.Write(lenBuf)
	_, _ = d.WriteString(s)
}
