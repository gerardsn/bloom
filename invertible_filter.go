package bloom

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
)

type ibf struct {
	Buckets    []*bucket `json:"Buckets"`
	NumBuckets int       `json:"num_buckets"`
	K          int       `json:"K"`
	KeySeed    uint32    `json:"key_seed"`
	KeyLength  int       `json:"key_length"`
}

func (i *ibf) String() string {
	out := fmt.Sprintf("IBF\n"+
		"number of buckets: %d\n"+
		"indexing seeds: %v\n"+
		"key seed: %d\n"+
		"key length (B): %d\n"+
		"\tbucket count keySum           hashSum\n",
		i.NumBuckets, i.K, i.KeySeed, i.KeyLength)
	for idx, b := range i.Buckets {
		out += fmt.Sprintf("\t%6d %5d %x %10d\n", idx, b.count, b.keySum, b.hashSum)
	}
	return out
}

func NewIbf(numBuckets int) *ibf {
	buckets := make([]*bucket, numBuckets)
	for i := 0; i < numBuckets; i++ {
		buckets[i] = newBucket(KeyLength)
	}
	return &ibf{
		Buckets:    buckets,
		K:          4,
		KeySeed:    uint32(33),
		KeyLength:  KeyLength,
		NumBuckets: numBuckets,
	}
}

func (i *ibf) clone() *ibf {
	data, _ := MarshalJson(i)
	newIbf, _ := UnmarshalJson(data)
	return newIbf
}

func MarshalJson(ibf *ibf) ([]byte, error) {
	data, err := json.Marshal(ibf)
	return data, err
}

func UnmarshalJson(data []byte) (*ibf, error) {
	newIbf := &ibf{}
	err := json.Unmarshal(data, newIbf)
	return newIbf, err
}

func (i *ibf) Add(key []byte) {
	hash := i.hashKey(key)
	for _, h := range i.bucketIndices(hash) {
		i.Buckets[h].add(key, hash)
	}
}

func (i *ibf) Delete(key []byte) {
	hash := i.hashKey(key)
	for _, h := range i.bucketIndices(hash) {
		i.Buckets[h].delete(key, hash)
	}
}

func (i *ibf) Subtract(other *ibf) error {
	if err := i.validateSubtrahend(other); err != nil {
		return fmt.Errorf("subtraction failed: %w", err)
	}
	for idx, b := range i.Buckets {
		b.subtract(other.Buckets[idx])
	}
	return nil
}

func (i *ibf) validateSubtrahend(o *ibf) error {
	if i.NumBuckets != o.NumBuckets {
		return fmt.Errorf("unequal number of Buckets, expected (%d) got (%d)", i.NumBuckets, o.NumBuckets)
	}
	if i.KeySeed != o.KeySeed {
		return fmt.Errorf("keySeeds do not match, expected (%d) got (%d)", i.KeySeed, o.KeySeed)
	}
	if i.KeyLength != o.KeyLength {
		return fmt.Errorf("keyLengths do not match, expected (%d) got (%d)", i.KeySeed, o.KeySeed)
	}
	if i.K != o.K {
		return fmt.Errorf("unequal number of K, expected (%d) got (%d)", i.K, o.K)
	}
	return nil
}

func (i *ibf) Decode() (remaining [][]byte, missing [][]byte, err error) {
	for {
		updated := false

		// for each pure (count == +1 or -1), if hashSum = h(key) -> Add(count == -1)/Delete(count == 1) key
		for _, b := range i.Buckets {
			if (b.count == 1 || b.count == -1) && i.hashKey(b.keySum) == b.hashSum {
				if b.count == 1 {
					remaining = append(remaining, b.keySum)
					i.Delete(b.keySum)
				} else { // b.count == -1
					missing = append(missing, b.keySum)
					i.Add(b.keySum)
				}
				updated = true
			}
		}

		// if no pures exist, the ibf is empty or cannot be decoded
		if !updated {
			for _, b := range i.Buckets {
				if !b.isEmpty() {
					return remaining, missing, errors.New("decode failed")
				}
			}
			return remaining, missing, nil
		}
	}
}

func (i *ibf) bucketIndices(hash uint64) []uint64 {
	rng := xorshift64{state: hash}
	bucketUsed := make(map[uint64]bool, i.K)
	var indices []uint64
	for len(indices) < i.K {
		next := rng.next()
		minBucket := next % uint64(MinBuckets)
		if bucketUsed[minBucket] {
			continue
		}
		indices = append(indices, next % uint64(i.NumBuckets))
		bucketUsed[minBucket] = true
	}
	return indices
}

func (i *ibf) hashKey(key []byte) uint64 {
	return murmur3.Sum64WithSeed(key, i.KeySeed)
}

// bucket
type bucket struct {
	// count is signed to allow for negative counts after subtraction
	count   int
	keySum  []byte
	hashSum uint64
}

func newBucket(keyLength int) *bucket {
	return &bucket{
		count:   0,
		keySum:  make([]byte, keyLength),
		hashSum: 0,
	}
}

func (b *bucket) add(key []byte, hash uint64) {
	b.count++
	b.update(key, hash)
}

func (b *bucket) delete(key []byte, hash uint64) {
	b.count--
	b.update(key, hash)
}

func (b *bucket) subtract(o *bucket) {
	b.count -= o.count
	b.update(o.keySum, o.hashSum)
}

func (b *bucket) update(key []byte, hash uint64) {
	b.keySum = xor(b.keySum, key)
	b.hashSum ^= hash
}

func (b *bucket) isEmpty() bool {
	return b.count == 0 && b.hashSum == 0 && eq(b.keySum, make([]byte, KeyLength))
}

func (b *bucket) String() string {
	return fmt.Sprintf("[count: %3d, keySum: %x, hashSum: %d]", b.count, b.keySum, b.hashSum)
}

// xorshift64 is am RNG form the xorshift family with period 2^64-1.
type xorshift64 struct {
	state uint64
}

// next number in the RNG sequence.
func (x *xorshift64) next() uint64 {
	x.state ^= x.state << 13
	x.state ^= x.state >> 7
	x.state ^= x.state << 17
	return x.state
}
