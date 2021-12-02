package bloom

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"sort"
)

//type IBF interface {
//	Add(keySum []byte) bool
//	Bloom
//
//	// Delete remove keySum from IBF. Does not verify if the IBF contains the keySum and can results in negative bucket counts.
//	Delete(keySum []byte)
//
//	// Subtract returns some other IBF subtracted from this IBF. Returns an error if the number of Buckets or the hashSum Seeds differ.
//	Subtract(other IBF)
//
//	// Decode peels of 'pure' entries into remaining (in this ibf) or missing (in subtracted ibf). This returns an error if
//	Decode(remaining, missing [][]byte) error
//}

type ibf struct {
	Buckets    []*bucket `json:"Buckets"`
	NumBuckets int       `json:"num_buckets"`
	Seeds      []uint32  `json:"Seeds"`
	KeySeed    uint32    `json:"key_seed"`
	KeyLength  int       `json:"key_length"`
}

func (i *ibf) String() string {
	out := fmt.Sprintf("IBF\n" +
		"number of buckets: %d\n" +
		"indexing seeds: %v\n" +
		"key seed: %d\n" +
		"key length (B): %d\n" +
		"\tbucket count keySum           hashSum\n",
		i.NumBuckets, i.Seeds, i.KeySeed, i.KeyLength)
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
		Seeds:      []uint32{0, 1, 2, 4},
		KeySeed:    uint32(33),
		KeyLength:  KeyLength,
		NumBuckets: numBuckets,
	}
}

func (i *ibf) clone() Bloom {
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

func (i *ibf) Add(key []byte) bool {
	hash := i.hashKey(key)
	idxs := i.hashIndices(key)
	for _, h := range idxs {
		i.Buckets[h].add(key, hash)
	}

	// validity can only be guaranteed if nothing has been subtracted or deleted from the ibf
	for _, h := range idxs {
		if i.Buckets[h].count < 2 {
			return true
		}
	}
	return false
}

func (i *ibf) Delete(key []byte) {
	hash := i.hashKey(key)
	for _, h := range i.hashIndices(key) {
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
	if len(i.Seeds) != len(o.Seeds) {
		return fmt.Errorf("kunequal number of Seeds, expected (%d) got (%d)", i.Seeds, o.Seeds)
	}
	sort.Slice(i.Seeds, func(x, y int) bool { return i.Seeds[x] < i.Seeds[y] })
	sort.Slice(o.Seeds, func(x, y int) bool { return o.Seeds[x] < o.Seeds[y] })
	for idx := range i.Seeds {
		if i.Seeds[idx] != o.Seeds[idx] {
			return fmt.Errorf("Seeds do not match, expected %v got %v", i.Seeds, o.Seeds)
		}
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

func (i *ibf) hashIndices(key []byte) []uint32 {
	hashes := make([]uint32, len(i.Seeds))
	for idx, seed := range i.Seeds {
		hashes[idx] = murmur3.Sum32WithSeed(key, seed) % uint32(i.NumBuckets)
	}
	return unique(hashes)
}

func unique(uintSlice []uint32) []uint32 {
	keys := make(map[uint32]struct{})
	var uniques []uint32
	for _, v := range uintSlice {
		if _, exists := keys[v]; !exists {
			keys[v] = struct{}{}
			uniques = append(uniques, v)
		}
	}
	return uniques
}

func (i *ibf) hashKey(key []byte) uint32 {
	return murmur3.Sum32WithSeed(key, i.KeySeed)
}

// bucket
type bucket struct {
	count   int // signed to allow for negative counts after subtraction
	keySum  []byte
	hashSum uint32
}

func newBucket(keyLength int) *bucket {
	return &bucket{
		count:   0,
		keySum:  make([]byte, keyLength),
		hashSum: 0,
	}
}

func (b *bucket) add(key []byte, hash uint32) {
	b.count++
	b.update(key, hash)
}

func (b *bucket) delete(key []byte, hash uint32) {
	b.count--
	b.update(key, hash)
}

func (b *bucket) subtract(o *bucket) {
	b.count -= o.count
	b.update(o.keySum, o.hashSum)
}

func (b *bucket) update(key []byte, hash uint32) {
	b.keySum = xor(b.keySum, key)
	b.hashSum ^= hash
}

func (b *bucket) isEmpty() bool {
	return b.count == 0 && b.hashSum == 0 && eq(b.keySum, make([]byte, KeyLength))
}

func (b *bucket) String() string {
	return fmt.Sprintf("[count: %3d, keySum: %x, hashSum: %10d]", b.count, b.keySum, b.hashSum)
}
