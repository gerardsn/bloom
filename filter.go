package bloom

import (
	"github.com/spaolacci/murmur3"
)

type Filter struct {
	// fingerprint is the main data structure for the bloom filter
	fingerprint []byte

	// checksum is used to detect collisions for the checker
	checksum [32]byte

	// seeds is a list of hash seeds that are used as input for the hash functions. The resulting bit places are set in the fingerprint.
	seeds []uint32
}

// NewFilter256 creates a bloom filter with 256 bytes.
func NewFilter256() Filter {
	return Filter{
		fingerprint: make([]byte, 256),
		seeds:       []uint32{0, 1, 2, 3, 4}, // should be randomized and communicated for each HEADS broadcast to prevent checker collisions
	}
}

// NewFilter512 creates a bloom filter with 512 bytes. This is enough for a data set of ~656 with a FPR of around 0.001 using 5 hash functions.
func NewFilter512() Filter {
	return Filter{
		fingerprint: make([]byte, 512),
		seeds:       []uint32{0, 1, 2, 3, 4}, // should be randomized and communicated for each HEADS broadcast to prevent checker collisions
	}
}

// NewFilter1024 creates a bloom filter with 1024 bytes.
func NewFilter1024() Filter {
	return Filter{
		fingerprint: make([]byte, 1024),
		seeds:       []uint32{0, 1, 2, 3, 4}, // should be randomized and communicated for each HEADS broadcast to prevent checker collisions
	}
}

func (f *Filter) clone() Filter {
	seeds := make([]uint32, len(f.seeds))
	copy(seeds, f.seeds)
	return Filter{
		fingerprint: make([]byte, len(f.fingerprint)),
		seeds:       seeds,
	}
}

// Add applies all hashing functions to a data point and adds it to the bloom filter. Returns false when the data point is a collision.
func (f *Filter) Add(data []byte) bool {
	dataFingerprint := f.hashValues(data)
	if eq(and(f.fingerprint, dataFingerprint), dataFingerprint) {
		return false
	}

	f.fingerprint = or(f.fingerprint, dataFingerprint)

	return true
}

// Check guarantees that a sample is not in the filter. Since bloom filters are probabilistic datastructures, a false result means that the data point does not exist.
func (f *Filter) Check(data []byte) bool {
	dataFingerprint := f.hashValues(data)
	return eq(and(f.fingerprint, dataFingerprint), dataFingerprint)
}

// hashValues creates a fingerprint of the given data where len(f.seeds) bits are set to 1
func (f *Filter) hashValues(data []byte) []byte {
	result := make([]byte, len(f.fingerprint))
	for _, v := range f.seeds {
		bitLoc := murmur3.Sum32WithSeed(data, v) % uint32(len(f.fingerprint)*8)
		result[bitLoc/8] |= uint8(1) << (bitLoc % 8)
	}
	return result
}
