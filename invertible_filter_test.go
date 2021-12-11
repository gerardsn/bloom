package bloom

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// Benchmarks
func BenchmarkIbf_a(b *testing.B) {
	var nDecodes = make([]int, b.N)

	for n := 0; n < b.N; n++ {
		nDecodes[n] = runTest()
	}
	b.Logf("iterations: %d\n", b.N)
	statistics(nDecodes, b)
}

// doubling the amount of buckets, more than doubles the set difference that can be solved
func runTest() int {
	numBuckets := 1024
	ibfA := NewIbf(numBuckets)
	ibfB := NewIbf(numBuckets)

	N := 768 // common size and set difference, each set has N/2 keys the other doesn't have
	for i := 0; i < N; i++ {
		a := generateData()
		ibfA.Add(a)
		if i%2 == 0 {
			ibfB.Add(generateData())
		} else {
			ibfB.Add(a)
		}
	}
	ibfA.Subtract(ibfB)
	if _, _, err := ibfA.Decode(); err != nil {
		return 0
	}

	return 1
}

// Test IBLT
func TestIbf_Add(t *testing.T) {

}

func TestIbf_Delete(t *testing.T) {

}

func TestIbf_Subtract(t *testing.T) {

}

func TestIbf_Decode(t *testing.T) {

}

func TestIbf_hashKey(t *testing.T) {

}

func TestIbf_bucketIndices(t *testing.T) {

}

func TestIbf_validateSubtrahend(t *testing.T) {

}

func TestIbf_JsonMarshalling(t *testing.T) {

}

// Test bucket
func TestBucket(t *testing.T) {
	keyLength := 2
	key1, key2 := []byte("Az"), []byte("zA")
	hash1, hash2 := uint64(123), uint64(222)
	keyXor, hashXor := []byte{key1[0] ^ key2[0], key1[1] ^ key2[1]}, hash1^hash2

	t.Run("newBucket isEmpty", func(t *testing.T) {
		b := newBucket(keyLength)
		assert.True(t, b.equals(testBucket(0, make([]byte, keyLength), 0)), "expected an empty bucket but got: %v", b)
		assert.True(t, b.isEmpty(), "bucket is empty")
	})

	t.Run("update() applies XOR operation on keySum and hashSum", func(t *testing.T) {
		b := testBucket(0, keyXor, hashXor)
		exp := testBucket(0, key1, hash1)

		b.update(key2, hash2)

		assert.True(t, b.equals(exp))
	})

	t.Run("add", func(t *testing.T) {
		exp := testBucket(1, key1, hash1)
		b := testBucket(0, make([]byte, keyLength), 0)

		b.add(key1, hash1)

		assert.True(t, b.equals(exp), "expected: %v\ngot: %v", exp, b)
	})

	t.Run("delete", func(t *testing.T) {
		exp := testBucket(-1, key1, hash1)
		b := testBucket(0, make([]byte, keyLength), 0)

		b.delete(key1, hash1)

		assert.True(t, b.equals(exp), "expected: %v\ngot: %v", exp, b)
	})

	t.Run("subtract", func(t *testing.T) {
		exp := testBucket(0, keyXor, hashXor)
		b1 := testBucket(1, key1, hash1)
		b2 := testBucket(1, key2, hash2)

		b1.subtract(b2)

		assert.True(t, b1.equals(exp), "expected: %v\ngot: %v", exp, b1)
	})
}

func (b *bucket) equals(o *bucket) bool {
	return b.count == o.count && b.hashSum == o.hashSum && eq(b.keySum, o.keySum)
}

func testBucket(count int, keySum []byte, hashSum uint64) *bucket {
	return &bucket{
		count:   count,
		keySum:  keySum,
		hashSum: hashSum,
	}
}

// Test xorshift
func TestXorshift(t *testing.T) {
	next0 := xorshift64(0)
	next1 := xorshift64(1)
	next2 := xorshift64(next1)
	assert.Less(t, uint64(0), next0, "should be larger than 0")
	assert.Less(t, uint64(1), next1, "should be larger than 1")
	assert.NotEqualf(t, next1, next2, "next should produce new values")
}
