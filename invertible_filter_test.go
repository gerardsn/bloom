package bloom

import (
	"testing"
)

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

func TestIbf_Add(t *testing.T) {

}

func TestIbf_Delete(t *testing.T) {

}

func TestIbf_Subtract(t *testing.T) {

}

func TestIbf_Decode(t *testing.T) {

}


