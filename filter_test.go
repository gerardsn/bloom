package bloom

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func BenchmarkFilter_Add(b *testing.B) {
	filter := NewFilter512()
	numEntries := 1000
	var nCollisions, firstCollision []int

	// benchmark loop
	for n := 0; n < b.N; n++ {
		nCollisions, firstCollision = benchmarkAdd(filter, n, numEntries)
	}

	b.Logf("Filtersize: %d bits, hashes: %d\n", len(filter.fingerprint)*8, len(filter.seeds))
	b.Logf("Support: %d", b.N)
	b.Logf("First collision - ")
	statistics(firstCollision, b)
	b.Logf("Average collisions for %d entries - ", numEntries)
	statistics(nCollisions, b)
}

func benchmarkAdd(f Filter, nEval, maxEntries int) (nCollisions []int, firstCollision []int) {
	nCollisions = make([]int, nEval)
	firstCollision = make([]int, nEval)
	for i := 0; i < nEval; i++ {
		nCollisions[i] = countCollisions(f.clone(), maxEntries)
		firstCollision[i] = addUntilCollision(f.clone())
	}
	return
}

func countCollisions(filter Filter, numEntries int) int {
	nCollisions := 0
	for n := 0; n < numEntries; n++ {
		if !filter.Add(generateData()) {
			nCollisions++
		}
	}
	return nCollisions
}

func addUntilCollision(filter Filter) int {
	for totalElem := 0; ; totalElem++ {
		if !filter.Add(generateData()) {
			return totalElem
		}
	}
}

func TestFilter_Add(t *testing.T) {
	filter := Filter{
		fingerprint: make([]byte, 32),
		seeds:       []uint32{0, 1, 2},
	}
	filter = NewFilter512()
	d1 := generateData()
	d2 := generateData()
	assert.True(t, filter.Add(d1), "failed to add first data point")
	assert.True(t, filter.Add(d2), "failed to add second data point")
	assert.False(t, filter.Add(d2), "adding data point for the second time should fail")
}

func TestFilter_Check(t *testing.T) {
	filter := Filter{
		fingerprint: make([]byte, 32),
		seeds:       []uint32{0, 1, 2},
	}
	filter = NewFilter512()
	d1 := generateData()
	d2 := generateData()

	if !filter.Add(d1) {
		t.Errorf("Failed to add data to an empty filter")
	}

	assert.True(t, filter.Check(d1), "failed to identify only datapoint in filter")
	assert.False(t, filter.Check(d2), "identified datapoint not in filter")
}

func TestFilter_hashValues(t *testing.T) {
	// TODO
}

func TestNewFilter512(t *testing.T) {
	flt := NewFilter512()
	assert.Equal(t, 512, len(flt.fingerprint), "incorrect fingerprint length")
	assert.Equal(t, 5, len(flt.seeds), "incorrect number of hash seeds")
	assert.Equal(t, [32]byte{}, flt.checksum, "initial checksum not zero")
}
