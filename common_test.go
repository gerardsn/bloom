package bloom

import (
	"math"
	"math/rand"
	"testing"
)

func statistics(values []int, b *testing.B) (mean float64, std float64, min int, max int) {
	sum := 0
	min = math.MaxInt
	max = math.MinInt
	for _, v := range values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	mean = float64(sum) / float64(len(values))

	ssd := 0.
	for _, v := range values {
		ssd += math.Pow(float64(v)-mean, 2)
	}
	std = math.Sqrt(ssd / float64(len(values)-1))
	b.Logf("mean: %.2f, std: %.2f, min: %d, max: %d\n", mean, std, min, max)

	return
}

func generateData() []byte {
	bytes := make([]byte, keyLength) // Tx ids use 256-bit hashes
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return bytes
}
