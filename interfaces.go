package bloom

const (
	KeyLength  = 32
	MinBuckets = 128
)

type Bloom interface {
	Add(data []byte) bool
	clone() Bloom
}
