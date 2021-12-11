package bloom

/** byte slice operators.
inputs must satisfy:
- 	len(a) == len(b)
- 	len(a) > 0
*/

// and
func and(a, b []byte) []byte {
	r := make([]byte, len(a))
	for i := range a {
		r[i] = a[i] & b[i]
	}
	return r
}

// or
func or(a, b []byte) []byte {
	r := make([]byte, len(a))
	for i := range a {
		r[i] = a[i] | b[i]
	}
	return r
}

// xor
func xor(a, b []byte) []byte {
	r := make([]byte, len(a))
	for i := range a {
		r[i] = a[i] ^ b[i]
	}
	return r
}

// eq
func eq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
