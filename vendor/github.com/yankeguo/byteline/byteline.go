package byteline

type Operation interface {
	// Execute process the bytes
	Execute(in []byte) (out []byte, ok bool)
}

// Run run operations through a slice of bytes, returns result, ok and number of operations executed
func Run(bytes []byte, ops ...Operation) ([]byte, int, bool) {
	var ok bool
	var i int
	for _, op := range ops {
		if bytes, ok = op.Execute(bytes); !ok {
			break
		}
		i++
	}
	return bytes, i, ok
}
