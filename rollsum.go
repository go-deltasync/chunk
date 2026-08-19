package chunk

// A RollSum is the rsync/bup rolling hash bita uses. Unlike [BuzHash] it needs
// no priming: its window starts as zeroes and rolls straight away. See
// bitar/src/rolling_hash/rollsum.rs. All arithmetic is u32 wrapping, which Go
// provides natively for uint32.
type RollSum struct {
	s1, s2 uint32
	window []byte
	offset int
}

const charOffset uint32 = 31

// NewRollSum returns one over a window of the given size.
func NewRollSum(windowSize int) *RollSum {
	w := uint32(windowSize)
	return &RollSum{
		s1:     w * charOffset,
		s2:     w * (w - 1) * charOffset,
		window: make([]byte, windowSize),
	}
}

// Prime is what [BuzHash] uses to fill its window. A RollSum has nothing to
// fill, so this does nothing and [RollSum.Primed] is true from the start.
func (r *RollSum) Prime(b byte) { _ = b }

// Primed is always true.
func (r *RollSum) Primed() bool { return true }

// Roll feeds a byte, dropping the one that leaves the window.
func (r *RollSum) Roll(b byte) {
	drop := r.window[r.offset]
	r.s1 += uint32(b) - uint32(drop)
	r.s2 += r.s1 - uint32(len(r.window))*(uint32(drop)+charOffset)
	r.window[r.offset] = b
	r.offset++
	if r.offset >= len(r.window) {
		r.offset = 0
	}
}

// Sum is the hash of the window as it stands.
func (r *RollSum) Sum() uint32 {
	return (r.s1 << 16) | (r.s2 & 0xffff)
}
