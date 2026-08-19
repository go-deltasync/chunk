package chunk

import "testing"

// These came with the hashes from github.com/go-deltasync/bita, where they were
// written against the Rust reference. They are what says the cuts made here are
// the cuts bita makes — TestReferenceBuzHashLastValidVector in particular pins a concrete
// value, so a change to the table, the seed or the rotation fails rather than
// quietly making archives the other cannot read.
//
// equalSums verifies that two byte ranges sharing a suffix converge to the same
// rolling-hash sum once the window has rolled past the differing prefix.
func TestReferenceBuzHashEqualSumsForEqualRange(t *testing.T) {
	h := NewBuzHash(8)
	data1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}
	data2 := []byte{1, 99, 99, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}
	var s1, s2 []uint32
	for _, v := range data1 {
		h.Roll(v)
		s1 = append(s1, h.Sum())
	}
	for _, v := range data2 {
		h.Roll(v)
		s2 = append(s2, h.Sum())
	}
	for i := 11; i < len(s1); i++ {
		if s1[i] != s2[i] {
			t.Fatalf("sum %d differs: %d vs %d", i, s1[i], s2[i])
		}
	}
}

func TestReferenceBuzHashLastValidVector(t *testing.T) {
	h := NewBuzHash(5)
	var sums []uint32
	for _, v := range []byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 1, 2, 3, 4, 5} {
		if !h.Primed() {
			h.Prime(v)
		} else {
			h.Roll(v)
		}
		if h.Primed() {
			sums = append(sums, h.Sum())
		}
	}
	if sums[9] != 1406929643 {
		t.Fatalf("last valid sum = %d", sums[9])
	}
}

func TestReferenceBuzHashRepeatedInput(t *testing.T) {
	// Feeding the same byte more than `window` times must not change the sum
	// (exercises the repeated-input short-circuit).
	h := NewBuzHash(4)
	for i := 0; i < 4; i++ {
		h.Roll(0x77)
	}
	stable := h.Sum()
	for i := 0; i < 10; i++ {
		h.Roll(0x77)
		if h.Sum() != stable {
			t.Fatalf("sum changed on repeated input")
		}
	}
}

func TestReferenceRollSumEqualSumsForEqualRange(t *testing.T) {
	h := NewRollSum(8)
	data1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	data2 := []byte{9, 9, 9, 9, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var s1, s2 []uint32
	for _, v := range data1 {
		h.Roll(v)
		s1 = append(s1, h.Sum())
	}
	for _, v := range data2 {
		h.Roll(v)
		s2 = append(s2, h.Sum())
	}
	// Window is 8 and the prefixes differ through index 3, so sums converge
	// only once the window has fully rolled past it (index 3 + 8 = 11).
	for i := 11; i < len(s1); i++ {
		if s1[i] != s2[i] {
			t.Fatalf("rollsum %d differs: %d vs %d", i, s1[i], s2[i])
		}
	}
}

func TestReferenceRollSumInitIsNoOp(t *testing.T) {
	// RollSum has no priming phase; init must be a no-op and initDone true.
	h := NewRollSum(4)
	if !h.Primed() {
		t.Fatal("rollsum initDone should be true")
	}
	before := h.Sum()
	h.Prime(123)
	if h.Sum() != before {
		t.Fatal("rollsum init changed the sum")
	}
}
