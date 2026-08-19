package chunk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
)

// noise makes bytes that look nothing like each other, so a boundary found in
// one place says nothing about where the next one is.
func noise(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(rng.UintN(256))
	}
	return out
}

// small is a config with sizes a test can hold in its head, and boundaries often
// enough that a few kilobytes is several chunks.
func small() Config {
	return Config{Average: 256, Min: 64, Max: 4096, Window: 16}
}

func rejoin(pieces [][]byte) []byte {
	var out []byte
	for _, piece := range pieces {
		out = append(out, piece...)
	}
	return out
}

func TestThePiecesAreTheStream(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 1000, 100_000} {
		data := noise(1, n)
		pieces := Bytes(data, small())
		if got := rejoin(pieces); !bytes.Equal(got, data) {
			t.Fatalf("%d bytes came back as %d", n, len(got))
		}
		// And every piece is within the bounds asked for, but the last, which is
		// whatever the stream had left.
		for i, piece := range pieces {
			if len(piece) == 0 {
				t.Fatalf("%d bytes produced an empty piece at %d", n, i)
			}
			if len(piece) > small().Max {
				t.Fatalf("a piece of %d bytes is over the maximum", len(piece))
			}
			if i < len(pieces)-1 && len(piece) < small().Min {
				t.Fatalf("piece %d of %d is %d bytes, under the minimum", i, len(pieces), len(piece))
			}
		}
	}
}

// The reason the package exists. Inserting a byte near the front leaves the
// chunks after the disturbance exactly as they were — where cutting every n
// bytes leaves nothing the same.
func TestAnEditDisturbsOnlyWhatIsNearIt(t *testing.T) {
	data := noise(2, 200_000)
	before := Bytes(data, small())

	// One byte inserted a tenth of the way in.
	edited := make([]byte, 0, len(data)+1)
	edited = append(edited, data[:20_000]...)
	edited = append(edited, 0x42)
	edited = append(edited, data[20_000:]...)
	after := Bytes(edited, small())

	// The chunks are the same objects at the end, so count from the back.
	same := 0
	for same < len(before) && same < len(after) {
		if !bytes.Equal(before[len(before)-1-same], after[len(after)-1-same]) {
			break
		}
		same++
	}
	if same < len(before)*8/10 {
		t.Fatalf("only %d of %d chunks survived a one-byte insertion", same, len(before))
	}
	t.Logf("%d of %d chunks unchanged after inserting one byte at 10%%", same, len(before))

	// The same measurement against cutting every n bytes, which is what this is
	// better than. It is in the test rather than in a comment because a claim
	// about being better than something should fail when it stops being true.
	fixed := func(in []byte) [][]byte {
		var out [][]byte
		for at := 0; at < len(in); at += 256 {
			out = append(out, in[at:min(at+256, len(in))])
		}
		return out
	}
	fixedBefore, fixedAfter := fixed(data), fixed(edited)
	fixedSame := 0
	for fixedSame < len(fixedBefore) && fixedSame < len(fixedAfter) {
		if !bytes.Equal(fixedBefore[len(fixedBefore)-1-fixedSame], fixedAfter[len(fixedAfter)-1-fixedSame]) {
			break
		}
		fixedSame++
	}
	if fixedSame >= same {
		t.Fatalf("cutting every 256 bytes kept %d chunks and cutting on the content kept %d",
			fixedSame, same)
	}
	t.Logf("cutting every 256 bytes kept %d of %d", fixedSame, len(fixedBefore))
}

// Cutting the same bytes twice gives the same cuts, whatever the config, which
// is what makes a chunk a name for its contents.
func TestCuttingIsDeterministic(t *testing.T) {
	data := noise(3, 50_000)
	for _, cfg := range []Config{{}, small(), BuzHashConfig(), RollSumConfig(),
		{Rolling: NewRollSumRolling, Window: RollSumWindow, Average: 512, Min: 128, Max: 8192}} {
		first, second := Bytes(data, cfg), Bytes(data, cfg)
		if len(first) != len(second) {
			t.Fatalf("the same bytes cut into %d pieces and then %d", len(first), len(second))
		}
		for i := range first {
			if !bytes.Equal(first[i], second[i]) {
				t.Fatalf("piece %d differs between two runs of the same config", i)
			}
		}
		if got := rejoin(first); !bytes.Equal(got, data) {
			t.Fatal("the pieces are not the stream")
		}
	}
}

// Reading in awkward pieces must not move a boundary: the cuts depend on the
// bytes, not on how they arrived.
func TestHowTheStreamArrivesDoesNotMatter(t *testing.T) {
	data := noise(4, 80_000)
	want := Bytes(data, small())

	for _, size := range []int{1, 7, 1000, 1 << 20} {
		c := New(&dribble{data: data, size: size}, small())
		var got [][]byte
		var at uint64
		for {
			offset, piece, err := c.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if offset != at {
				t.Fatalf("a chunk claims to start at %d, want %d", offset, at)
			}
			at += uint64(len(piece))
			got = append(got, piece)
		}
		if len(got) != len(want) {
			t.Fatalf("reading %d bytes at a time gave %d pieces, want %d", size, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("reading %d bytes at a time moved piece %d", size, i)
			}
		}
	}
}

// dribble hands out at most size bytes per read, which is what a socket does.
type dribble struct {
	data []byte
	size int
	at   int
}

func (d *dribble) Read(p []byte) (int, error) {
	if d.at >= len(d.data) {
		return 0, io.EOF
	}
	n := copy(p[:min(len(p), d.size)], d.data[d.at:])
	d.at += n
	return n, nil
}

// A reader that fails is reported, not swallowed.
func TestAReaderThatFails(t *testing.T) {
	boom := errors.New("the disc went away")
	c := New(&failing{err: boom}, small())
	if _, _, err := c.Next(); !errors.Is(err, boom) {
		t.Fatalf("the chunker returned %v, want the reader's error", err)
	}
}

type failing struct{ err error }

func (f *failing) Read([]byte) (int, error) { return 0, f.err }

// A run of the same byte never trips the boundary test, so the maximum is what
// ends the chunk. Without it a file of zeroes would be one chunk.
func TestARunOfOneByteIsCutAtTheMaximum(t *testing.T) {
	data := make([]byte, 10_000) // all zero
	pieces := Bytes(data, small())
	if len(pieces) < 2 {
		t.Fatalf("ten thousand identical bytes came out as %d piece(s)", len(pieces))
	}
	for i, piece := range pieces[:len(pieces)-1] {
		if len(piece) != small().Max {
			t.Fatalf("piece %d of a run is %d bytes, want the maximum %d",
				i, len(piece), small().Max)
		}
	}
	if got := rejoin(pieces); !bytes.Equal(got, data) {
		t.Fatal("the pieces are not the stream")
	}
}

func TestTheDefaultsAndWhatFillsThem(t *testing.T) {
	got := Config{}.filled()
	if got.Window != DefaultWindow || got.Average != DefaultAverage ||
		got.Min != DefaultMin || got.Max != DefaultMax || got.Rolling == nil {
		t.Fatalf("the zero config filled to %+v", got)
	}
	// Negative and zero are the same as absent.
	if bad := (Config{Window: -1, Average: -1, Min: -1, Max: -1}).filled(); bad.Window != DefaultWindow ||
		bad.Average != DefaultAverage || bad.Min != DefaultMin || bad.Max != DefaultMax {
		t.Fatalf("a config of negatives filled to %+v", bad)
	}
	// A maximum below the minimum is raised to it, rather than making a chunk
	// that has to be both.
	if got := (Config{Min: 100, Max: 10}).filled(); got.Max != 100 {
		t.Fatalf("a maximum under the minimum filled to %d", got.Max)
	}
	// The two named configurations are the two hashes at their own windows.
	if BuzHashConfig().Window != DefaultWindow || RollSumConfig().Window != RollSumWindow {
		t.Fatal("the named configurations do not carry their own windows")
	}
	if _, ok := BuzHashConfig().Rolling(4).(*BuzHash); !ok {
		t.Fatal("BuzHashConfig does not roll a BuzHash")
	}
	if _, ok := RollSumConfig().Rolling(4).(*RollSum); !ok {
		t.Fatal("RollSumConfig does not roll a RollSum")
	}
}

// The mask is what turns an average into a boundary test. The bit count is
// bita's, which is one fewer than the average's own power of two — pinned here
// because a stream cut with a different count is cut in different places, and
// then an archive written by bita and one written here disagree.
func TestTheMaskFollowsTheAverage(t *testing.T) {
	for _, c := range []struct {
		average int
		bits    int
	}{{1024, 9}, {2048, 10}, {4096, 11}, {64 << 10, 15}} {
		mask := Config{Average: c.average}.filled().mask()
		if got := onesIn(mask); got != c.bits {
			t.Fatalf("an average of %d gave a mask of %d bits, want %d", c.average, got, c.bits)
		}
	}
	// An average that is not a power of two rounds down to one, so the two give
	// the same mask.
	if (Config{Average: 4096}).filled().mask() != (Config{Average: 5000}).filled().mask() {
		t.Fatal("4096 and 5000 gave different masks")
	}
}

// What the average is for, measured rather than asserted from the mask: the
// chunks that come out are near the size asked for. It is the pair — the mask
// deciding how often a boundary offers itself and the minimum refusing the ones
// that come too soon — that lands them there, so this is the check that the two
// are set against each other and not just each on its own.
func TestTheChunksComeOutNearTheAverage(t *testing.T) {
	for _, c := range []struct{ average, min int }{
		{DefaultAverage, DefaultMin},
		{1024, 256},
		{4096, 1024},
	} {
		cfg := Config{Average: c.average, Min: c.min, Max: c.average * 64}
		data := noise(9, c.average*400)
		pieces := Bytes(data, cfg)
		mean := len(data) / len(pieces)
		if mean < c.average/2 || mean > c.average*2 {
			t.Fatalf("an average of %d with a minimum of %d produced chunks of %d",
				c.average, c.min, mean)
		}
		t.Logf("average %d, minimum %d: %d chunks of %d bytes on average",
			c.average, c.min, len(pieces), mean)
	}
}

func onesIn(mask uint32) int {
	n := 0
	for ; mask != 0; mask >>= 1 {
		if mask&1 == 1 {
			n++
		}
	}
	return n
}

// Cut is the shape a caller that only wants the pieces asks for.
func TestCutReturnsAFunction(t *testing.T) {
	data := noise(5, 20_000)
	cut := Cut(small())
	if got := rejoin(cut(data)); !bytes.Equal(got, data) {
		t.Fatal("the function Cut returned does not give the stream back")
	}
	if len(cut(data)) != len(Bytes(data, small())) {
		t.Fatal("Cut and Bytes disagree")
	}
	// The zero config works, and cuts a big enough thing into more than one.
	if n := len(Cut(Config{})(noise(6, DefaultMin*8))); n < 2 {
		t.Fatalf("the defaults cut %d times into a stream eight minimums long", n)
	}
}

// The two hashes, on their own terms: a window's worth of bytes hashes to the
// same thing however the window was reached.
func TestARollingHashForgetsWhatLeavesItsWindow(t *testing.T) {
	for _, newHash := range []func(int) Rolling{NewBuzHashRolling, NewRollSumRolling} {
		window := 8
		tail := []byte("abcdefgh")

		// One fed rubbish first, one fed nothing first; both then fed the tail.
		first, second := newHash(window), newHash(window)
		for _, b := range []byte("zzzzzzzzzzzzzzzz") {
			feed(first, b)
		}
		for _, b := range tail {
			feed(first, b)
		}
		for _, b := range append([]byte("qqqqqqqq"), tail...) {
			feed(second, b)
		}
		if first.Sum() != second.Sum() {
			t.Fatalf("two windows of %q hash differently: %#x and %#x",
				tail, first.Sum(), second.Sum())
		}
		// And a different window hashes differently.
		other := newHash(window)
		for _, b := range []byte("abcdefgi") {
			feed(other, b)
		}
		if other.Sum() == first.Sum() {
			t.Fatal("two different windows hash the same")
		}
	}
}

// feed puts a byte in, priming while the window is filling and rolling after.
func feed(h Rolling, b byte) {
	if !h.Primed() {
		h.Prime(b)
		return
	}
	h.Roll(b)
}

// A RollSum needs no priming, which is what it says.
func TestARollSumIsPrimedFromTheStart(t *testing.T) {
	r := NewRollSum(8)
	if !r.Primed() {
		t.Fatal("a fresh RollSum is not primed")
	}
	before := r.Sum()
	r.Prime('x') // does nothing
	if r.Sum() != before {
		t.Fatal("priming a RollSum changed it")
	}
	r.Roll('x')
	if r.Sum() == before {
		t.Fatal("rolling a RollSum did not change it")
	}
}

// A BuzHash primes until its window is full and not after.
func TestABuzHashPrimesItsWindow(t *testing.T) {
	b := NewBuzHash(4)
	for n := range 4 {
		if b.Primed() {
			t.Fatalf("a BuzHash of window 4 was primed after %d bytes", n)
		}
		b.Prime('a')
	}
	if !b.Primed() {
		t.Fatal("a BuzHash of window 4 is not primed after four bytes")
	}
	before := b.Sum()
	b.Prime('z') // past priming, this does nothing
	if b.Sum() != before {
		t.Fatal("priming a full BuzHash changed it")
	}
	// A window of one repeated byte is where the repeat shortcut lives: rolling
	// the same byte again cannot change the hash.
	for range 10 {
		b.Roll('a')
	}
	if b.Sum() != before {
		t.Fatalf("rolling the same byte through a full window changed the hash")
	}
	b.Roll('b')
	if b.Sum() == before {
		t.Fatal("rolling a different byte did not change the hash")
	}
}

// A randomised sweep: whatever the config and whatever the bytes, the pieces are
// the stream and the bounds hold.
func TestRandomisedCutting(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for run := range 200 {
		cfg := Config{
			Window:  1 + int(rng.UintN(32)),
			Average: 1 << (4 + rng.UintN(8)),
			Min:     int(rng.UintN(512)),
			Max:     int(rng.UintN(4096)),
		}
		if rng.UintN(2) == 0 {
			cfg.Rolling = NewRollSumRolling
		}
		data := noise(uint64(run), int(rng.UintN(20_000)))
		pieces := Bytes(data, cfg)
		if got := rejoin(pieces); !bytes.Equal(got, data) {
			t.Fatalf("run %d (%+v, %d bytes): the pieces are not the stream", run, cfg, len(data))
		}
		full := cfg.filled()
		for i, piece := range pieces {
			if len(piece) > full.Max {
				t.Fatalf("run %d: a piece of %d is over the maximum %d", run, len(piece), full.Max)
			}
			if i < len(pieces)-1 && len(piece) < full.Min {
				t.Fatalf("run %d: piece %d is %d, under the minimum %d", run, i, len(piece), full.Min)
			}
		}
	}
}

func ExampleCut() {
	data := bytes.Repeat([]byte("the quick brown fox "), 5000)
	pieces := Cut(Config{Average: 1024, Min: 256, Max: 8192})(data)
	fmt.Println(len(pieces) > 1, len(rejoin(pieces)) == len(data))
	// Output: true true
}

// Bits is for a caller that has the count rather than a size: bita reads it from
// an archive header, and a chunker rebuilt to read that archive has to cut where
// the count says.
func TestBitsOverridesTheAverage(t *testing.T) {
	// The same count, reached two ways, cuts the same stream the same way.
	data := noise(21, 60_000)
	byAverage := Bytes(data, Config{Average: 1024, Min: 64, Max: 4096, Window: 16})
	byBits := Bytes(data, Config{Bits: BitsFromAverage(1024), Min: 64, Max: 4096, Window: 16})
	if len(byAverage) != len(byBits) {
		t.Fatalf("an average of 1024 gave %d pieces and its bit count gave %d",
			len(byAverage), len(byBits))
	}
	for i := range byAverage {
		if !bytes.Equal(byAverage[i], byBits[i]) {
			t.Fatalf("piece %d differs between an average and its own bit count", i)
		}
	}
	// And a count set explicitly is used whatever the average says.
	if got := onesIn((Config{Bits: 4, Average: 1 << 20}).filled().mask()); got != 4 {
		t.Fatalf("Bits: 4 beside a large average gave %d bits", got)
	}
	if got := BitsFromAverage(0); got != BitsFromAverage(DefaultAverage) {
		t.Fatalf("BitsFromAverage(0) gave %d, want the default's %d",
			got, BitsFromAverage(DefaultAverage))
	}
	// A count no hash can satisfy is a boundary that never comes, which the
	// maximum answers rather than a shift nobody can read.
	pieces := Bytes(noise(22, 5000), Config{Bits: 99, Min: 64, Max: 1024, Window: 16})
	if len(pieces) != 5 {
		t.Fatalf("a boundary that never comes gave %d pieces, want the stream cut at the maximum", len(pieces))
	}
	if got := rejoin(pieces); len(got) != 5000 {
		t.Fatal("the pieces are not the stream")
	}
}
