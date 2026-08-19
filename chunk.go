// Package chunk cuts a stream into pieces at boundaries the content decides.
//
// # What it is for
//
// Anything that stores or sends a large thing in pieces wants the pieces to
// depend on the bytes rather than on how far into the stream they are. Cut every
// sixty-four kilobytes and inserting one byte at the front moves every later
// boundary, so nothing matches what was stored before and the whole thing is
// written and sent again. Cut where a rolling hash over the last few bytes has
// some shape, and inserting one byte disturbs the two chunks around it and
// nothing else.
//
// That is the whole idea, and it is what makes deduplication and delta transfer
// work: a backup that keeps only what changed, an archive that fetches only the
// chunks it lacks, a content-addressed store where the same bytes are stored
// once.
//
// # Where it comes from
//
// The rolling hashes, the seed, the table and the boundary test are bita's, so a
// stream cut here is cut in the same places bita cuts it. They were written for
// github.com/go-deltasync/bita and lived inside it, where nothing else could
// reach them; this is the same code with a name, so that the next thing needing
// content-defined chunks does not write a fourth rolling hash in this
// organisation.
//
// # Using it
//
// For a stream, [New] and [Chunker.Next]. For bytes already in memory — a figure
// being put into a document, say — [Cut] returns the function a caller of that
// shape usually wants:
//
//	blobs.PutWith("figure.png", data, chunk.Cut(chunk.Config{}))
package chunk

import (
	"bytes"
	"io"
	"math/bits"
)

// A Rolling is a rolling hash over a window of bytes: it is fed the stream one
// byte at a time and says what the last window's worth of it hashes to.
//
// Prime and Primed exist because a hash may need its window filled before its
// value means anything. [RollSum] does not and says so; [BuzHash] does.
type Rolling interface {
	// Prime feeds a byte while the window is still filling.
	Prime(b byte)
	// Primed reports whether the window is full.
	Primed() bool
	// Roll feeds a byte, dropping the one that leaves the window.
	Roll(b byte)
	// Sum is the hash of the window as it stands.
	Sum() uint32
}

// The defaults, which are bita's.
const (
	// DefaultAverage is the chunk size boundaries are chosen to average.
	DefaultAverage = 64 * 1024
	// DefaultMin and DefaultMax bound what an average is allowed to produce: a
	// run of bytes that never trips the boundary test would otherwise be one
	// chunk as long as the stream, and a stream that trips it constantly would
	// be one chunk per byte.
	DefaultMin = 16 * 1024
	DefaultMax = 16 * 1024 * 1024
	// DefaultWindow is the window a [BuzHash] rolls over. RollSum's is
	// RollSumWindow, because the two hashes were tuned separately.
	DefaultWindow = 16
	// RollSumWindow is the window bita rolls a [RollSum] over.
	RollSumWindow = 64
)

// A Config says where the cuts fall. The zero Config is [BuzHash] at the
// defaults above, which is what bita writes by default.
type Config struct {
	// Rolling makes the hash to roll. Nil is [NewBuzHash].
	Rolling func(window int) Rolling
	// Window is how many bytes the hash rolls over. Zero is [DefaultWindow].
	Window int
	// Average is the chunk size to aim for. Zero is [DefaultAverage].
	//
	// What it sets is how many bits of the hash a boundary needs, so it is
	// rounded down to a power of two and two averages within a factor of two of
	// each other cut the same way. The count is log2(Average) minus one, which
	// is bita's — a boundary then falls every Average/2 bytes on average, and
	// Min pushes the chunks that result back up towards Average. Aiming through
	// the pair rather than through the mask alone is what makes the defaults
	// come out near the size they name; see TestTheChunksComeOutNearTheAverage.
	Average int
	// Min and Max bound a chunk. Zero is [DefaultMin] and [DefaultMax].
	Min, Max int
}

// BuzHashConfig and RollSumConfig are bita's two rolling-hash configurations,
// each with the window that hash was tuned for.
func BuzHashConfig() Config {
	return Config{Rolling: NewBuzHashRolling, Window: DefaultWindow}
}

func RollSumConfig() Config {
	return Config{Rolling: NewRollSumRolling, Window: RollSumWindow}
}

// NewBuzHashRolling and NewRollSumRolling are the two hashes as a [Config] wants
// them. They exist because a method set on a pointer type is not a func value
// until something writes one.
func NewBuzHashRolling(window int) Rolling { return NewBuzHash(window) }

func NewRollSumRolling(window int) Rolling { return NewRollSum(window) }

// filled returns the config with every zero replaced by its default.
func (c Config) filled() Config {
	if c.Rolling == nil {
		c.Rolling = NewBuzHashRolling
	}
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if c.Average <= 0 {
		c.Average = DefaultAverage
	}
	if c.Min <= 0 {
		c.Min = DefaultMin
	}
	if c.Max <= 0 {
		c.Max = DefaultMax
	}
	if c.Max < c.Min {
		c.Max = c.Min
	}
	return c
}

// mask is the boundary test: a chunk ends where every bit of the mask is set in
// the hash. A mask of n bits is tripped by one hash in 2^n, so a boundary falls
// every 2^n bytes on average. The count is bita's, so that a stream cut here is
// cut where bita cuts it; see [Config.Average] for what it means for the sizes
// that come out.
func (c Config) mask() uint32 {
	bitsSet := 30 - uint32(bits.LeadingZeros32(uint32(c.Average)))
	return uint32(0xffffffff) >> (32 - bitsSet)
}

// refillSize is how much a Chunker reads at a time, matching bita.
const refillSize = 1024 * 1024

// A Chunker cuts what it reads. It is not safe for concurrent use.
type Chunker struct {
	r    io.Reader
	cfg  Config
	mask uint32

	hash  Rolling
	buf   []byte
	base  int // where the unconsumed bytes start in buf
	at    uint64
	done  bool
	scan  int // how far into the unconsumed bytes the hash has been fed
	limit int // how far to feed the hash without looking for a boundary
}

// New returns a Chunker over r.
func New(r io.Reader, cfg Config) *Chunker {
	cfg = cfg.filled()
	c := &Chunker{r: r, cfg: cfg, mask: cfg.mask(), hash: cfg.Rolling(cfg.Window)}
	// The hash is rolled over the bytes before the minimum, so that the window
	// entering the region where a boundary may be taken is a window of real
	// bytes; there is no point looking for one before then.
	if c.limit = cfg.Min - cfg.Window; c.limit < 0 {
		c.limit = 0
	}
	return c
}

// Next returns the next chunk and where in the stream it starts. It returns
// [io.EOF] when there is nothing left, and whatever the reader returned if that
// failed.
func (c *Chunker) Next() (offset uint64, data []byte, err error) {
	for {
		if c.base < len(c.buf) {
			if n, found := c.boundary(); found {
				at, data := c.take(n)
				return at, data, nil
			}
		}
		if c.done {
			return 0, nil, io.EOF
		}
		read, err := c.refill()
		if err != nil {
			return 0, nil, err
		}
		if read == 0 {
			c.done = true
			if c.base < len(c.buf) {
				// The tail of the stream is a chunk whatever the hash says: a
				// boundary is where a chunk may end and the end of the stream is
				// where it must.
				at, data := c.take(len(c.buf) - c.base)
				return at, data, nil
			}
			return 0, nil, io.EOF
		}
	}
}

// take slices n bytes off the front of the unconsumed buffer. The bytes are
// copied out, because the buffer they are in is reused on the next refill.
func (c *Chunker) take(n int) (uint64, []byte) {
	data := append([]byte(nil), c.buf[c.base:c.base+n]...)
	c.base += n
	c.scan = 0
	at := c.at
	c.at += uint64(n)
	return at, data
}

// refill drops what has been consumed and reads more.
func (c *Chunker) refill() (int, error) {
	if c.base > 0 {
		n := copy(c.buf, c.buf[c.base:])
		c.buf, c.base = c.buf[:n], 0
	}
	start := len(c.buf)
	if cap(c.buf) < start+refillSize {
		grown := make([]byte, start, start+refillSize)
		copy(grown, c.buf)
		c.buf = grown
	}
	c.buf = c.buf[:start+refillSize]
	n, err := c.r.Read(c.buf[start:])
	c.buf = c.buf[:start+n]
	if err == io.EOF {
		err = nil
	}
	return n, err
}

// boundary looks for where the current chunk ends, and says how long it is.
func (c *Chunker) boundary() (int, bool) {
	buf := c.buf[c.base:]
	for !c.hash.Primed() && c.scan < len(buf) {
		c.hash.Prime(buf[c.scan])
		c.scan++
	}
	// Nothing before the minimum can be a boundary, so the bytes up to it are
	// rolled through without the test being made.
	if c.limit > 0 && c.scan < c.limit {
		c.scan = min(c.limit-1, len(buf))
	}
	if c.cfg.Min > 0 && c.scan < c.cfg.Min {
		end := min(c.cfg.Min-1, len(buf))
		for c.scan < end {
			c.hash.Roll(buf[c.scan])
			c.scan++
		}
		c.scan = end
	}
	stop := min(c.cfg.Max, len(buf))
	for c.scan < stop {
		c.hash.Roll(buf[c.scan])
		c.scan++
		if sum := c.hash.Sum(); sum|c.mask == sum {
			return c.scan, true
		}
	}
	if c.scan >= c.cfg.Max {
		// No boundary within the maximum, so one is made: a chunk as long as
		// the stream is not a chunk.
		return c.scan, true
	}
	return 0, false
}

// Bytes cuts data and returns the pieces, which together are data again.
func Bytes(data []byte, cfg Config) [][]byte {
	c := New(bytes.NewReader(data), cfg)
	var out [][]byte
	for {
		_, piece, err := c.Next()
		if err != nil {
			// A bytes.Reader fails only at its end, so the error here is io.EOF
			// and nothing else can arrive.
			return out
		}
		out = append(out, piece)
	}
}

// Cut returns a function that cuts a byte slice, which is the shape a caller
// that only wants the pieces asks for — go-crdt's blob store, for one:
//
//	blobs.PutWith("figure.png", data, chunk.Cut(chunk.Config{}))
//
// The config is read once, here, rather than on every call.
func Cut(cfg Config) func(data []byte) [][]byte {
	cfg = cfg.filled()
	return func(data []byte) [][]byte { return Bytes(data, cfg) }
}
