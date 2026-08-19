# chunk

[![ci](https://github.com/go-deltasync/chunk/actions/workflows/ci.yml/badge.svg)](https://github.com/go-deltasync/chunk/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/go-deltasync/chunk.svg)](https://pkg.go.dev/github.com/go-deltasync/chunk)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-deltasync/chunk)](https://goreportcard.com/report/github.com/go-deltasync/chunk)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

Cuts a stream into pieces at boundaries the content decides. Pure Go, no
dependencies, `CGO_ENABLED=0`.

## Why

Cut every sixty-four kilobytes, and inserting one byte at the front moves every
later boundary: nothing matches what was stored before, so the whole thing is
written and sent again. Cut where a rolling hash over the last few bytes has some
shape, and inserting one byte disturbs the two chunks around it and nothing else.

That is what makes deduplication and delta transfer work — a backup that keeps
only what changed, an archive that fetches only the chunks it lacks, a
content-addressed store where the same bytes are stored once.

The difference is measured rather than asserted. `TestAnEditDisturbsOnlyWhatIsNearIt`
inserts one byte a tenth of the way into 200 kB and counts what survives:

```
958 of 1067 chunks unchanged after inserting one byte at 10%
cutting every 256 bytes kept 0 of 782
```

## Use

```go
// A stream.
c := chunk.New(reader, chunk.Config{})
for {
    offset, piece, err := c.Next()
    if errors.Is(err, io.EOF) {
        break
    }
    // …
}

// Bytes already in hand, for a caller that wants the pieces and nothing else.
pieces := chunk.Cut(chunk.Config{})(data)
```

`Cut` returns the shape a content-addressed store usually asks for — for
instance `go-crdt/crdt`'s blob store, which cuts a file into operations of its
own and takes any chunker:

```go
blobs.PutWith("figure.png", data, chunk.Cut(chunk.Config{}))
```

## Where it comes from

The rolling hashes, the seed, the table and the boundary test are
[bita](https://github.com/go-deltasync/bita)'s, so a stream cut here is cut in
the same places bita cuts it and an archive written by either is readable by the
other.

They were written for bita and lived inside it, where nothing else could reach
them. This is the same code with a name, so that the next thing needing
content-defined chunks does not write a fourth rolling hash in this organisation.

## Configuration

The zero `Config` is BuzHash over a sixteen-byte window, aiming at 64 KiB chunks
between 16 KiB and 16 MiB — bita's defaults. `RollSumConfig` is bita's other
hash, over the window it was tuned for.

`Average` sets how many bits of the hash a boundary needs, so it is rounded down
to a power of two; `Min` and `Max` bound what that can produce. A run of one
repeated byte never trips the test, so `Max` is what ends the chunk — without it
a file of zeroes would be a single chunk.

## Licence

BSD-3-Clause.
