package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var ErrInvalidSignature = fmt.Errorf("commit signature is not valid")

func ExtractRecords(ctx context.Context, b io.Reader, signingKey string) (map[string]json.RawMessage, error) {
	log := zerolog.Ctx(ctx)

	r, err := car.NewCarReader(b)
	if err != nil {
		return nil, fmt.Errorf("failed to construct CAR reader: %w", err)
	}

	blocks := map[cid.Cid][]byte{}
	for {
		block, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading next block: %w", err)
		}
		c, err := block.Cid().Prefix().Sum(block.RawData())
		if err != nil {
			return nil, fmt.Errorf("failed to calculate CID from content")
		}
		if c.Equals(block.Cid()) {
			blocks[block.Cid()] = block.RawData()
		} else {
			log.Debug().Str("cid", block.Cid().String()).
				Msgf("CID doesn't match block content: %s != %s", block.Cid().String(), c.String())
		}
	}

	records := map[string]cid.Cid{}
	if len(r.Header.Roots) == 0 {
		return nil, fmt.Errorf("CAR has zero roots specified")
	}

	// https://atproto.com/specs/repository specifies that the first root
	// must be a commit object. Meaning of subsequent roots is not yet defined.
	root := r.Header.Roots[0]

	// TODO: verify that a root is a commit record and validate signature
	if _, found := blocks[root]; !found {
		return nil, fmt.Errorf("root block is missing")
	}
	valid, err := verifyCommitSignature(ctx, blocks[root], signingKey)
	if err != nil {
		return nil, fmt.Errorf("commit signature verification failed: %w", err)
	}
	if !valid {
		return nil, ErrInvalidSignature
	}

	cids, err := findRecords(blocks, root, nil, nil, 0)
	if err != nil {
		return nil, err
	}
	for k, v := range cids {
		records[k] = v
	}

	res := map[string]json.RawMessage{}
	for k, c := range records {
		builder := basicnode.Prototype.Any.NewBuilder()
		if err := (&dagcbor.DecodeOptions{AllowLinks: true}).Decode(builder, bytes.NewReader(blocks[c])); err != nil {
			return nil, fmt.Errorf("unmarshaling %q: %w", c.String(), err)
		}
		w := bytes.NewBuffer(nil)
		if err := (dagjson.EncodeOptions{EncodeLinks: true, EncodeBytes: true}).Encode(builder.Build(), w); err != nil {
			return nil, fmt.Errorf("marshaling %q as JSON: %w", c.String(), err)
		}
		res[k] = w.Bytes()
	}
	return res, nil
}

const maxDepth = 128

func findRecords(blocks map[cid.Cid][]byte, root cid.Cid, key []byte, visited map[cid.Cid]bool, depth int) (map[string]cid.Cid, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("reached maximum depth at %q", root.String())
	}

	if visited == nil {
		visited = map[cid.Cid]bool{}
	}

	visited[root] = true

	builder := basicnode.Prototype.Any.NewBuilder()
	if err := (&dagcbor.DecodeOptions{AllowLinks: true}).Decode(builder, bytes.NewReader(blocks[root])); err != nil {
		return nil, fmt.Errorf("unmarshaling %q: %w", root.String(), err)
	}
	node := builder.Build()

	if node.Kind() != datamodel.Kind_Map {
		return nil, nil
	}

	m, err := parseMap(node)
	if err != nil {
		return nil, err
	}

	if _, ok := m["$type"]; ok {
		return map[string]cid.Cid{string(key): root}, nil
	}

	if d, ok := m["data"]; ok {
		// Commit record
		if d.Kind() == datamodel.Kind_Link {
			l, _ := d.AsLink()
			if l != nil {
				c, err := cid.Parse([]byte(l.Binary()))
				if err != nil {
					return nil, fmt.Errorf("failed to parse %q as CID: %w", l.String(), err)
				}
				if _, ok := blocks[c]; ok && !visited[c] {
					return findRecords(blocks, c, nil, visited, depth+1)
				}
			}
		}
		return nil, nil
	}

	if entries, ok := m["e"]; ok {
		// MST node
		r := map[string]cid.Cid{}
		iter := entries.ListIterator()
		key = []byte{}
		for !iter.Done() {
			_, item, err := iter.Next()
			if err != nil {
				return nil, fmt.Errorf("failed to read the next list item in block %q: %w", root.String(), err)
			}
			if item.Kind() != datamodel.Kind_Map {
				continue
			}

			m, err := parseMap(item)
			if err != nil {
				return nil, err
			}

			for _, field := range []string{"k", "p", "v", "t"} {
				if _, ok := m[field]; !ok {
					return nil, fmt.Errorf("TreeEntry is missing field %q", field)
				}
			}
			prefixLen, err := m["p"].AsInt()
			if err != nil {
				return nil, fmt.Errorf("m[\"p\"].AsInt(): %w", err)
			}
			prefixPart, err := m["k"].AsBytes()
			if err != nil {
				return nil, fmt.Errorf("m[\"k\"].AsBytes(): %w", err)
			}
			val, err := m["v"].AsLink()
			if err != nil {
				return nil, fmt.Errorf("m[\"v\"].AsLink(): %w", err)
			}
			c, err := cid.Parse([]byte(val.Binary()))
			if err != nil {
				return nil, fmt.Errorf("failed to parse %q as CID: %w", val.String(), err)
			}

			if len(key) == 0 {
				// First entry, must have a full key.
				if prefixLen != 0 {
					return nil, fmt.Errorf("incomplete key in the first entry")
				}
				key = prefixPart
			}

			if prefixLen > int64(len(key)) {
				return nil, fmt.Errorf("specified prefix length is larger than the key length: %d > %d", prefixLen, len(key))
			}
			key = append(key[:prefixLen], prefixPart...)

			if _, ok := blocks[c]; ok && !visited[c] {
				results, err := findRecords(blocks, c, key, visited, depth+1)
				if err != nil {
					return nil, err
				}
				for k, v := range results {
					r[k] = v
				}
			}

			if m["t"] != nil && m["t"].Kind() == datamodel.Kind_Link {
				subtree, err := m["t"].AsLink()
				if err != nil {
					return nil, fmt.Errorf("m[\"t\"].AsLink(): %w", err)
				}
				subtreeCid, err := cid.Parse([]byte(subtree.Binary()))
				if err != nil {
					return nil, fmt.Errorf("failed to parse %q as CID: %w", val.String(), err)
				}
				if _, ok := blocks[subtreeCid]; ok && !visited[subtreeCid] {
					results, err := findRecords(blocks, subtreeCid, key, visited, depth+1)
					if err != nil {
						return nil, err
					}
					for k, v := range results {
						r[k] = v
					}
				}
			}
		}

		left, ok := m["l"]
		if ok && left.Kind() == datamodel.Kind_Link {
			l, _ := left.AsLink()
			if l != nil {
				c, err := cid.Parse([]byte(l.Binary()))
				if err != nil {
					return nil, fmt.Errorf("failed to parse %q as CID: %w", l.String(), err)
				}
				if _, ok := blocks[c]; ok && !visited[c] {
					results, err := findRecords(blocks, c, nil, visited, depth+1)
					if err != nil {
						return nil, err
					}
					for k, v := range results {
						r[k] = v
					}
				}
			}
		}

		return r, nil
	}

	return nil, fmt.Errorf("unrecognized block %q", root.String())
}

func parseMap(node datamodel.Node) (map[string]datamodel.Node, error) {
	if node.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("not a map")
	}

	m := map[string]datamodel.Node{}
	iter := node.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating over map fields: %w", err)
		}
		if k.Kind() != datamodel.Kind_String {
			continue
		}
		ks, _ := k.AsString()
		m[ks] = v
	}
	return m, nil
}

var ErrZeroBlocks = fmt.Errorf("zero blocks found")

func GetRev(ctx context.Context, b io.Reader) (string, error) {
	r, err := car.NewCarReader(b)
	if err != nil {
		return "", fmt.Errorf("failed to construct CAR reader: %w", err)
	}

	if len(r.Header.Roots) == 0 {
		return "", fmt.Errorf("no roots specified in CAR header")
	}

	blocks := map[cid.Cid][]byte{}
	for {
		block, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("reading next block: %w", err)
		}
		c, err := block.Cid().Prefix().Sum(block.RawData())
		if err != nil {
			return "", fmt.Errorf("failed to calculate CID from content")
		}
		if c.Equals(block.Cid()) {
			blocks[block.Cid()] = block.RawData()
		}
	}

	if len(blocks) == 0 {
		return "", ErrZeroBlocks
	}

	builder := basicnode.Prototype.Any.NewBuilder()
	if err := (&dagcbor.DecodeOptions{AllowLinks: true}).Decode(builder, bytes.NewReader(blocks[r.Header.Roots[0]])); err != nil {
		return "", fmt.Errorf("unmarshaling %q: %w", r.Header.Roots[0].String(), err)
	}
	node := builder.Build()

	v, err := node.LookupByString("rev")
	if err != nil {
		return "", fmt.Errorf("looking up 'rev' field: %w", err)
	}

	s, err := v.AsString()
	if err != nil {
		return "", fmt.Errorf("rev.AsString(): %w", err)
	}
	return s, nil
}

func GetLang(ctx context.Context, value json.RawMessage) ([]string, time.Time, error) {
	var content struct {
		Type  string   `json:"$type"`
		Langs []string `json:"langs"`
		Time  string   `json:"createdAt"`
	}
	err := json.Unmarshal([]byte(value), &content)

	if err != nil {
		return nil, time.Now(), fmt.Errorf("failed to extract lang from content: %w", err)
	}
	if content.Type != "app.bsky.feed.post" {
		return nil, time.Now(), errors.New("not a post")
	}

	var timestamp time.Time
	if t, err := time.Parse(time.RFC3339, content.Time); err != nil {
		return nil, time.Now(), fmt.Errorf("failed to extract time from content: %w", err)
	} else {
		timestamp = t
	}
	return content.Langs, timestamp, nil
}
