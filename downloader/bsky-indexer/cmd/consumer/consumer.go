package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/gocqlx/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/uabluerail/indexer/models"
	"github.com/uabluerail/indexer/pds"
	"github.com/uabluerail/indexer/repo"
	"github.com/uabluerail/indexer/util/fix"
	"github.com/uabluerail/indexer/util/resolver"
)

const lastRevUpdateInterval = 24 * time.Hour

type Consumer struct {
	db                  *gorm.DB
	recordsDB           *gocqlx.Session
	remote              pds.PDS
	running             chan struct{}
	collectionBlacklist map[string]bool
	contactInfo         string

	lastCursorPersist time.Time
}

func NewConsumer(ctx context.Context, remote *pds.PDS, db *gorm.DB, session *gocqlx.Session, contactInfo string) (*Consumer, error) {
	if err := db.AutoMigrate(&repo.BadRecord{}); err != nil {
		return nil, fmt.Errorf("db.AutoMigrate: %s", err)
	}

	return &Consumer{
		db:                  db,
		recordsDB:           session,
		remote:              *remote,
		running:             make(chan struct{}),
		collectionBlacklist: map[string]bool{},
		contactInfo:         contactInfo,
	}, nil
}

func (c *Consumer) BlacklistCollections(colls []string) {
	for _, coll := range colls {
		c.collectionBlacklist[coll] = true
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	go c.run(ctx)
	return nil
}

func (c *Consumer) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.running:
		// Channel got closed
		return nil
	}
}

func (c *Consumer) run(ctx context.Context) {
	log := zerolog.Ctx(ctx).With().Str("pds", c.remote.Host).Logger()
	ctx = log.WithContext(ctx)

	backoffTimer := backoff.NewExponentialBackOff(
		backoff.WithMaxElapsedTime(0),
		backoff.WithInitialInterval(time.Second),
		backoff.WithMaxInterval(5*time.Minute),
	)
	pdsOnline.WithLabelValues(c.remote.Host).Set(0)

	defer close(c.running)

	for {
		select {
		case <-c.running:
			log.Error().Msgf("Attempt to start previously stopped consumer")
			return
		case <-ctx.Done():
			log.Info().Msgf("Consumer stopped")
			lastEventTimestamp.DeletePartialMatch(prometheus.Labels{"remote": c.remote.Host})
			eventCounter.DeletePartialMatch(prometheus.Labels{"remote": c.remote.Host})
			reposDiscovered.DeletePartialMatch(prometheus.Labels{"remote": c.remote.Host})
			postsByLanguageIndexed.DeletePartialMatch(prometheus.Labels{"remote": c.remote.Host})
			pdsOnline.DeletePartialMatch(prometheus.Labels{"remote": c.remote.Host})
			return
		default:
			start := time.Now()
			if err := c.runOnce(ctx); err != nil {
				log.Error().Err(err).Msgf("Consumer of %q failed (will be restarted): %s", c.remote.Host, err)
				connectionFailures.WithLabelValues(c.remote.Host).Inc()
			}
			if time.Since(start) > backoffTimer.MaxInterval*3 {
				// XXX: assume that c.runOnce did some useful work in this case,
				// even though it might have been stuck on some absurdly long timeouts.
				backoffTimer.Reset()
			}
			time.Sleep(backoffTimer.NextBackOff())
		}
	}
}

func (c *Consumer) runOnce(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	log.Info().
		Int64("cursor", c.remote.Cursor).
		Int64("first_cursor_since_reset", c.remote.FirstCursorSinceReset).
		Msgf("Connecting to firehose of %s...", c.remote.Host)

	addr, err := url.Parse(c.remote.Host)
	if err != nil {
		return fmt.Errorf("parsing URL %q: %s", c.remote.Host, err)
	}
	addr.Scheme = "wss"
	addr.Path = path.Join(addr.Path, "xrpc/com.atproto.sync.subscribeRepos")

	if c.remote.Cursor > 0 {
		params := url.Values{"cursor": []string{fmt.Sprint(c.remote.Cursor)}}
		addr.RawQuery = params.Encode()
	}

	headers := http.Header{}
	headers.Set("User-Agent", fmt.Sprintf("Go-http-client/1.1 indexerbot/0.1 (based on github.com/uabluerail/indexer; %s)", c.contactInfo))
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, addr.String(), headers)
	if err != nil {
		return fmt.Errorf("establishing websocker connection: %w", err)
	}
	defer conn.Close()

	pdsOnline.WithLabelValues(c.remote.Host).Set(1)
	defer func() { pdsOnline.WithLabelValues(c.remote.Host).Set(0) }()

	ch := make(chan bool)
	defer close(ch)
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ch:
				return
			case <-t.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Minute)); err != nil {
					log.Error().Err(err).Msgf("Failed to send ping: %s", err)
				}
			}
		}
	}()

	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, b, err := conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("websocket.ReadMessage: %w", err)
			}

			r := bytes.NewReader(b)
			proto := basicnode.Prototype.Any
			headerNode := proto.NewBuilder()
			if err := (&dagcbor.DecodeOptions{DontParseBeyondEnd: true}).Decode(headerNode, r); err != nil {
				return fmt.Errorf("unmarshaling message header: %w", err)
			}
			header, err := parseHeader(headerNode.Build())
			if err != nil {
				return fmt.Errorf("parsing message header: %w", err)
			}
			switch header.Op {
			case 1:
				if err := c.processMessage(ctx, header.Type, r, first); err != nil {
					if ctx.Err() != nil {
						// We're shutting down, so the error is most likely due to that.
						return err
					}

					const maxBadRecords = 500
					var count int64
					if err2 := c.db.Model(&repo.BadRecord{}).Where(&repo.BadRecord{PDS: c.remote.ID}).Count(&count).Error; err2 != nil {
						return err
					}

					if count >= maxBadRecords {
						return err
					}

					log.Error().Err(err).Str("pds", c.remote.Host).Msgf("Failed to process message at cursor %d: %s", c.remote.Cursor, err)
					err := c.db.Create(&repo.BadRecord{
						PDS:     c.remote.ID,
						Cursor:  c.remote.Cursor,
						Error:   err.Error(),
						Content: b,
					}).Error
					if err != nil {
						return fmt.Errorf("failed to store bad message: %s", err)
					}
				}
			case -1:
				bodyNode := proto.NewBuilder()
				if err := (&dagcbor.DecodeOptions{DontParseBeyondEnd: true, AllowLinks: true}).Decode(bodyNode, r); err != nil {
					return fmt.Errorf("unmarshaling message body: %w", err)
				}
				body, err := parseError(bodyNode.Build())
				if err != nil {
					return fmt.Errorf("parsing error payload: %w", err)
				}
				return &body
			default:
				log.Warn().Msgf("Unknown 'op' value received: %d", header.Op)
			}
			first = false
		}
	}
}

func (c *Consumer) resetCursor(ctx context.Context, seq int64) error {
	zerolog.Ctx(ctx).Warn().Str("pds", c.remote.Host).Msgf("Cursor reset: %d -> %d", c.remote.Cursor, seq)
	err := c.db.Model(&c.remote).
		Where(&pds.PDS{ID: c.remote.ID}).
		Updates(&pds.PDS{FirstCursorSinceReset: seq}).Error
	if err != nil {
		return fmt.Errorf("updating FirstCursorSinceReset: %w", err)
	}
	c.remote.FirstCursorSinceReset = seq
	return nil
}

func (c *Consumer) updateCursor(ctx context.Context, seq int64) error {
	if math.Abs(float64(seq-c.remote.Cursor)) < 100000 && time.Since(c.lastCursorPersist) < 15*time.Second {
		c.remote.Cursor = seq
		return nil
	}

	err := c.db.Model(&c.remote).
		Where(&pds.PDS{ID: c.remote.ID}).
		Updates(&pds.PDS{Cursor: seq}).Error
	if err != nil {
		return fmt.Errorf("updating Cursor: %w", err)
	}
	c.remote.Cursor = seq
	c.lastCursorPersist = time.Now()
	return nil

}

func (c *Consumer) processMessage(ctx context.Context, typ string, r io.Reader, first bool) error {
	log := zerolog.Ctx(ctx)

	eventCounter.WithLabelValues(c.remote.Host, typ).Inc()

	switch typ {
	case "#commit":
		payload := &comatproto.SyncSubscribeRepos_Commit{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		exportEventTimestamp(ctx, c.remote.Host, payload.Time)

		if c.remote.FirstCursorSinceReset == 0 {
			if err := c.resetCursor(ctx, payload.Seq); err != nil {
				return fmt.Errorf("handling cursor reset: %w", err)
			}
		}

		repoInfo, created, err := repo.EnsureExists(ctx, c.db, payload.Repo)
		if err != nil {
			return fmt.Errorf("repo.EnsureExists(%q): %w", payload.Repo, err)
		}

		if repoInfo.LastKnownKey == "" {
			_, pubKey, err := resolver.GetPDSEndpointAndPublicKey(ctx, payload.Repo)
			if err != nil {
				return fmt.Errorf("failed to get DID doc for %q: %w", payload.Repo, err)
			}
			repoInfo.LastKnownKey = pubKey
			err = c.db.Model(repoInfo).Where(&repo.Repo{ID: repoInfo.ID}).Updates(&repo.Repo{LastKnownKey: pubKey}).Error
			if err != nil {
				return fmt.Errorf("failed to update the key for %q: %w", payload.Repo, err)
			}
		}

		if repoInfo.PDS != c.remote.ID {
			u, _, err := resolver.GetPDSEndpointAndPublicKey(ctx, payload.Repo)
			if err == nil {
				cur, err := pds.EnsureExists(ctx, c.db, u.String())
				if err == nil {
					if repoInfo.PDS != cur.ID {
						// Repo was migrated, lets update our record.
						err := c.db.Model(repoInfo).Where(&repo.Repo{ID: repoInfo.ID}).Updates(&repo.Repo{PDS: cur.ID}).Error
						if err != nil {
							log.Error().Err(err).Msgf("Repo %q was migrated to %q, but updating the repo has failed: %s", payload.Repo, cur.Host, err)
						}
					}
					repoInfo.PDS = cur.ID
				} else {
					log.Error().Err(err).Msgf("Failed to get PDS record for %q: %s", u, err)
				}
			} else {
				log.Error().Err(err).Msgf("Failed to get PDS endpoint for repo %q: %s", payload.Repo, err)
			}

			if repoInfo.PDS != c.remote.ID {
				// We checked a recent version of DID doc and this is still not a correct PDS.
				log.Error().Str("did", payload.Repo).Str("rev", payload.Rev).
					Msgf("Commit from an incorrect PDS, skipping")
				return nil
			}
		}
		if created {
			reposDiscovered.WithLabelValues(c.remote.Host).Inc()
		}

		expectRecords := false
		deletions := []string{}
		for _, op := range payload.Ops {
			switch op.Action {
			case "create":
				expectRecords = true
			case "update":
				expectRecords = true
			case "delete":
				deletions = append(deletions, op.Path)
			}
		}
		if c.recordsDB != nil && len(deletions) > 0 {
			for _, d := range deletions {
				parts := strings.SplitN(d, "/", 2)
				if len(parts) != 2 {
					continue
				}

				err := c.recordsDB.Query(qb.Insert("bluesky.records").
					Columns("repo", "collection", "rkey", "at_rev", "deleted", "created_at").
					ToCql()).WithContext(ctx).
					Bind(repoInfo.DID, parts[0], parts[1], payload.Rev, true, time.Now()).ExecRelease()
				if err != nil {
					return fmt.Errorf("failed to mark %s/%s as deleted: %w", payload.Repo, d, err)
				}
			}

		} else {
			for _, d := range deletions {
				parts := strings.SplitN(d, "/", 2)
				if len(parts) != 2 {
					continue
				}
				err := c.db.Model(&repo.Record{}).
					Where(&repo.Record{
						Repo:       models.ID(repoInfo.ID),
						Collection: parts[0],
						Rkey:       parts[1]}).
					Updates(&repo.Record{Deleted: true}).Error
				if err != nil {
					return fmt.Errorf("failed to mark %s/%s as deleted: %w", payload.Repo, d, err)
				}
			}
		}

		newRecs, err := repo.ExtractRecords(ctx, bytes.NewReader(payload.Blocks), repoInfo.LastKnownKey)
		if errors.Is(err, repo.ErrInvalidSignature) {
			// Key might have been updated recently.
			_, pubKey, err2 := resolver.GetPDSEndpointAndPublicKey(ctx, payload.Repo)
			if err2 != nil {
				return fmt.Errorf("failed to get DID doc for %q: %w", payload.Repo, err2)
			}
			if repoInfo.LastKnownKey != pubKey {
				repoInfo.LastKnownKey = pubKey
				err2 = c.db.Model(repoInfo).Where(&repo.Repo{ID: repoInfo.ID}).Updates(&repo.Repo{LastKnownKey: pubKey}).Error
				if err2 != nil {
					return fmt.Errorf("failed to update the key for %q: %w", payload.Repo, err2)
				}

				// Retry with the new key.
				newRecs, err = repo.ExtractRecords(ctx, bytes.NewReader(payload.Blocks), pubKey)
			}
		}

		if err != nil {
			return fmt.Errorf("failed to extract records: %w", err)
		}

		recs := []repo.Record{}
		for k, v := range newRecs {
			parts := strings.SplitN(k, "/", 2)
			if len(parts) != 2 {
				log.Warn().Msgf("Unexpected key format: %q", k)
				continue
			}
			if c.collectionBlacklist[parts[0]] {
				continue
			}
			langs, _, err := repo.GetLang(ctx, v)
			if err == nil {
				for _, lang := range langs {
					postsByLanguageIndexed.WithLabelValues(c.remote.Host, lang).Inc()
				}
			}
			rec := repo.Record{
				Repo:       models.ID(repoInfo.ID),
				Collection: parts[0],
				Rkey:       parts[1],
				Content:    v,
				AtRev:      payload.Rev,
			}
			if c.recordsDB == nil {
				// XXX: proper replacement of \u0000 would require full parsing of JSON
				// and recursive iteration over all string values, but this
				// should work well enough for now.
				rec.Content = fix.EscapeNullCharForPostgres(rec.Content)
			}
			recs = append(recs, rec)
		}
		if len(c.collectionBlacklist) == 0 && len(recs) == 0 && expectRecords {
			log.Debug().Int64("seq", payload.Seq).Str("pds", c.remote.Host).Msgf("len(recs) == 0")
		}
		if len(recs) > 0 {
			if c.recordsDB != nil {
				for _, r := range recs {
					err := c.recordsDB.Query(qb.Insert("bluesky.records").
						Columns("repo", "collection", "rkey", "at_rev", "record", "created_at").
						ToCql()).WithContext(ctx).
						Bind(repoInfo.DID, r.Collection, r.Rkey, r.AtRev, r.Content, time.Now()).ExecRelease()
					if err != nil {
						return fmt.Errorf("inserting record %s/%s/%s into the database: %w", repoInfo.DID, r.Collection, r.Rkey, err)
					}
				}

			} else {
				err = c.db.Model(&repo.Record{}).
					Clauses(clause.OnConflict{
						Where: clause.Where{Exprs: []clause.Expression{clause.Or(
							clause.Eq{Column: clause.Column{Name: "at_rev", Table: "records"}, Value: nil},
							clause.Eq{Column: clause.Column{Name: "at_rev", Table: "records"}, Value: ""},
							clause.Lt{
								Column: clause.Column{Name: "at_rev", Table: "records"},
								Value:  clause.Column{Name: "at_rev", Table: "excluded"}},
						)}},
						DoUpdates: clause.AssignmentColumns([]string{"content", "at_rev"}),
						Columns:   []clause.Column{{Name: "repo"}, {Name: "collection"}, {Name: "rkey"}}}).
					Create(recs).Error
				if err != nil {
					return fmt.Errorf("inserting records into the database: %w", err)
				}
			}
		}

		if repoInfo.FirstCursorSinceReset > 0 && repoInfo.FirstRevSinceReset != "" &&
			repoInfo.LastIndexedRev != "" &&
			c.remote.FirstCursorSinceReset > 0 &&
			repoInfo.FirstCursorSinceReset >= c.remote.FirstCursorSinceReset &&
			repoInfo.FirstRevSinceReset <= repoInfo.LastIndexedRev &&
			time.Since(repoInfo.UpdatedAt) > lastRevUpdateInterval {

			err = c.db.Model(&repo.Repo{}).Where(&repo.Repo{ID: repoInfo.ID}).
				Updates(&repo.Repo{
					LastFirehoseRev: payload.Rev,
				}).Error
			if err != nil {
				log.Error().Err(err).Msgf("Failed to update last_firehose_rev for %q: %s", repoInfo.DID, err)
			}
		}

		if payload.TooBig {
			// Just trigger a re-index by resetting rev.
			err := c.db.Model(&repo.Repo{}).Where(&repo.Repo{ID: repoInfo.ID}).
				Updates(&repo.Repo{
					FirstCursorSinceReset: c.remote.FirstCursorSinceReset,
					FirstRevSinceReset:    payload.Rev,
				}).Error
			if err != nil {
				return fmt.Errorf("failed to update repo info after cursor reset: %w", err)
			}
		}

		if repoInfo.FirstCursorSinceReset != c.remote.FirstCursorSinceReset {
			err := c.db.Model(&repo.Repo{}).Debug().Where(&repo.Repo{ID: repoInfo.ID}).
				Updates(&repo.Repo{
					FirstCursorSinceReset: c.remote.FirstCursorSinceReset,
					FirstRevSinceReset:    payload.Rev,
				}).Error
			if err != nil {
				return fmt.Errorf("failed to update repo info after cursor reset: %w", err)
			}
		}

		if err := c.updateCursor(ctx, payload.Seq); err != nil {
			return err
		}
	case "#handle":
		payload := &comatproto.SyncSubscribeRepos_Handle{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		exportEventTimestamp(ctx, c.remote.Host, payload.Time)

		if c.remote.FirstCursorSinceReset == 0 {
			if err := c.resetCursor(ctx, payload.Seq); err != nil {
				return fmt.Errorf("handling cursor reset: %w", err)
			}
		}
		// No-op, we don't store handles.
		if err := c.updateCursor(ctx, payload.Seq); err != nil {
			return err
		}
	case "#migrate":
		payload := &comatproto.SyncSubscribeRepos_Migrate{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		exportEventTimestamp(ctx, c.remote.Host, payload.Time)

		if c.remote.FirstCursorSinceReset == 0 {
			if err := c.resetCursor(ctx, payload.Seq); err != nil {
				return fmt.Errorf("handling cursor reset: %w", err)
			}
		}

		log.Debug().Interface("payload", payload).Str("did", payload.Did).Msgf("MIGRATION")
		// TODO
		if err := c.updateCursor(ctx, payload.Seq); err != nil {
			return err
		}
	case "#tombstone":
		payload := &comatproto.SyncSubscribeRepos_Tombstone{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		exportEventTimestamp(ctx, c.remote.Host, payload.Time)

		if c.remote.FirstCursorSinceReset == 0 {
			if err := c.resetCursor(ctx, payload.Seq); err != nil {
				return fmt.Errorf("handling cursor reset: %w", err)
			}
		}
		// TODO
		if err := c.updateCursor(ctx, payload.Seq); err != nil {
			return err
		}
	case "#info":
		payload := &comatproto.SyncSubscribeRepos_Info{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}
		switch payload.Name {
		case "OutdatedCursor":
			if !first {
				log.Warn().Msgf("Received cursor reset notification in the middle of a stream: %+v", payload)
			}
			c.remote.FirstCursorSinceReset = 0
		default:
			log.Error().Msgf("Unknown #info message %q: %+v", payload.Name, payload)
		}
	case "#identity":
		payload := &comatproto.SyncSubscribeRepos_Identity{}
		if err := payload.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("failed to unmarshal commit: %w", err)
		}

		exportEventTimestamp(ctx, c.remote.Host, payload.Time)
		log.Trace().Str("did", payload.Did).Str("type", typ).Int64("seq", payload.Seq).
			Msgf("#identity message: %s seq=%d time=%q", payload.Did, payload.Seq, payload.Time)

		resolver.Resolver.FlushCacheFor(payload.Did)

		// TODO: fetch DID doc and update PDS field?

	case "#account":
		// Ignore for now.

	default:
		b, err := io.ReadAll(r)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to read message payload: %s", err)
		}
		log.Warn().Msgf("Unknown message type received: %s payload=%q", typ, string(b))
	}
	return nil
}

type Header struct {
	Op   int64
	Type string
}

func parseHeader(node datamodel.Node) (Header, error) {
	r := Header{}
	op, err := node.LookupByString("op")
	if err != nil {
		return r, fmt.Errorf("missing 'op': %w", err)
	}
	r.Op, err = op.AsInt()
	if err != nil {
		return r, fmt.Errorf("op.AsInt(): %w", err)
	}
	if r.Op == -1 {
		// Error frame, type should not be present
		return r, nil
	}
	t, err := node.LookupByString("t")
	if err != nil {
		return r, fmt.Errorf("missing 't': %w", err)
	}
	r.Type, err = t.AsString()
	if err != nil {
		return r, fmt.Errorf("t.AsString(): %w", err)
	}
	return r, nil
}

func parseError(node datamodel.Node) (xrpc.XRPCError, error) {
	r := xrpc.XRPCError{}
	e, err := node.LookupByString("error")
	if err != nil {
		return r, fmt.Errorf("missing 'error': %w", err)
	}
	r.ErrStr, err = e.AsString()
	if err != nil {
		return r, fmt.Errorf("error.AsString(): %w", err)
	}
	m, err := node.LookupByString("message")
	if err == nil {
		r.Message, err = m.AsString()
		if err != nil {
			return r, fmt.Errorf("message.AsString(): %w", err)
		}
	} else if !errors.Is(err, datamodel.ErrNotExists{}) {
		return r, fmt.Errorf("looking up 'message': %w", err)
	}

	return r, nil
}

func exportEventTimestamp(ctx context.Context, remote string, timestamp string) {
	if t, err := time.Parse(time.RFC3339, timestamp); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Str("pds", remote).Msgf("Failed to parse %q as a timestamp: %s", timestamp, err)
	} else {
		lastEventTimestamp.WithLabelValues(remote).Set(float64(t.Unix()))
	}
}
