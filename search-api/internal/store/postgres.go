package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Store struct{ DB *sql.DB }

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &Store{DB: db}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.DB.PingContext(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
		`CREATE EXTENSION IF NOT EXISTS cube;`,
		`CREATE EXTENSION IF NOT EXISTS earthdistance;`,
		`CREATE TABLE IF NOT EXISTS ingest_properties (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            property_key    TEXT NOT NULL,
            address_line1   TEXT NOT NULL,
            city            TEXT NOT NULL,
            state           TEXT NOT NULL,
            zip             TEXT NOT NULL,
            lat             DOUBLE PRECISION,
            lon             DOUBLE PRECISION,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_fetch_at   TIMESTAMPTZ,
            stale_after     TIMESTAMPTZ
        );`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_properties_property_key ON ingest_properties(property_key);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_properties_geo ON ingest_properties USING GIST (ll_to_earth(lat, lon));`,
		`CREATE TABLE IF NOT EXISTS ingest_listings (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            property_id       UUID NOT NULL REFERENCES ingest_properties(id) ON DELETE CASCADE,
            provider          TEXT NOT NULL,
            source_id         TEXT NOT NULL,
            listing_id        TEXT,
            status            TEXT NOT NULL,
            list_price        NUMERIC,
            list_date         TIMESTAMPTZ,
            permalink         TEXT,
            mls_org_id        TEXT,
            beds              SMALLINT,
            baths             NUMERIC,
            sqft              INTEGER,
            lot_sqft          INTEGER,
            property_type     TEXT,
            flags             JSONB,
            agents            JSONB,
            extras            JSONB,
            coords            POINT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_fetch_at     TIMESTAMPTZ,
            stale_after       TIMESTAMPTZ
        );`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_listings_provider_ids ON ingest_listings(provider, source_id, listing_id);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_listings_property ON ingest_listings(property_id);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_listings_status ON ingest_listings(status);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_listings_list_date ON ingest_listings(list_date);`,
		`CREATE TABLE IF NOT EXISTS ingest_listing_photos (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            listing_id    UUID NOT NULL REFERENCES ingest_listings(id) ON DELETE CASCADE,
            href          TEXT NOT NULL,
            description   TEXT,
            media_type    TEXT,
            kind          TEXT,
            tags          JSONB,
            title         TEXT,
            position      INTEGER,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_listphotos_listing ON ingest_listing_photos(listing_id);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_listphotos_listing_href ON ingest_listing_photos(listing_id, href);`,
		`CREATE TABLE IF NOT EXISTS ingest_listing_photo_tags (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            photo_id UUID NOT NULL REFERENCES ingest_listing_photos(id) ON DELETE CASCADE,
            label    TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_listing_photo_tags_unique ON ingest_listing_photo_tags(photo_id, label);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_listing_photo_tags_photo ON ingest_listing_photo_tags(photo_id);`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS description TEXT;`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS media_type TEXT;`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS tags JSONB;`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS kind TEXT;`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS title TEXT;`,
		`ALTER TABLE ingest_listing_photos ADD COLUMN IF NOT EXISTS position INTEGER;`,
		`CREATE TABLE IF NOT EXISTS ingest_provider_raw_snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            provider       TEXT NOT NULL,
            endpoint       TEXT NOT NULL,
            external_id    TEXT,
            payload        JSONB NOT NULL,
            fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload_sha256 TEXT NOT NULL
        );`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_snapshots_provider ON ingest_provider_raw_snapshots(provider, endpoint, fetched_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_ingest_snapshots_external ON ingest_provider_raw_snapshots(provider, external_id);`,
		`CREATE TABLE IF NOT EXISTS ingest_hydrate_jobs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            idempotency_key  TEXT NOT NULL,
            provider         TEXT NOT NULL,
            endpoint         TEXT NOT NULL,
            external_id      TEXT,
            property_key     TEXT,
            scope            TEXT NOT NULL,
            state            TEXT NOT NULL,
            attempts         INT NOT NULL DEFAULT 0,
            last_error       TEXT,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        );`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ux_ingest_jobs_idem ON ingest_hydrate_jobs(idempotency_key);`,
	}
	for _, q := range stmts {
		if _, err := s.DB.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

type ListingPhotoInput struct {
	Href        string
	Description string
	Title       string
	Kind        string
	MediaType   string
	Tags        []string
	Position    int
}
type UpsertInput struct {
	PropertyKey string
	Address1    string
	City        string
	State       string
	Zip         string
	Lat         sql.NullFloat64
	Lon         sql.NullFloat64
	// Listing bits
	Provider  string
	SourceID  string
	ListingID sql.NullString
	Status    string
	ListPrice sql.NullFloat64
	Beds      sql.NullInt64
	Baths     sql.NullFloat64
	Sqft      sql.NullInt64
	Photos    []ListingPhotoInput
	// Raw snapshot
	Endpoint    string
	ExternalID  string
	PayloadJSON []byte
}

type UpsertResult struct {
	PropertyID string
	ListingID  string
}

type ListingRecord struct {
	PropertyKey       string
	AddressLine1      string
	City              string
	State             string
	Zip               string
	Lat               sql.NullFloat64
	Lon               sql.NullFloat64
	ListingID         string
	ListingExternalID sql.NullString
	ListPrice         sql.NullFloat64
	Beds              sql.NullInt64
	Baths             sql.NullFloat64
	Sqft              sql.NullInt64
	PropertyType      sql.NullString
	Photos            []string
}

func (s *Store) WriteSnapshotAndUpsert(ctx context.Context, in UpsertInput) (UpsertResult, error) {
	var res UpsertResult
	if s.DB == nil {
		return res, errors.New("nil db")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// ingest_properties upsert
	err = tx.QueryRowContext(ctx, `
        INSERT INTO ingest_properties (property_key, address_line1, city, state, zip, lat, lon, last_fetch_at, stale_after)
        VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now() + interval '5 minutes')
        ON CONFLICT (property_key)
        DO UPDATE SET address_line1=EXCLUDED.address_line1, city=EXCLUDED.city, state=EXCLUDED.state, zip=EXCLUDED.zip, lat=EXCLUDED.lat, lon=EXCLUDED.lon, updated_at=now(), last_fetch_at=now(), stale_after=now() + interval '5 minutes'
        RETURNING id`,
		in.PropertyKey, in.Address1, in.City, in.State, in.Zip, in.Lat, in.Lon,
	).Scan(&res.PropertyID)
	if err != nil {
		return res, err
	}

	// ingest_listings upsert
	err = tx.QueryRowContext(ctx, `
        INSERT INTO ingest_listings (property_id, provider, source_id, listing_id, status, list_price, beds, baths, sqft, coords, last_fetch_at, stale_after)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NULL, now(), now() + interval '5 minutes')
        ON CONFLICT (provider, source_id, listing_id)
        DO UPDATE SET property_id=EXCLUDED.property_id, status=EXCLUDED.status, list_price=EXCLUDED.list_price, beds=EXCLUDED.beds, baths=EXCLUDED.baths, sqft=EXCLUDED.sqft, updated_at=now(), last_fetch_at=now(), stale_after=now() + interval '5 minutes'
        RETURNING id`,
		res.PropertyID, in.Provider, in.SourceID, in.ListingID, in.Status, in.ListPrice, in.Beds, in.Baths, in.Sqft,
	).Scan(&res.ListingID)
	if err != nil {
		return res, err
	}

	if len(in.Photos) > 0 {
		if err = replaceListingPhotosTx(ctx, tx, res.ListingID, in.Photos); err != nil {
			return res, err
		}
	}

	// raw snapshot for ingestion audit
	sum := sha256.Sum256(in.PayloadJSON)
	sha := hex.EncodeToString(sum[:])
	if _, err = tx.ExecContext(ctx, `
        INSERT INTO ingest_provider_raw_snapshots (provider, endpoint, external_id, payload, payload_sha256)
        VALUES ($1,$2,$3,$4,$5)
    `, in.Provider, in.Endpoint, in.ExternalID, string(in.PayloadJSON), sha); err != nil {
		return res, err
	}

	err = tx.Commit()
	if err != nil {
		return res, err
	}
	return res, nil
}

func (s *Store) FetchListingsByPostal(ctx context.Context, postal string, limit, offset int, propertyType string) ([]ListingRecord, error) {
	if s.DB == nil {
		return nil, errors.New("nil db")
	}
	if limit <= 0 {
		limit = 5
	}
	if offset < 0 {
		offset = 0
	}
	args := []any{postal, limit, offset}
	query := strings.Builder{}
	query.WriteString(`
		SELECT p.property_key, p.address_line1, p.city, p.state, p.zip,
		       p.lat, p.lon, l.id, l.listing_id, l.list_price, l.beds, l.baths, l.sqft, l.property_type
		FROM ingest_properties p
		JOIN ingest_listings l ON l.property_id = p.id
		WHERE p.zip = $1
	`)
	if propertyType != "" {
		query.WriteString(" AND l.property_type = $4")
		args = append(args, propertyType)
	}
	query.WriteString(`
		ORDER BY l.updated_at DESC
		LIMIT $2 OFFSET $3
	`)
	rows, err := s.DB.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []ListingRecord
	for rows.Next() {
		var rec ListingRecord
		if err := rows.Scan(&rec.PropertyKey, &rec.AddressLine1, &rec.City, &rec.State, &rec.Zip,
			&rec.Lat, &rec.Lon, &rec.ListingID, &rec.ListingExternalID, &rec.ListPrice, &rec.Beds, &rec.Baths, &rec.Sqft, &rec.PropertyType); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return records, nil
	}
	placeholders := make([]string, len(records))
	photoArgs := make([]any, len(records))
	for i, rec := range records {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		photoArgs[i] = rec.ListingID
	}
	photoRows, err := s.DB.QueryContext(ctx,
		`SELECT listing_id, href FROM ingest_listing_photos WHERE listing_id IN (`+strings.Join(placeholders, ",")+`) ORDER BY listing_id, position`,
		photoArgs...,
	)
	if err != nil {
		return nil, err
	}
	defer photoRows.Close()
	photosByListing := make(map[string][]string)
	for photoRows.Next() {
		var listingID, href string
		if err := photoRows.Scan(&listingID, &href); err != nil {
			return nil, err
		}
		photosByListing[listingID] = append(photosByListing[listingID], href)
	}
	if err := photoRows.Err(); err != nil {
		return nil, err
	}
	for i := range records {
		records[i].Photos = photosByListing[records[i].ListingID]
	}
	return records, nil
}

func (s *Store) FetchListingPhotos(ctx context.Context, providerListingID string) ([]string, error) {
	if s.DB == nil {
		return nil, errors.New("nil db")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT lp.href
		FROM ingest_listings l
		JOIN ingest_listing_photos lp ON lp.listing_id = l.id
		WHERE l.listing_id = $1
		ORDER BY lp.position, lp.created_at
	`, providerListingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var photos []string
	for rows.Next() {
		var href string
		if err := rows.Scan(&href); err != nil {
			return nil, err
		}
		photos = append(photos, href)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return photos, nil
}

func (s *Store) ReplaceListingPhotos(ctx context.Context, providerListingID string, photos []ListingPhotoInput) error {
	if s.DB == nil {
		return errors.New("nil db")
	}
	var listingUUID string
	err := s.DB.QueryRowContext(ctx, `SELECT id FROM ingest_listings WHERE listing_id=$1 ORDER BY updated_at DESC LIMIT 1`, providerListingID).Scan(&listingUUID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = replaceListingPhotosTx(ctx, tx, listingUUID, photos); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) LookupPropertyKeyByListing(ctx context.Context, providerListingID string) (string, error) {
	if s.DB == nil {
		return "", errors.New("nil db")
	}
	var propertyKey string
	err := s.DB.QueryRowContext(ctx, `
		SELECT p.property_key
		FROM ingest_listings l
		JOIN ingest_properties p ON p.id = l.property_id
		WHERE l.listing_id = $1
		ORDER BY l.updated_at DESC
		LIMIT 1
	`, providerListingID).Scan(&propertyKey)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return propertyKey, nil
}

func replaceListingPhotosTx(ctx context.Context, tx *sql.Tx, listingUUID string, photos []ListingPhotoInput) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM ingest_listing_photos WHERE listing_id=$1`, listingUUID); err != nil {
		return err
	}
	for idx, photo := range photos {
		if photo.Href == "" {
			continue
		}
		position := photo.Position
		if position < 0 {
			position = idx
		}
		var tagsJSON any
		if len(photo.Tags) > 0 {
			b, err := json.Marshal(photo.Tags)
			if err != nil {
				return err
			}
			tagsJSON = b
		}
		var photoID string
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO ingest_listing_photos (listing_id, href, description, media_type, kind, tags, title, position)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			RETURNING id
		`,
			listingUUID,
			photo.Href,
			nullString(photo.Description),
			nullString(photo.MediaType),
			nullString(photo.Kind),
			tagsJSON,
			nullString(photo.Title),
			position,
		).Scan(&photoID); err != nil {
			return err
		}
		for _, label := range photo.Tags {
			if label == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO ingest_listing_photo_tags (photo_id, label)
				VALUES ($1,$2)
				ON CONFLICT (photo_id, label) DO NOTHING
			`, photoID, label); err != nil {
				return err
			}
		}
	}
	return nil
}

func nullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}
