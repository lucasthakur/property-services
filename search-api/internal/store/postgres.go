package store

import (
    "context"
    "crypto/sha256"
    "database/sql"
    "encoding/hex"
    "errors"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
)

type Store struct { DB *sql.DB }

func Open(dsn string) (*Store, error) {
    db, err := sql.Open("pgx", dsn)
    if err != nil { return nil, err }
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
        `CREATE TABLE IF NOT EXISTS properties (
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
        `CREATE UNIQUE INDEX IF NOT EXISTS ux_properties_property_key ON properties(property_key);`,
        `CREATE INDEX IF NOT EXISTS idx_properties_geo ON properties USING GIST (ll_to_earth(lat, lon));`,
        `CREATE TABLE IF NOT EXISTS listings (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            property_id       UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
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
        `CREATE UNIQUE INDEX IF NOT EXISTS ux_listings_provider_ids ON listings(provider, source_id, listing_id);`,
        `CREATE INDEX IF NOT EXISTS idx_listings_property ON listings(property_id);`,
        `CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);`,
        `CREATE INDEX IF NOT EXISTS idx_listings_list_date ON listings(list_date);`,
        `CREATE TABLE IF NOT EXISTS listing_photos (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            listing_id    UUID NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
            href          TEXT NOT NULL,
            kind          TEXT,
            tags          JSONB,
            title         TEXT,
            position      INTEGER,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );`,
        `CREATE INDEX IF NOT EXISTS idx_listphotos_listing ON listing_photos(listing_id);`,
        `CREATE TABLE IF NOT EXISTS provider_raw_snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            provider       TEXT NOT NULL,
            endpoint       TEXT NOT NULL,
            external_id    TEXT,
            payload        JSONB NOT NULL,
            fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload_sha256 TEXT NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS idx_snapshots_provider ON provider_raw_snapshots(provider, endpoint, fetched_at DESC);`,
        `CREATE INDEX IF NOT EXISTS idx_snapshots_external ON provider_raw_snapshots(provider, external_id);`,
        `CREATE TABLE IF NOT EXISTS hydrate_jobs (
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
        `CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_idem ON hydrate_jobs(idempotency_key);`,
    }
    for _, q := range stmts {
        if _, err := s.DB.ExecContext(ctx, q); err != nil { return err }
    }
    return nil
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
    Provider    string
    SourceID    string
    ListingID   sql.NullString
    Status      string
    ListPrice   sql.NullFloat64
    Beds        sql.NullInt64
    Baths       sql.NullFloat64
    Sqft        sql.NullInt64
    Photos      []string
    // Raw snapshot
    Endpoint    string
    ExternalID  string
    PayloadJSON []byte
}

type UpsertResult struct {
    PropertyID string
    ListingID  string
}

func (s *Store) WriteSnapshotAndUpsert(ctx context.Context, in UpsertInput) (UpsertResult, error) {
    var res UpsertResult
    if s.DB == nil { return res, errors.New("nil db") }
    tx, err := s.DB.BeginTx(ctx, nil)
    if err != nil { return res, err }
    defer func() { if err != nil { _ = tx.Rollback() } }()

    // properties upsert
    err = tx.QueryRowContext(ctx, `
        INSERT INTO properties (property_key, address_line1, city, state, zip, lat, lon, last_fetch_at, stale_after)
        VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now() + interval '5 minutes')
        ON CONFLICT (property_key)
        DO UPDATE SET address_line1=EXCLUDED.address_line1, city=EXCLUDED.city, state=EXCLUDED.state, zip=EXCLUDED.zip, lat=EXCLUDED.lat, lon=EXCLUDED.lon, updated_at=now(), last_fetch_at=now(), stale_after=now() + interval '5 minutes'
        RETURNING id`,
        in.PropertyKey, in.Address1, in.City, in.State, in.Zip, in.Lat, in.Lon,
    ).Scan(&res.PropertyID)
    if err != nil { return res, err }

    // listings upsert
    err = tx.QueryRowContext(ctx, `
        INSERT INTO listings (property_id, provider, source_id, listing_id, status, list_price, beds, baths, sqft, coords, last_fetch_at, stale_after)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NULL, now(), now() + interval '5 minutes')
        ON CONFLICT (provider, source_id, listing_id)
        DO UPDATE SET property_id=EXCLUDED.property_id, status=EXCLUDED.status, list_price=EXCLUDED.list_price, beds=EXCLUDED.beds, baths=EXCLUDED.baths, sqft=EXCLUDED.sqft, updated_at=now(), last_fetch_at=now(), stale_after=now() + interval '5 minutes'
        RETURNING id`,
        res.PropertyID, in.Provider, in.SourceID, in.ListingID, in.Status, in.ListPrice, in.Beds, in.Baths, in.Sqft,
    ).Scan(&res.ListingID)
    if err != nil { return res, err }

    // photos: replace current set with new set
    if _, err = tx.ExecContext(ctx, `DELETE FROM listing_photos WHERE listing_id=$1`, res.ListingID); err != nil { return res, err }
    for i, href := range in.Photos {
        if href == "" { continue }
        if _, err = tx.ExecContext(ctx, `INSERT INTO listing_photos (listing_id, href, position) VALUES ($1,$2,$3)`, res.ListingID, href, i); err != nil { return res, err }
    }

    // raw snapshot
    sum := sha256.Sum256(in.PayloadJSON)
    sha := hex.EncodeToString(sum[:])
    if _, err = tx.ExecContext(ctx, `
        INSERT INTO provider_raw_snapshots (provider, endpoint, external_id, payload, payload_sha256)
        VALUES ($1,$2,$3,$4,$5)
    `, in.Provider, in.Endpoint, in.ExternalID, string(in.PayloadJSON), sha); err != nil { return res, err }

    err = tx.Commit()
    if err != nil { return res, err }
    return res, nil
}
