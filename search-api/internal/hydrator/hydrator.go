package hydrator

import (
    "context"
    "database/sql"

    "github.com/yourorg/search-api/attom"
    "github.com/yourorg/search-api/internal/events"
    "github.com/yourorg/search-api/internal/store"
)

type Hydrator struct {
    Store *store.Store
    Pub   events.Publisher
}

func (h *Hydrator) Enabled() bool { return h != nil && h.Store != nil }

func (h *Hydrator) Write(ctx context.Context, provider string, endpoint string, raw []byte, norm map[string]string, card attom.PropertyCard) error {
    if !h.Enabled() { return nil }
    in := store.UpsertInput{
        PropertyKey: norm["property_key"],
        Address1:    norm["line1"],
        City:        norm["city"],
        State:       norm["state"],
        Zip:         norm["zip"],
        Lat:         sqlNullFloat(card.Coords[1]),
        Lon:         sqlNullFloat(card.Coords[0]),
        Provider:    provider,
        SourceID:    card.ID,
        ListingID:   sqlNullString(card.ID),
        Status:      "for_sale",
        ListPrice:   sqlNullFloat64(float64(card.Price)),
        Beds:        sqlNullInt(int64(card.Beds)),
        Baths:       sqlNullFloat64(float64(card.Baths)),
        Sqft:        sqlNullInt(int64(card.Sqft)),
        Photos:      card.Images,
        Endpoint:    endpoint,
        ExternalID:  card.ID,
        PayloadJSON: raw,
    }
    res, err := h.Store.WriteSnapshotAndUpsert(ctx, in)
    if err != nil { return err }
    if h.Pub != nil {
        h.Pub.PublishPropertyUpdated(ctx, events.PropertyUpdated{PropertyID: res.PropertyID, PropertyKey: norm["property_key"]})
    }
    return nil
}

func sqlNullFloat(v float64) sql.NullFloat64 {
    if v == 0 { return sql.NullFloat64{} }
    return sql.NullFloat64{Float64: v, Valid: true}
}
func sqlNullFloat64(v float64) sql.NullFloat64 { return sqlNullFloat(v) }
func sqlNullInt(v int64) sql.NullInt64 {
    if v == 0 { return sql.NullInt64{} }
    return sql.NullInt64{Int64: v, Valid: true}
}
func sqlNullString(s string) sql.NullString {
    if s == "" { return sql.NullString{} }
    return sql.NullString{String: s, Valid: true}
}
