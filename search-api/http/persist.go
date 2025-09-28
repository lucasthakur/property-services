package httpapi

import (
	"context"
	"math"

	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/internal/canon"
	"github.com/yourorg/search-api/internal/hydrator"
	"github.com/yourorg/search-api/internal/store"
)

func persistCards(ctx context.Context, hydr *hydrator.Hydrator, endpoint string, raw []byte, cards []attom.PropertyCard) {
	if hydr == nil || len(cards) == 0 {
		return
	}
	for _, card := range cards {
		if card.Address == "" || card.City == "" || card.State == "" || card.Zip == "" {
			continue
		}
		line1, city, st, zip, pk := canon.Canonicalize(card.Address, card.City, card.State, card.Zip)
		if pk == "" {
			continue
		}
		norm := map[string]string{
			"line1":        line1,
			"city":         city,
			"state":        st,
			"zip":          zip,
			"property_key": pk,
		}
		_ = hydr.Write(ctx, "rapidapi.realtor16", endpoint, raw, norm, card)
	}
}

func recordsToCards(records []store.ListingRecord) []attom.PropertyCard {
	cards := make([]attom.PropertyCard, 0, len(records))
	for _, rec := range records {
		var card attom.PropertyCard
		if rec.ListingExternalID.Valid && rec.ListingExternalID.String != "" {
			card.ID = rec.ListingExternalID.String
		} else {
			card.ID = rec.PropertyKey
		}
		card.Address = rec.AddressLine1
		card.City = rec.City
		card.State = rec.State
		card.Zip = rec.Zip
		if rec.PropertyType.Valid {
			card.Type = rec.PropertyType.String
		}
		if rec.ListPrice.Valid {
			card.Price = int(math.Round(rec.ListPrice.Float64))
		}
		if rec.Beds.Valid {
			card.Beds = int(rec.Beds.Int64)
		}
		if rec.Baths.Valid {
			card.Baths = int(math.Round(rec.Baths.Float64))
		}
		if rec.Sqft.Valid {
			card.Sqft = int(rec.Sqft.Int64)
		}
		if rec.Lon.Valid || rec.Lat.Valid {
			card.Coords = [2]float64{rec.Lon.Float64, rec.Lat.Float64}
		}
		if len(rec.Photos) > 0 {
			card.Images = append([]string(nil), rec.Photos...)
		}
		card.Source = "database"
		cards = append(cards, card)
	}
	return cards
}
