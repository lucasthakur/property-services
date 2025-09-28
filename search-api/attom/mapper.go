package attom

import (
	"encoding/json"
	"strconv"
)

// stringNumber accepts string or number JSON and stores as string
type stringNumber string

func (s *stringNumber) UnmarshalJSON(b []byte) error {
	// empty/null -> empty string
	if string(b) == "null" {
		*s = ""
		return nil
	}
	// If already a quoted string
	if len(b) > 0 && b[0] == '"' {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*s = stringNumber(str)
		return nil
	}
	// Try as number, keep textual form
	var num json.Number
	if err := json.Unmarshal(b, &num); err != nil {
		return err
	}
	*s = stringNumber(num.String())
	return nil
}

func MapSearchPayloadToCards(raw []byte) ([]PropertyCard, error) {
	// RapidAPI Realtor search payload: { count, properties: [ {...} ] }
	type rCoord struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	}
	type rAddr struct {
		City       string `json:"city"`
		State      string `json:"state"`
		StateCode  string `json:"state_code"`
		PostalCode string `json:"postal_code"`
		Line       string `json:"line"`
		Coordinate rCoord `json:"coordinate"`
	}
	type rDesc struct {
		Beds              int    `json:"beds"`
		BathsConsolidated string `json:"baths_consolidated"`
		Sqft              int    `json:"sqft"`
		Type              string `json:"type"`
	}
	type rPhoto struct {
		Href string `json:"href"`
	}
	type rProp struct {
		ListingID string `json:"listing_id"`
		ListPrice int    `json:"list_price"`
		Location  struct {
			Address rAddr `json:"address"`
		} `json:"location"`
		Description  rDesc    `json:"description"`
		PrimaryPhoto rPhoto   `json:"primary_photo"`
		Photos       []rPhoto `json:"photos"`
		Status       string   `json:"status"`
	}
	var root struct {
		Properties []rProp `json:"properties"`
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}

	out := make([]PropertyCard, 0, len(root.Properties))
	for _, p := range root.Properties {
		// baths
		baths := 0
		if p.Description.BathsConsolidated != "" {
			if i, err := strconv.Atoi(p.Description.BathsConsolidated); err == nil {
				baths = i
			}
		}
		// images (primary + inline photos)
		imgs := make([]string, 0, 1+len(p.Photos))
		if p.PrimaryPhoto.Href != "" {
			imgs = append(imgs, upgradePhotoURL(p.PrimaryPhoto.Href))
		}
		for _, ph := range p.Photos {
			if ph.Href != "" {
				imgs = append(imgs, upgradePhotoURL(ph.Href))
			}
		}

		state := p.Location.Address.StateCode
		if state == "" {
			state = p.Location.Address.State
		}

		out = append(out, PropertyCard{
			ID:        p.ListingID,
			Address:   p.Location.Address.Line,
			City:      p.Location.Address.City,
			State:     state,
			Zip:       p.Location.Address.PostalCode,
			Type:      p.Description.Type,
			Price:     p.ListPrice,
			Beds:      maxInt(p.Description.Beds, 0),
			Baths:     maxInt(baths, 0),
			Sqft:      maxInt(p.Description.Sqft, 0),
			YearBuilt: 0,
			Images:    imgs,
			Coords:    [2]float64{p.Location.Address.Coordinate.Lon, p.Location.Address.Coordinate.Lat},
			MLS:       "",
			Source:    "rapidapi",
		})
	}
	return out, nil
}

// MapListingPayloadToCards maps ATTOM Listing Snapshot payload to PropertyCard slice.
// This function is intentionally defensive to tolerate minor schema differences across plans.
func MapListingPayloadToCards(raw []byte) ([]PropertyCard, error) {
	// Same mapping as search for RapidAPI Realtor.
	return MapSearchPayloadToCards(raw)
}

func nonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
func maxInt(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}
