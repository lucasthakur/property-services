package attom

import (
	"encoding/json"
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
		if err := json.Unmarshal(b, &str); err != nil { return err }
		*s = stringNumber(str)
		return nil
	}
	// Try as number, keep textual form
	var num json.Number
	if err := json.Unmarshal(b, &num); err != nil { return err }
	*s = stringNumber(num.String())
	return nil
}

func MapSearchPayloadToCards(raw []byte) ([]PropertyCard, error) {
	// ATTOM payload shape differs by product; map defensively.
	// Expect something like { "property": [ { "address": {...}, "lot": {...}, "building": {...}, ... } ] }
	type aAddress struct {
		OneLine string `json:"oneLine"`
		Line1   string `json:"line1"`
		City    string `json:"locality"`
		State   string `json:"region"`
		Zip     string `json:"postal1"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	}
	type aBuilding struct {
		Rooms int `json:"rooms"`
		Beds  int `json:"bedrooms"`
		Baths struct {
			Full int `json:"full"`
			Total float64 `json:"baths"` // sometimes decimal
		} `json:"bathrooms"`
		Sqft int `json:"size"`
		Year int `json:"yearBuilt"`
	}

    type aProperty struct {
        Identifier struct {
            AttomID stringNumber `json:"attomId"`
            APN     stringNumber `json:"apn"`
            MLS     stringNumber `json:"mlsId"`
        } `json:"identifier"`
        Address  aAddress  `json:"address"`
        Building aBuilding `json:"building"`
        Sale     struct {
            Price int `json:"amount"`
        } `json:"sale"`
		AVM struct {
			Value int `json:"amount"`
		} `json:"avm"`
		Use struct {
			Type string `json:"propClass"`
		} `json:"use"`
	}

	var root struct {
		Property []aProperty `json:"property"`
	}
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}

	out := make([]PropertyCard, 0, len(root.Property))
	for _, p := range root.Property {
		price := p.Sale.Price
		if price == 0 && p.AVM.Value > 0 {
			price = p.AVM.Value
		}
		baths := int(p.Building.Baths.Total)
		if baths == 0 { baths = p.Building.Baths.Full }

        out = append(out, PropertyCard{
            ID:        firstNonEmpty(string(p.Identifier.AttomID), string(p.Identifier.APN), string(p.Identifier.MLS)),
            Address:   nonEmpty(p.Address.Line1, p.Address.OneLine),
            City:      p.Address.City,
            State:     p.Address.State,
            Zip:       p.Address.Zip,
            Type:      p.Use.Type,
			Price:     price,
			Beds:      maxInt(p.Building.Beds, 0),
			Baths:     maxInt(baths, 0),
			Sqft:      maxInt(p.Building.Sqft, 0),
			YearBuilt: maxInt(p.Building.Year, 0),
			Images:    []string{}, // ATTOM may not provide; fill from your CDN later
			Coords:    [2]float64{p.Address.Longitude, p.Address.Latitude},
			MLS:       string(p.Identifier.MLS),
			Source:    "attom",
		})
	}
	return out, nil
}

// MapListingPayloadToCards maps ATTOM Listing Snapshot payload to PropertyCard slice.
// This function is intentionally defensive to tolerate minor schema differences across plans.
func MapListingPayloadToCards(raw []byte) ([]PropertyCard, error) {

    type lAddress struct {
        OneLine   string   `json:"oneLine"`
        Line1     string   `json:"line1"`
        City      string   `json:"locality"`
        State     string   `json:"region"`
        Zip       string   `json:"postal1"`
        Latitude  float64  `json:"latitude"`
        Longitude float64  `json:"longitude"`
    }
    type lBuilding struct {
        Beds  int `json:"bedrooms"`
        Sqft  int `json:"size"`
        Year  int `json:"yearBuilt"`
        Baths struct {
            Total float64 `json:"baths"`
            Full  int     `json:"full"`
        } `json:"bathrooms"`
    }
    type lListing struct {
        ListPrice int `json:"listPrice"`
        Status    string `json:"status"`
        Photos    []struct{
            Href string `json:"href"`
        } `json:"photos"`
        Mls struct{
            Id string `json:"id"`
        } `json:"mls"`
    }
    type lProperty struct {
        Identifier struct {
            AttomID stringNumber `json:"attomId"`
            APN     stringNumber `json:"apn"`
            MLS     stringNumber `json:"mlsId"`
        } `json:"identifier"`
        Address  lAddress  `json:"address"`
        Building lBuilding `json:"building"`
        Listing  lListing  `json:"listing"`
    }
    var root struct {
        Property []lProperty `json:"property"`
    }
    if err := json.Unmarshal(raw, &root); err != nil { return nil, err }

    out := make([]PropertyCard, 0, len(root.Property))
    for _, p := range root.Property {
        baths := int(p.Building.Baths.Total)
        if baths == 0 { baths = p.Building.Baths.Full }
        imgs := make([]string, 0, len(p.Listing.Photos))
        for _, ph := range p.Listing.Photos { if ph.Href != "" { imgs = append(imgs, ph.Href) } }
        out = append(out, PropertyCard{
            ID:        firstNonEmpty(string(p.Identifier.AttomID), string(p.Identifier.APN), string(p.Identifier.MLS), p.Listing.Mls.Id),
            Address:   nonEmpty(p.Address.Line1, p.Address.OneLine),
            City:      p.Address.City,
            State:     p.Address.State,
            Zip:       p.Address.Zip,
            Type:      "listing",
            Price:     p.Listing.ListPrice,
            Beds:      maxInt(p.Building.Beds, 0),
            Baths:     maxInt(baths, 0),
            Sqft:      maxInt(p.Building.Sqft, 0),
            YearBuilt: maxInt(p.Building.Year, 0),
            Images:    imgs,
            Coords:    [2]float64{p.Address.Longitude, p.Address.Latitude},
            MLS:       string(p.Identifier.MLS),
            Source:    "attom_listings",
        })
    }
    return out, nil
}

func nonEmpty(a, b string) string {
	if a != "" { return a }
	return b
}
func firstNonEmpty(vals ...string) string {
	for _, v := range vals { if v != "" { return v } }
	return ""
}
func maxInt(v, def int) int {
	if v > 0 { return v }
	return def
}
