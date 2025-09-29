package attom

type PropertyCard struct {
	ID         string     `json:"id"`
	ListingID  string     `json:"listingId,omitempty"`
	PropertyID string     `json:"propertyId,omitempty"`
	Address    string     `json:"address"`
	City       string     `json:"city"`
	State      string     `json:"state"`
	Zip        string     `json:"zip"`
	Type       string     `json:"type"`
	Price      int        `json:"price"` // prefer last sale or AVM if available
	Beds       int        `json:"beds"`
	Baths      int        `json:"baths"`
	Sqft       int        `json:"sqft"`
	YearBuilt  int        `json:"yearBuilt"`
	Images     []string   `json:"images"` // may be empty
	Coords     [2]float64 `json:"coords"` // [lng, lat]
	MLS        string     `json:"mls"`
	Source     string     `json:"source"` // e.g., "rapidapi"
}

type PhotoAsset struct {
	Href        string   `json:"href"`
	Description string   `json:"description,omitempty"`
	Title       string   `json:"title,omitempty"`
	Kind        string   `json:"type,omitempty"`
	MediaType   string   `json:"mediaType,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Position    int      `json:"position"`
}
