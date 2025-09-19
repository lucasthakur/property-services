package httpapi

import (
    "encoding/json"
    "net/http"
    "strconv"

    "github.com/go-chi/chi/v5"
    "github.com/go-chi/render"
    "github.com/yourorg/search-api/attom"
)

type ListingsDeps struct {
    ATTOM *attom.Client
}

type ListingsRequest struct {
    PostalCode   string `json:"postalcode,omitempty"`
    PropertyType string `json:"property_type,omitempty"`
    OrderBy      string `json:"orderby,omitempty"`
    Limit        *int   `json:"limit,omitempty"`   // pagesize
    Page         *int   `json:"page,omitempty"`
    Beds         *int   `json:"beds,omitempty"`
    Baths        *int   `json:"baths,omitempty"`
    MinPrice     *int   `json:"minprice,omitempty"`
    MaxPrice     *int   `json:"maxprice,omitempty"`
}

// use defInt from search_handler.go (same package)

func RegisterListings(r chi.Router, d ListingsDeps) {
    // POST JSON
    r.Post("/search/listings", func(w http.ResponseWriter, req *http.Request) {
        var body ListingsRequest
        if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
            render.Status(req, http.StatusBadRequest)
            _ = json.NewEncoder(w).Encode(map[string]any{"error":"invalid_json","detail":err.Error()})
            return
        }
        handleListingsRequest(w, req, d, body)
    })

    // GET query
    r.Get("/search/listings", func(w http.ResponseWriter, req *http.Request) {
        q := req.URL.Query()
        var body ListingsRequest
        body.PostalCode = q.Get("postalcode")
        body.PropertyType = q.Get("property_type")
        body.OrderBy = q.Get("orderby")
        if v := q.Get("limit"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Limit = &i } }
        if v := q.Get("page"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Page = &i } }
        if v := q.Get("beds"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Beds = &i } }
        if v := q.Get("baths"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Baths = &i } }
        if v := q.Get("minprice"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.MinPrice = &i } }
        if v := q.Get("maxprice"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.MaxPrice = &i } }
        handleListingsRequest(w, req, d, body)
    })
}

func handleListingsRequest(w http.ResponseWriter, req *http.Request, d ListingsDeps, body ListingsRequest) {
    if body.PostalCode == "" {
        render.Status(req, http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]any{"error":"postalcode_required"})
        return
    }
    // Default to 5 listings as requested
    pagesize := defInt(body.Limit, 5)
    page := defInt(body.Page, 1)
    beds := defInt(body.Beds, 0)
    baths := defInt(body.Baths, 0)
    minp := defInt(body.MinPrice, 0)
    maxp := defInt(body.MaxPrice, 0)

    raw, err := d.ATTOM.SearchListingsByPostal(req.Context(), body.PostalCode, pagesize, page, beds, baths, minp, maxp, body.PropertyType, body.OrderBy)
    if err != nil {
        render.Status(req, http.StatusBadGateway)
        _ = json.NewEncoder(w).Encode(map[string]any{"error":"upstream_error","detail":err.Error()})
        return
    }
    cards, err := attom.MapListingPayloadToCards(raw)
    if err != nil {
        render.Status(req, http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]any{"error":"map_error","detail":err.Error()})
        return
    }
    // Hydrate photos only when images are missing to avoid 429s
    for i := range cards {
        if cards[i].ID == "" { continue }
        if len(cards[i].Images) > 0 { continue }
        photos, err := d.ATTOM.GetPhotos(req.Context(), cards[i].ID)
        if err == nil && len(photos) > 0 {
            cards[i].Images = photos
        }
    }
    render.JSON(w, req, map[string]any{"ok":true, "count": len(cards), "properties": cards})
}
