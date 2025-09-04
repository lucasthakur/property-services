package httpapi

import (
    "encoding/json"
    "net/http"
    "strconv"

    "github.com/go-chi/chi/v5"
    "github.com/go-chi/render"
    "github.com/yourorg/search-api/attom"
)

type SearchDeps struct {
    ATTOM *attom.Client
}

type SearchRequest struct {
    // Postal-based search (preferred)
    PostalCode   string  `json:"postalcode,omitempty"`
    PropertyType string  `json:"property_type,omitempty"`
    OrderBy      string  `json:"orderby,omitempty"`
    Limit        *int    `json:"limit,omitempty"` // maps to pagesize
    Page         *int    `json:"page,omitempty"`

    // Legacy radius fields (optional fallback)
    Lat          *float64 `json:"lat,omitempty"`
    Lon          *float64 `json:"lon,omitempty"`
    Radius       *float64 `json:"radius,omitempty"` // miles
}

func defFloat(v *float64, d float64) float64 { if v == nil { return d }; return *v }
func defInt(v *int, d int) int            { if v == nil { return d }; return *v }

func RegisterSearch(r chi.Router, d SearchDeps) {
    // POST: JSON body
    r.Post("/search", func(w http.ResponseWriter, req *http.Request) {
        var body SearchRequest
        if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
            render.Status(req, http.StatusBadRequest)
            _ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_json", "detail": err.Error()})
            return
        }
        handleSearchRequest(w, req, d, body)
    })

    // GET: query params (compatibility)
    r.Get("/search", func(w http.ResponseWriter, req *http.Request) {
        q := req.URL.Query()
        var body SearchRequest
        // Postal-based
        body.PostalCode = q.Get("postalcode")
        if v := q.Get("limit"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Limit = &i } }
        if v := q.Get("page"); v != "" { if i, err := strconv.Atoi(v); err == nil { body.Page = &i } }
        body.PropertyType = q.Get("property_type")
        body.OrderBy = q.Get("orderby")

        // Legacy radius (optional)
        if v := q.Get("lat"); v != "" { if f, err := strconv.ParseFloat(v, 64); err == nil { body.Lat = &f } }
        if v := q.Get("lon"); v != "" { if f, err := strconv.ParseFloat(v, 64); err == nil { body.Lon = &f } }
        if v := q.Get("radius"); v != "" { if f, err := strconv.ParseFloat(v, 64); err == nil { body.Radius = &f } }
        handleSearchRequest(w, req, d, body)
    })
}

func handleSearchRequest(w http.ResponseWriter, req *http.Request, d SearchDeps, body SearchRequest) {
    // Prefer postal-based search
    if body.PostalCode != "" {
        pagesize := defInt(body.Limit, 40)
        page := defInt(body.Page, 1)
        raw, err := d.ATTOM.SearchByPostal(req.Context(), body.PostalCode, pagesize, page, body.PropertyType, body.OrderBy)
        if err != nil {
            render.Status(req, http.StatusBadGateway)
            _ = json.NewEncoder(w).Encode(map[string]any{"error": "upstream_error", "detail": err.Error()})
            return
        }
        cards, err := attom.MapSearchPayloadToCards(raw)
        if err != nil {
            render.Status(req, http.StatusInternalServerError)
            _ = json.NewEncoder(w).Encode(map[string]any{"error": "map_error", "detail": err.Error()})
            return
        }
        render.JSON(w, req, map[string]any{
            "ok":         true,
            "count":      len(cards),
            "properties": cards,
        })
        return
    }

    // Legacy radius fallback
    if body.Lat == nil || body.Lon == nil {
        render.Status(req, http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "postalcode_required", "detail": "postalcode is required"})
        return
    }
    lat := *body.Lat
    lon := *body.Lon
    radius := defFloat(body.Radius, 0.5)
    limit := defInt(body.Limit, 40)
    raw, err := d.ATTOM.SearchByRadius(req.Context(), lat, lon, radius, limit, 0, 0, 0, 0, "")
    if err != nil {
        render.Status(req, http.StatusBadGateway)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "upstream_error", "detail": err.Error()})
        return
    }
    cards, err := attom.MapSearchPayloadToCards(raw)
    if err != nil {
        render.Status(req, http.StatusInternalServerError)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "map_error", "detail": err.Error()})
        return
    }
    render.JSON(w, req, map[string]any{
        "ok":         true,
        "count":      len(cards),
        "properties": cards,
    })
}

