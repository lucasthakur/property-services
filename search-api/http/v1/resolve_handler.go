package v1

import (
    "context"
    "encoding/json"
    "net/http"
    "time"

    "github.com/go-chi/chi/v5"
    "github.com/go-chi/render"
    "github.com/yourorg/search-api/attom"
    "github.com/yourorg/search-api/internal/canon"
    "github.com/yourorg/search-api/internal/redisx"
    "github.com/yourorg/search-api/internal/hydrator"
)

type ResolveDeps struct {
    Redis          *redisx.Client
    Rapid          *attom.Client
    Refetch        func(propertyKey, line1, city, state, zip string)
    Hydrator       *hydrator.Hydrator
    // TTL and staleness tuning
    CacheTTL       time.Duration
    StaleAfter     time.Duration
    NegativeTTL    time.Duration
}

type ResolveRequest struct {
    Address string `json:"address"`
    City    string `json:"city"`
    State   string `json:"state"`
    Zip     string `json:"zip"`
}

type cachedEnvelope struct {
    Data any `json:"data"`
    Meta struct {
        LastFetch  time.Time `json:"last_fetch_at"`
        StaleAfter time.Time `json:"stale_after"`
        TTLSeconds int       `json:"ttl_seconds"`
        Source     string    `json:"source"`
    } `json:"meta"`
    Norm struct {
        Line1 string `json:"line1"`
        City  string `json:"city"`
        State string `json:"state"`
        Zip   string `json:"zip"`
    } `json:"normalized"`
}

func RegisterResolve(r chi.Router, d ResolveDeps) {
    r.Route("/v1/properties", func(r chi.Router) {
        r.Post("/resolve", func(w http.ResponseWriter, req *http.Request) {
            var body ResolveRequest
            if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
                render.Status(req, http.StatusBadRequest)
                _ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_json", "detail": err.Error()})
                return
            }
            resolve(w, req, d, body)
        })
        r.Get("/resolve", func(w http.ResponseWriter, req *http.Request) {
            q := req.URL.Query()
            body := ResolveRequest{
                Address: q.Get("address"),
                City:    q.Get("city"),
                State:   q.Get("state"),
                Zip:     q.Get("zip"),
            }
            resolve(w, req, d, body)
        })
    })
}

func resolve(w http.ResponseWriter, req *http.Request, d ResolveDeps, body ResolveRequest) {
    if body.Address == "" || body.City == "" || body.State == "" || body.Zip == "" {
        render.Status(req, http.StatusBadRequest)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "address_required", "detail": "address, city, state, zip are required"})
        return
    }
    line1, city, st, zip, pkey := canon.Canonicalize(body.Address, body.City, body.State, body.Zip)
    ctx := req.Context()
    missKey := "prop:miss:" + pkey
    cacheKey := "prop:pk:" + pkey

    if ok, _ := d.Redis.Exists(ctx, missKey); ok {
        render.Status(req, http.StatusNotFound)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "not_found", "property_key": pkey, "cache_miss_cooldown": true})
        return
    }

    if val, err := d.Redis.Get(ctx, cacheKey); err == nil && val != "" {
        var env cachedEnvelope
        if err := json.Unmarshal([]byte(val), &env); err == nil {
            stale := time.Now().After(env.Meta.StaleAfter)
            // fire-and-forget background refresh if stale
            if stale && d.Refetch != nil { d.Refetch(pkey, line1, city, st, zip) }
            // Serve cached immediately
            render.JSON(w, req, map[string]any{
                "ok":           true,
                "source":       "cache",
                "stale":        stale,
                "property_key": pkey,
                "normalized":   map[string]string{"line1": line1, "city": city, "state": st, "zip": zip},
                "data":         env.Data,
            })
            return
        }
    }

    // Cache miss: attempt a short lock to avoid stampedes
    if ok, _ := d.Redis.SetNX(ctx, "prop:lock:"+pkey, "1", 8*time.Second); !ok {
        render.Status(req, http.StatusAccepted)
        _ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "in_progress": true, "property_key": pkey})
        return
    }

    // Cache miss and lock acquired: do a best-effort fetch via RapidAPI provider
    raw, data, found := fetchResolveRaw(ctx, d.Rapid, zip, line1, city, st)
    if !found {
        _ = d.Redis.Set(ctx, missKey, "1", d.NegativeTTL)
        render.Status(req, http.StatusNotFound)
        _ = json.NewEncoder(w).Encode(map[string]any{"error": "not_found", "property_key": pkey})
        return
    }
    env := cachedEnvelope{Data: data}
    env.Meta.LastFetch = time.Now()
    env.Meta.StaleAfter = env.Meta.LastFetch.Add(maxDur(d.StaleAfter, 5*time.Minute))
    env.Meta.TTLSeconds = int(maxDur(d.CacheTTL, time.Hour).Seconds())
    env.Meta.Source = "rapidapi"
    env.Norm.Line1, env.Norm.City, env.Norm.State, env.Norm.Zip = line1, city, st, zip
    b, _ := json.Marshal(env)
    _ = d.Redis.Set(ctx, cacheKey, string(b), time.Duration(env.Meta.TTLSeconds)*time.Second)

    // Optional write-behind: persist and publish
    if d.Hydrator != nil {
        norm := map[string]string{"line1": line1, "city": city, "state": st, "zip": zip, "property_key": pkey}
        if card, ok := data.(attom.PropertyCard); ok {
            _ = d.Hydrator.Write(req.Context(), "rapidapi.realtor16", "search/forsale", raw, norm, card)
        }
    }

    render.JSON(w, req, map[string]any{
        "ok":           true,
        "source":       "fresh",
        "stale":        false,
        "property_key": pkey,
        "normalized":   map[string]string{"line1": line1, "city": city, "state": st, "zip": zip},
        "data":         data,
    })
}

// fetchResolve uses a ZIP search and filters by normalized address to find a match.
func fetchResolveRaw(ctx context.Context, rapid *attom.Client, zip string, line1 string, city string, state string) ([]byte, any, bool) {
    raw, err := rapid.SearchByPostal(ctx, zip, 20, 1, "", "")
    if err != nil { return nil, nil, false }
    cards, err := attom.MapSearchPayloadToCards(raw)
    if err != nil { return nil, nil, false }
    n1, c, st, _, _ := canon.Canonicalize(line1, city, state, zip)
    for _, card := range cards {
        ln1, cy, st2, _, _ := canon.Canonicalize(card.Address, card.City, card.State, card.Zip)
        if ln1 == n1 && cy == c && st2 == st { return raw, card, true }
    }
    // not found in first page; give up for now to avoid heavy quota
    return raw, nil, false
}

func maxDur(a, b time.Duration) time.Duration { if a > 0 { return a }; return b }
