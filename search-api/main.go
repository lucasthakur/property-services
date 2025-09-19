package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/yourorg/search-api/attom"
    "github.com/yourorg/search-api/internal/canon"
    "github.com/yourorg/search-api/internal/env"
    "github.com/yourorg/search-api/internal/events"
    "github.com/yourorg/search-api/internal/hydrator"
    "github.com/yourorg/search-api/internal/logger"
    "github.com/yourorg/search-api/internal/redisx"
    "github.com/yourorg/search-api/internal/refresh"
    "github.com/yourorg/search-api/internal/search"
    "github.com/yourorg/search-api/internal/store"
    httpv1 "github.com/yourorg/search-api/http/v1"
)

func main() {
    port := env.GetInt("PORT", 4002)
    apiKey := env.Must("RAPIDAPI_KEY")

    attomClient := attom.NewClient(apiKey)

    // Redis setup
    redisAddr := env.Get("REDIS_ADDR", "127.0.0.1:6379")
    redisPass := env.Get("REDIS_PASSWORD", "")
    redisDB := env.GetInt("REDIS_DB", 0)
    rdb := redisx.New(redisAddr, redisPass, redisDB)
    if err := rdb.Ping(reqCtx()); err != nil {
        log.Printf("warning: redis ping failed: %v", err)
    }

    // Optional Postgres + events + indexer
    var pgStore *store.Store
    if dsn := os.Getenv("PG_DSN"); dsn != "" {
        s, err := store.Open(dsn)
        if err != nil { log.Printf("postgres open error: %v", err) } else {
            pgStore = s
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            _ = s.Ping(ctx)
            _ = s.Migrate(ctx)
            cancel()
        }
    }
    pub := events.NewInMemory(256)
    if os.Getenv("ENABLE_INDEXER") == "1" {
        go (&search.Indexer{Pub: pub}).Run(context.Background())
    }
    var hydr *hydrator.Hydrator
    if pgStore != nil {
        hydr = &hydrator.Hydrator{Store: pgStore, Pub: pub}
    }

    // Background refresher: resolves stale keys via RapidAPI and writes back into Redis
    ref := refresh.New(256, 2, func(ctx context.Context, j refresh.Job) {
        // Background refresh: run a ZIP search and filter, then upsert cache
        // j.PropertyKey is used for the cache key
        cacheKey := "prop:pk:" + j.PropertyKey
        // We don't have normalized fields on the job in this simple struct, so this Do function is shadowed by the closure below.
        _ = rdb.Set(ctx, cacheKey+":touch", time.Now().Format(time.RFC3339), 5*time.Second)
    })

    deps := httpv1.ResolveDeps{
        Redis:       rdb,
        Rapid:       attomClient,
        Refetch:     func(pk, line1, city, state, zip string) {
            // Enqueue a job that will perform the refresh inline here using a goroutine, to avoid changing refresh.Job shape.
            go func() {
                ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
                defer cancel()
                // Fetch fresh
                raw, err := attomClient.SearchByPostal(ctx, zip, 20, 1, "", "")
                if err != nil { return }
                cards, err := attom.MapSearchPayloadToCards(raw)
                if err != nil { return }
                var found any
                var foundCard attom.PropertyCard
                for _, c := range cards {
                    // match by canonicalized address
                    ln1, cy, st2, _, _ := canon.Canonicalize(c.Address, c.City, c.State, c.Zip)
                    ln1q, cyq, stq, _, _ := canon.Canonicalize(line1, city, state, zip)
                    if ln1 == ln1q && cy == cyq && st2 == stq { found = c; foundCard = c; break }
                }
                if found == nil { return }
                // Write back to Redis with SWR envelope
                env := struct {
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
                }{Data: found}
                env.Meta.LastFetch = time.Now()
                env.Meta.StaleAfter = env.Meta.LastFetch.Add(5 * time.Minute)
                env.Meta.TTLSeconds = int((time.Hour).Seconds())
                env.Meta.Source = "rapidapi"
                env.Norm.Line1, env.Norm.City, env.Norm.State, env.Norm.Zip = line1, city, state, zip
                b, _ := json.Marshal(env)
                _ = rdb.Set(ctx, "prop:pk:"+pk, string(b), time.Hour)

                // Optional write-behind
                if hydr != nil {
                    norm := map[string]string{"line1": env.Norm.Line1, "city": env.Norm.City, "state": env.Norm.State, "zip": env.Norm.Zip, "property_key": pk}
                    _ = hydr.Write(ctx, "rapidapi.realtor16", "search/forsale", raw, norm, foundCard)
                }
            }()
            // also mark the job de-dup queue so the generic refresher doesn't enqueue duplicate work
            ref.Enqueue(refresh.Job{PropertyKey: pk})
        },
        CacheTTL:    time.Hour,
        StaleAfter:  5 * time.Minute,
        NegativeTTL: 60 * time.Second,
        Hydrator:    hydr,
    }

    router := BuildRouter(attomClient, deps)

    log.Printf("search-api listening on :%d", port)
	if err := http.ListenAndServe((":" + os.Getenv("PORT")), logger.Middleware(router)); err != nil {
		log.Fatal(err)
	}
}

// reqCtx returns a short-lived context for setup checks.
func reqCtx() context.Context { return context.TODO() }
