package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/internal/env"
	"github.com/yourorg/search-api/internal/events"
	"github.com/yourorg/search-api/internal/hydrator"
	"github.com/yourorg/search-api/internal/store"
)

func main() {
	apiKey := env.Must("RAPIDAPI_KEY")
	dsn := env.Must("PG_DSN")

	zips := splitList(os.Getenv("HYDRATOR_ZIPS"))
	if len(zips) == 0 {
		log.Fatal("HYDRATOR_ZIPS must be provided")
	}

	interval := parseDuration(os.Getenv("HYDRATOR_INTERVAL"), 6*time.Hour)
	pageSize := parseInt(os.Getenv("HYDRATOR_PAGE_SIZE"), 50)
	maxPages := parseInt(os.Getenv("HYDRATOR_MAX_PAGES"), 5)
	pause := parseDuration(os.Getenv("HYDRATOR_PAUSE"), 1500*time.Millisecond)
	requestTimeout := parseDuration(os.Getenv("HYDRATOR_REQUEST_TIMEOUT"), 12*time.Second)
	fetchPhotos := parseBool(os.Getenv("HYDRATOR_FETCH_PHOTOS"), false)
	runOnce := parseBool(os.Getenv("HYDRATOR_RUN_ONCE"), false)

	propertyTypes := splitList(os.Getenv("HYDRATOR_PROPERTY_TYPES"))
	orderBy := os.Getenv("HYDRATOR_ORDER_BY")
	provider := env.Get("HYDRATOR_PROVIDER", "rapidapi.realtor16")
	endpoint := env.Get("HYDRATOR_ENDPOINT", "search/forsale")
	minBeds := parseInt(os.Getenv("HYDRATOR_MIN_BEDS"), 0)
	minBaths := parseInt(os.Getenv("HYDRATOR_MIN_BATHS"), 0)
	minPrice := parseInt(os.Getenv("HYDRATOR_MIN_PRICE"), 0)
	maxPrice := parseInt(os.Getenv("HYDRATOR_MAX_PRICE"), 0)

	client := attom.NewClient(apiKey)

	st, err := store.Open(dsn)
	if err != nil {
		log.Fatalf("store open error: %v", err)
	}
	defer st.DB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := st.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("postgres ping error: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		cancel()
		log.Fatalf("postgres migrate error: %v", err)
	}
	cancel()

	pub := events.NewInMemory(256)
	hyd := &hydrator.Hydrator{Store: st, Pub: pub}

	job := &hydrator.BulkJob{
		Client:   client,
		Hydrator: hyd,
		Config: hydrator.BulkConfig{
			Zips:                 zips,
			PropertyTypes:        propertyTypes,
			PageSize:             pageSize,
			MaxPagesPerZip:       maxPages,
			Interval:             interval,
			PauseBetweenRequests: pause,
			RequestTimeout:       requestTimeout,
			FetchPhotos:          fetchPhotos,
			Provider:             provider,
			Endpoint:             endpoint,
			OrderBy:              orderBy,
			Beds:                 minBeds,
			Baths:                minBaths,
			MinPrice:             minPrice,
			MaxPrice:             maxPrice,
		},
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if runOnce {
		if err := job.RunOnce(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("hydrator bulk run failed: %v", err)
		}
		return
	}

	if err := job.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("hydrator job stopped with error: %v", err)
	}
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	fields := strings.FieldsFunc(v, func(r rune) bool {
		switch r {
		case ',', ';', '\n', '\r', '\t':
			return true
		default:
			return false
		}
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func parseDuration(v string, def time.Duration) time.Duration {
	if v == "" {
		return def
	}
	dur, err := time.ParseDuration(v)
	if err == nil {
		return dur
	}
	if i, err2 := strconv.Atoi(v); err2 == nil {
		return time.Duration(i) * time.Second
	}
	return def
}

func parseInt(v string, def int) int {
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func parseBool(v string, def bool) bool {
	if v == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}
