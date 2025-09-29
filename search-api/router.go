package main

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httprate"
	"github.com/go-chi/render"
	"github.com/yourorg/search-api/attom"
	httpapi "github.com/yourorg/search-api/http"
	httpv1 "github.com/yourorg/search-api/http/v1"
	"github.com/yourorg/search-api/internal/store"
)

func BuildRouter(listingClient *attom.Client, deps httpv1.ResolveDeps) http.Handler {
	r := chi.NewRouter()
	r.Use(httprate.LimitByIP(100, 1*time.Minute)) // protect upstream quota
	r.Use(render.SetContentType(render.ContentTypeJSON))
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"ok":true}`)) })

	var storeRef *store.Store
	if deps.Hydrator != nil {
		storeRef = deps.Hydrator.Store
	}
	httpapi.RegisterSearch(r, httpapi.SearchDeps{Hydrator: deps.Hydrator, ListingsClient: listingClient})
	httpapi.RegisterHydrate(r, httpapi.HydrateDeps{})
	httpapi.RegisterListings(r, httpapi.ListingsDeps{Hydrator: deps.Hydrator, Store: storeRef, ListingsClient: listingClient})

	// v1 resolve endpoint with Redis + SWR
	httpv1.RegisterResolve(r, deps)

	return r
}
