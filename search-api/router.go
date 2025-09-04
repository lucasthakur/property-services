package main

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httprate"
	"github.com/go-chi/render"
	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/http"
)

func BuildRouter(attomClient *attom.Client) http.Handler {
    r := chi.NewRouter()
    r.Use(httprate.LimitByIP(100, 1*time.Minute)) // protect ATTOM quota
    r.Use(render.SetContentType(render.ContentTypeJSON))
    r.Get("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"ok":true}`)) })

    httpapi.RegisterSearch(r, httpapi.SearchDeps{ATTOM: attomClient})
    httpapi.RegisterHydrate(r, httpapi.HydrateDeps{})
    httpapi.RegisterListings(r, httpapi.ListingsDeps{ATTOM: attomClient})

    return r
}
