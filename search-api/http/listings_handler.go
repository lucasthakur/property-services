package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/internal/canon"
	"github.com/yourorg/search-api/internal/hydrator"
	"github.com/yourorg/search-api/internal/store"
)

type ListingsDeps struct {
	Hydrator       *hydrator.Hydrator
	Store          *store.Store
	ListingsClient *attom.Client
}

type ListingsRequest struct {
	PostalCode   string `json:"postalcode,omitempty"`
	PropertyType string `json:"property_type,omitempty"`
	OrderBy      string `json:"orderby,omitempty"`
	Limit        *int   `json:"limit,omitempty"` // pagesize
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
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_json", "detail": err.Error()})
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
		if v := q.Get("limit"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.Limit = &i
			}
		}
		if v := q.Get("page"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.Page = &i
			}
		}
		if v := q.Get("beds"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.Beds = &i
			}
		}
		if v := q.Get("baths"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.Baths = &i
			}
		}
		if v := q.Get("minprice"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.MinPrice = &i
			}
		}
		if v := q.Get("maxprice"); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				body.MaxPrice = &i
			}
		}
		handleListingsRequest(w, req, d, body)
	})

	r.Get("/search/listings/{listingID}/photos", func(w http.ResponseWriter, req *http.Request) {
		listingID := chi.URLParam(req, "listingID")
		if listingID == "" {
			render.Status(req, http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "listing_id_required"})
			return
		}
		photos, err := fetchListingPhotos(req.Context(), listingID, d)
		if err != nil {
			render.Status(req, http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "photos_error", "detail": err.Error()})
			return
		}
		render.JSON(w, req, map[string]any{"ok": true, "count": len(photos), "photos": photos})
	})
}

func handleListingsRequest(w http.ResponseWriter, req *http.Request, d ListingsDeps, body ListingsRequest) {
	if body.PostalCode == "" {
		render.Status(req, http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "postalcode_required"})
		return
	}
	// Default to 5 listings as requested
	pagesize := defInt(body.Limit, 5)
	page := defInt(body.Page, 1)
	beds := defInt(body.Beds, 0)
	baths := defInt(body.Baths, 0)
	minp := defInt(body.MinPrice, 0)
	maxp := defInt(body.MaxPrice, 0)

	offset := (page - 1) * pagesize
	store := d.Store
	if store == nil && d.Hydrator != nil {
		store = d.Hydrator.Store
	}
	if store != nil {
		records, err := store.FetchListingsByPostal(req.Context(), body.PostalCode, pagesize, offset, body.PropertyType)
		if err != nil {
			log.Printf("[WARN] db lookup failed for postal %s: %v", body.PostalCode, err)
		} else if len(records) > 0 {
			cards := recordsToCards(records)
			log.Printf("[INFO] serving listings for %s from database (%d listings)", body.PostalCode, len(cards))
			render.JSON(w, req, map[string]any{"ok": true, "count": len(cards), "properties": cards})
			return
		} else {
			log.Printf("[INFO] no database listings for %s; falling back to RapidAPI", body.PostalCode)
		}
	}
	raw, err := d.ListingsClient.SearchListingsByPostal(req.Context(), body.PostalCode, pagesize, page, beds, baths, minp, maxp, body.PropertyType, body.OrderBy)
	if err != nil {
		if errors.Is(err, attom.ErrDailyLimitExceeded) {
			render.Status(req, http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "provider_quota", "detail": "daily quota reached"})
			return
		}
		render.Status(req, http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "upstream_error", "detail": err.Error()})
		return
	}
	cards, err := attom.MapListingPayloadToCards(raw)
	if err != nil {
		render.Status(req, http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "map_error", "detail": err.Error()})
		return
	}
	persistCards(req.Context(), d.Hydrator, "search/forsale", raw, cards)
	for i := range cards {
		listingID := cards[i].ListingID
		if listingID == "" {
			listingID = cards[i].ID
		}
		propertyID := cards[i].PropertyID
		if propertyID == "" {
			if _, _, _, _, pk := canon.Canonicalize(cards[i].Address, cards[i].City, cards[i].State, cards[i].Zip); pk != "" {
				propertyID = pk
				cards[i].PropertyID = pk
			}
		}
		if listingID == "" && propertyID == "" {
			continue
		}
		cards[i].ListingID = listingID
		photos, err := loadListingPhotos(req.Context(), listingID, propertyID, store, d.ListingsClient)
		if err != nil {
			log.Printf("[WARN] unable to load photos for listing %s: %v", listingID, err)
			continue
		}
		cards[i].Images = photos
	}
	log.Printf("[INFO] served listings for %s from RapidAPI (%d listings)", body.PostalCode, len(cards))
	render.JSON(w, req, map[string]any{"ok": true, "count": len(cards), "properties": cards})
}

func fetchListingPhotos(ctx context.Context, listingID string, d ListingsDeps) ([]string, error) {
	store := d.Store
	if store == nil && d.Hydrator != nil {
		store = d.Hydrator.Store
	}
	var propertyID string
	if store != nil && listingID != "" {
		pk, err := store.LookupPropertyKeyByListing(ctx, listingID)
		if err != nil {
			log.Printf("[WARN] property lookup failed for listing %s: %v", listingID, err)
		} else {
			propertyID = pk
		}
	}
	return loadListingPhotos(ctx, listingID, propertyID, store, d.ListingsClient)
}

func photoHrefs(assets []attom.PhotoAsset) []string {
	hrefs := make([]string, 0, len(assets))
	for _, asset := range assets {
		if asset.Href == "" {
			continue
		}
		hrefs = append(hrefs, asset.Href)
	}
	return hrefs
}

func toStorePhotoInputs(assets []attom.PhotoAsset) []store.ListingPhotoInput {
	out := make([]store.ListingPhotoInput, 0, len(assets))
	for idx, asset := range assets {
		if asset.Href == "" {
			continue
		}
		mediaType := asset.MediaType
		if mediaType == "" {
			mediaType = asset.Kind
		}
		out = append(out, store.ListingPhotoInput{
			Href:        asset.Href,
			Description: asset.Description,
			Title:       asset.Title,
			Kind:        asset.Kind,
			MediaType:   mediaType,
			Tags:        asset.Tags,
			Position:    idx,
		})
	}
	return out
}

func loadListingPhotos(ctx context.Context, listingID, propertyID string, st *store.Store, client *attom.Client) ([]string, error) {
	if listingID == "" && propertyID == "" {
		return nil, nil
	}
	if listingID != "" && st != nil {
		urls, err := st.FetchListingPhotos(ctx, listingID)
		if err == nil {
			if len(urls) > 0 {
				return urls, nil
			}
		} else {
			log.Printf("[WARN] store photo lookup failed for listing %s: %v", listingID, err)
		}
	}
	if client == nil {
		return nil, nil
	}
	targetID := propertyID
	if targetID == "" {
		targetID = listingID
	}
	assets, err := client.GetPhotos(ctx, targetID)
	if err != nil {
		return nil, err
	}
	if st != nil && listingID != "" && len(assets) > 0 {
		if err := st.ReplaceListingPhotos(ctx, listingID, toStorePhotoInputs(assets)); err != nil {
			log.Printf("[WARN] unable to persist photos for %s: %v", listingID, err)
		}
	}
	return photoHrefs(assets), nil
}
