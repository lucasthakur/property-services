package hydrator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/internal/canon"
	"github.com/yourorg/search-api/internal/store"
)

type BulkConfig struct {
	Zips                 []string
	PropertyTypes        []string
	PageSize             int
	MaxPagesPerZip       int
	Interval             time.Duration
	PauseBetweenRequests time.Duration
	RequestTimeout       time.Duration
	FetchPhotos          bool
	Provider             string
	Endpoint             string
	OrderBy              string
	Beds                 int
	Baths                int
	MinPrice             int
	MaxPrice             int
}

type BulkJob struct {
	Client   *attom.Client
	Hydrator *Hydrator
	Store    *store.Store
	Logger   *log.Logger
	Config   BulkConfig
}

func (j *BulkJob) logf(format string, args ...any) {
	if j.Logger != nil {
		j.Logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

func (j *BulkJob) validate() error {
	if j == nil {
		return errors.New("nil bulk job")
	}
	if j.Client == nil {
		return errors.New("hydrator bulk job missing client")
	}
	if j.Hydrator == nil || j.Hydrator.Store == nil {
		return errors.New("hydrator bulk job requires hydrator with store")
	}
	if len(j.Config.Zips) == 0 {
		return errors.New("hydrator bulk job requires at least one zip")
	}
	if j.Config.Provider == "" {
		j.Config.Provider = "rapidapi.realtor16"
	}
	if j.Config.Endpoint == "" {
		j.Config.Endpoint = "search/forsale"
	}
	if j.Store == nil {
		j.Store = j.Hydrator.Store
	}
	return nil
}

func (j *BulkJob) Run(ctx context.Context) error {
	if err := j.validate(); err != nil {
		return err
	}
	interval := j.Config.Interval
	if interval <= 0 {
		return j.RunOnce(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	j.logf("hydrator bulk job starting with interval %s (%d zip(s))", interval, len(j.Config.Zips))
	if err := j.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		j.logf("hydrator bulk job initial run error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			j.logf("hydrator bulk job stopping: %v", ctx.Err())
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			if err := j.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				j.logf("hydrator bulk job iteration error: %v", err)
			}
		}
	}
}

func (j *BulkJob) RunOnce(ctx context.Context) error {
	if err := j.validate(); err != nil {
		return err
	}
	propTypes := j.Config.PropertyTypes
	if len(propTypes) == 0 {
		propTypes = []string{""}
	}
	var joined error
	for _, rawZip := range j.Config.Zips {
		zip := strings.TrimSpace(rawZip)
		if zip == "" {
			continue
		}
		for _, propType := range propTypes {
			if err := j.ingestZip(ctx, zip, propType); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, attom.ErrDailyLimitExceeded) {
					return err
				}
				joined = errors.Join(joined, err)
			}
		}
	}
	return joined
}

func (j *BulkJob) ingestZip(ctx context.Context, zip string, propertyType string) error {
	pageSize := j.Config.PageSize
	if pageSize <= 0 {
		pageSize = 50
	}
	maxPages := j.Config.MaxPagesPerZip
	if maxPages <= 0 {
		maxPages = 5
	}
	timeout := j.Config.RequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	pause := j.Config.PauseBetweenRequests
	fetched := 0
	for page := 1; page <= maxPages; page++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		raw, err := j.Client.SearchListingsByPostal(reqCtx, zip, pageSize, page, j.Config.Beds, j.Config.Baths, j.Config.MinPrice, j.Config.MaxPrice, propertyType, j.Config.OrderBy)
		cancel()
		if err != nil {
			if errors.Is(err, attom.ErrDailyLimitExceeded) {
				return err
			}
			return fmt.Errorf("zip %s page %d fetch: %w", zip, page, err)
		}
		cards, err := attom.MapListingPayloadToCards(raw)
		if err != nil {
			return fmt.Errorf("zip %s page %d map: %w", zip, page, err)
		}
		if len(cards) == 0 {
			if page == 1 {
				j.logf("hydrator bulk job zip %s returned 0 listings", zip)
			}
			break
		}
		for _, card := range cards {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := j.persistCard(ctx, raw, card); err != nil {
				if errors.Is(err, attom.ErrDailyLimitExceeded) {
					return err
				}
				j.logf("hydrator bulk job zip %s listing %s error: %v", zip, card.ID, err)
				continue
			}
			fetched++
		}
		if len(cards) < pageSize {
			break
		}
		if pause > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}
	if fetched > 0 {
		if propertyType != "" {
			j.logf("hydrator bulk job zip %s (%s) persisted %d listings", zip, propertyType, fetched)
		} else {
			j.logf("hydrator bulk job zip %s persisted %d listings", zip, fetched)
		}
	}
	return nil
}

func (j *BulkJob) persistCard(ctx context.Context, raw []byte, card attom.PropertyCard) error {
	if card.Address == "" || card.City == "" || card.State == "" || card.Zip == "" {
		return errors.New("incomplete address data")
	}
	line1, city, st, zip, pk := canon.Canonicalize(card.Address, card.City, card.State, card.Zip)
	if pk == "" {
		return errors.New("empty property key")
	}
	norm := map[string]string{
		"line1":        line1,
		"city":         city,
		"state":        st,
		"zip":          zip,
		"property_key": pk,
	}
	if err := j.Hydrator.Write(ctx, j.Config.Provider, j.Config.Endpoint, raw, norm, card); err != nil {
		return err
	}
	if !j.Config.FetchPhotos || j.Store == nil {
		return nil
	}
	listingID := card.ListingID
	if listingID == "" {
		listingID = card.ID
	}
	if listingID == "" {
		return nil
	}
	targetID := card.PropertyID
	if targetID == "" {
		targetID = card.ID
	}
	reqCtx, cancel := context.WithTimeout(ctx, j.Config.RequestTimeout)
	assets, err := j.Client.GetPhotos(reqCtx, targetID)
	cancel()
	if err != nil {
		if errors.Is(err, attom.ErrDailyLimitExceeded) {
			return err
		}
		return fmt.Errorf("photos fetch: %w", err)
	}
	if len(assets) == 0 {
		return nil
	}
	inputs := make([]store.ListingPhotoInput, 0, len(assets))
	for idx, asset := range assets {
		if asset.Href == "" {
			continue
		}
		mediaType := asset.MediaType
		if mediaType == "" {
			mediaType = asset.Kind
		}
		inputs = append(inputs, store.ListingPhotoInput{
			Href:        asset.Href,
			Description: asset.Description,
			Title:       asset.Title,
			Kind:        asset.Kind,
			MediaType:   mediaType,
			Tags:        append([]string(nil), asset.Tags...),
			Position:    idx,
		})
	}
	if len(inputs) == 0 {
		return nil
	}
	if err := j.Store.ReplaceListingPhotos(ctx, listingID, inputs); err != nil {
		return fmt.Errorf("persist photos: %w", err)
	}
	return nil
}
