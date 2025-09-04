package attom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

type Client struct {
	key     string
	baseURL string
	http    *retryablehttp.Client
}

func NewClient(apiKey string) *Client {
	rc := retryablehttp.NewClient()
	rc.RetryWaitMin = 100 * time.Millisecond
	rc.RetryWaitMax = 900 * time.Millisecond
	rc.RetryMax = 3
	rc.HTTPClient.Timeout = 6 * time.Second

	return &Client{
		key:     apiKey,
		baseURL: "https://api.gateway.attomdata.com", // ATTOM gateway
		http:    rc,
	}
}

// Search by radius around lat/lon. ATTOM has an address/parcel endpoint that supports radius.
// We keep this generic; tweak path/params to your plan.
func (c *Client) SearchByRadius(ctx context.Context, lat, lon float64, radiusMiles float64, limit int, beds, baths int, minPrice, maxPrice int, propType string) ([]byte, error) {
    // Use ATTOM Property API "nearby" endpoint
    // Docs: /propertyapi/v1.0.0/property/nearby
    // Required: latitude, longitude, radius (miles)
    // Pagination: page (default 1), pagesize
    q := url.Values{}
    q.Set("latitude", fmt.Sprintf("%.6f", lat))
    q.Set("longitude", fmt.Sprintf("%.6f", lon))
    q.Set("radius", fmt.Sprintf("%.2f", radiusMiles))
    q.Set("page", "1")
    if limit > 0 { q.Set("pagesize", fmt.Sprintf("%d", limit)) }

    // Note: many filters (beds/baths/price/type) are not supported on nearby
    // endpoints; omit to avoid 4xx. Apply filtering client-side after mapping.

    u := fmt.Sprintf("%s/propertyapi/v1.0.0/property/nearby?%s", c.baseURL, q.Encode())

	req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("accept", "application/json")
	req.Header.Set("apikey", c.key)

	resp, err := c.http.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, fmt.Errorf("attom error %d: %v", resp.StatusCode, body)
	}
	return ioReadAllLimit(resp.Body, 4<<20) // 4MB guard
}

// Search by postal code using ATTOM property/address endpoint.
// Docs: GET /propertyapi/v1.0.0/property/address
// Params: postalcode (required), propertytype (optional), orderby (optional), page, pagesize
func (c *Client) SearchByPostal(ctx context.Context, postal string, pagesize, page int, propertyType, orderBy string) ([]byte, error) {
    q := url.Values{}
    q.Set("postalcode", postal)
    if pagesize > 0 { q.Set("pagesize", fmt.Sprintf("%d", pagesize)) }
    if page > 0 { q.Set("page", fmt.Sprintf("%d", page)) } else { q.Set("page", "1") }
    if propertyType != "" { q.Set("propertytype", propertyType) }
    if orderBy != "" { q.Set("orderby", orderBy) }

    u := fmt.Sprintf("%s/propertyapi/v1.0.0/property/address?%s", c.baseURL, q.Encode())

    req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    req.Header.Set("accept", "application/json")
    req.Header.Set("apikey", c.key)

    resp, err := c.http.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()
    if resp.StatusCode >= 400 {
        var body map[string]any
        _ = json.NewDecoder(resp.Body).Decode(&body)
        return nil, fmt.Errorf("attom error %d: %v", resp.StatusCode, body)
    }
    return ioReadAllLimit(resp.Body, 4<<20)
}

// Search active listings by postal code (Listing Snapshot)
// Docs: GET /propertyapi/v1.0.0/listing/snapshot
// Common filters: postalcode, page, pagesize, beds, baths, minprice, maxprice, propertytype, orderby
func (c *Client) SearchListingsByPostal(ctx context.Context, postal string, pagesize, page int, beds, baths, minPrice, maxPrice int, propertyType, orderBy string) ([]byte, error) {
    q := url.Values{}
    q.Set("postalcode", postal)
    if pagesize > 0 { q.Set("pagesize", fmt.Sprintf("%d", pagesize)) }
    if page > 0 { q.Set("page", fmt.Sprintf("%d", page)) } else { q.Set("page", "1") }
    if beds > 0 { q.Set("beds", fmt.Sprintf("%d", beds)) }
    if baths > 0 { q.Set("baths", fmt.Sprintf("%d", baths)) }
    if minPrice > 0 { q.Set("minprice", fmt.Sprintf("%d", minPrice)) }
    if maxPrice > 0 { q.Set("maxprice", fmt.Sprintf("%d", maxPrice)) }
    if propertyType != "" { q.Set("propertytype", propertyType) }
    if orderBy != "" { q.Set("orderby", orderBy) }

    u := fmt.Sprintf("%s/propertyapi/v1.0.0/listing/snapshot?%s", c.baseURL, q.Encode())

    req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    req.Header.Set("accept", "application/json")
    req.Header.Set("apikey", c.key)

    resp, err := c.http.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()
    if resp.StatusCode >= 400 {
        var body map[string]any
        _ = json.NewDecoder(resp.Body).Decode(&body)
        return nil, fmt.Errorf("attom error %d: %v", resp.StatusCode, body)
    }
    return ioReadAllLimit(resp.Body, 4<<20)
}

func ioReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	var buf []byte
	lr := io.LimitReader(r, limit+1)
	b, err := io.ReadAll(lr)
	if err != nil { return nil, err }
	if int64(len(b)) > limit { return nil, errors.New("payload too large") }
	buf = append(buf, b...)
	return buf, nil
}
