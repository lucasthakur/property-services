package attom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// Client now targets RapidAPI Realtor endpoints.
type Client struct {
	key     string
	baseURL string
	host    string
	http    *retryablehttp.Client
}

func NewClient(apiKey string) *Client {
	rc := retryablehttp.NewClient()
	rc.RetryWaitMin = 100 * time.Millisecond
	rc.RetryWaitMax = 900 * time.Millisecond
	rc.RetryMax = 3
	rc.HTTPClient.Timeout = 8 * time.Second

	return &Client{
		key:     apiKey,
		baseURL: "https://realtor16.p.rapidapi.com",
		host:    "realtor16.p.rapidapi.com",
		http:    rc,
	}
}

// SearchByRadius is not supported by the Rapid Realtor API; return a clear error.
func (c *Client) SearchByRadius(ctx context.Context, lat, lon float64, radiusMiles float64, limit int, beds, baths int, minPrice, maxPrice int, propType string) ([]byte, error) {
	return nil, fmt.Errorf("radius search not supported by provider")
}

// SearchByPostal uses RapidAPI Realtor: GET /search/forsale?location=ZIP&page=&limit=
func (c *Client) SearchByPostal(ctx context.Context, postal string, pagesize, page int, propertyType, orderBy string) ([]byte, error) {
	if pagesize <= 0 {
		pagesize = 5
	}
	if page <= 0 {
		page = 1
	}
	q := url.Values{}
	q.Set("location", postal)
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("limit", fmt.Sprintf("%d", pagesize))

	u := fmt.Sprintf("%s/search/forsale?%s", c.baseURL, q.Encode())
	req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, fmt.Errorf("rapidapi error %d: %v", resp.StatusCode, body)
	}
	b, err := ioReadAllLimit(resp.Body, 4<<20)
	if err != nil {
		return nil, err
	}
	logBody("SearchByPostal", b)
	return b, nil
}

// SearchListingsByPostal mirrors SearchByPostal for listings.
func (c *Client) SearchListingsByPostal(ctx context.Context, postal string, pagesize, page int, beds, baths, minPrice, maxPrice int, propertyType, orderBy string) ([]byte, error) {
	// Basic support: location/page/limit.
	if pagesize <= 0 {
		pagesize = 5
	}
	if page <= 0 {
		page = 1
	}
	q := url.Values{}
	q.Set("location", postal)
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("limit", fmt.Sprintf("%d", pagesize))

	u := fmt.Sprintf("%s/search/forsale?%s", c.baseURL, q.Encode())
	req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, fmt.Errorf("rapidapi error %d: %v", resp.StatusCode, body)
	}
	b, err := ioReadAllLimit(resp.Body, 4<<20)
	if err != nil {
		return nil, err
	}
	logBody("SearchListingsByPostal", b)
	return b, nil
}

// GetPhotos fetches photo URLs for a provider property_id.
func (c *Client) GetPhotos(ctx context.Context, propertyID string) ([]PhotoAsset, error) {
	q := url.Values{}
	q.Set("property_id", propertyID)
	u := fmt.Sprintf("%s/property/photos?%s", c.baseURL, q.Encode())

	req, _ := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var body any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, fmt.Errorf("rapidapi error %d: %v", resp.StatusCode, body)
	}
	b, err := ioReadAllLimit(resp.Body, 6<<20)
	if err != nil {
		return nil, err
	}
	log.Printf("[DEBUG] photos response for property %s: %s", propertyID, string(b))
	var arr []struct {
		Description string `json:"description"`
		Href        string `json:"href"`
		Tags        []struct {
			Label string `json:"label"`
		} `json:"tags"`
		Title string `json:"title"`
		Type  string `json:"type"`
	}
	if err := json.Unmarshal(b, &arr); err != nil {
		return nil, err
	}
	assets := make([]PhotoAsset, 0, len(arr))
	for idx, it := range arr {
		if it.Href == "" {
			continue
		}
		tags := make([]string, 0, len(it.Tags))
		for _, tag := range it.Tags {
			if tag.Label != "" {
				tags = append(tags, tag.Label)
			}
		}
		assets = append(assets, PhotoAsset{
			Href:        upgradePhotoURL(it.Href),
			Description: it.Description,
			Title:       it.Title,
			Kind:        it.Type,
			Tags:        tags,
			Position:    idx,
		})
	}
	return assets, nil
}

func logBody(label string, body []byte) {
	const max = 2048
	preview := body
	if len(body) > max {
		preview = body[:max]
	}
	log.Printf("[DEBUG] %s (%d bytes): %s", label, len(body), string(preview))
}

func ioReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	lr := io.LimitReader(r, limit+1)
	b, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("payload too large")
	}
	return b, nil
}
