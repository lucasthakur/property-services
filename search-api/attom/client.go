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
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/time/rate"
)

var ErrDailyLimitExceeded = errors.New("attom: daily quota exceeded")

const (
	defaultRequestsPerSecond = 3.0
	defaultRateBurst         = 3
	defaultDailyLimit        = 20000
)

type quotaTransport struct {
	base   http.RoundTripper
	client *Client
}

func (t *quotaTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	ctx := req.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := t.client.beforeRequest(ctx); err != nil {
		return nil, err
	}
	return base.RoundTrip(req)
}

// Client targets RapidAPI Realtor endpoints with quota protections.
type Client struct {
	key        string
	baseURL    string
	host       string
	http       *retryablehttp.Client
	limiter    *rate.Limiter
	dailyLimit int

	mu       sync.Mutex
	dayKey   string
	dayCount int
}

func NewClient(apiKey string) *Client {
	return NewClientWithLimits(apiKey, defaultRequestsPerSecond, defaultRateBurst, defaultDailyLimit)
}

func NewClientWithLimits(apiKey string, perSecond float64, burst int, dailyLimit int) *Client {
	rc := retryablehttp.NewClient()
	rc.RetryWaitMin = 100 * time.Millisecond
	rc.RetryWaitMax = 900 * time.Millisecond
	rc.RetryMax = 3
	rc.HTTPClient.Timeout = 8 * time.Second

	var limiter *rate.Limiter
	if perSecond > 0 {
		if burst <= 0 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(perSecond), burst)
	}

	c := &Client{
		key:        apiKey,
		baseURL:    "https://realtor16.p.rapidapi.com",
		host:       "realtor16.p.rapidapi.com",
		http:       rc,
		limiter:    limiter,
		dailyLimit: dailyLimit,
	}

	qt := &quotaTransport{client: c}
	if rc.HTTPClient.Transport != nil {
		qt.base = rc.HTTPClient.Transport
	}
	rc.HTTPClient.Transport = qt

	rc.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		if errors.Is(err, ErrDailyLimitExceeded) {
			return false, err
		}
		return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
	}

	return c
}

func (c *Client) beforeRequest(ctx context.Context) error {
	if c.limiter != nil {
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	if c.dailyLimit <= 0 {
		return nil
	}
	now := time.Now().UTC()
	dayKey := now.Format("2006-01-02")
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dayKey != dayKey {
		c.dayKey = dayKey
		c.dayCount = 0
	}
	if c.dailyLimit > 0 && c.dayCount >= c.dailyLimit {
		return ErrDailyLimitExceeded
	}
	c.dayCount++
	return nil
}

func (c *Client) RemainingDailyQuota() int {
	if c.dailyLimit <= 0 {
		return -1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dailyLimit - c.dayCount
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
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, ErrDailyLimitExceeded
	}
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
	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, ErrDailyLimitExceeded
	}
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

	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("X-RapidAPI-Key", c.key)
	req.Header.Set("X-RapidAPI-Host", c.host)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, ErrDailyLimitExceeded
	}
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
