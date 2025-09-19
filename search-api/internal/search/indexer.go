package search

import (
    "context"
    "log"
    "time"

    "github.com/yourorg/search-api/internal/events"
)

// Indexer is a stub that consumes property.updated events and logs them.
// Swap this with a real OpenSearch client later.
type Indexer struct {
    Pub events.Publisher
}

func (i *Indexer) Run(ctx context.Context) {
    sub := i.Pub.SubscribePropertyUpdated()
    for {
        select {
        case <-ctx.Done():
            return
        case evt := <-sub:
            // TODO: map and upsert into OpenSearch
            log.Printf("indexer: property.updated id=%s key=%s at=%s", evt.PropertyID, evt.PropertyKey, time.Now().Format(time.RFC3339))
        }
    }
}

