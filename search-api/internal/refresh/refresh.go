package refresh

import (
    "context"
    "sync"
    "time"
)

type Job struct {
    PropertyKey string
}

type Refresher struct {
    ch    chan Job
    inFly sync.Map // key -> struct{}
    Do    func(ctx context.Context, j Job)
}

func New(capacity int, workerCount int, do func(ctx context.Context, j Job)) *Refresher {
    if capacity <= 0 { capacity = 256 }
    if workerCount <= 0 { workerCount = 2 }
    r := &Refresher{ ch: make(chan Job, capacity), Do: do }
    for i := 0; i < workerCount; i++ {
        go r.worker()
    }
    return r
}

func (r *Refresher) Enqueue(j Job) {
    if _, exists := r.inFly.LoadOrStore(j.PropertyKey, struct{}{}); exists {
        return
    }
    select {
    case r.ch <- j:
    default:
        // drop if saturated
        r.inFly.Delete(j.PropertyKey)
    }
}

func (r *Refresher) worker() {
    for j := range r.ch {
        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        func() {
            defer func() {
                r.inFly.Delete(j.PropertyKey)
                cancel()
            }()
            if r.Do != nil { r.Do(ctx, j) }
        }()
    }
}

