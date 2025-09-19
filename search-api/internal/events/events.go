package events

import (
    "context"
)

type PropertyUpdated struct {
    PropertyID   string
    PropertyKey  string
}

type Publisher interface {
    PublishPropertyUpdated(ctx context.Context, evt PropertyUpdated)
    SubscribePropertyUpdated() <-chan PropertyUpdated
}

type inMemory struct { ch chan PropertyUpdated }

func NewInMemory(buffer int) Publisher {
    if buffer <= 0 { buffer = 256 }
    return &inMemory{ ch: make(chan PropertyUpdated, buffer) }
}

func (m *inMemory) PublishPropertyUpdated(_ context.Context, evt PropertyUpdated) {
    select { case m.ch <- evt: default: }
}

func (m *inMemory) SubscribePropertyUpdated() <-chan PropertyUpdated { return m.ch }

