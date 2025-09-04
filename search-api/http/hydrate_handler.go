package httpapi

import (
    "encoding/json"
    "net/http"

    "github.com/go-chi/chi/v5"
    "github.com/go-chi/render"
)

type HydrateDeps struct {
    // e.g., Kafka producer, etc.
}

func RegisterHydrate(r chi.Router, _ HydrateDeps) {
    r.Post("/hydrate", func(w http.ResponseWriter, req *http.Request) {
        var body struct {
            Address string `json:"address"`
            Scope   string `json:"scope"`
        }
        if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
            render.Status(req, http.StatusBadRequest)
            _ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid_json", "detail": err.Error()})
            return
        }
        if body.Address == "" {
            render.Status(req, http.StatusBadRequest)
            _ = json.NewEncoder(w).Encode(map[string]any{"error": "address_required"})
            return
        }
        // TODO: enqueue into Kafka "hydrate-jobs" (out of scope for listing)
        render.JSON(w, req, map[string]any{"ok": true})
    })
}
