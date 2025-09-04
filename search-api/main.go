package main

import (
	"log"
	"net/http"
	"os"

	"github.com/yourorg/search-api/attom"
	"github.com/yourorg/search-api/internal/env"
	"github.com/yourorg/search-api/internal/logger"
)

func main() {
	port := env.GetInt("PORT", 4002)
	apiKey := env.Must("ATTOM_API_KEY")

	attomClient := attom.NewClient(apiKey)
	router := BuildRouter(attomClient)

	log.Printf("search-api listening on :%d", port)
	if err := http.ListenAndServe((":" + os.Getenv("PORT")), logger.Middleware(router)); err != nil {
		log.Fatal(err)
	}
}
