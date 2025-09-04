package env

import (
	"log"
	"os"
	"strconv"
)

func Must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing required env %s", k)
	}
	return v
}
func GetInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" { return def }
	i, err := strconv.Atoi(v)
	if err != nil { return def }
	return i
}
