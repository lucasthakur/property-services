FROM golang:1.24-alpine AS build
WORKDIR /app

# Copy module definition from subfolder and download deps
COPY search-api/go.mod ./
RUN go mod download

# Copy the rest of the source from the subfolder
COPY search-api/. ./

ENV CGO_ENABLED=0 GOOS=linux

RUN go build -o /build/search-api ./
RUN go build -o /build/hydrator ./cmd/hydrator

FROM alpine:3.19
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=build /build/search-api /app/bin/search-api
COPY --from=build /build/hydrator /app/bin/hydrator

EXPOSE 4002
ENTRYPOINT ["/app/bin/search-api"]
