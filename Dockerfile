FROM golang:1.22-alpine AS build
WORKDIR /app

# Copy module definition from subfolder and download deps
COPY search-api/go.mod ./
RUN go mod download

# Copy the rest of the source from the subfolder
COPY search-api/. ./

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server .

FROM alpine:3.19
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=build /app/server /app/server
# App logs indicate it listens on :4002
EXPOSE 4002
ENTRYPOINT ["/app/server"]
