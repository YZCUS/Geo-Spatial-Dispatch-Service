FROM golang:1.21-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dispatch-service ./cmd/server

FROM scratch
COPY --from=build /out/dispatch-service /dispatch-service
USER 65532:65532
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=5 CMD ["/dispatch-service", "healthcheck"]
ENTRYPOINT ["/dispatch-service"]
