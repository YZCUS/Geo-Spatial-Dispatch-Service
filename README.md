# Geo-Spatial Dispatch Service

A real-time driver dispatch demo built with Go, Redis geospatial indexing, REST,
and WebSockets.

The diagram below is a **logical single-process demo architecture** and does not
represent a deployed load-balancer setup.

## Architecture

```
                         +----------------------------------+
                         | Demo Server (single Go process)   |
                         +----------------+-----------------+
                                          |
              +-------------+-------------+---------------+--------------+
              |             |             |               |
      +-------v-------+ +---v-----------+ +v-----------+ +v------------+
      | HTTP REST     | | WebSocket     | |Demo Handler| | Redis Health |
      | Endpoints     | | Endpoints     | | + assets   | | Checks       |
      +-------+-------+ +------+--------+ +------+-----+ +------+------+
              |                |               |              |
              +----------------+---------------+--------------+
                               |
                        +------+v------+
                        |   Server     |
                        +------+-------+
                               |
              +----------------+------+-----------------+---------------+
              |                |      |                 |               |
          +---v----+      +----v--+   +v--------+   +----v--------+ +---v------+
          | Geo    |      |Driver  |   |Dispatch |   |Realtime Hub | |RateLimit |
          |Service |      |Service |   |         |   |             | |          |
          +--------+      +--------+   +---------+   +-------------+ +----------+
              |                |          |              |
              +----------------+----------+--------------+
                               |
                        +------v-------+
                        |    Redis     |
                        | (GEO + Keys) |
                        +--------------+
```

## Features

### Core Dispatch Engine
- Location-based driver matching using Redis Geospatial commands (GEOADD, GEORADIUS)
- Distance-sorted results with Redis-calculated straight-line distances (`GEORADIUS`);
  these are not road-route ETA estimates
- Atomic available-to-busy transitions and distributed driver locks
- Redis-backed `en_route -> cancelled|arrived` transitions for requests that
  include a rider ID

### Real-Time Communication
- WebSocket connections for drivers and riders
- Hub pattern for managing concurrent connections
- In-process WebSocket subscription fan-out (hub-local; no external pub/sub layer)

### Concurrency and Race Condition Handling
- Distributed locking with Redis SETNX to prevent double-assignment
- Atomic state transitions using Lua scripts
- Concurrent-safe driver status management (available/busy/offline)

### Driver Management
- TTL-based driver liveness detection
- Heartbeats refresh an already-online driver's status
- Fresh location updates bring expired drivers back online

### Rate Limiting
- Per-client budget management
- Namespaced, atomic budget deductions

> Performance note: this repository does not yet include a reproducible load
> benchmark, so latency and connection-scale claims should be measured in the
> target environment before production use.

## Project Structure

```
.
├── cmd/
│   └── server/
│       └── main.go              # Application entry point
├── internal/
│   ├── geospatial/
│   │   ├── geo.go               # Redis GEO operations
│   │   └── geo_test.go
│   ├── driver/
│   │   ├── driver.go            # Driver status management
│   │   └── driver_test.go
│   ├── dispatch/
│   │   ├── dispatcher.go        # Assignment logic
│   │   ├── lock.go              # Distributed locking
│   │   └── dispatch_test.go
│   ├── realtime/
│   │   ├── hub.go               # WebSocket hub
│   │   ├── client.go            # WebSocket client
│   │   ├── messages.go          # Message types
│   │   └── realtime_test.go
│   ├── ratelimiter/
│   │   ├── ratelimiter.go       # Atomic budget implementation
│   │   └── ratelimiter_test.go
│   └── server/
│       ├── server.go            # Server initialization
│       ├── demo/                 # Self-contained browser demo
│       ├── demo_handler.go       # Embedded demo route
│       ├── geo_handlers.go      # Geo API handlers
│       ├── dispatch_handlers.go # Dispatch API handlers
│       ├── ws_handlers.go       # WebSocket handlers
│       └── server_test.go
├── Dockerfile                   # Static Go service image
└── docker-compose.yml           # App + Redis demo stack
```

## Quick Demo

Prerequisite: Docker with Docker Compose.

```bash
make demo-up
```

Open [http://localhost:8080](http://localhost:8080). The interview configuration
automatically prepares eight Manhattan drivers, opens 11 WebSocket connections,
and starts browser-simulated fleet movement while three riders remain fixed
inside buildings. Each rider is paired with the closest point on the road grid.
A request ranks drivers around the rider's location, then the assigned car stays
on the road and travels to that roadside pickup in roughly 8–15 seconds while
unassigned cars keep roaming.

Use this exact walkthrough:

1. Reset to the known state and confirm eight roaming cars, three fixed riders,
   three roadside pickup rings, and 11 open sockets.
2. Request one rider. Confirm the row shows **En route**, the assigned driver
   turns toward the closest roadside pickup without entering a building, and
   the row exposes **Cancel ride**.
3. Cancel it. Confirm the row shows **Cancelled · rebook available**, the car
   returns to roaming/available, and that rider can be requested again.
4. Request the rider again and let the car reach the roadside pickup. Confirm
   the backend accepts `/dispatch/arrive`, the row shows **Arrived**, the car
   remains on the road, and cancellation and rebooking are closed until Reset.
5. Reset, choose **Request all 3 at once**, and confirm three unique assigned
   drivers independently travel toward their riders' roadside pickups.

The assigned IDs and distances can change with request timing because the cars
are already moving. The invariant is that each request ranks the current
straight-line positions and concurrent requests cannot claim the same driver.
Redis GEO provides straight-line match distances; the browser—not the backend—
simulates the road-grid movement. Rider coordinates are used for matching;
optional `pickup_longitude` and `pickup_latitude` coordinates identify the
roadside arrival target. The backend owns request, cancel, and arrive state.

```bash
make demo-smoke
make demo-down
```

`demo-smoke` performs the same core path without a browser and fails with a
non-zero exit code if the nearest driver is not assigned. It does not validate
the browser-only movement animation.

## Local Development

Prerequisites:

- Go 1.21+
- Redis 6.0+
- Docker Compose (recommended for Redis)

Start Redis:

```bash
docker compose up -d --wait redis
```

Run the service:

```bash
REDIS_ADDR=localhost:6379 HTTP_ADDR=:8080 go run ./cmd/server
```

```bash
make test
```

The integration tests use Redis databases 1–5 and flush those databases.
Do not point the test suite at a Redis instance containing important data.

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `localhost:6379` | Redis host and port |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `HTTP_PORT` | `8080` | Host port used by Docker Compose |
| `REDIS_PORT` | `6379` | Host Redis port used by Docker Compose |

## API Reference

### REST Endpoints

#### Geospatial

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/geo/add` | Add or update a location |
| GET | `/geo/get?id={id}` | Get a location by ID |
| POST | `/geo/nearby` | Find locations within radius |

#### Driver Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/driver/status?driver_id={id}` | Get driver status |
| POST | `/driver/status` | Set driver status |
| POST | `/driver/location` | Update driver location |

#### Dispatch

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/dispatch/request` | Request a driver |
| POST | `/dispatch/cancel` | Cancel an en-route request and release its driver |
| POST | `/dispatch/arrive` | Mark an en-route request arrived at pickup |
| GET | `/dispatch/stats` | Get dispatch statistics |

#### Rate Limiting

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/ratelimit/budget/set` | Set rate limit budget |
| GET | `/ratelimit/budget/get?key={key}` | Get current budget |
| POST | `/ratelimit/check` | Check and consume budget |

### WebSocket Endpoints

| Endpoint | Query Param | Description |
|----------|-------------|-------------|
| `/ws/driver` | `driver_id` | Driver connection for location updates |
| `/ws/rider` | `rider_id` | Rider connection for tracking drivers |
| `/ws/stats` | - | Real-time connection statistics |

## Usage Examples

### Add a Driver Location

```bash
curl -X POST http://localhost:8080/driver/location \
  -H "Content-Type: application/json" \
  -d '{"driver_id":"driver1","longitude":-73.9857,"latitude":40.7484}'
```

`/driver/location` writes the GEO position and refreshes driver liveness. A new
driver becomes available automatically.

### Reset Driver as Available

```bash
curl -X POST http://localhost:8080/driver/status \
  -H "Content-Type: application/json" \
  -d '{"driver_id":"driver1","status":"available"}'
```

### Request a Driver

```bash
curl -X POST http://localhost:8080/dispatch/request \
  -H "Content-Type: application/json" \
  -d '{"request_id":"request-1","rider_id":"rider1","longitude":-73.9857,"latitude":40.7484,"radius_km":5}'
```

Successful requests return `status: "en_route"`. Use the returned request ID
for the next lifecycle transition. When the rider is not standing on a drivable
road, provide `pickup_longitude` and `pickup_latitude` together: matching still
uses the rider coordinates, while arrival uses the separate pickup coordinates.

```bash
curl -X POST http://localhost:8080/dispatch/cancel \
  -H "Content-Type: application/json" \
  -d '{"request_id":"request-1"}'

curl -X POST http://localhost:8080/dispatch/arrive \
  -H "Content-Type: application/json" \
  -d '{"request_id":"request-1"}'
```

Cancel and arrive are alternative transitions for an en-route request; use one,
not both, in a real flow.

### Connect via WebSocket

```bash
# Optional CLI client
npm install -g wscat

# Connect as driver
wscat -c "ws://localhost:8080/ws/driver?driver_id=driver1"

# Connect as rider
wscat -c "ws://localhost:8080/ws/rider?rider_id=rider1"
```

## WebSocket Message Types

### Client to Server

```json
{"type":"location_update","payload":{"longitude":-73.9857,"latitude":40.7484}}
{"type":"subscribe","payload":{"driver_id":"driver1"}}
{"type":"unsubscribe","payload":{"driver_id":"driver1"}}
{"type":"heartbeat"}
```

### Server to Client

```json
{"type":"driver_location","payload":{"driver_id":"driver1","longitude":-73.9857,"latitude":40.7484}}
{"type":"order_update","payload":{"order_id":"order123","status":"assigned"}}
{"type":"ack","payload":{"status":"ok"}}
{"type":"error","payload":{"code":"invalid_request","message":"..."}}
```

## Current Scope and Known Limits

- Lifecycle records are created only when a dispatch includes `rider_id`.
  Legacy requests without one remain stateless, so retrying their request ID
  can assign another driver. The Redis lifecycle record is not a production
  trip ledger or an outbox for downstream side effects.
- Driver status has a 30-second TTL. REST and WebSocket location updates can
  bring a driver online; heartbeat messages only refresh an existing status, so
  they cannot revive a stale GEO position by themselves.
- Historical GEO members are retained and large-radius searches do not cap the
  candidate set. Dispatch batches status reads, but production should add stale
  location cleanup, pagination, or a separate available-driver index.
- WebSocket fanout is process-local and scans connected riders. Horizontal
  scale requires cross-node pub/sub and a reverse subscription index.
- WebSocket origin checks are intentionally permissive for the local demo and
  must be restricted before exposing the service beyond a trusted environment.
- The rate-limit endpoints demonstrate an atomic budget, not a refilling token
  bucket, and are not middleware on the dispatch routes.
- The worker-pool package is an isolated example and is not part of the running
  service.

## License

MIT
