# Geo-Spatial Dispatch Service

A high-performance, real-time driver dispatch system built with Go and Redis.

## Architecture

```
                                   +------------------+
                                   |   Load Balancer  |
                                   +--------+---------+
                                            |
                    +-----------------------+-----------------------+
                    |                                               |
            +-------v-------+                               +-------v-------+
            |   HTTP REST   |                               |   WebSocket   |
            |   Endpoints   |                               |   Endpoints   |
            +-------+-------+                               +-------+-------+
                    |                                               |
                    +-------------------+---------------------------+
                                        |
                                +-------v-------+
                                |    Server     |
                                +-------+-------+
                                        |
        +-------------+--------+--------+--------+-------------+
        |             |        |        |        |             |
+-------v---+  +------v----+  +v-------+  +------v----+  +-----v------+
| GeoService|  |  Driver   |  |Dispatch|  |  Realtime |  |RateLimiter |
|           |  |  Service  |  |        |  |    Hub    |  |            |
+-----------+  +-----------+  +--------+  +-----------+  +------------+
        |             |            |            |              |
        +-------------+------------+------------+--------------+
                                   |
                           +-------v-------+
                           |     Redis     |
                           | (GEO + Keys)  |
                           +---------------+
```

## Features

### Core Dispatch Engine
- Location-based driver matching using Redis Geospatial commands (GEOADD, GEORADIUS)
- Sub-5ms latency for radius queries
- Distance-sorted results for optimal driver selection

### Real-Time Communication
- WebSocket connections for drivers and riders
- Hub pattern for managing thousands of concurrent connections
- Publish-subscribe model for location updates

### Concurrency and Race Condition Handling
- Distributed locking with Redis SETNX to prevent double-assignment
- Atomic state transitions using Lua scripts
- Concurrent-safe driver status management (available/busy/offline)

### Driver Management
- TTL-based driver liveness detection
- Heartbeat mechanism to maintain online status
- Automatic cleanup of stale driver records

### Rate Limiting
- Token bucket algorithm for API rate limiting
- Per-client budget management
- Atomic budget operations

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
│   │   ├── ratelimiter.go       # Token bucket implementation
│   │   └── ratelimiter_test.go
│   └── server/
│       ├── server.go            # Server initialization
│       ├── geo_handlers.go      # Geo API handlers
│       ├── dispatch_handlers.go # Dispatch API handlers
│       ├── ws_handlers.go       # WebSocket handlers
│       └── server_test.go
└── docker-compose.yml           # Redis container
```

## Prerequisites

- Go 1.21+
- Redis 6.0+
- Docker (optional)

## Getting Started

### 1. Start Redis

Using Docker:
```bash
docker-compose up -d
```

Or connect to an existing Redis instance on `localhost:6379`.

### 2. Run the Server

```bash
go run cmd/server/main.go
```

The server starts on `http://localhost:8080`.

### 3. Run Tests

```bash
go test ./... -cover
```

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
curl -X POST http://localhost:8080/geo/add \
  -H "Content-Type: application/json" \
  -d '{"id":"driver1","longitude":-73.9857,"latitude":40.7484}'
```

### Set Driver as Available

```bash
curl -X POST http://localhost:8080/driver/status \
  -H "Content-Type: application/json" \
  -d '{"driver_id":"driver1","status":"available"}'
```

### Request a Driver

```bash
curl -X POST http://localhost:8080/dispatch/request \
  -H "Content-Type: application/json" \
  -d '{"longitude":-73.9857,"latitude":40.7484,"radius_km":5}'
```

### Connect via WebSocket

```bash
# Install wscat
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

## Test Coverage

| Package | Coverage |
|---------|----------|
| ratelimiter | 88.9% |
| geospatial | 82.9% |
| driver | 81.0% |
| dispatch | 67.4% |
| realtime | 52.5% |
| server | 45.5% |

## License

MIT
