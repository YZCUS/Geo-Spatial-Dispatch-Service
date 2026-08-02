package geospatial

import (
	"context"
	"errors"
	"log"
	"math"
	"strings"

	"github.com/go-redis/redis/v8"
)

var (
	ErrInvalidCoordinates = errors.New("invalid coordinates")
	ErrInvalidLocationID  = errors.New("invalid location id")
	ErrInvalidRadius      = errors.New("invalid radius")
	ErrLocationNotFound   = errors.New("location not found")
)

type Location struct {
	ID         string  `json:"id"`
	Longitude  float64 `json:"longitude"`
	Latitude   float64 `json:"latitude"`
	DistanceKm float64 `json:"distance_km,omitempty"`
}

type GeoService struct {
	redis *redis.Client
	key   string
}

func New(rdb *redis.Client, key string) *GeoService {
	if key == "" {
		key = "locations"
	}
	log.Printf("[GeoService] Initialized with key=%s", key)
	return &GeoService{
		redis: rdb,
		key:   key,
	}
}

// AddLocation adds or updates a location
func (gs *GeoService) AddLocation(ctx context.Context, loc Location) error {
	if strings.TrimSpace(loc.ID) == "" {
		return ErrInvalidLocationID
	}
	if err := ValidateCoordinates(loc.Longitude, loc.Latitude); err != nil {
		log.Printf("[GeoService] Invalid coordinates: longitude=%v latitude=%v", loc.Longitude, loc.Latitude)
		return err
	}

	err := gs.redis.GeoAdd(ctx, gs.key, &redis.GeoLocation{
		Name:      loc.ID,
		Longitude: loc.Longitude,
		Latitude:  loc.Latitude,
	}).Err()
	if err != nil {
		log.Printf("[GeoService] Redis GeoAdd error: %v", err)
	}
	return err
}

// GetLocation retrieves location coordinates
func (gs *GeoService) GetLocation(ctx context.Context, id string) (*Location, error) {
	if strings.TrimSpace(id) == "" {
		return nil, ErrInvalidLocationID
	}

	positions, err := gs.redis.GeoPos(ctx, gs.key, id).Result()
	if err != nil {
		log.Printf("[GeoService] Redis GeoPos error for id=%s: %v", id, err)
		return nil, err
	}

	if len(positions) == 0 || positions[0] == nil {
		log.Printf("[GeoService] Location not found id=%s", id)
		return nil, ErrLocationNotFound
	}

	return &Location{
		ID:        id,
		Longitude: positions[0].Longitude,
		Latitude:  positions[0].Latitude,
	}, nil
}

// RemoveLocation deletes a location
func (gs *GeoService) RemoveLocation(ctx context.Context, id string) error {
	result, err := gs.redis.ZRem(ctx, gs.key, id).Result()
	if err != nil {
		return err
	}

	if result == 0 {
		return ErrLocationNotFound
	}

	return nil
}

// FindNearby finds locations within radius (km)
func (gs *GeoService) FindNearby(ctx context.Context, lon, lat, radiusKm float64) ([]Location, error) {
	if err := ValidateCoordinates(lon, lat); err != nil {
		return nil, err
	}
	if err := ValidateRadius(radiusKm); err != nil {
		return nil, err
	}

	results, err := gs.redis.GeoRadius(ctx, gs.key, lon, lat, &redis.GeoRadiusQuery{
		Radius:    radiusKm,
		Unit:      "km",
		WithCoord: true,
		WithDist:  true,
		Sort:      "ASC",
	}).Result()

	if err != nil {
		log.Printf("[GeoService] Redis GeoRadius error: %v", err)
		return nil, err
	}

	locations := make([]Location, 0, len(results))
	for _, result := range results {
		locations = append(locations, Location{
			ID:         result.Name,
			Longitude:  result.Longitude,
			Latitude:   result.Latitude,
			DistanceKm: result.Dist,
		})
	}

	log.Printf("[GeoService] FindNearby found %d locations within %.2fkm", len(locations), radiusKm)
	return locations, nil
}

// Distance calculates distance between two locations (km)
func (gs *GeoService) Distance(ctx context.Context, id1, id2 string) (float64, error) {
	dist, err := gs.redis.GeoDist(ctx, gs.key, id1, id2, "km").Result()
	if err != nil {
		return 0, err
	}
	return dist, nil
}

func ValidateCoordinates(lon, lat float64) error {
	if math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 {
		return ErrInvalidCoordinates
	}
	if math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
		return ErrInvalidCoordinates
	}
	return nil
}

func ValidateRadius(radiusKm float64) error {
	if math.IsNaN(radiusKm) || math.IsInf(radiusKm, 0) || radiusKm <= 0 {
		return ErrInvalidRadius
	}
	return nil
}
