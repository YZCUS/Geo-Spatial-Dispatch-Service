package geospatial

import (
	"context"
	"math"
	"testing"

	"github.com/go-redis/redis/v8"
)

func setupTestGeo(t *testing.T) *GeoService {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush Redis test DB: %v", err)
	}

	return New(rdb, "test-locations")
}

func TestGeoService_New_DefaultKey(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})
	defer rdb.Close()

	gs := New(rdb, "")
	if gs.key != "locations" {
		t.Errorf("Expected default key 'locations', got '%s'", gs.key)
	}
}

func TestGeoService_AddAndGetLocation(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	loc := Location{
		ID:        "location1",
		Longitude: -73.9857,
		Latitude:  40.7484,
	}

	err := gs.AddLocation(ctx, loc)
	if err != nil {
		t.Fatalf("AddLocation failed: %v", err)
	}

	retrieved, err := gs.GetLocation(ctx, "location1")
	if err != nil {
		t.Fatalf("GetLocation failed: %v", err)
	}

	if math.Abs(retrieved.Longitude-loc.Longitude) > 0.0001 {
		t.Errorf("Longitude mismatch")
	}
	if math.Abs(retrieved.Latitude-loc.Latitude) > 0.0001 {
		t.Errorf("Latitude mismatch")
	}
}

func TestGeoService_GetLocation_NotFound(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	_, err := gs.GetLocation(ctx, "non-existent")
	if err != ErrLocationNotFound {
		t.Errorf("Expected ErrLocationNotFound, got %v", err)
	}
}

func TestGeoService_RemoveLocation(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	// Add and then remove
	gs.AddLocation(ctx, Location{ID: "to-remove", Longitude: 0, Latitude: 0})

	err := gs.RemoveLocation(ctx, "to-remove")
	if err != nil {
		t.Fatalf("RemoveLocation failed: %v", err)
	}

	// Verify removed
	_, err = gs.GetLocation(ctx, "to-remove")
	if err != ErrLocationNotFound {
		t.Errorf("Expected location to be removed")
	}
}

func TestGeoService_RemoveLocation_NotFound(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	err := gs.RemoveLocation(ctx, "non-existent")
	if err != ErrLocationNotFound {
		t.Errorf("Expected ErrLocationNotFound, got %v", err)
	}
}

func TestGeoService_FindNearby(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	// Add multiple locations
	locations := []Location{
		{ID: "loc1", Longitude: -73.9857, Latitude: 40.7484},
		{ID: "loc2", Longitude: -73.9855, Latitude: 40.7580},
		{ID: "loc3", Longitude: -74.0445, Latitude: 40.6892},
	}

	for _, loc := range locations {
		gs.AddLocation(ctx, loc)
	}

	// Find nearby
	nearby, err := gs.FindNearby(ctx, -73.9857, 40.7484, 2.0)
	if err != nil {
		t.Fatalf("FindNearby failed: %v", err)
	}

	if len(nearby) < 2 {
		t.Errorf("Expected at least 2 nearby locations, got %d", len(nearby))
	}
}

func TestGeoService_FindNearby_Empty(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	// No locations added, should return empty
	nearby, err := gs.FindNearby(ctx, 0, 0, 1.0)
	if err != nil {
		t.Fatalf("FindNearby failed: %v", err)
	}

	if len(nearby) != 0 {
		t.Errorf("Expected 0 nearby locations, got %d", len(nearby))
	}
}

func TestGeoService_Distance(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	gs.AddLocation(ctx, Location{ID: "a", Longitude: -73.9857, Latitude: 40.7484})
	gs.AddLocation(ctx, Location{ID: "b", Longitude: -73.9855, Latitude: 40.7580})

	dist, err := gs.Distance(ctx, "a", "b")
	if err != nil {
		t.Fatalf("Distance failed: %v", err)
	}

	if dist < 0.5 || dist > 1.5 {
		t.Errorf("Expected distance ~1km, got %.2f km", dist)
	}
}

func TestGeoService_InvalidCoordinates(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()

	// Invalid longitude
	err := gs.AddLocation(ctx, Location{
		ID:        "test",
		Longitude: 200, // Invalid
		Latitude:  40,
	})

	if err != ErrInvalidCoordinates {
		t.Errorf("Expected ErrInvalidCoordinates, got %v", err)
	}

	// Invalid latitude
	err = gs.AddLocation(ctx, Location{
		ID:        "test2",
		Longitude: 0,
		Latitude:  100, // Invalid
	})

	if err != ErrInvalidCoordinates {
		t.Errorf("Expected ErrInvalidCoordinates for invalid latitude, got %v", err)
	}

	err = gs.AddLocation(ctx, Location{
		ID:        "nan",
		Longitude: math.NaN(),
		Latitude:  0,
	})
	if err != ErrInvalidCoordinates {
		t.Errorf("Expected ErrInvalidCoordinates for NaN, got %v", err)
	}
}

func TestGeoService_RejectsEmptyIDAndInvalidRadius(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()
	if err := gs.AddLocation(ctx, Location{Longitude: 0, Latitude: 0}); err != ErrInvalidLocationID {
		t.Errorf("Expected ErrInvalidLocationID, got %v", err)
	}

	if _, err := gs.FindNearby(ctx, 0, 0, 0); err != ErrInvalidRadius {
		t.Errorf("Expected ErrInvalidRadius, got %v", err)
	}
}

func TestGeoService_FindNearbyIncludesRedisDistance(t *testing.T) {
	gs := setupTestGeo(t)
	defer gs.redis.Close()

	ctx := context.Background()
	if err := gs.AddLocation(ctx, Location{ID: "north", Longitude: 0, Latitude: 1}); err != nil {
		t.Fatalf("AddLocation failed: %v", err)
	}

	nearby, err := gs.FindNearby(ctx, 0, 0, 120)
	if err != nil {
		t.Fatalf("FindNearby failed: %v", err)
	}
	if len(nearby) != 1 {
		t.Fatalf("Expected 1 location, got %d", len(nearby))
	}
	if nearby[0].DistanceKm < 110 || nearby[0].DistanceKm > 112 {
		t.Errorf("Expected Redis distance around 111km, got %.2f", nearby[0].DistanceKm)
	}
}
