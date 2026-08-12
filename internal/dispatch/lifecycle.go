package dispatch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/go-redis/redis/v8"
)

type AssignmentStatus string

const (
	AssignmentEnRoute   AssignmentStatus = "en_route"
	AssignmentArrived   AssignmentStatus = "arrived"
	AssignmentCancelled AssignmentStatus = "cancelled"
)

var (
	ErrActiveAssignment        = errors.New("rider already has an active assignment")
	ErrAssignmentNotFound      = errors.New("assignment not found")
	ErrAssignmentStateConflict = errors.New("assignment state conflict")
	ErrRequestAlreadyExists    = errors.New("request already exists")
	ErrDriverOwnershipConflict = errors.New("driver is owned by another request")
	ErrDriverTooFar            = errors.New("driver is too far from pickup")
)

const (
	defaultAssignmentPrefix   = "dispatch:assignment"
	defaultRiderActivePrefix  = "dispatch:rider-active"
	defaultDriverActivePrefix = "dispatch:driver-active"
	defaultDriverStatusPrefix = "driver:status"
	defaultAssignmentTTL      = 24 * time.Hour
	defaultDriverStatusTTL    = 30 * time.Second
	defaultArrivalThresholdKm = 0.05
)

// LifecycleConfig contains only the Redis key/TTL details needed to make ride
// state changes atomic with the existing driver status and assignment lock.
type LifecycleConfig struct {
	AssignmentPrefix   string
	RiderActivePrefix  string
	DriverActivePrefix string
	DriverStatusPrefix string
	AssignmentTTL      time.Duration
	DriverStatusTTL    time.Duration
	ArrivalThresholdKm float64
}

type Assignment struct {
	RequestID       string           `json:"request_id"`
	RiderID         string           `json:"rider_id"`
	DriverID        string           `json:"driver_id"`
	PickupLongitude float64          `json:"pickup_longitude"`
	PickupLatitude  float64          `json:"pickup_latitude"`
	Status          AssignmentStatus `json:"status"`
}

type lifecycleManager struct {
	redis              *redis.Client
	lockManager        *LockManager
	assignmentPrefix   string
	riderActivePrefix  string
	driverActivePrefix string
	driverStatusPrefix string
	assignmentTTL      time.Duration
	driverStatusTTL    time.Duration
	arrivalThresholdKm float64
}

var createAssignmentScript = redis.NewScript(`
local active_request = redis.call('GET', KEYS[2])
if active_request then
	local active_status = redis.call('HGET', ARGV[1] .. ':' .. active_request, 'status')
	if active_status == 'en_route' or active_status == 'arrived' then
		return 0
	end
end

if redis.call('EXISTS', KEYS[1]) == 1 then
	return -1
end
local driver_owner = redis.call('GET', KEYS[3])
if driver_owner and driver_owner ~= ARGV[2] then
	return -2
end

redis.call('HSET', KEYS[1],
	'request_id', ARGV[2],
	'rider_id', ARGV[3],
	'driver_id', ARGV[4],
	'pickup_longitude', ARGV[5],
	'pickup_latitude', ARGV[6],
	'status', 'en_route')
redis.call('EXPIRE', KEYS[1], ARGV[7])
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[7])
redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[7])
return 1
`)

var cancelAssignmentScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
	return -1
end
if redis.call('HGET', KEYS[1], 'status') ~= 'en_route' then
	return 0
end
if redis.call('HGET', KEYS[1], 'driver_id') ~= ARGV[2]
	or redis.call('HGET', KEYS[1], 'rider_id') ~= ARGV[3] then
	return -2
end
if redis.call('GET', KEYS[5]) ~= ARGV[1] then
	return -3
end

local lock_owner = redis.call('GET', KEYS[3])
if lock_owner and lock_owner ~= ARGV[1] then
	return -4
end

if redis.call('GET', KEYS[2]) == 'busy' then
	redis.call('SET', KEYS[2], 'available', 'EX', ARGV[4])
end
redis.call('HSET', KEYS[1], 'status', 'cancelled')
redis.call('EXPIRE', KEYS[1], ARGV[5])
if lock_owner == ARGV[1] then
	redis.call('DEL', KEYS[3])
end
if redis.call('GET', KEYS[4]) == ARGV[1] then
	redis.call('DEL', KEYS[4])
end
redis.call('DEL', KEYS[5])
return 1
`)

var arriveAssignmentScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
	return -1
end
if redis.call('HGET', KEYS[1], 'status') ~= 'en_route' then
	return 0
end
if redis.call('GET', KEYS[2]) ~= ARGV[1]
	or redis.call('GET', KEYS[3]) ~= ARGV[1]
	or redis.call('GET', KEYS[4]) ~= 'busy' then
	return -2
end
redis.call('HSET', KEYS[1], 'status', 'arrived')
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
redis.call('EXPIRE', KEYS[3], ARGV[2])
redis.call('EXPIRE', KEYS[4], ARGV[3])
return 1
`)

var refreshActiveDriverScript = redis.NewScript(`
local request_id = redis.call('GET', KEYS[1])
if not request_id then
	return 0
end
if redis.call('HGET', ARGV[1] .. ':' .. request_id, 'driver_id') ~= ARGV[2] then
	return 0
end
local status = redis.call('HGET', ARGV[1] .. ':' .. request_id, 'status')
if status ~= 'en_route' and status ~= 'arrived' then
	return 0
end
redis.call('SET', KEYS[2], 'busy', 'EX', ARGV[3])
return 1
`)

var clearActiveAssignmentScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

var clearResetAssignmentScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('DEL', KEYS[1])
end
if redis.call('GET', KEYS[2]) == ARGV[1] then
	redis.call('DEL', KEYS[2])
end
return 1
`)

func newLifecycleManager(rdb *redis.Client, lockManager *LockManager, config LifecycleConfig) *lifecycleManager {
	if config.AssignmentPrefix == "" {
		config.AssignmentPrefix = defaultAssignmentPrefix
	}
	if config.RiderActivePrefix == "" {
		config.RiderActivePrefix = defaultRiderActivePrefix
	}
	if config.DriverActivePrefix == "" {
		config.DriverActivePrefix = defaultDriverActivePrefix
	}
	if config.DriverStatusPrefix == "" {
		config.DriverStatusPrefix = defaultDriverStatusPrefix
	}
	if config.AssignmentTTL <= 0 {
		config.AssignmentTTL = defaultAssignmentTTL
	}
	if config.DriverStatusTTL <= 0 {
		config.DriverStatusTTL = defaultDriverStatusTTL
	}
	if config.ArrivalThresholdKm <= 0 {
		config.ArrivalThresholdKm = defaultArrivalThresholdKm
	}
	return &lifecycleManager{
		redis:              rdb,
		lockManager:        lockManager,
		assignmentPrefix:   config.AssignmentPrefix,
		riderActivePrefix:  config.RiderActivePrefix,
		driverActivePrefix: config.DriverActivePrefix,
		driverStatusPrefix: config.DriverStatusPrefix,
		assignmentTTL:      config.AssignmentTTL,
		driverStatusTTL:    config.DriverStatusTTL,
		arrivalThresholdKm: config.ArrivalThresholdKm,
	}
}

func (m *lifecycleManager) assignmentKey(requestID string) string {
	return fmt.Sprintf("%s:%s", m.assignmentPrefix, requestID)
}

func (m *lifecycleManager) activeRiderKey(riderID string) string {
	return fmt.Sprintf("%s:%s", m.riderActivePrefix, riderID)
}

func (m *lifecycleManager) driverStatusKey(driverID string) string {
	return fmt.Sprintf("%s:%s", m.driverStatusPrefix, driverID)
}

func (m *lifecycleManager) activeDriverKey(driverID string) string {
	return fmt.Sprintf("%s:%s", m.driverActivePrefix, driverID)
}

func (m *lifecycleManager) driverOwner(ctx context.Context, driverID string) (string, error) {
	requestID, err := m.redis.Get(ctx, m.activeDriverKey(driverID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return requestID, err
}

// refreshActiveDriver atomically verifies durable assignment ownership before
// restoring the short-lived driver status to busy.
func (m *lifecycleManager) refreshActiveDriver(ctx context.Context, driverID string) (bool, error) {
	result, err := refreshActiveDriverScript.Run(
		ctx,
		m.redis,
		[]string{
			m.activeDriverKey(driverID),
			m.driverStatusKey(driverID),
		},
		m.assignmentPrefix,
		driverID,
		int64(m.driverStatusTTL/time.Second),
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (m *lifecycleManager) create(ctx context.Context, assignment Assignment) error {
	result, err := createAssignmentScript.Run(
		ctx,
		m.redis,
		[]string{
			m.assignmentKey(assignment.RequestID),
			m.activeRiderKey(assignment.RiderID),
			m.activeDriverKey(assignment.DriverID),
		},
		m.assignmentPrefix,
		assignment.RequestID,
		assignment.RiderID,
		assignment.DriverID,
		strconv.FormatFloat(assignment.PickupLongitude, 'g', -1, 64),
		strconv.FormatFloat(assignment.PickupLatitude, 'g', -1, 64),
		int64(m.assignmentTTL/time.Second),
	).Int()
	if err != nil {
		return err
	}
	switch result {
	case 1:
		return nil
	case 0:
		return ErrActiveAssignment
	case -1:
		return ErrRequestAlreadyExists
	case -2:
		return ErrDriverOwnershipConflict
	default:
		return fmt.Errorf("unexpected assignment create result: %d", result)
	}
}

func (m *lifecycleManager) get(ctx context.Context, requestID string) (*Assignment, error) {
	values, err := m.redis.HGetAll(ctx, m.assignmentKey(requestID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrAssignmentNotFound
	}

	lon, err := strconv.ParseFloat(values["pickup_longitude"], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid assignment pickup longitude: %w", err)
	}
	lat, err := strconv.ParseFloat(values["pickup_latitude"], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid assignment pickup latitude: %w", err)
	}
	return &Assignment{
		RequestID:       values["request_id"],
		RiderID:         values["rider_id"],
		DriverID:        values["driver_id"],
		PickupLongitude: lon,
		PickupLatitude:  lat,
		Status:          AssignmentStatus(values["status"]),
	}, nil
}

func (m *lifecycleManager) active(ctx context.Context, riderID string) (*Assignment, error) {
	requestID, err := m.redis.Get(ctx, m.activeRiderKey(riderID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	assignment, err := m.get(ctx, requestID)
	if errors.Is(err, ErrAssignmentNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if assignment.Status != AssignmentEnRoute && assignment.Status != AssignmentArrived {
		return nil, nil
	}
	return assignment, nil
}

func (m *lifecycleManager) cancel(ctx context.Context, requestID string) (*Assignment, error) {
	assignment, err := m.get(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if assignment.Status != AssignmentEnRoute {
		return nil, ErrAssignmentStateConflict
	}

	result, err := cancelAssignmentScript.Run(
		ctx,
		m.redis,
		[]string{
			m.assignmentKey(requestID),
			m.driverStatusKey(assignment.DriverID),
			m.lockManager.lockKey(assignment.DriverID),
			m.activeRiderKey(assignment.RiderID),
			m.activeDriverKey(assignment.DriverID),
		},
		requestID,
		assignment.DriverID,
		assignment.RiderID,
		int64(m.driverStatusTTL/time.Second),
		int64(m.assignmentTTL/time.Second),
	).Int()
	if err != nil {
		return nil, err
	}
	switch result {
	case 1:
		assignment.Status = AssignmentCancelled
		return assignment, nil
	case 0:
		return nil, ErrAssignmentStateConflict
	case -1:
		return nil, ErrAssignmentNotFound
	case -2:
		return nil, ErrAssignmentStateConflict
	case -3, -4:
		return nil, ErrDriverOwnershipConflict
	default:
		return nil, fmt.Errorf("assignment changed while cancelling")
	}
}

func (m *lifecycleManager) arrive(ctx context.Context, assignment *Assignment) (*Assignment, error) {
	result, err := arriveAssignmentScript.Run(
		ctx,
		m.redis,
		[]string{
			m.assignmentKey(assignment.RequestID),
			m.activeRiderKey(assignment.RiderID),
			m.activeDriverKey(assignment.DriverID),
			m.driverStatusKey(assignment.DriverID),
		},
		assignment.RequestID,
		int64(m.assignmentTTL/time.Second),
		int64(m.driverStatusTTL/time.Second),
	).Int()
	if err != nil {
		return nil, err
	}
	switch result {
	case 1:
		assignment.Status = AssignmentArrived
		return assignment, nil
	case 0:
		return nil, ErrAssignmentStateConflict
	case -1:
		return nil, ErrAssignmentNotFound
	case -2:
		return nil, ErrAssignmentStateConflict
	default:
		return nil, fmt.Errorf("unexpected assignment arrival result: %d", result)
	}
}

func (m *lifecycleManager) clearActive(ctx context.Context, riderID, requestID string) error {
	return clearActiveAssignmentScript.Run(
		ctx,
		m.redis,
		[]string{m.activeRiderKey(riderID)},
		requestID,
	).Err()
}

func (m *lifecycleManager) resetRider(ctx context.Context, riderID string) error {
	requestID, err := m.redis.Get(ctx, m.activeRiderKey(riderID)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	assignment, err := m.get(ctx, requestID)
	if errors.Is(err, ErrAssignmentNotFound) {
		return m.clearActive(ctx, riderID, requestID)
	}
	if err != nil {
		return err
	}
	if assignment.Status == AssignmentEnRoute {
		if _, err := m.cancel(ctx, requestID); err == nil {
			return nil
		} else if !errors.Is(err, ErrAssignmentStateConflict) {
			return err
		}
	}
	return clearResetAssignmentScript.Run(
		ctx,
		m.redis,
		[]string{
			m.activeRiderKey(riderID),
			m.activeDriverKey(assignment.DriverID),
		},
		requestID,
	).Err()
}

func distanceKm(lon1, lat1, lon2, lat2 float64) float64 {
	const earthRadiusKm = 6371.0088
	toRadians := func(degrees float64) float64 { return degrees * math.Pi / 180 }
	lat1Rad := toRadians(lat1)
	lat2Rad := toRadians(lat2)
	dLat := lat2Rad - lat1Rad
	dLon := toRadians(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKm * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func validateLifecycleID(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("id is required")
	}
	return nil
}

func validatePickup(assignment Assignment) error {
	return geospatial.ValidateCoordinates(assignment.PickupLongitude, assignment.PickupLatitude)
}
