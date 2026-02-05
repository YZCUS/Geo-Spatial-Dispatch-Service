package ratelimiter

// Lua script for atomic budget deduction
const deductBudgetScript = `
-- KEYS[1]: budget key
-- ARGV[1]: cost amount
-- Returns: remaining budget if successful, -1 if insufficient

local current = redis.call('GET', KEYS[1])
if not current then
    return -1
end

current = tonumber(current)
local cost = tonumber(ARGV[1])

if current >= cost then
    local remaining = current - cost
    redis.call('SET', KEYS[1], remaining)
    return remaining
else
    return -1
end
`
