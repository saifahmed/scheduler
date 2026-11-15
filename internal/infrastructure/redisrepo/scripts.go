package redisrepo

import "github.com/redis/go-redis/v9"

// KEYS layout (all share the {ns} hash tag → one Redis Cluster slot):
//   KEYS[1] pending ZSET  member=jobID score=scheduledAt (epoch ms)
//   KEYS[2] backup  ZSET  member=jobID score=lease deadline (epoch ms)
//   KEYS[3] data    HASH  jobID -> JSON envelope {type,payload,scheduled_at,created_at}
//   KEYS[4] lease   HASH  jobID -> owner token of current attempt
//   KEYS[5] attempt HASH  jobID -> attempt count
//   KEYS[6] dead    ZSET  member=jobID score=time moved to DLQ (Dead Letter Queue)

// ARGV: id, scheduledAt, envelope → 1 created, 0 duplicate
var scheduleScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[3], ARGV[1]) == 1 then return 0 end
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

// Atomically claims up to N due jobs: pending → backup (score = now + lease).
// Nothing is deleted here; deletion happens only on ack.
// ARGV: now, limit, leaseMs, workerID → flat {id, envelope, token, attempt, ...}
var fetchScript = redis.NewScript(`
local now, limit, lease, worker = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
local out = {}
for _, id in ipairs(due) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    local env = redis.call('HGET', KEYS[3], id)
    if env then
      local attempt = redis.call('HINCRBY', KEYS[5], id, 1)
      local token = worker .. ':' .. now .. ':' .. attempt
      redis.call('ZADD', KEYS[2], now + lease, id)
      redis.call('HSET', KEYS[4], id, token)
      table.insert(out, id); table.insert(out, env)
      table.insert(out, token); table.insert(out, attempt)
    end
  end
end
return out
`)

// ARGV: id, token → 1 acked, 0 lease lost
var ackScript = redis.NewScript(`
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
return 1
`)

// ARGV: id, token, newDeadline → 1 extended, 0 lease lost
var extendScript = redis.NewScript(`
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[2], 'XX', ARGV[3], ARGV[1])
return 1
`)

// ARGV: id, token, retryAt, maxAttempts, now → 0 lease lost, 1 retried, 2 dead-lettered
var nackScript = redis.NewScript(`
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
local attempt = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
if attempt >= tonumber(ARGV[4]) then
  redis.call('ZADD', KEYS[6], ARGV[5], ARGV[1])
  return 2
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
`)

// Recovers expired leases (crashed/partitioned workers): backup → pending or DLQ.
// ARGV: now, limit, maxAttempts → count recovered
var reapScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[2], id)
  redis.call('HDEL', KEYS[4], id)
  local attempt = tonumber(redis.call('HGET', KEYS[5], id) or '0')
  if attempt >= tonumber(ARGV[3]) then
    redis.call('ZADD', KEYS[6], now, id)
  else
    redis.call('ZADD', KEYS[1], now, id)
  end
end
return #expired
`)

// ARGV: id → {envelope, status, attempt} or nil
var getScript = redis.NewScript(`
local env = redis.call('HGET', KEYS[3], ARGV[1])
if not env then return nil end
local status = 'pending'
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then status = 'running'
elseif redis.call('ZSCORE', KEYS[6], ARGV[1]) then status = 'dead' end
local attempt = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
return {env, status, attempt}
`)

// ARGV: id → 1 cancelled, 0 not found, -1 running
var cancelScript = redis.NewScript(`
if redis.call('ZSCORE', KEYS[2], ARGV[1]) then return -1 end
local removed = redis.call('ZREM', KEYS[1], ARGV[1]) + redis.call('ZREM', KEYS[6], ARGV[1])
if removed == 0 then return 0 end
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
return 1
`)

// ARGV: id, at → 1 requeued, 0 not in DLQ
var requeueScript = redis.NewScript(`
if redis.call('ZREM', KEYS[6], ARGV[1]) == 0 then return 0 end
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

// ARGV: now → {pending, due, running, dead}
var statsScript = redis.NewScript(`
return {
  redis.call('ZCARD', KEYS[1]),
  redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[1]),
  redis.call('ZCARD', KEYS[2]),
  redis.call('ZCARD', KEYS[6])
}
`)
