"""Lab: TicketRush, selling concert tickets in Redis."""

import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis

# Kết nối Redis (mặc định localhost:6379 hoặc lấy từ biến môi trường nếu dùng Redis Cloud)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)


def race(fn, n=50):
    """Run fn(i) from n threads at once and return the list of results."""
    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(fn, range(n)))


# ------------------------------------------------------------------
# Step 1: Seat holds and the race condition
# ------------------------------------------------------------------
def hold_naive(event, seat, user):
    key = f"hold:{event}:{seat}"
    if r.get(key) is None:
        time.sleep(0.01)  # widen the window so the bug is easy to see
        r.set(key, user, ex=300)
        return True
    return False


def hold_nx(event, seat, user, ttl=300):
    return bool(r.set(f"hold:{event}:{seat}", user, nx=True, ex=ttl))


# ------------------------------------------------------------------
# Step 2: Hold, confirm and release atomically (Lua scripts)
# ------------------------------------------------------------------
HOLD = r.register_script("""
if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then return 0 end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3]) then return 1 end
return 0
""")

CONFIRM = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SADD', KEYS[2], ARGV[2])
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
""")

RELEASE = r.register_script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
""")


def hold(event, seat, user, ttl=300):
    return bool(HOLD(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat, ttl]))


def confirm(event, seat, user):
    return bool(CONFIRM(keys=[f"hold:{event}:{seat}", f"sold:{event}"], args=[user, seat]))


def release(event, seat, user):
    return bool(RELEASE(keys=[f"hold:{event}:{seat}"], args=[user]))


def seat_map(event, seats):
    sold = r.smembers(f"sold:{event}")
    return {
        s: "sold" if s in sold else "held" if r.exists(f"hold:{event}:{s}") else "free"
        for s in seats
    }


# ------------------------------------------------------------------
# Step 3: Flash sale on general admission
# ------------------------------------------------------------------
def buy_general(event, user):
    left = r.decr(f"stock:{event}")
    if left < 0:
        r.incr(f"stock:{event}")  # undo: nothing was left
        return False
    r.sadd(f"buyers:{event}", user)
    return True


def buy_general_broken(event, user):
    """Broken version on purpose (check-then-act race -> overselling)."""
    cur = r.get(f"stock:{event}")
    if cur is not None and int(cur) > 0:
        time.sleep(0.005)
        r.decr(f"stock:{event}")
        r.sadd(f"buyers:{event}", user)
        return True
    return False


# ------------------------------------------------------------------
# Step 4: Rate limiting (Fixed window vs Sliding window)
# ------------------------------------------------------------------
def allow_fixed(user, limit=5, window=10):
    key = f"rl:fixed:{user}:{int(time.time() // window)}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, window, nx=True)  # set the TTL only the first time
    count, _ = pipe.execute()
    return count <= limit


SLIDING = r.register_script("""
local now, window, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
if redis.call('ZCARD', KEYS[1]) < limit then
    redis.call('ZADD', KEYS[1], now, ARGV[4])
    redis.call('PEXPIRE', KEYS[1], window)
    return 1
end
return 0
""")


def allow_sliding(user, limit=5, window_ms=10_000):
    now = int(time.time() * 1000)
    return bool(SLIDING(keys=[f"rl:sliding:{user}"], args=[now, window_ms, limit, uuid.uuid4().hex]))


def try_hold(event, seat, user):
    if not allow_sliding(user):
        return "rate_limited"
    return "ok" if hold(event, seat, user) else "taken"


# ------------------------------------------------------------------
# Step 5: Virtual waiting room
# ------------------------------------------------------------------
def join_queue(event, user):
    r.zadd(f"queue:{event}", {user: time.time()}, nx=True)  # NX: re-joining keeps your place
    return r.zrank(f"queue:{event}", user) + 1


def admit(event, n):
    return [user for user, _ in r.zpopmin(f"queue:{event}", n)]


# ------------------------------------------------------------------
# Run all experiments & Step 6: Final stress test
# ------------------------------------------------------------------
if __name__ == "__main__":
    print("=== STEP 1: Seat holds & Race condition ===")
    r.flushdb()
    wins_naive = race(lambda i: hold_naive("c1", "A1", f"user{i}"))
    print("Naive winners (expect 1, got many):", sum(wins_naive))

    r.flushdb()
    wins_nx = race(lambda i: hold_nx("c1", "A1", f"user{i}"))
    print("SET NX EX winners (always 1):", sum(wins_nx))
    print("TTL of hold:c1:A1:", r.ttl("hold:c1:A1"))

    print("\n=== STEP 2: Atomic hold, confirm, release ===")
    r.flushdb()
    print("hold(c1, A1, linh):", hold("c1", "A1", "linh"))
    print("hold(c1, A1, nam):", hold("c1", "A1", "nam"))
    print("confirm(c1, A1, nam):", confirm("c1", "A1", "nam"))
    print("confirm(c1, A1, linh):", confirm("c1", "A1", "linh"))
    print("hold(c1, A1, nam) after sold:", hold("c1", "A1", "nam"))
    print("seat_map:", seat_map("c1", ["A1", "A2"]))

    print("\n=== STEP 3: Flash sale on general admission ===")
    r.flushdb()
    r.set("stock:g1", 100)
    ok = race(lambda i: buy_general("g1", f"user{i}"), n=500)
    print("Atomic DECR (sold, stock_left, buyers):", sum(ok), r.get("stock:g1"), r.scard("buyers:g1"))

    r.flushdb()
    r.set("stock:g1", 100)
    ok_broken = race(lambda i: buy_general_broken("g1", f"user{i}"), n=500)
    print("Broken GET+DECR (overselling!):", sum(ok_broken), r.get("stock:g1"), r.scard("buyers:g1"))

    print("\n=== STEP 4: Rate limiting ===")
    r.flushdb()
    print("Fixed window 8 reqs:", [allow_fixed("linh") for _ in range(8)])
    print("Sliding window 8 reqs:", [allow_sliding("linh") for _ in range(8)])

    print("\n=== STEP 5: Virtual waiting room ===")
    r.flushdb()
    for name in ["u1", "u2", "u3"]:
        print(f"join {name} -> pos:", join_queue("c1", name))
    print("re-join u1 -> pos:", join_queue("c1", "u1"))
    print("admit 2:", admit("c1", 2))
    print("u3 new pos:", join_queue("c1", "u3"))

    print("\n=== STEP 6: Final stress test (10 runs) ===")
    for run in range(1, 11):
        r.flushdb()
        SEATS = [f"S{i}" for i in range(1, 21)]

        def buyer(i):
            user = f"user{i}"
            for seat in SEATS:
                if hold("c1", seat, user):
                    time.sleep(random.random() * 0.05)  # paying
                    if confirm("c1", seat, user):
                        return seat
            return None

        got = [s for s in race(buyer, n=100) if s]
        print(f"Run {run:02d}: sold={len(got)}, distinct={len(set(got))}, redis_sold={r.scard('sold:c1')}")
        