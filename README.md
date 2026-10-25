# 📰 Scalable News Feed Engine

A social media feed architecture implementing all three fan-out strategies (push, pull, hybrid), celebrity thresholding, cursor-based pagination, and time-decay ranking — the architecture behind Twitter/X, Instagram, and LinkedIn feeds.

**Live:** [http://localhost:8004](http://localhost:8004)  
**Stack:** FastAPI · Python · Vanilla JS

---

## 📦 Where Data Lives

All state is stored **in-memory**:

| Store | What it holds | Location in code |
|-------|--------------|-----------------|
| `users` dict | `dict[user_id → {name, created_at, follower_count}]` | `main.py: users` |
| `FollowGraph._adj` | `dict[user_id → set[follower_ids]]` — who follows who | `main.py: FollowGraph` |
| `PostStore._posts` | `dict[post_id → post_dict]` — all posts | `main.py: PostStore` |
| `PostStore._by_author` | `dict[author_id → list[post_ids]]` — posts per author | `main.py: PostStore` |
| `FeedStore._feeds` | `dict[user_id → list[post_ids]]` — pre-computed feed timeline per user | `main.py: FeedStore` |
| `FanoutQueue._q` | `deque[{post_id, author_id}]` — async fan-out tasks | `main.py: FanoutQueue` |

> ⚠️ In production: `PostStore` → Cassandra or MySQL; `FeedStore` → Redis sorted sets (ZADD/ZRANGE); `FanoutQueue` → Kafka or SQS; `FollowGraph` → Neo4j or a sharded follow table.

---

## 🚀 How to Run

```bash
cd 04_news_feed
python -m uvicorn main:app --port 8004 --host 0.0.0.0 --reload
```

Open [http://localhost:8004](http://localhost:8004).  
Demo users (Alice, Bob, Charlie, Diana, Eve) with follow relationships are pre-seeded on startup.

---

## 🛠️ API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/users` | Create a user |
| `GET`  | `/api/users` | List all users with follower counts |
| `POST` | `/api/follow` | Follow a user |
| `GET`  | `/api/follow/graph` | Full follow graph (adjacency list) |
| `POST` | `/api/posts` | Create a post (auto-selects fan-out strategy) |
| `POST` | `/api/posts/{id}/like` | Like a post |
| `GET`  | `/api/feed/{user_id}` | Get feed with strategy + cursor pagination |
| `GET`  | `/api/health` | Health check |

### Example — Create a Post

```bash
curl -X POST http://localhost:8004/api/posts \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "text": "Just shipped a new feature!"}'
```

```json
{
  "post_id": "a1b2c3d4",
  "user_id": "alice",
  "text": "Just shipped a new feature!",
  "created_at": "2025-10-25T01:30:00",
  "followers_to_notify": 3,
  "fanout_strategy": "write (async)"
}
```

### Example — Get Feed with Cursor Pagination

```bash
# Page 1
curl "http://localhost:8004/api/feed/bob?strategy=hybrid&limit=5"

# Page 2 (use next_cursor from page 1 response)
curl "http://localhost:8004/api/feed/bob?strategy=hybrid&limit=5&cursor=<next_cursor>"
```

---

## ⚙️ Concepts Implemented

### 1. Fan-Out-on-Write (Push Model)
When a user posts, the post ID is immediately pushed into **every follower's feed timeline**.

```
Alice posts → System finds Alice's 1,000 followers
           → Writes post_id to Feed[bob], Feed[charlie], ... (1,000 writes)
           → Feed reads are instant O(1) — just read a list
```

**Pros:** Feed reads are blazing fast (pre-computed).  
**Cons:** Write amplification — posting once causes N writes where N = follower count. Unusable for celebrities with millions of followers.

### 2. Fan-Out-on-Read (Pull Model)
Feed is computed **on demand** when a user requests it.

```
Bob requests feed → System finds all users Bob follows
                  → Fetches recent posts from each followed user
                  → Merges + ranks the posts in memory
                  → Returns sorted result
```

**Pros:** Zero write cost — posting is O(1).  
**Cons:** Read is expensive — O(K × P) where K = users followed, P = posts per user. Requires multi-way merge.

### 3. Hybrid Strategy (Twitter/Instagram Approach)
- **Normal users** (< 5 followers in demo): Fan-out-on-Write. Their posts are pushed to all follower feeds immediately.
- **Celebrities** (≥ 5 followers): Fan-out-on-Read only. Celeb posts are never pushed to follower feeds.
- **At read time:** Fetch pre-computed feed (from write fan-out) + query recent celeb posts + merge.

This eliminates the write amplification problem for accounts with massive followings.

```python
CELEBRITY_THRESHOLD = 5  # followers needed to be treated as celebrity

is_celeb = graph.follower_count(user_id) >= CELEBRITY_THRESHOLD
if is_celeb:
    feed_store.push(user_id, post_id)   # Only self-feed
else:
    fanout_q.enqueue({...})              # Push to all followers async
```

### 4. Async Fan-Out Queue
- Fan-out is done asynchronously — the `POST /api/posts` endpoint returns immediately.
- A background coroutine drains the fan-out queue and writes to follower feed timelines.
- Prevents the post endpoint from blocking on thousands of Redis writes.

### 5. Cursor-Based Pagination

**Traditional OFFSET pagination (bad):**
```sql
SELECT * FROM posts ORDER BY score DESC OFFSET 20 LIMIT 10
-- DB scans and discards 20 rows every page
-- If new posts arrive, page 2 shows items already seen on page 1
```

**Cursor-based pagination (good):**
```
First page: GET /api/feed/bob?limit=5
Response: posts + next_cursor = "0.823456|post_a1b2c3d4"

Next page: GET /api/feed/bob?limit=5&cursor=0.823456|post_a1b2c3d4
→ WHERE (score, post_id) < (0.823456, "post_a1b2c3d4") LIMIT 5
```

- **Stable:** New posts don't shift page boundaries.
- **Efficient:** O(log N) with index on (score, post_id), not O(N) scan.
- Cursor encodes `(score, post_id)` as a deterministic bookmark.

### 6. Feed Ranking Engine
Posts in a feed are scored using time decay + engagement weighting:

```python
time_score = 1 / (1 + hours_since_post)       # newer = higher
engagement = likes * 0.1 + comments * 0.2      # viral = boosted
score = time_score + engagement
```

This pushes recent and engaging posts to the top, matching the behavior of real social feed ranking systems.

---

## 💡 Interview Talking Points

- **"How do you handle celebrities with 100M followers posting?"** → Hybrid model — celebrity posts are never pushed. Followers pull them at read time and merge with pre-computed feed.
- **"Why is OFFSET pagination bad for social feeds?"** → O(N) scan cost + page shifting when new content is inserted. Cursor pagination uses a stable bookmark tied to content, not position.
- **"How does write amplification affect system design?"** → A post from someone with 1M followers causes 1M DB writes in fan-out-on-write. This requires async queues (Kafka), fan-out workers, and careful celebrity thresholding.
- **"How do you rank feeds?"** → Time decay ensures recency; engagement signals (likes, comments, shares) boost quality content. In production, ML-powered ranking (collaborative filtering, CTR prediction) replaces heuristic formulas.
