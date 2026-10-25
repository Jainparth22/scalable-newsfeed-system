"""
╔══════════════════════════════════════════════════════════════╗
║  PROJECT 04 — News Feed  |  Port 8004                       ║
╠══════════════════════════════════════════════════════════════╣
║  Core Concepts Implemented:                                  ║
║  • Fan-out-on-Write  — push post to all followers' feeds     ║
║  • Fan-out-on-Read   — pull posts at query time              ║
║  • Hybrid strategy  — write for normal users, read for       ║
║                       celebrities (>10k followers)           ║
║  • Follow graph (adjacency list)                             ║
║  • Cursor-based pagination  (no OFFSET, no skipped rows)     ║
║  • Feed cache with invalidation                              ║
║  • Ranking pipeline  (recency + engagement score)            ║
║  • Eventual consistency  (feed may lag by seconds)           ║
║  • Event-driven fan-out  via async queue                     ║
║  • Database indexes for efficient feed retrieval             ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="News Feed", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─────────────────────────────────────────────
#  CELEBRITY THRESHOLD
#  Users with > CELEB_THRESHOLD followers use
#  fan-out-on-READ (not write) to avoid massive
#  write amplification on post creation.
# ─────────────────────────────────────────────
CELEB_THRESHOLD = 5   # kept low for demo (real: 10_000+)


# ─────────────────────────────────────────────
#  FOLLOW GRAPH  (adjacency list)
#  followers[B] = {A, C}  → A and C follow B
#  following[A] = {B, D}  → A follows B and D
#  Sharding: partition by user_id % N_SHARDS
# ─────────────────────────────────────────────
class FollowGraph:
    def __init__(self):
        self._followers: Dict[str, set] = defaultdict(set)
        self._following: Dict[str, set] = defaultdict(set)

    def follow(self, follower: str, target: str):
        if follower == target:
            raise ValueError("Cannot follow yourself")
        self._followers[target].add(follower)
        self._following[follower].add(target)

    def unfollow(self, follower: str, target: str):
        self._followers[target].discard(follower)
        self._following[follower].discard(target)

    def followers_of(self, user_id: str) -> set:
        return self._followers[user_id]

    def following_of(self, user_id: str) -> set:
        return self._following[user_id]

    def follower_count(self, user_id: str) -> int:
        return len(self._followers[user_id])

    def is_celebrity(self, user_id: str) -> bool:
        return self.follower_count(user_id) >= CELEB_THRESHOLD


# ─────────────────────────────────────────────
#  POST STORE  (simulates PostgreSQL)
#  Index: posts_by_user[user_id] sorted by created_at DESC
#  In production: composite index (user_id, created_at)
# ─────────────────────────────────────────────
class PostStore:
    def __init__(self):
        self._posts: Dict[str, dict] = {}            # post_id → post
        self._by_user: Dict[str, deque] = defaultdict(deque)  # user_id → [post_ids] (newest first)

    def save(self, post: dict):
        self._posts[post["post_id"]] = post
        self._by_user[post["user_id"]].appendleft(post["post_id"])

    def get(self, post_id: str) -> Optional[dict]:
        return self._posts.get(post_id)

    def by_user(self, user_id: str) -> list:
        return [self._posts[pid] for pid in self._by_user[user_id] if pid in self._posts]

    def all_posts(self) -> list:
        return list(self._posts.values())


# ─────────────────────────────────────────────
#  FEED STORE  (simulates Redis sorted sets)
#  fan-out-on-write: push post_id into each
#  follower's feed list immediately on post creation.
#  Sorted by timestamp → cursor pagination works.
# ─────────────────────────────────────────────
class FeedStore:
    MAX_FEED_SIZE = 200   # trim older entries (like Redis ZREMRANGEBYRANK)

    def __init__(self):
        self._feeds: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.MAX_FEED_SIZE))

    def push(self, user_id: str, post_id: str):
        self._feeds[user_id].appendleft(post_id)

    def get(self, user_id: str, limit: int = 20) -> list:
        return list(self._feeds[user_id])[:limit]

    def invalidate(self, user_id: str):
        """Cache invalidation on unfollow / post delete"""
        self._feeds[user_id].clear()


# ─────────────────────────────────────────────
#  RANKING ENGINE
#  Score = recency_score * 0.7 + engagement * 0.3
#  In production: ML model, collaborative filtering
# ─────────────────────────────────────────────
class RankingEngine:
    RECENCY_HALF_LIFE = 3600  # posts decay in relevance over 1h

    def score(self, post: dict, viewer_id: str) -> float:
        age_sec = time.time() - post["ts"]
        recency = 1 / (1 + age_sec / self.RECENCY_HALF_LIFE)
        engagement = post.get("likes", 0) * 0.1 + post.get("comments", 0) * 0.2
        return round(recency * 0.7 + min(engagement, 1.0) * 0.3, 4)

    def rank(self, posts: list, viewer_id: str) -> list:
        return sorted(posts, key=lambda p: self.score(p, viewer_id), reverse=True)


# ─────────────────────────────────────────────
#  FAN-OUT QUEUE  (async write fan-out)
#  Post creation enqueues fan-out job.
#  Worker pushes post_id to all follower feeds.
#  This decouples post latency from fan-out cost.
#  For celebrities: skip write fan-out (use read)
# ─────────────────────────────────────────────
class FanOutQueue:
    def __init__(self):
        self._q: deque = deque()
        self.processed = 0

    def enqueue(self, job: dict):
        self._q.append(job)

    async def worker(self, graph: FollowGraph, feed_store: FeedStore):
        while True:
            await asyncio.sleep(0.02)
            while self._q:
                job = self._q.popleft()
                post_id  = job["post_id"]
                author   = job["author_id"]
                followers = graph.followers_of(author)
                # Fan-out-on-write: push to all follower feeds
                for uid in followers:
                    feed_store.push(uid, post_id)
                # Also push to author's own feed
                feed_store.push(author, post_id)
                self.processed += 1


# ─── Singletons ───────────────────────────────
graph      = FollowGraph()
post_store = PostStore()
feed_store = FeedStore()
ranker     = RankingEngine()
fanout_q   = FanOutQueue()

# User registry
users: Dict[str, dict] = {}


# ─────────────────────────────────────────────
#  STARTUP — seed demo data
# ─────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    asyncio.create_task(fanout_q.worker(graph, feed_store))
    # Seed 5 users
    demo_users = [
        ("alice", "Alice Chen"),
        ("bob",   "Bob Singh"),
        ("carol", "Carol Sharma"),
        ("dave",  "Dave Patel"),
        ("eve",   "Eve Gupta"),   # will become celebrity
    ]
    for uid, name in demo_users:
        users[uid] = {"user_id": uid, "name": name,
                      "created_at": datetime.now().isoformat()}
    # Follows
    for u in ["alice", "bob", "carol", "dave"]:
        graph.follow(u, "eve")
    graph.follow("alice", "bob")
    graph.follow("bob",   "carol")

    # Seed posts
    for i, (author, text) in enumerate([
        ("eve",   "Fan-out-on-write pushes your post to all follower feeds immediately 🚀"),
        ("alice", "Cursor pagination beats OFFSET — no skipped rows on concurrent inserts"),
        ("bob",   "Sliding window counter: O(1) space, ~99% accurate. Used by Cloudflare"),
        ("eve",   "Celebrity problem: 10M followers × 1 post = 10M writes. Use hybrid!"),
        ("carol", "Transactional outbox: save to DB + outbox in one TX, relay separately"),
    ]):
        pid = f"post_{i+1}"
        post = {"post_id": pid, "user_id": author,
                "text": text, "ts": time.time() - (5-i)*120,
                "created_at": datetime.now().isoformat(),
                "likes": (i+1)*3, "comments": i}
        post_store.save(post)
        if not graph.is_celebrity(author):
            fanout_q.enqueue({"post_id": pid, "author_id": author})
        else:
            # celebrity: skip write fan-out
            feed_store.push(author, pid)


# ─────────────────────────────────────────────
#  CURSOR PAGINATION HELPER
#  Cursor = base64(last_post_ts + ":" + last_post_id)
#  Avoids OFFSET: no rows skipped during concurrent inserts
# ─────────────────────────────────────────────
import base64

def encode_cursor(post: dict) -> str:
    raw = f"{post['ts']}:{post['post_id']}"
    return base64.b64encode(raw.encode()).decode()

def decode_cursor(cursor: str) -> tuple:
    try:
        raw = base64.b64decode(cursor).decode()
        ts_str, pid = raw.rsplit(":", 1)
        return float(ts_str), pid
    except Exception:
        return (float("inf"), "")


# ─────────────────────────────────────────────
#  MODELS
# ─────────────────────────────────────────────
class CreateUserReq(BaseModel):
    user_id: str
    name: str

class FollowReq(BaseModel):
    follower_id: str
    target_id: str

class CreatePostReq(BaseModel):
    user_id: str
    text: str

class LikeReq(BaseModel):
    post_id: str
    user_id: str


# ─────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────
@app.post("/api/users")
def create_user(req: CreateUserReq):
    if req.user_id in users:
        raise HTTPException(400, "User already exists")
    users[req.user_id] = {"user_id": req.user_id, "name": req.name,
                          "created_at": datetime.now().isoformat()}
    return users[req.user_id]

@app.get("/api/users")
def list_users():
    return [{"user_id": u["user_id"], "name": u["name"],
             "followers": graph.follower_count(u["user_id"]),
             "following": len(graph.following_of(u["user_id"])),
             "is_celebrity": graph.is_celebrity(u["user_id"])}
            for u in users.values()]

@app.post("/api/follow")
def follow(req: FollowReq):
    if req.follower_id not in users or req.target_id not in users:
        raise HTTPException(404, "User not found")
    try:
        graph.follow(req.follower_id, req.target_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    # Invalidate follower's feed cache (will be rebuilt on next read)
    feed_store.invalidate(req.follower_id)
    return {"followed": True, "target": req.target_id,
            "target_followers": graph.follower_count(req.target_id),
            "target_is_celebrity": graph.is_celebrity(req.target_id)}

@app.post("/api/unfollow")
def unfollow(req: FollowReq):
    graph.unfollow(req.follower_id, req.target_id)
    feed_store.invalidate(req.follower_id)
    return {"unfollowed": True}

@app.post("/api/posts")
def create_post(req: CreatePostReq):
    if req.user_id not in users:
        raise HTTPException(404, "User not found")

    is_celeb = graph.is_celebrity(req.user_id)
    post_id  = str(uuid.uuid4())[:8]
    post = {"post_id": post_id, "user_id": req.user_id, "text": req.text,
            "ts": time.time(), "created_at": datetime.now().isoformat(),
            "likes": 0, "comments": 0,
            "fanout_strategy": "read" if is_celeb else "write"}
    post_store.save(post)

    if is_celeb:
        # Fan-out-on-READ: don't write to follower feeds.
        # Followers will pull this post on next feed request.
        feed_store.push(req.user_id, post_id)
    else:
        # Fan-out-on-WRITE: push to all follower feeds async
        fanout_q.enqueue({"post_id": post_id, "author_id": req.user_id})

    return {**post, "followers_to_notify": graph.follower_count(req.user_id),
            "fanout_strategy": "read (celebrity)" if is_celeb else "write (async)"}

@app.post("/api/posts/{post_id}/like")
def like_post(post_id: str, req: LikeReq):
    post = post_store.get(post_id)
    if not post: raise HTTPException(404, "Post not found")
    post["likes"] += 1
    return {"post_id": post_id, "likes": post["likes"]}

@app.get("/api/feed/{user_id}")
def get_feed(user_id: str, strategy: str = "hybrid",
             cursor: Optional[str] = None, limit: int = 10):
    if user_id not in users:
        raise HTTPException(404, "User not found")

    posts_to_rank = []

    if strategy == "write":
        # Pure fan-out-on-write: read pre-computed feed
        post_ids = feed_store.get(user_id, limit * 3)
        posts_to_rank = [p for pid in post_ids if (p := post_store.get(pid))]

    elif strategy == "read":
        # Pure fan-out-on-read: merge all followees' timelines at read time
        following = graph.following_of(user_id)
        for fid in following:
            posts_to_rank.extend(post_store.by_user(fid))
        posts_to_rank.extend(post_store.by_user(user_id))  # own posts

    else:  # hybrid (default)
        # Normal followees: use pre-computed feed (write)
        post_ids = feed_store.get(user_id, limit * 2)
        posts_to_rank = [p for pid in post_ids if (p := post_store.get(pid))]
        # Celebrity followees: merge on read
        for fid in graph.following_of(user_id):
            if graph.is_celebrity(fid):
                posts_to_rank.extend(post_store.by_user(fid))

    # Deduplicate
    seen_ids = set()
    unique = []
    for p in posts_to_rank:
        if p["post_id"] not in seen_ids:
            seen_ids.add(p["post_id"])
            unique.append(p)

    # Rank
    ranked = ranker.rank(unique, user_id)

    # Cursor-based pagination
    if cursor:
        cursor_ts, cursor_id = decode_cursor(cursor)
        ranked = [p for p in ranked if p["ts"] < cursor_ts or
                  (p["ts"] == cursor_ts and p["post_id"] > cursor_id)]

    page = ranked[:limit]
    next_cursor = encode_cursor(page[-1]) if len(page) == limit else None

    return {
        "user_id":   user_id,
        "strategy":  strategy,
        "posts":     page,
        "next_cursor": next_cursor,
        "has_more":  next_cursor is not None,
        "total_ranked": len(ranked),
        "fanout_processed": fanout_q.processed,
    }

@app.get("/api/health")
def health():
    return {"status": "ok", "project": "News Feed", "port": 8004,
            "fanout_processed": fanout_q.processed}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
