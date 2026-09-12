import os
import uuid
import hashlib
import secrets
import hmac
import base64
import time
import asyncio
import queue
import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form, UploadFile, File, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

try:
    from pywebpush import webpush, WebPushException
except Exception:
    webpush = None
    WebPushException = Exception

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY_TEXT = os.getenv("SECRET_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL در فایل .env پیدا نشد.")
if not SECRET_KEY_TEXT:
    raise RuntimeError("SECRET_KEY در فایل .env پیدا نشد.")
if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL در فایل .env پیدا نشد.")
if not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_KEY در فایل .env پیدا نشد.")

SECRET_KEY = SECRET_KEY_TEXT.encode("utf-8")

app = FastAPI()

BUCKET_NAME = "voices"
MAX_AUDIO_SIZE = 10 * 1024 * 1024
MAX_IMAGE_SIZE = 8 * 1024 * 1024
MAX_AVATAR_SIZE = 5 * 1024 * 1024
SIGNED_URL_SECONDS = 3600
ALLOWED_IMAGE_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
ALLOWED_AUDIO_TYPES = {"audio/webm", "audio/ogg", "audio/mp4", "audio/mpeg"}

OWNER_USERNAME = "MS__Hesam"

# username -> set of active WebSocket connections (multi-device friendly)
connections = {}
connection_kinds = {}

VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS_EMAIL = os.getenv("VAPID_CLAIMS_EMAIL", "mailto:admin@example.com")

# WebRTC ICE/TURN configuration. STUN is free; TURN is strongly recommended
# for users behind restrictive NAT/firewalls. TURN credentials stay server-side
# and are exposed only as short-lived/configured ICE values through the API.
TURN_URLS = [x.strip() for x in os.getenv("TURN_URLS", "").split(",") if x.strip()]
TURN_USERNAME = os.getenv("TURN_USERNAME", "")
TURN_CREDENTIAL = os.getenv("TURN_CREDENTIAL", "")
TURN_ICE_SERVERS_JSON = os.getenv("TURN_ICE_SERVERS_JSON", "")
TURN_CREDENTIALS_URL = os.getenv("TURN_CREDENTIALS_URL", "")
RTC_CONFIG_CACHE_TTL = 240
_rtc_config_cache = None
_rtc_config_cache_until = 0.0

# Reuse HTTP connections to Supabase instead of opening a fresh TCP/TLS
# connection for every upload/sign request.
SUPABASE_SESSION = requests.Session()

# Small in-process caches for read-heavy data.
_users_cache = None
_users_cache_until = 0.0
_membership_cache = {}
MEMBERSHIP_CACHE_TTL = 15
USERS_CACHE_TTL = 2

# ---------------------------------------------------------
# PERFORMANCE CACHES
# Signed Supabase URLs are expensive to generate. Cache them
# briefly so the user list/history does not trigger the same
# network request over and over.
SIGNED_URL_CACHE_TTL = 3300  # 55 minutes
_signed_url_cache = {}

# Keep history responses reasonably small for fast first render.
MAX_HISTORY_MESSAGES = 200
GROUP_INFO_CACHE_TTL = 15
_group_info_cache = {}

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_FOLDER), name="uploads")


DB_POOL_SIZE = 4

class _DBPool:
    def __init__(self, size=DB_POOL_SIZE):
        self.size = size
        self.q = queue.LifoQueue(maxsize=size)
        self.connections = []
        for _ in range(size):
            conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=15)
            self.connections.append(conn)
            self.q.put(conn)

    @contextmanager
    def connection(self):
        conn = self.q.get()
        try:
            yield conn
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self.q.put(conn)

DB_POOL = None
_DB_POOL_LOCK = threading.Lock()
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False

def init_db():
    """Run schema setup once per process, safely serialized across Render instances."""
    last_error = None
    for attempt in range(1, 4):
        try:
            # Use one dedicated connection for startup migrations.
            with psycopg.connect(
                DATABASE_URL,
                row_factory=dict_row,
                connect_timeout=20,
            ) as connection:
                with connection.cursor() as cursor:
                    # Do not let the DB/pooler's default statement timeout kill
                    # schema creation while another instance is deploying.
                    cursor.execute("SET statement_timeout = 15000")
                    cursor.execute("SET lock_timeout = 5000")
                    cursor.execute("SELECT pg_advisory_lock(hashtext('ms_chat_schema_v6'))")

                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            id BIGSERIAL PRIMARY KEY,
                            username TEXT UNIQUE NOT NULL,
                            password_hash TEXT NOT NULL,
                            display_name TEXT,
                            avatar TEXT,
                            last_seen TIMESTAMP,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS messages (
                            id BIGSERIAL PRIMARY KEY,
                            sender TEXT NOT NULL,
                            receiver TEXT NOT NULL,
                            message TEXT,
                            audio TEXT,
                            status TEXT DEFAULT 'sent',
                            message_type TEXT DEFAULT 'text',
                            media TEXT,
                            reply_to BIGINT,
                            edited BOOLEAN DEFAULT FALSE,
                            deleted BOOLEAN DEFAULT FALSE,
                            group_id BIGINT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS groups (
                            id BIGSERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            avatar TEXT,
                            owner TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS group_create_requests (
                            request_key TEXT PRIMARY KEY,
                            group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS group_members (
                            group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
                            username TEXT NOT NULL,
                            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (group_id, username)
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS push_subscriptions (
                            id BIGSERIAL PRIMARY KEY,
                            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                            endpoint TEXT UNIQUE NOT NULL,
                            subscription_json TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS hidden_conversations (
                            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                            other_username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                            hidden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (username, other_username)
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS group_message_reads (
                            message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                            read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (message_id, username)
                        )
                    """)

                    for sql in [
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT",
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT",
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS audio TEXT",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'sent'",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS message_type TEXT DEFAULT 'text'",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS media TEXT",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to BIGINT",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT FALSE",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE",
                        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS group_id BIGINT",
                    ]:
                        cursor.execute(sql)

                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_direct ON messages(sender, receiver, id DESC) WHERE group_id IS NULL")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_direct_reverse ON messages(receiver, sender, id DESC) WHERE group_id IS NULL")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(group_id, id DESC) WHERE group_id IS NOT NULL")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_receiver_status ON messages(receiver, sender, status) WHERE group_id IS NULL AND deleted=FALSE")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(username, group_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_display_name ON users(LOWER(COALESCE(display_name, username)))")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(sender, receiver, id DESC)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_unread ON messages(receiver, status, id DESC) WHERE group_id IS NULL AND deleted=FALSE")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_push_subscriptions_username ON push_subscriptions(username)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_message_reads_message ON group_message_reads(message_id, read_at)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_message_reads_user ON group_message_reads(username, message_id)")

                    cursor.execute("SELECT pg_advisory_unlock(hashtext('ms_chat_schema_v6'))")
                connection.commit()
            print("✅ Database schema is ready.")
            return
        except Exception as error:
            last_error = error
            print(f"⚠️ Database init attempt {attempt}/3 failed: {error}")
            time.sleep(attempt * 2)
    raise RuntimeError(f"Database initialization failed: {last_error}")


def _ensure_schema():
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        last_error = None
        for attempt in range(1, 4):
            try:
                init_db()
                _SCHEMA_READY = True
                print("✅ Database schema is ready.")
                return
            except Exception as error:
                last_error = error
                print(f"⚠️ Database schema attempt {attempt}/3 failed: {error}")
                if attempt < 3:
                    time.sleep(attempt * 1.5)
        raise RuntimeError(f"Database schema initialization failed: {last_error}")


def _get_pool():
    global DB_POOL
    if DB_POOL is None:
        with _DB_POOL_LOCK:
            if DB_POOL is None:
                DB_POOL = _DBPool()
    return DB_POOL


def get_db():
    # Lazy initialization keeps Render's port-binding path free of DB work.
    _ensure_schema()
    return _get_pool().connection()


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    password_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, 200_000
    )
    return (
        base64.b64encode(salt).decode("utf-8")
        + ":"
        + base64.b64encode(password_hash).decode("utf-8")
    )


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt_text, hash_text = stored_hash.split(":")
        salt = base64.b64decode(salt_text)
        original_hash = base64.b64decode(hash_text)
        new_hash = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, 200_000
        )
        return hmac.compare_digest(original_hash, new_hash)
    except Exception:
        return False


def create_token(username: str) -> str:
    signature = hmac.new(
        SECRET_KEY, username.encode("utf-8"), hashlib.sha256
    ).digest()
    return base64.urlsafe_b64encode(signature).decode("utf-8")


def verify_token(username: str, token: str) -> bool:
    return hmac.compare_digest(create_token(username), token)


def now_iso(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def user_exists(username: str) -> bool:
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM users WHERE username=%s", (username,)
            )
            return cursor.fetchone() is not None


def get_user_info(username: str):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT username, display_name, avatar, last_seen
                FROM users WHERE username=%s
            """, (username,))
            user = cursor.fetchone()
    if not user:
        return None
    return {
        "username": user["username"],
        "display_name": user["display_name"] or user["username"],
        "avatar": user["avatar"],
        "last_seen": now_iso(user["last_seen"]),
        "online": bool(connections.get(username)),
    }


def invalidate_users_cache():
    global _users_cache, _users_cache_until
    _users_cache = None
    _users_cache_until = 0.0


def get_all_users():
    import time as _time
    global _users_cache, _users_cache_until
    now = _time.time()
    if _users_cache is None or _users_cache_until <= now:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT username, display_name, avatar, last_seen
                    FROM users ORDER BY LOWER(COALESCE(display_name, username))
                """)
                rows = cursor.fetchall()
        base = {}
        for row in rows:
            username = row["username"]
            base[username] = {
                "username": username,
                "display_name": row["display_name"] or username,
                "avatar": row["avatar"],
                "last_seen": now_iso(row["last_seen"]),
            }
        _users_cache = base
        _users_cache_until = now + USERS_CACHE_TTL
    return [
        {**info, "online": bool(connections.get(username))}
        for username, info in _users_cache.items()
    ]


def storage_headers(content_type="application/octet-stream", upsert=False):
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": content_type,
        "x-upsert": "true" if upsert else "false",
    }


def upload_storage(path: str, content: bytes, content_type: str):
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{path}"
    response = SUPABASE_SESSION.post(
        url,
        headers=storage_headers(content_type),
        data=content,
        timeout=60,
    )
    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Storage upload failed: {response.status_code} {response.text}"
        )


def delete_storage(path: str):
    if not path:
        return
    filename = path.split("storage:", 1)[1] if path.startswith("storage:") else path
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}"
    response = SUPABASE_SESSION.delete(
        url,
        headers=storage_headers("application/json"),
        json={"prefixes": [filename]},
        timeout=30,
    )
    if response.status_code not in (200, 204):
        print("Storage delete warning:", response.status_code, response.text)


def _storage_path_only(storage_path: str):
    if not storage_path:
        return None
    if storage_path.startswith("storage:"):
        return storage_path[len("storage:"):]
    return storage_path


def _cache_signed_url(cache_key, value):
    import time as _time
    _signed_url_cache[cache_key] = (_time.time() + SIGNED_URL_CACHE_TTL, value)


def create_signed_urls(storage_paths):
    """Create signed URLs for many files in one Supabase Storage request.

    Supabase exposes a batch signed-URL endpoint, so a history containing
    many images/voices no longer causes one HTTP request per message.
    """
    if not storage_paths:
        return {}

    import time as _time
    result = {}
    pending = []
    seen = set()

    for original in storage_paths:
        if not original:
            continue
        path = _storage_path_only(original)
        if not path:
            continue
        if original.startswith("/uploads/") or original.startswith("http"):
            result[original] = original
            continue
        cached = _signed_url_cache.get((original, True))
        if cached and cached[0] > _time.time():
            result[original] = cached[1]
            continue
        if path not in seen:
            seen.add(path)
            pending.append(path)

    if not pending:
        return result

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET_NAME}"
    try:
        response = SUPABASE_SESSION.post(
            url,
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "apikey": SUPABASE_SERVICE_KEY,
                "Content-Type": "application/json",
            },
            json={"expiresIn": SIGNED_URL_SECONDS, "paths": pending},
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("signedURLs") or []
        by_path = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            path = row.get("path")
            signed = row.get("signedURL") or row.get("signedUrl") or row.get("signed_url")
            if path and signed:
                full = signed if signed.startswith("http") else SUPABASE_URL + signed
                by_path[path] = full

        for original in storage_paths:
            path = _storage_path_only(original)
            if path and path in by_path:
                value = by_path[path]
                result[original] = value
                _cache_signed_url((original, True), value)
        return result
    except Exception as error:
        print("Batch signed URL error:", error)
        # Fallback to the old single-file route only for paths that failed.
        for original in storage_paths:
            if original in result:
                continue
            value = create_signed_url(original, _fallback=True)
            if value:
                result[original] = value
        return result


def create_signed_url(storage_path: str, is_media=False, _fallback=False):
    if not storage_path:
        return None
    if storage_path.startswith("/uploads/") or storage_path.startswith("http"):
        return storage_path

    import time as _time
    cache_key = (storage_path, bool(is_media))
    cached = _signed_url_cache.get(cache_key)
    if cached and cached[0] > _time.time():
        return cached[1]

    # Use the batch API for normal calls; the private fallback is used only
    # when the batch request itself failed.
    if not _fallback:
        return create_signed_urls([storage_path]).get(storage_path)

    filename = _storage_path_only(storage_path)
    url = f"{SUPABASE_URL}/storage/v1/object/sign/{BUCKET_NAME}/{filename}"
    try:
        response = SUPABASE_SESSION.post(
            url,
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "apikey": SUPABASE_SERVICE_KEY,
                "Content-Type": "application/json",
            },
            json={"expiresIn": SIGNED_URL_SECONDS},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        signed = (
            data.get("signedURL")
            or data.get("signedUrl")
            or data.get("signed_url")
            or data.get("path")
        )
        if not signed:
            return None
        result = signed if signed.startswith("http") else SUPABASE_URL + signed
        _cache_signed_url(cache_key, result)
        return result
    except Exception as error:
        print("Signed URL exception:", error)
        return None

def add_storage_prefix(path: str):
    return f"storage:{path}"


def download_storage_object(storage_path: str, range_header=None):
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{storage_path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }
    if range_header:
        headers["Range"] = range_header
    return SUPABASE_SESSION.get(url, headers=headers, timeout=30)


def mark_last_seen(username: str):
    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE username=%s",
                    (username,),
                )
            connection.commit()
    except Exception as error:
        print("Last seen error:", error)


async def send_to(username: str, data: dict) -> bool:
    sockets=list(connections.get(username, set()))
    if not sockets:
        return False
    delivered=False
    dead=[]
    for ws in sockets:
        try:
            await ws.send_json(data)
            delivered=True
        except Exception:
            dead.append(ws)
    if dead and username in connections:
        for ws in dead:
            connections[username].discard(ws)
        if not connections[username]:
            connections.pop(username,None)
            connection_kinds.pop(username,None)
    return delivered


async def broadcast_users():
    users = get_recent_or_search_broadcast_users()
    payload = {"type": "presence", "users": users}
    for username, sockets in list(connections.items()):
        for ws in list(sockets):
            try:
                await ws.send_json(payload)
            except Exception:
                sockets.discard(ws)
        if not sockets:
            connections.pop(username,None)
            connection_kinds.pop(username,None)

def get_recent_or_search_broadcast_users():
    # Broadcast all users for presence/search compatibility; the UI decides which to show.
    return get_all_users()


def message_row_to_dict(row, signed_media=None):
    signed_media = signed_media or {}
    return {
        "id": row["id"],
        "sender": row["sender"],
        "receiver": row["receiver"],
        "message": "" if row["deleted"] else (row["message"] or ""),
        "audio": row["audio"] if row["audio"] else None,
        "media": row["media"] if row["media"] else None,
        "status": row["status"] or "sent",
        "message_type": row["message_type"] or "text",
        "reply_to": row["reply_to"],
        "edited": bool(row["edited"]),
        "deleted": bool(row["deleted"]),
        "group_id": row["group_id"],
        "created_at": now_iso(row["created_at"]),
    }


def rows_to_messages(rows):
    return [message_row_to_dict(row) for row in reversed(rows)]

def get_direct_history(user1, user2):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, sender, receiver, message, audio, media, status,
                       message_type, reply_to, edited, deleted, group_id, created_at
                FROM messages
                WHERE group_id IS NULL
                  AND ((sender=%s AND receiver=%s) OR (sender=%s AND receiver=%s))
                ORDER BY id DESC LIMIT %s
            """, (user1, user2, user2, user1, MAX_HISTORY_MESSAGES))
            rows = cursor.fetchall()
    return rows_to_messages(rows)


def invalidate_membership_cache(group_id=None):
    if group_id is None:
        _membership_cache.clear()
        return
    for key in list(_membership_cache):
        if key[0] == int(group_id):
            _membership_cache.pop(key, None)


def is_group_member(group_id, username):
    import time as _time
    key = (int(group_id), username)
    cached = _membership_cache.get(key)
    if cached and cached[0] > _time.time():
        return cached[1]
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM group_members WHERE group_id=%s AND username=%s",
                (group_id, username),
            )
            value = cursor.fetchone() is not None
    _membership_cache[key] = (_time.time() + MEMBERSHIP_CACHE_TTL, value)
    return value


def get_group_info(group_id):
    now = time.time()
    cached = _group_info_cache.get(group_id)
    if cached and cached[0] > now:
        return cached[1]

    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT g.id, g.name, g.avatar, g.owner, g.created_at,
                       COUNT(gm.username) AS members_count
                FROM groups g
                LEFT JOIN group_members gm ON gm.group_id = g.id
                WHERE g.id=%s
                GROUP BY g.id
            """, (group_id,))
            group = cursor.fetchone()
            if not group:
                return None
            cursor.execute("""
                SELECT gm.username, u.display_name, u.avatar
                FROM group_members gm
                JOIN users u ON u.username = gm.username
                WHERE gm.group_id=%s
                ORDER BY gm.joined_at, gm.username
            """, (group_id,))
            member_rows = cursor.fetchall()

    members = [{
        "username": r["username"],
        "display_name": r["display_name"] or r["username"],
        "avatar": r["avatar"],
    } for r in member_rows]

    info = {
        "id": group["id"],
        "name": group["name"],
        "avatar": group["avatar"],
        "owner": group["owner"],
        "members": members,
        "members_count": int(group["members_count"] or 0),
        "created_at": now_iso(group["created_at"]),
    }
    _group_info_cache[group_id] = (now + GROUP_INFO_CACHE_TTL, info)
    return info


def get_user_groups(username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT g.id, g.name, g.avatar, g.owner, COUNT(allm.username) AS members_count
                FROM groups g
                JOIN group_members mine ON mine.group_id=g.id AND mine.username=%s
                LEFT JOIN group_members allm ON allm.group_id=g.id
                GROUP BY g.id
                ORDER BY g.id DESC
            """, (username,))
            rows = cursor.fetchall()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "avatar": r["avatar"],
            "owner": r["owner"],
            "members_count": int(r["members_count"] or 0),
        }
        for r in rows
    ]


def get_group_history(group_id, viewer=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, sender, receiver, message, audio, media, status,
                       message_type, reply_to, edited, deleted, group_id, created_at
                FROM messages WHERE group_id=%s ORDER BY id DESC LIMIT %s
            """, (group_id, MAX_HISTORY_MESSAGES))
            rows = cursor.fetchall()
            messages = rows_to_messages(rows)

            # For the current user's own group messages, include who has read them.
            own_ids = [int(m["id"]) for m in messages if viewer and m.get("sender") == viewer and m.get("group_id")]
            if own_ids:
                placeholders = ",".join(["%s"] * len(own_ids))
                cursor.execute(f"""
                    SELECT r.message_id, r.username,
                           COALESCE(u.display_name, u.username) AS display_name,
                           r.read_at
                    FROM group_message_reads r
                    JOIN users u ON u.username=r.username
                    WHERE r.message_id IN ({placeholders})
                    ORDER BY r.message_id, r.read_at, r.username
                """, own_ids)
                read_rows = cursor.fetchall()
            else:
                read_rows = []

    readers_by_message = {}
    for r in read_rows:
        readers_by_message.setdefault(str(r["message_id"]), []).append({
            "username": r["username"],
            "display_name": r["display_name"] or r["username"],
            "read_at": now_iso(r["read_at"]),
        })
    for msg in messages:
        if msg.get("group_id") and msg.get("sender") == viewer:
            msg["readers"] = readers_by_message.get(str(msg["id"]), [])
            msg["read_count"] = len(msg["readers"])
    return messages


def mark_group_messages_read(group_id, username):
    """Mark all currently unread incoming group messages as read for one user.

    Group message state is per-member, so we never change the shared messages.status.
    Returns the newly-read message ids grouped by sender for live receipt updates.
    """
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, sender
                FROM messages
                WHERE group_id=%s
                  AND sender<>%s
                  AND deleted=FALSE
                  AND NOT EXISTS (
                      SELECT 1 FROM group_message_reads r
                      WHERE r.message_id=messages.id AND r.username=%s
                  )
            """, (group_id, username, username))
            rows = cursor.fetchall()

            if rows:
                ids = [r["id"] for r in rows]
                values = [(int(mid), username) for mid in ids]
                cursor.executemany("""
                    INSERT INTO group_message_reads(message_id, username)
                    VALUES (%s,%s)
                    ON CONFLICT (message_id, username) DO NOTHING
                """, values)
            connection.commit()
    by_sender = {}
    for r in rows:
        by_sender.setdefault(r["sender"], []).append(int(r["id"]))
    return by_sender


def get_group_message_readers(message_id, viewer):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT m.group_id, m.sender
                FROM messages m
                WHERE m.id=%s
            """, (message_id,))
            msg = cursor.fetchone()
            if not msg:
                return None, []
            if not msg["group_id"] or msg["sender"] != viewer:
                return None, []
            cursor.execute("""
                SELECT r.username, COALESCE(u.display_name, u.username) AS display_name,
                       r.read_at
                FROM group_message_reads r
                JOIN users u ON u.username=r.username
                JOIN group_members gm ON gm.group_id=%s AND gm.username=r.username
                WHERE r.message_id=%s
                ORDER BY r.read_at, r.username
            """, (msg["group_id"], message_id))
            rows = cursor.fetchall()
    return msg["group_id"], [{
        "username": r["username"],
        "display_name": r["display_name"] or r["username"],
        "read_at": now_iso(r["read_at"]),
    } for r in rows]


def save_message(sender, receiver, message="", message_type="text",
                 media=None, audio=None, reply_to=None, group_id=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
            # A new direct message makes a previously hidden conversation
            # visible again for both sides. Group messages are unaffected.
            if group_id is None and receiver:
                cursor.execute("""
                    DELETE FROM hidden_conversations
                    WHERE (username=%s AND other_username=%s)
                       OR (username=%s AND other_username=%s)
                """, (sender, receiver, receiver, sender))
            cursor.execute("""
                INSERT INTO messages
                (sender, receiver, message, audio, status, message_type, media,
                 reply_to, edited, deleted, group_id)
                VALUES (%s,%s,%s,%s,'sent',%s,%s,%s,FALSE,FALSE,%s)
                RETURNING id
            """, (
                sender, receiver, message, audio, message_type, media,
                reply_to, group_id
            ))
            message_id = cursor.fetchone()["id"]
        connection.commit()
    return message_id


def get_message(message_id):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, sender, receiver, message, audio, media, status,
                       message_type, reply_to, edited, deleted, group_id, created_at
                FROM messages WHERE id=%s
            """, (message_id,))
            row = cursor.fetchone()
    if not row:
        return None
    return message_row_to_dict(row)


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def home():
    return FileResponse("index.html")


@app.get("/background.png")
async def background_image():
    return FileResponse("background.png", media_type="image/png")


@app.get("/manifest.webmanifest")
async def manifest_file():
    return FileResponse("manifest.webmanifest", media_type="application/manifest+json", headers={"Cache-Control":"no-cache"})


@app.get("/sw.js")
async def service_worker():
    return FileResponse("sw.js", media_type="application/javascript", headers={"Cache-Control":"no-cache"})




@app.post("/register")
async def register(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if len(username) < 3:
        return {"success": False, "message": "نام کاربری باید حداقل ۳ کاراکتر باشد."}
    if len(username) > 30:
        return {"success": False, "message": "نام کاربری بیش از حد طولانی است."}
    if len(password) < 6:
        return {"success": False, "message": "رمز عبور باید حداقل ۶ کاراکتر باشد."}
    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO users (username,password_hash,display_name)
                    VALUES (%s,%s,%s)
                """, (username, hash_password(password), username))
            connection.commit()
        invalidate_users_cache()
        return {"success": True, "message": "✅ حساب شما با موفقیت ساخته شد. حالا می‌توانید با همین نام کاربری وارد شوید."}
    except psycopg.errors.UniqueViolation:
        return {"success": False, "message": "این نام کاربری قبلاً ثبت شده است."}



def get_recent_chat_users(username):
    """Return visible direct-chat users ordered by latest message."""
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH direct AS (
                    SELECT
                        id,
                        CASE WHEN sender=%s THEN receiver ELSE sender END AS other_user
                    FROM messages
                    WHERE group_id IS NULL
                      AND deleted = FALSE
                      AND (sender=%s OR receiver=%s)
                ),
                latest AS (
                    SELECT other_user, MAX(id) AS last_id
                    FROM direct
                    GROUP BY other_user
                ),
                unread AS (
                    SELECT sender AS other_user, COUNT(*) AS unread_count
                    FROM messages
                    WHERE group_id IS NULL
                      AND receiver=%s
                      AND status <> 'read'
                      AND deleted = FALSE
                    GROUP BY sender
                )
                SELECT
                    u.username,
                    u.display_name,
                    u.avatar,
                    u.last_seen,
                    COALESCE(unread.unread_count, 0) AS unread_count
                FROM latest
                JOIN users u ON u.username = latest.other_user
                LEFT JOIN unread ON unread.other_user = u.username
                LEFT JOIN hidden_conversations hc
                  ON hc.username=%s AND hc.other_username=u.username
                WHERE hc.username IS NULL
                ORDER BY latest.last_id DESC
            """,
                (username, username, username, username, username)
            )
            rows = cursor.fetchall()

    return [{
        "username": r["username"],
        "display_name": r["display_name"] or r["username"],
        "avatar": r["avatar"],
        "last_seen": now_iso(r["last_seen"]),
        "online": bool(connections.get(r["username"])),
        "unread_count": int(r["unread_count"] or 0),
    } for r in rows]


def hide_conversation(username: str, other_username: str):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO hidden_conversations(username, other_username)
                VALUES (%s,%s)
                ON CONFLICT (username, other_username)
                DO UPDATE SET hidden_at=CURRENT_TIMESTAMP
            """, (username, other_username))
        connection.commit()


def unhide_conversation(username: str, other_username: str):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                DELETE FROM hidden_conversations
                WHERE username=%s AND other_username=%s
            """, (username, other_username))
        connection.commit()


def search_users(query, limit=50):
    q=(query or '').strip()
    with get_db() as connection:
        with connection.cursor() as cursor:
            if q:
                like=f"%{q.lower()}%"
                cursor.execute("""
                    SELECT username, display_name, avatar, last_seen
                    FROM users
                    WHERE LOWER(username) LIKE %s OR LOWER(COALESCE(display_name,username)) LIKE %s
                    ORDER BY LOWER(COALESCE(display_name,username)), username
                    LIMIT %s
                """, (like,like,limit))
            else:
                cursor.execute("""
                    SELECT username, display_name, avatar, last_seen
                    FROM users
                    ORDER BY LOWER(COALESCE(display_name,username)), username
                    LIMIT %s
                """, (limit,))
            rows=cursor.fetchall()
    return [{
        "username":r["username"],
        "display_name":r["display_name"] or r["username"],
        "avatar":r["avatar"],
        "last_seen":now_iso(r["last_seen"]),
        "online":bool(connections.get(r["username"])),
    } for r in rows]


def get_unread_counts(username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT sender, COUNT(*) AS unread_count
                FROM messages
                WHERE group_id IS NULL AND receiver=%s AND status<>'read' AND deleted=FALSE
                GROUP BY sender
            """, (username,))
            direct={r["sender"]:int(r["unread_count"]) for r in cursor.fetchall()}
            cursor.execute("""
                SELECT group_id, COUNT(*) AS unread_count
                FROM messages
                WHERE group_id IS NOT NULL AND deleted=FALSE
                  AND sender<>%s
                  AND EXISTS (
                    SELECT 1 FROM group_members gm WHERE gm.group_id=messages.group_id AND gm.username=%s
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM group_message_reads r
                    WHERE r.message_id=messages.id AND r.username=%s
                  )
                GROUP BY group_id
            """, (username,username,username))
            groups={str(r["group_id"]):int(r["unread_count"]) for r in cursor.fetchall()}
    return {"direct":direct,"groups":groups}

@app.get("/api/users")
async def api_users(username: str, token: str):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود.", "users": []}
    try:
        users = await asyncio.to_thread(get_recent_chat_users, username)
        return {"success": True, "users": users}
    except Exception as error:
        print("Recent users API error:", error)
        return {"success": False, "message": "دریافت چت‌ها ناموفق بود.", "users": []}


@app.get("/api/recent-users")
async def api_recent_users(username: str, token: str):
    return await api_users(username, token)


@app.get("/api/search-users")
async def api_search_users(username: str, token: str, q: str = ""):
    if not verify_token(username, token):
        return {"success": False, "users": []}
    return {"success": True, "users": await asyncio.to_thread(search_users, q, 50)}


@app.get("/api/unread")
async def api_unread(username: str, token: str):
    if not verify_token(username, token):
        return {"success": False, "unread": {}}
    return {"success": True, "unread": await asyncio.to_thread(get_unread_counts, username)}


@app.get("/api/admin/users")
async def admin_users(username: str, token: str):
    if not verify_token(username, token) or username != OWNER_USERNAME:
        return {"success": False, "message": "دسترسی ندارید.", "users": []}
    return {"success": True, "users": await asyncio.to_thread(get_all_users)}


@app.get("/api/push-public-key")
async def push_public_key():
    return {"success": bool(VAPID_PUBLIC_KEY), "public_key": VAPID_PUBLIC_KEY}


@app.get("/api/rtc-config")
async def rtc_config(username: str, token: str):
    if not verify_token(username, token):
        return {"success": False, "ice_servers": []}
    return {"success": True, "ice_servers": await asyncio.to_thread(get_rtc_ice_servers)}


@app.post("/api/push-subscribe")
async def push_subscribe(username: str = Form(...), token: str = Form(...), subscription: str = Form(...)):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    try:
        obj=json.loads(subscription)
        endpoint=obj.get("endpoint")
        if not endpoint:
            return {"success": False, "message": "اشتراک اعلان نامعتبر است."}
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO push_subscriptions(username,endpoint,subscription_json)
                    VALUES(%s,%s,%s)
                    ON CONFLICT(endpoint) DO UPDATE SET username=EXCLUDED.username, subscription_json=EXCLUDED.subscription_json
                """,(username,endpoint,json.dumps(obj,separators=(',',':'))))
            connection.commit()
        return {"success": True}
    except Exception as error:
        print("Push subscribe error:", error)
        return {"success": False, "message": "ثبت اعلان ناموفق بود."}


@app.post("/api/push-unsubscribe")
async def push_unsubscribe(username: str = Form(...), token: str = Form(...), endpoint: str = Form(...)):
    if not verify_token(username, token):
        return {"success": False}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM push_subscriptions WHERE username=%s AND endpoint=%s",(username,endpoint))
        connection.commit()
    return {"success": True}


@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT username,password_hash,display_name,avatar
                FROM users WHERE username=%s
            """, (username,))
            user = cursor.fetchone()
    if not user or not verify_password(password, user["password_hash"]):
        return {"success": False, "message": "نام کاربری یا رمز عبور اشتباه است."}
    return {
        "success": True,
        "username": username,
        "display_name": user["display_name"] or username,
        "avatar": user["avatar"],
        "token": create_token(username),
    }


@app.post("/logout")
async def logout(username: str = Form(...), token: str = Form("")):
    if token and not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    sockets=list(connections.get(username, set()))
    connections.pop(username, None)
    connection_kinds.pop(username, None)
    mark_last_seen(username)
    for ws in sockets:
        try:
            await ws.close()
        except Exception:
            pass
    await broadcast_users()
    return {"success": True}


@app.post("/delete-account")
async def delete_account(username: str = Form(...), token: str = Form(...), target_username: str = Form("")):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    target=target_username.strip() or username
    if target != username and username != OWNER_USERNAME:
        return {"success": False, "message": "فقط صاحب MS Chat می‌تواند حساب شخص دیگری را حذف کند."}
    if not await asyncio.to_thread(user_exists, target):
        return {"success": False, "message": "حساب پیدا نشد."}

    # Snapshot groups/messages/media before cascading deletion.
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM groups WHERE owner=%s", (target,))
            owned_groups=[r["id"] for r in cursor.fetchall()]
            for gid in owned_groups:
                cursor.execute("DELETE FROM messages WHERE group_id=%s", (gid,))
            if owned_groups:
                cursor.execute("DELETE FROM groups WHERE owner=%s", (target,))
            cursor.execute("DELETE FROM group_members WHERE username=%s", (target,))
            cursor.execute("DELETE FROM messages WHERE sender=%s OR receiver=%s", (target,target))
            cursor.execute("DELETE FROM push_subscriptions WHERE username=%s", (target,))
            cursor.execute("DELETE FROM users WHERE username=%s", (target,))
        connection.commit()
    _group_info_cache.clear(); invalidate_users_cache(); invalidate_membership_cache()
    for ws in list(connections.get(target,set())):
        try: await ws.close()
        except Exception: pass
    connections.pop(target,None); connection_kinds.pop(target,None)
    await broadcast_users()
    return {"success": True, "deleted_username": target}


@app.post("/update-profile")
async def update_profile(
    username: str = Form(...),
    token: str = Form(...),
    display_name: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    display_name = display_name.strip()
    if not display_name:
        return {"success": False, "message": "نام نمایشی نمی‌تواند خالی باشد."}
    if len(display_name) > 40:
        return {"success": False, "message": "نام نمایشی بیش از حد طولانی است."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE users SET display_name=%s WHERE username=%s",
                (display_name, username),
            )
        connection.commit()
    invalidate_users_cache()
    await broadcast_users()
    return {"success": True, "display_name": display_name}


@app.post("/upload-avatar")
async def upload_avatar(
    username: str = Form(...),
    token: str = Form(...),
    avatar: UploadFile = File(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    content_type = (avatar.content_type or "").lower()
    if content_type not in ALLOWED_IMAGE_TYPES:
        return {"success": False, "message": "فرمت عکس پروفایل باید JPG، PNG، WEBP یا GIF باشد."}
    content = await avatar.read()
    if not content or len(content) > MAX_AVATAR_SIZE:
        return {"success": False, "message": "حجم عکس پروفایل نباید بیشتر از ۵ مگابایت باشد."}
    ext = ALLOWED_IMAGE_TYPES[content_type]
    path = f"avatars/{uuid.uuid4()}{ext}"
    try:
        old_path = None
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT avatar FROM users WHERE username=%s", (username,))
                row = cursor.fetchone()
                old_path = row["avatar"] if row else None
        await asyncio.to_thread(upload_storage, path, content, content_type)
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE users SET avatar=%s WHERE username=%s",
                    (add_storage_prefix(path), username),
                )
            connection.commit()
        if old_path and old_path.startswith("storage:"):
            try:
                await asyncio.to_thread(delete_storage, old_path)
            except Exception:
                pass
        _group_info_cache.clear()
        invalidate_users_cache()
        await broadcast_users()
        return {
            "success": True,
            "avatar": add_storage_prefix(path),
        }
    except Exception as error:
        print("Avatar upload error:", error)
        return {"success": False, "message": "آپلود عکس پروفایل ناموفق بود."}


@app.post("/delete-avatar")
async def delete_avatar(username: str = Form(...), token: str = Form(...)):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT avatar FROM users WHERE username=%s", (username,))
            row = cursor.fetchone()
            cursor.execute("UPDATE users SET avatar=NULL WHERE username=%s", (username,))
        connection.commit()
    if row and row["avatar"]:
        try:
            await asyncio.to_thread(delete_storage, row["avatar"])
        except Exception:
            pass
    _group_info_cache.clear()
    invalidate_users_cache()
    await broadcast_users()
    return {"success": True}


@app.post("/change-password")
async def change_password(
    username: str = Form(...),
    token: str = Form(...),
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    if len(new_password) < 6:
        return {"success": False, "message": "رمز جدید باید حداقل ۶ کاراکتر باشد."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT password_hash FROM users WHERE username=%s", (username,)
            )
            user = cursor.fetchone()
            if not user or not verify_password(current_password, user["password_hash"]):
                return {"success": False, "message": "رمز فعلی اشتباه است."}
            cursor.execute(
                "UPDATE users SET password_hash=%s WHERE username=%s",
                (hash_password(new_password), username),
            )
        connection.commit()
    return {"success": True, "message": "رمز عبور با موفقیت تغییر کرد."}


@app.post("/upload-image")
async def upload_image(
    sender: str = Form(...),
    receiver: str = Form(...),
    token: str = Form(...),
    image: UploadFile = File(...),
    reply_to: str = Form(""),
):
    if not verify_token(sender, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    if receiver.startswith("group:"):
        group_id = int(receiver.split(":", 1)[1])
        if not await asyncio.to_thread(is_group_member, group_id, sender):
            return {"success": False, "message": "شما عضو این گروه نیستید."}
        target_receiver = ""
    else:
        group_id = None
        if not user_exists(receiver):
            return {"success": False, "message": "گیرنده وجود ندارد."}
        target_receiver = receiver
    content_type = (image.content_type or "").lower()
    if content_type not in ALLOWED_IMAGE_TYPES:
        return {"success": False, "message": "فرمت عکس مجاز نیست."}
    content = await image.read()
    if not content or len(content) > MAX_IMAGE_SIZE:
        return {"success": False, "message": "حجم عکس نباید بیشتر از ۸ مگابایت باشد."}
    ext = ALLOWED_IMAGE_TYPES[content_type]
    path = f"images/{uuid.uuid4()}{ext}"
    try:
        await asyncio.to_thread(upload_storage, path, content, content_type)
        rid = int(reply_to) if reply_to else None
        mid = save_message(
            sender, target_receiver, "", "image", add_storage_prefix(path), None, rid, group_id
        )
        msg = get_message(mid)
        await deliver_message(msg)
        return {"success": True, "message": msg}
    except Exception as error:
        print("Image upload error:", error)
        return {"success": False, "message": "ارسال عکس ناموفق بود."}



@app.get("/message-file/{message_id}")
async def message_file(message_id: int, username: str, token: str, download: int = 0, request: Request = None):
    """Serve private image/audio safely through the app.

    The browser no longer needs a Supabase signed URL for every message.
    It asks this endpoint only when the media is actually displayed/played.
    """
    if not verify_token(username, token):
        return Response(status_code=401, content="Unauthorized")

    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, sender, receiver, media, audio, group_id, deleted
                FROM messages WHERE id=%s
                """,
                (message_id,),
            )
            row = cursor.fetchone()

    if not row or row["deleted"]:
        return Response(status_code=404, content="File not found")

    allowed = False
    if row["group_id"]:
        allowed = is_group_member(row["group_id"], username)
    else:
        allowed = username in {row["sender"], row["receiver"]}
    if not allowed:
        return Response(status_code=403, content="Forbidden")

    storage_path = row["media"] or row["audio"]
    if not storage_path:
        return Response(status_code=404, content="File not found")

    if storage_path.startswith("/uploads/"):
        local_path = storage_path.lstrip("/")
        try:
            return FileResponse(
                local_path,
                media_type="application/octet-stream",
                headers={"Content-Disposition": "attachment" if download else "inline"},
            )
        except Exception:
            return Response(status_code=404, content="File not found")

    if storage_path.startswith("storage:"):
        storage_path = storage_path[len("storage:"):]

    range_header = None
    # Avoid importing Request just for the annotation; FastAPI still passes the request object.
    if request is not None:
        try:
            range_header = request.headers.get("range")
        except Exception:
            range_header = None

    try:
        response = await asyncio.to_thread(download_storage_object, storage_path, range_header)
    except Exception as error:
        print("Media download error:", error)
        return Response(status_code=502, content="Media download failed")

    if response.status_code not in (200, 206):
        print("Media download status:", response.status_code, response.text[:300])
        return Response(status_code=404, content="File not found")

    content_type = response.headers.get("content-type", "application/octet-stream")
    headers = {"Accept-Ranges": "bytes"}
    for key in ("content-range", "content-length", "cache-control", "etag"):
        if response.headers.get(key):
            headers[key.title()] = response.headers[key]
    headers["Content-Disposition"] = "attachment" if download else "inline"
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=content_type,
        headers=headers,
    )

@app.get("/message-media/{message_id}")
async def message_media(message_id: int, username: str, token: str, request: Request = None):
    # Backward-compatible alias used by older index.html versions.
    return await message_file(message_id, username, token, 0, request)

@app.get("/avatar/{target_username}")
async def avatar_file(target_username: str, viewer: str, token: str):
    if not verify_token(viewer, token):
        return Response(status_code=401, content="Unauthorized")
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT avatar FROM users WHERE username=%s", (target_username,))
            row = cursor.fetchone()
    if not row or not row["avatar"]:
        return Response(status_code=404, content="Avatar not found")
    storage_path = row["avatar"]
    if storage_path.startswith("storage:"):
        storage_path = storage_path[len("storage:"):]
    try:
        response = await asyncio.to_thread(download_storage_object, storage_path, None)
    except Exception as error:
        print("Avatar download error:", error)
        return Response(status_code=502, content="Avatar download failed")
    if response.status_code != 200:
        return Response(status_code=404, content="Avatar not found")
    return Response(
        content=response.content,
        media_type=response.headers.get("content-type", "image/*"),
        headers={"Cache-Control": "private, max-age=1800"},
    )

@app.get("/group-avatar/{group_id}")
async def group_avatar_file(group_id: int, username: str, token: str):
    if not verify_token(username, token):
        return Response(status_code=401, content="Unauthorized")
    if not is_group_member(group_id, username):
        return Response(status_code=403, content="Forbidden")
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT avatar FROM groups WHERE id=%s", (group_id,))
            row = cursor.fetchone()
    if not row or not row["avatar"]:
        return Response(status_code=404, content="Group avatar not found")
    storage_path = row["avatar"]
    if storage_path.startswith("storage:"):
        storage_path = storage_path[len("storage:"):]
    try:
        response = await asyncio.to_thread(download_storage_object, storage_path, None)
    except Exception as error:
        print("Group avatar download error:", error)
        return Response(status_code=502, content="Group avatar download failed")
    if response.status_code != 200:
        return Response(status_code=404, content="Group avatar not found")
    return Response(
        content=response.content,
        media_type=response.headers.get("content-type", "image/*"),
        headers={"Cache-Control": "private, max-age=1800"},
    )

@app.post("/upload-audio")
async def upload_audio(
    sender: str = Form(...),
    receiver: str = Form(...),
    token: str = Form(...),
    audio: UploadFile = File(...),
    reply_to: str = Form(""),
):
    if not verify_token(sender, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    if receiver.startswith("group:"):
        group_id = int(receiver.split(":", 1)[1])
        if not await asyncio.to_thread(is_group_member, group_id, sender):
            return {"success": False, "message": "شما عضو این گروه نیستید."}
        target_receiver = ""
    else:
        group_id = None
        if not user_exists(receiver):
            return {"success": False, "message": "گیرنده وجود ندارد."}
        target_receiver = receiver
    content_type = (audio.content_type or "audio/webm").lower()
    if content_type not in ALLOWED_AUDIO_TYPES:
        content_type = "audio/webm"
    content = await audio.read()
    if not content:
        return {"success": False, "message": "فایل صوتی خالی است."}
    if len(content) > MAX_AUDIO_SIZE:
        return {"success": False, "message": "حجم ویس نباید بیشتر از ۱۰ مگابایت باشد."}
    path = f"voices/{uuid.uuid4()}.webm"
    try:
        await asyncio.to_thread(upload_storage, path, content, content_type)
        rid = int(reply_to) if reply_to else None
        mid = save_message(
            sender, target_receiver, "", "audio", None, add_storage_prefix(path), rid, group_id
        )
        msg = get_message(mid)
        await deliver_message(msg)
        return {"success": True, "message": msg}
    except Exception as error:
        print("Audio upload error:", error)
        return {"success": False, "message": "آپلود ویس ناموفق بود."}


async def group_recipients(group_id, sender):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT username FROM group_members WHERE group_id=%s AND username<>%s",
                (group_id, sender),
            )
            return [r["username"] for r in cursor.fetchall()]



def _normalize_ice_servers(value):
    """Normalize a provider response/env JSON into a list of RTCIceServer dicts."""
    if isinstance(value, dict):
        value = value.get("iceServers") or value.get("ice_servers") or []
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        if not isinstance(item, dict) or not item.get("urls"):
            continue
        clean = {"urls": item["urls"]}
        if item.get("username") is not None:
            clean["username"] = item["username"]
        if item.get("credential") is not None:
            clean["credential"] = item["credential"]
        out.append(clean)
    return out


def get_rtc_ice_servers():
    # Always keep free STUN servers first. If TURN credentials are configured in
    # Render, append them so WebRTC can relay media when direct P2P is blocked by NAT.
    servers = [
        {"urls": "stun:stun.cloudflare.com:3478"},
        {"urls": "stun:stun.l.google.com:19302"},
    ]

    if TURN_URLS and TURN_USERNAME and TURN_CREDENTIAL:
        urls = [u.strip() for u in TURN_URLS.split(",") if u.strip()]
        if urls:
            servers.append({
                "urls": urls,
                "username": TURN_USERNAME,
                "credential": TURN_CREDENTIAL,
            })
    return servers


def _push_one(subscription_obj, title, body, url):
    if not webpush or not VAPID_PRIVATE_KEY:
        return True
    try:
        webpush(
            subscription_info=subscription_obj,
            data=json.dumps({"title":title,"body":body,"url":url}),
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub":VAPID_CLAIMS_EMAIL},
        )
        return True
    except WebPushException as error:
        try:
            status=getattr(error.response,"status_code",None)
            if status in (404,410):
                return False
        except Exception:
            pass
        print("Web push error:", error)
        return True
    except Exception as error:
        print("Web push exception:", error)
        return True


def get_push_user_display_name(username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(display_name, username) AS name FROM users WHERE username=%s", (username,))
            row = cursor.fetchone()
    return (row["name"] if row else username) or username


def get_push_group_name(group_id):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM groups WHERE id=%s", (group_id,))
            row = cursor.fetchone()
    return (row["name"] if row else "گروه") or "گروه"


async def send_push_to_user(username, title, body, url="/"):
    if not VAPID_PRIVATE_KEY:
        return
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id,endpoint,subscription_json FROM push_subscriptions WHERE username=%s",(username,))
            rows=cursor.fetchall()
    if not rows:
        return
    dead=[]
    for row in rows:
        try:
            obj=json.loads(row["subscription_json"])
        except Exception:
            dead.append(row["id"]); continue
        ok=await asyncio.to_thread(_push_one,obj,title,body,url)
        if not ok: dead.append(row["id"])
    if dead:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM push_subscriptions WHERE id = ANY(%s)",(dead,))
            connection.commit()


async def deliver_message(msg):
    if msg["group_id"]:
        recipients = await group_recipients(msg["group_id"], msg["sender"])
        payload = {"type": "message", "message": msg, "group_id": msg["group_id"]}
        results = await asyncio.gather(
            *(send_to(username, payload) for username in recipients),
            return_exceptions=True,
        )
        delivered = any(result is True for result in results)
        if delivered:
            with get_db() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE messages SET status='delivered' WHERE id=%s",
                        (msg["id"],),
                    )
                connection.commit()
            msg["status"] = "delivered"
        await send_to(msg["sender"], {
            "type": "sent",
            "message": msg,
            "group_id": msg["group_id"],
        })
        sender_name = await asyncio.to_thread(get_push_user_display_name, msg["sender"])
        group_name = await asyncio.to_thread(get_push_group_name, msg["group_id"])
        push_title = f"👥 {group_name} • {sender_name}"
        push_body = msg.get("message") or "📎 فایل جدید"
        for username in recipients:
            asyncio.create_task(send_push_to_user(username, push_title, push_body, "/"))
            asyncio.create_task(send_to(username, {
                "type":"unread",
                "unread":await asyncio.to_thread(get_unread_counts, username),
            }))
        return

    delivered = await send_to(msg["receiver"], {
        "type": "message",
        "message": msg,
    })
    if delivered:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE messages SET status='delivered' WHERE id=%s",
                    (msg["id"],),
                )
            connection.commit()
        msg["status"] = "delivered"
    await send_to(msg["sender"], {"type": "sent", "message": msg})
    sender_name = await asyncio.to_thread(get_push_user_display_name, msg["sender"])
    push_title = f"📩 پیام از {sender_name}"
    push_body = msg.get("message") or "📎 فایل جدید"
    asyncio.create_task(send_push_to_user(msg["receiver"], push_title, push_body, "/"))
    await send_to(msg["sender"], {"type":"recent-users","users":await asyncio.to_thread(get_recent_chat_users,msg["sender"])})
    await send_to(msg["receiver"], {"type":"recent-users","users":await asyncio.to_thread(get_recent_chat_users,msg["receiver"])})
    await send_to(msg["receiver"], {"type":"unread","unread":await asyncio.to_thread(get_unread_counts,msg["receiver"])})


async def notify_message_update(msg):
    if msg["group_id"]:
        recipients = await group_recipients(msg["group_id"], msg["sender"])
        for u in recipients:
            await send_to(u, {"type": "message-updated", "message": msg})
        await send_to(msg["sender"], {"type": "message-updated", "message": msg})
    else:
        await send_to(msg["receiver"], {"type": "message-updated", "message": msg})
        await send_to(msg["sender"], {"type": "message-updated", "message": msg})


async def notify_message_delete(msg):
    if msg["group_id"]:
        recipients = await group_recipients(msg["group_id"], msg["sender"])
        for u in recipients:
            await send_to(u, {"type": "message-deleted", "id": msg["id"]})
        await send_to(msg["sender"], {"type": "message-deleted", "id": msg["id"]})
    else:
        await send_to(msg["receiver"], {"type": "message-deleted", "id": msg["id"]})
        await send_to(msg["sender"], {"type": "message-deleted", "id": msg["id"]})


async def send_call_signal(receiver, data):
    return await send_to(receiver, data)


@app.post("/create-group")
async def create_group(
    username: str = Form(...),
    token: str = Form(...),
    name: str = Form(...),
    members: str = Form(""),
    avatar: UploadFile | None = File(None),
    request_key: str = Form(""),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    name = name.strip()
    if not name:
        return {"success": False, "message": "نام گروه را وارد کن."}
    request_key = request_key.strip()
    if request_key:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT group_id FROM group_create_requests WHERE request_key=%s AND group_id IN (SELECT id FROM groups WHERE owner=%s)",
                    (request_key, username),
                )
                existing = cursor.fetchone()
        if existing:
            info = get_group_info(existing["group_id"])
            if info:
                return {"success": True, "group": info, "duplicate": True}

    member_list = [x.strip() for x in members.split(",") if x.strip()]
    member_list.append(username)
    clean = []
    seen = set()
    for m in member_list:
        if m not in seen and user_exists(m):
            clean.append(m)
            seen.add(m)
    if username not in seen:
        return {"success": False, "message": "کاربر سازنده پیدا نشد."}
    avatar_path = None
    if avatar and avatar.filename:
        content_type = (avatar.content_type or "").lower()
        if content_type not in ALLOWED_IMAGE_TYPES:
            return {"success": False, "message": "عکس گروه باید JPG، PNG، WEBP یا GIF باشد."}
        data = await avatar.read()
        if len(data) > MAX_AVATAR_SIZE:
            return {"success": False, "message": "حجم عکس گروه زیاد است."}
        ext = ALLOWED_IMAGE_TYPES[content_type]
        p = f"groups/{uuid.uuid4()}{ext}"
        await asyncio.to_thread(upload_storage, p, data, content_type)
        avatar_path = add_storage_prefix(p)

    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO groups (name,avatar,owner) VALUES (%s,%s,%s) RETURNING id",
                (name, avatar_path, username),
            )
            gid = cursor.fetchone()["id"]
            if request_key:
                cursor.execute(
                    "INSERT INTO group_create_requests (request_key, group_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (request_key, gid),
                )
            for m in clean:
                cursor.execute(
                    "INSERT INTO group_members (group_id,username) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (gid, m),
                )
        connection.commit()

    _group_info_cache.pop(gid, None)
    info = await asyncio.to_thread(get_group_info, gid)
    for m in clean:
        await send_to(m, {"type": "group-created", "group": info})
    return {"success": True, "group": info}


@app.post("/add-group-members")
async def add_group_members(
    username: str = Form(...),
    token: str = Form(...),
    group_id: int = Form(...),
    members: str = Form(""),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}

    try:
        requested = [x.strip() for x in (members or "").split(",") if x.strip()]
        requested = list(dict.fromkeys(requested))
        if not requested:
            return {"success": False, "message": "حداقل یک کاربر را انتخاب کنید."}

        # Only the group owner can add members.
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id, name, owner FROM groups WHERE id=%s", (group_id,))
                group = cursor.fetchone()
                if not group:
                    return {"success": False, "message": "گروه پیدا نشد."}
                if group["owner"] != username:
                    return {"success": False, "message": "فقط سازنده گروه می‌تواند عضو جدید اضافه کند."}

                cursor.execute(
                    "SELECT username FROM group_members WHERE group_id=%s",
                    (group_id,),
                )
                existing = {r["username"] for r in cursor.fetchall()}

                valid = []
                for member in requested:
                    if member == username or member in existing:
                        continue
                    cursor.execute("SELECT 1 FROM users WHERE username=%s", (member,))
                    if cursor.fetchone():
                        valid.append(member)

                if not valid:
                    # Not an error: the selected users may already be members.
                    info = await asyncio.to_thread(get_group_info, group_id)
                    return {"success": True, "group": info, "added": []}

                for member in valid:
                    cursor.execute(
                        "INSERT INTO group_members (group_id, username) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (group_id, member),
                    )
            connection.commit()

        _group_info_cache.pop(group_id, None)
        invalidate_membership_cache(group_id)
        info = await asyncio.to_thread(get_group_info, group_id)
        if not info:
            return {"success": False, "message": "اطلاعات گروه پیدا نشد."}

        # Refresh the group for every member. New members receive group-created;
        # existing members also receive an updated groups list so the count changes immediately.
        all_members = [m["username"] for m in info.get("members", [])]
        await asyncio.gather(*[
            send_to(member, {"type": "group-created", "group": info})
            for member in all_members
        ], return_exceptions=True)
        await asyncio.gather(*[
            send_to(member, {"type": "group-info", "group": info})
            for member in all_members
        ], return_exceptions=True)
        await asyncio.gather(*[
            send_to(member, {"type": "groups", "groups": await asyncio.to_thread(get_user_groups, member)})
            for member in all_members
        ], return_exceptions=True)

        return {"success": True, "group": info, "added": valid}
    except Exception as error:
        print("Add group members error:", repr(error))
        return {"success": False, "message": f"خطا در افزودن اعضا: {type(error).__name__}"}


@app.post("/remove-group-member")
async def remove_group_member(
    username: str = Form(...),
    token: str = Form(...),
    group_id: int = Form(...),
    member_username: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    member_username = (member_username or "").strip()
    if not member_username or member_username == username:
        return {"success": False, "message": "عضو نامعتبر است."}
    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id, name, owner FROM groups WHERE id=%s", (group_id,))
                group = cursor.fetchone()
                if not group:
                    return {"success": False, "message": "گروه پیدا نشد."}
                if group["owner"] != username:
                    return {"success": False, "message": "فقط سازنده گروه می‌تواند اعضا را حذف کند."}
                if member_username == group["owner"]:
                    return {"success": False, "message": "سازنده گروه را نمی‌توان حذف کرد."}
                cursor.execute("SELECT 1 FROM group_members WHERE group_id=%s AND username=%s", (group_id, member_username))
                if not cursor.fetchone():
                    return {"success": False, "message": "این کاربر عضو گروه نیست."}
                cursor.execute("DELETE FROM group_members WHERE group_id=%s AND username=%s", (group_id, member_username))
            connection.commit()

        _group_info_cache.pop(group_id, None)
        invalidate_membership_cache(group_id)
        info = await asyncio.to_thread(get_group_info, group_id)
        remaining = [m["username"] for m in (info or {}).get("members", [])]
        await send_to(member_username, {"type":"group-removed", "group_id":group_id})
        for member in remaining:
            await send_to(member, {"type":"group-info", "group":info})
            await send_to(member, {"type":"groups", "groups":await asyncio.to_thread(get_user_groups, member)})
        return {"success": True, "group": info, "removed": member_username}
    except Exception as error:
        print("Remove group member error:", repr(error))
        return {"success": False, "message": f"خطا در حذف عضو: {type(error).__name__}"}


@app.post("/delete-group")
async def delete_group(
    username: str = Form(...),
    token: str = Form(...),
    group_id: int = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id, owner FROM groups WHERE id=%s", (group_id,))
            group = cursor.fetchone()
            if not group:
                return {"success": False, "message": "گروه پیدا نشد."}
            if group["owner"] != username:
                return {"success": False, "message": "فقط سازنده گروه می‌تواند آن را حذف کند."}
            cursor.execute("SELECT username FROM group_members WHERE group_id=%s", (group_id,))
            members = [r["username"] for r in cursor.fetchall()]
            cursor.execute("DELETE FROM groups WHERE id=%s", (group_id,))
        connection.commit()
    _group_info_cache.pop(group_id, None)
    for u in members:
        await send_to(u, {"type": "group-deleted", "group_id": group_id})
    return {"success": True, "group_id": group_id}


@app.post("/edit-message")
async def edit_message(
    username: str = Form(...),
    token: str = Form(...),
    message_id: int = Form(...),
    text: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    text = text.strip()
    if not text or len(text) > 5000:
        return {"success": False, "message": "متن پیام معتبر نیست."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT sender,message_type,deleted FROM messages WHERE id=%s",
                (message_id,),
            )
            row = cursor.fetchone()
            if not row or row["sender"] != username or row["message_type"] != "text" or row["deleted"]:
                return {"success": False, "message": "این پیام قابل ویرایش نیست."}
            cursor.execute(
                "UPDATE messages SET message=%s, edited=TRUE WHERE id=%s",
                (text, message_id),
            )
        connection.commit()
    msg = get_message(message_id)
    await notify_message_update(msg)
    return {"success": True, "message": msg}


@app.post("/delete-message")
async def delete_message(
    username: str = Form(...),
    token: str = Form(...),
    message_id: int = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT sender,audio,media,group_id FROM messages WHERE id=%s",
                (message_id,),
            )
            row = cursor.fetchone()
            if not row or row["sender"] != username:
                return {"success": False, "message": "این پیام متعلق به شما نیست."}
            cursor.execute(
                "UPDATE messages SET deleted=TRUE,message='',audio=NULL,media=NULL WHERE id=%s",
                (message_id,),
            )
        connection.commit()
    for p in (row["audio"], row["media"]):
        if p and p.startswith("storage:"):
            try:
                def _media_still_referenced(path_value, deleted_id):
                    with get_db() as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "SELECT 1 FROM messages WHERE id<>%s AND (audio=%s OR media=%s) LIMIT 1",
                                (deleted_id, path_value, path_value),
                            )
                            return cursor.fetchone() is not None
                if not await asyncio.to_thread(_media_still_referenced, p, message_id):
                    await asyncio.to_thread(delete_storage, p)
            except Exception:
                pass
    msg = get_message(message_id)
    await notify_message_delete(msg)
    return {"success": True, "id": message_id}



@app.post("/api/mark-read")
async def api_mark_read(
    username: str = Form(...),
    token: str = Form(...),
    sender: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    sender = (sender or "").strip()
    if not sender or sender == username:
        return {"success": False, "message": "فرستنده نامعتبر است."}
    if not await asyncio.to_thread(user_exists, sender):
        return {"success": False, "message": "این کاربر وجود ندارد."}

    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE messages
                    SET status='read'
                    WHERE sender=%s AND receiver=%s
                      AND group_id IS NULL
                      AND status<>'read'
                      AND deleted=FALSE
                """, (sender, username))
            connection.commit()

        await send_to(sender, {"type": "messages-read", "by": username})
        await send_to(username, {
            "type": "unread",
            "unread": await asyncio.to_thread(get_unread_counts, username),
        })
        await send_to(sender, {
            "type": "recent-users",
            "users": await asyncio.to_thread(get_recent_chat_users, sender),
        })
        await send_to(username, {
            "type": "recent-users",
            "users": await asyncio.to_thread(get_recent_chat_users, username),
        })
        return {"success": True}
    except Exception as error:
        print("Mark direct read error:", repr(error))
        return {"success": False, "message": "علامت‌گذاری پیام‌ها ناموفق بود."}


@app.post("/api/delete-conversation")
async def api_delete_conversation(
    username: str = Form(...),
    token: str = Form(...),
    other_username: str = Form(...),
):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    other_username = (other_username or "").strip()
    if not other_username or other_username == username:
        return {"success": False, "message": "گفتگوی نامعتبر است."}
    if not await asyncio.to_thread(user_exists, other_username):
        return {"success": False, "message": "این کاربر وجود ندارد."}
    try:
        await asyncio.to_thread(hide_conversation, username, other_username)
        users = await asyncio.to_thread(get_recent_chat_users, username)
        await send_to(username, {"type":"recent-users", "users":users})
        return {"success": True, "users": users}
    except Exception as error:
        print("Delete conversation error:", repr(error))
        return {"success": False, "message": "حذف گفتگو ناموفق بود."}


@app.get("/api/group-message-readers")
async def api_group_message_readers(username: str, token: str, message_id: int):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود.", "readers": []}
    try:
        gid, readers = await asyncio.to_thread(get_group_message_readers, message_id, username)
        if not gid:
            return {"success": False, "message": "این پیام متعلق به شما در یک گروه نیست.", "readers": []}
        if not await asyncio.to_thread(is_group_member, gid, username):
            return {"success": False, "message": "شما عضو این گروه نیستید.", "readers": []}
        return {"success": True, "message_id": message_id, "group_id": gid, "readers": readers}
    except Exception as error:
        print("Group readers API error:", repr(error))
        return {"success": False, "message": "دریافت بازدیدکنندگان ناموفق بود.", "readers": []}


@app.get("/api/history")
async def api_history(username: str, token: str, user: str):
    if not verify_token(username, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    user = (user or "").strip()
    if not user or user == username:
        return {"success": True, "messages": []}
    if not await asyncio.to_thread(user_exists, user):
        return {"success": False, "message": "این کاربر وجود ندارد.", "messages": []}
    messages = await asyncio.to_thread(get_direct_history, username, user)
    return {"success": True, "messages": messages}


@app.post("/send-sticker")
async def send_sticker_http(
    sender: str = Form(...),
    receiver: str = Form(...),
    token: str = Form(...),
    sticker: str = Form(...),
    reply_to: str = Form(""),
):
    if not verify_token(sender, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}
    allowed = {"😂","🤣","😍","🥰","😘","😎","🤩","🥳","😴","😱","🤔","🙄","😡","😭","🤯","🥹","❤️","💔","🔥","👏","👍","👎","🙏","🎉","💀","👀","🤝","🚀","🎮","⚡"}
    sticker = (sticker or "").strip()
    if sticker not in allowed:
        return {"success": False, "message": "استیکر نامعتبر است."}
    receiver=(receiver or "").strip()
    if not receiver:
        return {"success": False, "message": "گیرنده مشخص نیست."}
    if receiver.startswith("group:"):
        try: gid=int(receiver.split(":",1)[1])
        except (TypeError,ValueError): return {"success":False,"message":"گروه نامعتبر است."}
        if not await asyncio.to_thread(is_group_member,gid,sender):
            return {"success":False,"message":"شما عضو این گروه نیستید."}
        target_receiver=""
    else:
        gid=None
        if not await asyncio.to_thread(user_exists,receiver):
            return {"success":False,"message":"این کاربر وجود ندارد."}
        target_receiver=receiver
    try:
        rid=int(reply_to) if reply_to else None
    except (TypeError,ValueError): rid=None
    try:
        mid=await asyncio.to_thread(save_message,sender,target_receiver,sticker,"sticker",None,None,rid,gid)
        msg=await asyncio.to_thread(get_message,mid)
        if not msg:return {"success":False,"message":"استیکر ذخیره نشد."}
        asyncio.create_task(deliver_message_safe(msg))
        return {"success":True,"message":msg}
    except Exception as error:
        print("HTTP sticker error:",repr(error))
        return {"success":False,"message":f"خطا در ذخیره استیکر: {type(error).__name__}"}


@app.post("/send-message")
async def send_message_http(
    sender: str = Form(...),
    receiver: str = Form(...),
    token: str = Form(...),
    message: str = Form(...),
    reply_to: str = Form(""),
):
    """Reliable HTTP fallback for text messages.

    Text sending must not depend on a healthy WebSocket. This endpoint saves the
    message in PostgreSQL first, then delivers it over WebSocket when available.
    That keeps offline/search-started chats reliable even when WS is reconnecting.
    """
    if not verify_token(sender, token):
        return {"success": False, "message": "احراز هویت ناموفق بود."}

    receiver = (receiver or "").strip()
    text = (message or "").strip()
    if not receiver:
        return {"success": False, "message": "گیرنده مشخص نیست."}
    if not text:
        return {"success": False, "message": "پیام خالی است."}
    if len(text) > 5000:
        return {"success": False, "message": "پیام خیلی طولانی است."}

    if receiver.startswith("group:"):
        try:
            group_id = int(receiver.split(":", 1)[1])
        except (TypeError, ValueError):
            return {"success": False, "message": "گروه نامعتبر است."}
        if not await asyncio.to_thread(is_group_member, group_id, sender):
            return {"success": False, "message": "شما عضو این گروه نیستید."}
        target_receiver = ""
    else:
        group_id = None
        if not await asyncio.to_thread(user_exists, receiver):
            return {"success": False, "message": "این کاربر وجود ندارد."}
        target_receiver = receiver

    try:
        rid = int(reply_to) if reply_to else None
    except (TypeError, ValueError):
        rid = None

    try:
        mid = await asyncio.to_thread(
            save_message,
            sender,
            target_receiver,
            text,
            "text",
            None,
            None,
            rid,
            group_id,
        )
        msg = await asyncio.to_thread(get_message, mid)
        if not msg:
            return {"success": False, "message": "پیام ذخیره نشد."}

        # مهم: ذخیره‌سازی پیام از تحویل لحظه‌ای جداست.
        # در نسخه‌های قبل اگر WebSocket/اعلان/لیست گفتگو خطا می‌داد،
        # کل درخواست با «ارسال پیام ناموفق بود» برمی‌گشت. اینجا اول پیام را ذخیره
        # کرده‌ایم و بعد تحویل زنده را در پس‌زمینه انجام می‌دهیم.
        asyncio.create_task(deliver_message_safe(msg))
        return {"success": True, "message": msg}
    except Exception as error:
        print("HTTP text message error:", repr(error))
        return {"success": False, "message": f"خطا در ذخیره پیام: {type(error).__name__}"}


async def deliver_message_safe(msg):
    """Deliver a saved message without ever breaking the HTTP send request."""
    try:
        await deliver_message(msg)
    except Exception as error:
        print("Background message delivery error:", repr(error))


@app.websocket("/chat/{username}/{token}")
async def chat(websocket: WebSocket, username: str, token: str):
    if not verify_token(username, token):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    connections.setdefault(username, set()).add(websocket)
    connection_kinds.setdefault(username, set()).add("web")
    await broadcast_users()
    await websocket.send_json({"type": "presence", "users": await asyncio.to_thread(get_all_users)})
    await websocket.send_json({"type": "recent-users", "users": await asyncio.to_thread(get_recent_chat_users, username)})
    await websocket.send_json({"type": "groups", "groups": await asyncio.to_thread(get_user_groups, username)})
    await websocket.send_json({"type": "unread", "unread": await asyncio.to_thread(get_unread_counts, username)})
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "users":
                await websocket.send_json({"type": "recent-users", "users": await asyncio.to_thread(get_recent_chat_users, username)})
                await websocket.send_json({"type": "groups", "groups": await asyncio.to_thread(get_user_groups, username)})
                await websocket.send_json({"type": "unread", "unread": await asyncio.to_thread(get_unread_counts, username)})
                continue

            if action == "groups":
                await websocket.send_json({"type": "groups", "groups": await asyncio.to_thread(get_user_groups, username)})
                continue

            if action == "group-info":
                try:
                    gid = int(data.get("group_id"))
                except (TypeError, ValueError):
                    continue
                if not await asyncio.to_thread(is_group_member, gid, username):
                    await websocket.send_json({"type": "error", "message": "شما عضو این گروه نیستید."})
                    continue
                info = await asyncio.to_thread(get_group_info, gid)
                if info:
                    await websocket.send_json({"type": "group-info", "group": info})
                continue

            if action == "history":
                other_user = data.get("user")
                history = await asyncio.to_thread(get_direct_history, username, other_user) if other_user else []
                await websocket.send_json({"type": "history", "user": other_user, "messages": history})
                continue

            if action == "group-history":
                gid = int(data.get("group_id"))
                if not await asyncio.to_thread(is_group_member, gid, username):
                    continue
                await websocket.send_json({
                    "type": "group-history",
                    "group_id": gid,
                    "messages": await asyncio.to_thread(get_group_history, gid, username),
                })
                continue

            if action == "mark-group-read":
                try:
                    gid = int(data.get("group_id"))
                except (TypeError, ValueError):
                    continue
                if not await asyncio.to_thread(is_group_member, gid, username):
                    continue
                by_sender = await asyncio.to_thread(mark_group_messages_read, gid, username)
                reader_info = await asyncio.to_thread(get_push_user_display_name, username)
                for sender, message_ids in by_sender.items():
                    await send_to(sender, {
                        "type": "group-message-read",
                        "group_id": gid,
                        "message_ids": message_ids,
                        "reader": {"username": username, "display_name": reader_info},
                    })
                await send_to(username, {"type":"unread","unread":await asyncio.to_thread(get_unread_counts,username)})
                continue

            if action == "group-message-readers":
                try:
                    mid = int(data.get("message_id"))
                except (TypeError, ValueError):
                    continue
                gid, readers = await asyncio.to_thread(get_group_message_readers, mid, username)
                if gid and await asyncio.to_thread(is_group_member, gid, username):
                    await websocket.send_json({"type":"group-message-readers", "message_id":mid, "group_id":gid, "readers":readers})
                continue

            if action == "mark-read":
                sender = data.get("sender")
                with get_db() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            UPDATE messages SET status='read'
                            WHERE sender=%s AND receiver=%s AND group_id IS NULL
                              AND status<>'read' AND deleted=FALSE
                        """, (sender, username))
                    connection.commit()
                await send_to(sender, {"type": "messages-read", "by": username})
                await send_to(username, {"type":"unread","unread":await asyncio.to_thread(get_unread_counts,username)})
                await send_to(sender, {"type":"recent-users","users":await asyncio.to_thread(get_recent_chat_users,sender)})
                continue

            if action == "typing":
                is_typing = bool(data.get("is_typing"))
                receiver = (data.get("to") or "").strip()
                if receiver:
                    if await asyncio.to_thread(user_exists, receiver):
                        await send_to(receiver, {"type": "typing", "from": username, "is_typing": is_typing})
                    continue
                try:
                    gid = int(data.get("group_id"))
                except (TypeError, ValueError):
                    continue
                if not await asyncio.to_thread(is_group_member, gid, username):
                    continue
                recipients = await group_recipients(gid, username)
                await asyncio.gather(*(
                    send_to(u, {"type": "typing", "from": username, "group_id": gid, "is_typing": is_typing})
                    for u in recipients
                ), return_exceptions=True)
                continue

            if action == "message":
                receiver = data.get("to")
                text = (data.get("message") or "").strip()
                reply_to = data.get("reply_to")
                if not receiver or not text:
                    continue
                if len(text) > 5000:
                    await websocket.send_json({"type": "error", "message": "پیام خیلی طولانی است."})
                    continue

                if str(receiver).startswith("group:"):
                    gid = int(str(receiver).split(":", 1)[1])
                    if not await asyncio.to_thread(is_group_member, gid, username):
                        await websocket.send_json({"type": "error", "message": "شما عضو این گروه نیستید."})
                        continue
                    mid = await asyncio.to_thread(save_message, username, "", text, "text", None, None, int(reply_to) if reply_to else None, gid)
                else:
                    if not user_exists(receiver):
                        await websocket.send_json({"type": "error", "message": "این کاربر وجود ندارد."})
                        continue
                    mid = await asyncio.to_thread(save_message, username, receiver, text, "text", None, None, int(reply_to) if reply_to else None, None)
                msg = await asyncio.to_thread(get_message, mid)
                await deliver_message(msg)
                continue

            if action == "call-offer":
                receiver = (data.get("to") or "").strip()
                call_id = data.get("call_id") or str(uuid.uuid4())
                if not receiver or not data.get("offer"):
                    await websocket.send_json({
                        "type": "call-error",
                        "call_id": call_id,
                        "message": "اطلاعات شروع تماس ناقص است.",
                    })
                    continue
                ok = await send_call_signal(receiver, {
                    "type": "call-offer",
                    "from": username,
                    "offer": data.get("offer"),
                    "mode": data.get("mode", "audio"),
                    "call_id": call_id,
                })
                if not ok:
                    await websocket.send_json({
                        "type": "call-error",
                        "call_id": call_id,
                        "message": "کاربر مورد نظر آنلاین نیست.",
                    })
                continue

            if action in {"call-answer", "ice-candidate", "call-rejected", "call-ended"}:
                receiver = data.get("to")
                payload = dict(data)
                payload["from"] = username
                payload.pop("action", None)
                await send_call_signal(receiver, payload)
                continue

    except WebSocketDisconnect:
        sockets=connections.get(username,set())
        sockets.discard(websocket)
        if not sockets:
            connections.pop(username,None)
            connection_kinds.pop(username,None)
            mark_last_seen(username)
        await broadcast_users()
    except Exception as error:
        print("WebSocket error:", error)
        sockets=connections.get(username,set())
        sockets.discard(websocket)
        if not sockets:
            connections.pop(username,None)
            connection_kinds.pop(username,None)
            mark_last_seen(username)
        await broadcast_users()

# ===== SAVED MESSAGES + FORWARDING EXTENSION =====

# ===== SAVED MESSAGES + FORWARDING EXTENSION =====
# Personal saved-message bookmarks and forward metadata.
# This extension is intentionally appended after the stable V6 code so the
# existing chat/call/media features remain unchanged.

_BASE_INIT_DB = init_db

def init_db():
    # Only the core schema runs on the request-time lazy initialization path.
    # Optional feature migrations (bio, saved messages, reports, forwarded fields,
    # etc.) run separately in a non-fatal background task below, so a temporary
    # PostgreSQL relation lock can never take the web service down.
    _BASE_INIT_DB()


def _saved_ids_for_messages(username, message_ids):
    if not username or not message_ids:
        return set()
    ids = [int(x) for x in message_ids]
    placeholders = ",".join(["%s"] * len(ids))
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT message_id FROM saved_messages WHERE username=%s AND message_id IN ({placeholders})",
                [username, *ids],
            )
            return {int(r["message_id"]) for r in cursor.fetchall()}


def message_row_to_dict(row, signed_media=None, saved=False):
    signed_media = signed_media or {}
    raw_media = row["media"] if row.get("media") else None
    raw_audio = row["audio"] if row.get("audio") else None
    if raw_media in signed_media:
        raw_media = signed_media[raw_media]
    if raw_audio in signed_media:
        raw_audio = signed_media[raw_audio]
    return {
        "id": row["id"],
        "sender": row["sender"],
        "receiver": row["receiver"],
        "message": "" if row["deleted"] else (row["message"] or ""),
        "audio": raw_audio,
        "media": raw_media,
        "status": row["status"] or "sent",
        "message_type": row["message_type"] or "text",
        "reply_to": row["reply_to"],
        "edited": bool(row["edited"]),
        "deleted": bool(row["deleted"]),
        "group_id": row["group_id"],
        "created_at": now_iso(row["created_at"]),
        "saved": bool(saved),
        "forwarded_from_username": row.get("forwarded_from_username"),
        "forwarded_from_name": row.get("forwarded_from_name") or row.get("forwarded_from_username"),
        "forwarded_from_message_id": row.get("forwarded_from_message_id"),
    }


def rows_to_messages(rows, viewer=None):
    saved_ids = _saved_ids_for_messages(viewer, [r["id"] for r in rows]) if viewer else set()
    return [
        message_row_to_dict(row, saved=(int(row["id"]) in saved_ids))
        for row in reversed(rows)
    ]


def _message_select_sql():
    return """
        SELECT id, sender, receiver, message, audio, media, status,
               message_type, reply_to, edited, deleted, group_id, created_at,
               forwarded_from_username, forwarded_from_name, forwarded_from_message_id
        FROM messages
    """


def get_direct_history(user1, user2):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + """
                WHERE group_id IS NULL
                  AND ((sender=%s AND receiver=%s) OR (sender=%s AND receiver=%s))
                ORDER BY id DESC LIMIT %s
            """, (user1, user2, user2, user1, MAX_HISTORY_MESSAGES))
            rows = cursor.fetchall()
    return rows_to_messages(rows, viewer=user1)


def get_group_history(group_id, viewer=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + "WHERE group_id=%s ORDER BY id DESC LIMIT %s", (group_id, MAX_HISTORY_MESSAGES))
            rows = cursor.fetchall()
            messages = rows_to_messages(rows, viewer=viewer)

            own_ids = [int(m["id"]) for m in messages if viewer and m.get("sender") == viewer and m.get("group_id")]
            if own_ids:
                placeholders = ",".join(["%s"] * len(own_ids))
                cursor.execute(f"""
                    SELECT r.message_id, r.username,
                           COALESCE(u.display_name, u.username) AS display_name,
                           r.read_at
                    FROM group_message_reads r
                    JOIN users u ON u.username=r.username
                    WHERE r.message_id IN ({placeholders})
                    ORDER BY r.message_id, r.read_at, r.username
                """, own_ids)
                read_rows = cursor.fetchall()
            else:
                read_rows = []

    readers_by_message = {}
    for r in read_rows:
        readers_by_message.setdefault(str(r["message_id"]), []).append({
            "username": r["username"],
            "display_name": r["display_name"] or r["username"],
            "read_at": now_iso(r["read_at"]),
        })
    for msg in messages:
        if msg.get("group_id") and msg.get("sender") == viewer:
            msg["readers"] = readers_by_message.get(str(msg["id"]), [])
            msg["read_count"] = len(msg["readers"])
    return messages


def get_message(message_id, viewer=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + "WHERE id=%s", (message_id,))
            row = cursor.fetchone()
    if not row:
        return None
    saved = False
    if viewer:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM saved_messages WHERE username=%s AND message_id=%s", (viewer, message_id))
                saved = cursor.fetchone() is not None
    return message_row_to_dict(row, saved=saved)


def _get_message_row_with_access(message_id, username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + "WHERE id=%s", (message_id,))
            row = cursor.fetchone()
    if not row:
        return None
    if row["group_id"]:
        if not is_group_member(row["group_id"], username):
            return None
    else:
        if username not in {row["sender"], row["receiver"]}:
            return None
    if row["deleted"]:
        return None
    return row


def get_saved_messages(username, limit=300):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql().replace("FROM messages", "FROM messages m") + """
                JOIN saved_messages sm ON sm.message_id=m.id
                LEFT JOIN users su ON su.username=m.sender
                LEFT JOIN users ou ON ou.username=CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE m.sender END
                LEFT JOIN groups g ON g.id=m.group_id
                WHERE sm.username=%s
                ORDER BY sm.saved_at DESC, sm.id DESC
                LIMIT %s
            """, (username, username, username, limit))
            rows = cursor.fetchall()

    out=[]
    for r in rows:
        # message_row_to_dict expects direct row keys; row keys are still m.* because
        # the SELECT list was not prefixed in _message_select_sql. PostgreSQL returns
        # the same unique names, which is exactly what we need here.
        msg=message_row_to_dict(r, saved=True)
        if r["group_id"]:
            msg["source_context"] = r["name"] if "name" in r else None
            msg["source_type"] = "group"
            msg["source_group_name"] = r.get("name")
            msg["direct_other_username"] = None
            msg["direct_other_display_name"] = None
        else:
            other = r.get("CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE m.sender END")
            msg["source_type"] = "user"
            # The query uses aliases below in the next override if possible.
        out.append(msg)
    return out


# Replace get_saved_messages with a clean explicit query (kept separate from the
# helper above so schema/query changes are easy to audit).
def get_saved_messages(username, limit=300):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    m.id, m.sender, m.receiver, m.message, m.audio, m.media, m.status,
                    m.message_type, m.reply_to, m.edited, m.deleted, m.group_id, m.created_at,
                    m.forwarded_from_username, m.forwarded_from_name, m.forwarded_from_message_id,
                    sm.saved_at,
                    COALESCE(su.display_name, su.username) AS sender_display_name,
                    g.name AS group_name,
                    CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE m.sender END AS other_username,
                    COALESCE(ou.display_name, ou.username) AS other_display_name
                FROM saved_messages sm
                JOIN messages m ON m.id=sm.message_id
                LEFT JOIN users su ON su.username=m.sender
                LEFT JOIN groups g ON g.id=m.group_id
                LEFT JOIN users ou ON ou.username=CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE m.sender END
                WHERE sm.username=%s
                ORDER BY sm.saved_at DESC, sm.id DESC
                LIMIT %s
            """, (username, username, username, limit))
            rows=cursor.fetchall()

    out=[]
    for r in rows:
        msg=message_row_to_dict(r, saved=True)
        msg["sender_display_name"]=r["sender_display_name"] or r["sender"]
        msg["source_type"]="group" if r["group_id"] else "user"
        msg["source_group_name"]=r["group_name"]
        msg["direct_other_username"]=r["other_username"]
        msg["direct_other_display_name"]=r["other_display_name"] or r["other_username"]
        msg["saved_at"]=now_iso(r["saved_at"])
        out.append(msg)
    return out


def _get_sender_display_name(username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(display_name,username) AS display_name FROM users WHERE username=%s", (username,))
            row=cursor.fetchone()
    return (row["display_name"] if row else username) or username


def _copy_media_for_forward(message):
    """Copy the original private media into a new storage object so deleting the
    original message cannot break the forwarded copy."""
    source = message.get("media") or message.get("audio")
    if not source:
        return None
    source_path = _storage_path_only(source)
    content_type = "application/octet-stream"
    if source_path is None:
        return None
    if source_path.startswith("/uploads/"):
        local_path = source_path.lstrip("/")
        with open(local_path, "rb") as f:
            content=f.read()
    elif source_path.startswith("http://") or source_path.startswith("https://"):
        resp=requests.get(source_path, timeout=30)
        resp.raise_for_status()
        content=resp.content
        content_type=resp.headers.get("content-type", content_type)
    else:
        resp=download_storage_object(source_path)
        if resp.status_code != 200:
            raise RuntimeError(f"media source unavailable: {resp.status_code}")
        content=resp.content
        content_type=resp.headers.get("content-type", content_type)

    if message.get("message_type")=="audio":
        ext=".webm"
        if not content_type or content_type=="application/octet-stream":
            content_type="audio/webm"
        path=f"forwards/{uuid.uuid4()}{ext}"
        upload_storage(path, content, content_type)
        return add_storage_prefix(path)
    else:
        ext=".jpg"
        if "png" in content_type: ext=".png"
        elif "webp" in content_type: ext=".webp"
        elif "gif" in content_type: ext=".gif"
        if not content_type or content_type=="application/octet-stream":
            content_type="image/jpeg"
        path=f"forwards/{uuid.uuid4()}{ext}"
        upload_storage(path, content, content_type)
        return add_storage_prefix(path)


def _validate_forward_target(sender, target):
    target=(target or "").strip()
    if not target:
        return None, None, "مقصد مشخص نیست."
    if target.startswith("group:"):
        try: gid=int(target.split(":",1)[1])
        except (TypeError,ValueError): return None,None,"گروه نامعتبر است."
        if not is_group_member(gid, sender): return None,None,"شما عضو این گروه نیستید."
        if not get_group_info(gid): return None,None,"گروه پیدا نشد."
        return "group", gid, None
    if not user_exists(target): return None,None,"کاربر مقصد وجود ندارد."
    if target==sender: return None,None,"ارسال مجدد به خودت از این مسیر امکان‌پذیر نیست؛ برای خودت از پیام‌های ذخیره‌شده استفاده کن."
    return "user", target, None


# Extend save_message with optional forwarding metadata while keeping every old caller valid.
_BASE_SAVE_MESSAGE = save_message

def save_message(sender, receiver, message="", message_type="text", media=None, audio=None,
                 reply_to=None, group_id=None, forwarded_from_username=None,
                 forwarded_from_name=None, forwarded_from_message_id=None):
    if not forwarded_from_username and not forwarded_from_name and not forwarded_from_message_id:
        return _BASE_SAVE_MESSAGE(sender, receiver, message, message_type, media, audio, reply_to, group_id)
    with get_db() as connection:
        with connection.cursor() as cursor:
            if group_id is None and receiver:
                cursor.execute("""
                    DELETE FROM hidden_conversations
                    WHERE (username=%s AND other_username=%s)
                       OR (username=%s AND other_username=%s)
                """, (sender, receiver, receiver, sender))
            cursor.execute("""
                INSERT INTO messages
                (sender, receiver, message, audio, status, message_type, media,
                 reply_to, edited, deleted, group_id,
                 forwarded_from_username, forwarded_from_name, forwarded_from_message_id)
                VALUES (%s,%s,%s,%s,'sent',%s,%s,%s,FALSE,FALSE,%s,%s,%s,%s)
                RETURNING id
            """, (sender, receiver, message, audio, message_type, media, reply_to, group_id,
                  forwarded_from_username, forwarded_from_name, forwarded_from_message_id))
            mid=cursor.fetchone()["id"]
        connection.commit()
    return mid


@app.post("/api/save-message")
async def api_save_message(username: str = Form(...), token: str = Form(...), message_id: int = Form(...)):
    if not verify_token(username, token):
        return {"success":False,"message":"احراز هویت ناموفق بود."}
    try:
        row=await asyncio.to_thread(_get_message_row_with_access, message_id, username)
        if not row:
            return {"success":False,"message":"این پیام برای ذخیره‌کردن در دسترس نیست."}
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO saved_messages(username,message_id)
                    VALUES (%s,%s)
                    ON CONFLICT (username,message_id) DO NOTHING
                """, (username,message_id))
            connection.commit()
        msg=await asyncio.to_thread(get_message,message_id,username)
        return {"success":True,"saved":True,"message":msg}
    except Exception as error:
        print("Save message error:",repr(error))
        return {"success":False,"message":f"خطا در ذخیره پیام: {type(error).__name__}"}


@app.post("/api/unsave-message")
async def api_unsave_message(username: str = Form(...), token: str = Form(...), message_id: int = Form(...)):
    if not verify_token(username, token):
        return {"success":False,"message":"احراز هویت ناموفق بود."}
    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM saved_messages WHERE username=%s AND message_id=%s", (username,message_id))
            connection.commit()
        return {"success":True,"saved":False,"message_id":message_id}
    except Exception as error:
        print("Unsave message error:",repr(error))
        return {"success":False,"message":f"خطا در حذف ذخیره پیام: {type(error).__name__}"}


@app.get("/api/saved-messages")
async def api_saved_messages(username: str, token: str):
    if not verify_token(username, token):
        return {"success":False,"messages":[]}
    try:
        return {"success":True,"messages":await asyncio.to_thread(get_saved_messages,username)}
    except Exception as error:
        print("Saved messages error:",repr(error))
        return {"success":False,"messages":[],"message":f"خطا در دریافت پیام‌های ذخیره‌شده: {type(error).__name__}"}


@app.post("/api/forward-message")
async def api_forward_message(
    username: str = Form(...),
    token: str = Form(...),
    message_id: int = Form(...),
    target: str = Form(...),
):
    if not verify_token(username, token):
        return {"success":False,"message":"احراز هویت ناموفق بود."}
    try:
        source_row=await asyncio.to_thread(_get_message_row_with_access,message_id,username)
        if not source_row:
            return {"success":False,"message":"این پیام برای هدایت‌کردن در دسترس نیست یا حذف شده است."}
        target_kind,target_value,error=await asyncio.to_thread(_validate_forward_target,username,target)
        if error:
            return {"success":False,"message":error}
        source_name=source_row.get("forwarded_from_username") or source_row["sender"]
        source_display=source_row.get("forwarded_from_name") or await asyncio.to_thread(_get_sender_display_name,source_name)
        # Reuse the same private storage object for forwarded media. The delete-message
        # logic above keeps a media object as long as another message references it,
        # so forwarding stays fast and does not duplicate large voice/image files.
        media=source_row["media"] if source_row["message_type"]=="image" else None
        audio=source_row["audio"] if source_row["message_type"]=="audio" else None
        original_source_id=source_row.get("forwarded_from_message_id") or int(source_row["id"])

        receiver="" if target_kind=="group" else target_value
        group_id=target_value if target_kind=="group" else None
        mid=await asyncio.to_thread(
            save_message, username, receiver, source_row["message"] or "", source_row["message_type"],
            media, audio, None, group_id, source_name, source_display, original_source_id
        )
        msg=await asyncio.to_thread(get_message,mid,username)
        if not msg:
            return {"success":False,"message":"پیام هدایت‌شده ذخیره نشد."}
        asyncio.create_task(deliver_message_safe(msg))
        return {"success":True,"message":msg}
    except Exception as error:
        print("Forward message error:",repr(error))
        return {"success":False,"message":f"خطا در هدایت پیام: {type(error).__name__}"}


# =========================================================
# EXTRA FEATURES: saved chat, self-hide messages, reports, bio, music
# =========================================================
def ensure_extra_feature_schema():
    # Extra migrations are intentionally NOT fatal to application startup.
    # Render/Supabase may temporarily hold a relation lock while another instance
    # is starting. In that case retry in the background instead of killing uvicorn.
    with psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 8000")
            cursor.execute("SET lock_timeout = 1500")

            # Optional columns/objects: do them here, after the server is already
            # serving requests.  A short lock timeout plus retry makes deployments
            # resilient when another Render instance is touching the schema.
            cursor.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='users' AND column_name='bio'"
            )
            if cursor.fetchone() is None:
                try:
                    cursor.execute("ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''")
                except Exception as e:
                    if type(e).__name__ == 'LockNotAvailable':
                        connection.rollback()
                        print('⚠️ Bio column is temporarily locked; will retry on the next migration pass.')
                        return
                    raise

            for sql in [
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS forwarded_from_username TEXT",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS forwarded_from_name TEXT",
                "ALTER TABLE messages ADD COLUMN IF NOT EXISTS forwarded_from_message_id BIGINT",
            ]:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    if type(e).__name__ == 'LockNotAvailable':
                        connection.rollback()
                        print('⚠️ Message migration is temporarily locked; will retry on the next migration pass.')
                        return
                    raise

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS saved_messages (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(username, message_id)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_saved_messages_user ON saved_messages(username, saved_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_saved_messages_message ON saved_messages(message_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_forwarded_from ON messages(forwarded_from_message_id)")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hidden_messages (
                    username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    hidden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(username, message_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS saved_chat_messages (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_reports (
                    id BIGSERIAL PRIMARY KEY,
                    reporter_username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
                    reason TEXT NOT NULL,
                    status TEXT DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hidden_messages_user ON hidden_messages(username, message_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reports_status ON message_reports(status, created_at DESC)")
        connection.commit()


async def _run_extra_schema_migrations():
    # Never block Render startup on optional feature migrations.
    # A short PostgreSQL advisory lock makes multiple Render instances
    # serialize this work without waiting on a long relation lock.
    for attempt in range(12):
        try:
            async def migrate_once():
                with psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=8) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_try_advisory_lock(hashtext('mschat_extra_schema_v2')) AS locked")
                        row = cursor.fetchone()
                        if not row or not row["locked"]:
                            return False
                        try:
                            ensure_extra_feature_schema()
                            return True
                        finally:
                            try:
                                cursor.execute("SELECT pg_advisory_unlock(hashtext('mschat_extra_schema_v2'))")
                            except Exception:
                                pass
            done = await asyncio.to_thread(migrate_once)
            if done:
                print("✅ Extra feature schema is ready")
                return
            print(f"ℹ️ Another instance is running extra schema migration; retry {attempt + 1}/12")
        except Exception as error:
            print(f"⚠️ Extra feature schema attempt {attempt + 1}/12 failed: {type(error).__name__}: {error}")
        await asyncio.sleep(2.5)
    print("⚠️ Extra feature schema migration did not finish yet; app remains online and will retry on later startup.")


@app.on_event("startup")
async def _startup_background_migrations():
    asyncio.create_task(_run_extra_schema_migrations())

# ----- Hidden message helpers -----
def _hidden_message_ids(username, message_ids):
    if not username or not message_ids:
        return set()
    ids=[int(x) for x in message_ids]
    placeholders=','.join(['%s']*len(ids))
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT message_id FROM hidden_messages WHERE username=%s AND message_id IN ({placeholders})", [username,*ids])
            return {int(r['message_id']) for r in cursor.fetchall()}

def get_direct_history(user1, user2):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + """
                WHERE group_id IS NULL
                  AND ((sender=%s AND receiver=%s) OR (sender=%s AND receiver=%s))
                  AND NOT EXISTS (
                      SELECT 1 FROM hidden_messages h
                      WHERE h.username=%s AND h.message_id=messages.id
                  )
                ORDER BY id DESC LIMIT %s
            """, (user1,user2,user2,user1,user1,MAX_HISTORY_MESSAGES))
            rows=cursor.fetchall()
    return rows_to_messages(rows,viewer=user1)

def get_group_history(group_id, viewer=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(_message_select_sql() + """
                WHERE group_id=%s
                  AND NOT EXISTS (
                      SELECT 1 FROM hidden_messages h
                      WHERE h.username=%s AND h.message_id=messages.id
                  )
                ORDER BY id DESC LIMIT %s
            """, (group_id, viewer or '', MAX_HISTORY_MESSAGES))
            rows=cursor.fetchall()
            messages=rows_to_messages(rows,viewer=viewer)
            own_ids=[int(m['id']) for m in messages if viewer and m.get('sender')==viewer and m.get('group_id')]
            if own_ids:
                placeholders=','.join(['%s']*len(own_ids))
                cursor.execute(f"""
                    SELECT r.message_id,r.username,COALESCE(u.display_name,u.username) AS display_name,r.read_at
                    FROM group_message_reads r JOIN users u ON u.username=r.username
                    WHERE r.message_id IN ({placeholders}) ORDER BY r.message_id,r.read_at,r.username
                """, own_ids)
                rr=cursor.fetchall()
            else: rr=[]
    rb={}
    for r in rr: rb.setdefault(str(r['message_id']),[]).append({'username':r['username'],'display_name':r['display_name'] or r['username'],'read_at':now_iso(r['read_at'])})
    for m in messages:
        if m.get('group_id') and m.get('sender')==viewer:
            m['readers']=rb.get(str(m['id']),[]); m['read_count']=len(m['readers'])
    return messages

@app.post('/api/hide-message-for-me')
async def api_hide_message_for_me(username: str=Form(...), token: str=Form(...), message_id: int=Form(...)):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    try:
        row=await asyncio.to_thread(_get_message_row_with_access,message_id,username)
        if not row: return {'success':False,'message':'این پیام در دسترس نیست.'}
        if row['sender']==username: return {'success':False,'message':'برای پیام خودت از حذف پیام استفاده کن.'}
        with get_db() as c:
            with c.cursor() as cur:
                cur.execute('INSERT INTO hidden_messages(username,message_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(username,message_id))
            c.commit()
        return {'success':True,'message_id':message_id}
    except Exception as e:
        print('Hide message error:',repr(e)); return {'success':False,'message':'حذف پیام برای خودت ناموفق بود.'}

# ----- Bio / public profile -----
@app.get('/api/public-profile')
async def api_public_profile(username: str, token: str, target: str):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    target=(target or '').strip()
    info=await asyncio.to_thread(get_user_info,target)
    if not info: return {'success':False,'message':'کاربر پیدا نشد.'}
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute('SELECT username,COALESCE(display_name,username) display_name,avatar,COALESCE(bio,\'\') bio,last_seen FROM users WHERE username=%s',(target,))
            r=cur.fetchone()
    return {'success':True,'profile':{'username':r['username'],'display_name':r['display_name'],'avatar':r['avatar'],'bio':r['bio'] or '','last_seen':now_iso(r['last_seen']),'online':bool(connections.get(target))}}

@app.post('/api/update-bio')
async def api_update_bio(username: str=Form(...), token: str=Form(...), bio: str=Form('')):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    bio=(bio or '').strip()
    if len(bio)>250: return {'success':False,'message':'بیوگرافی نباید بیشتر از ۲۵۰ کاراکتر باشد.'}
    with get_db() as c:
        with c.cursor() as cur: cur.execute('UPDATE users SET bio=%s WHERE username=%s',(bio,username))
        c.commit()
    return {'success':True,'bio':bio}

# ----- Music upload -----
@app.post('/upload-music')
async def upload_music(sender: str=Form(...), receiver: str=Form(...), token: str=Form(...), music: UploadFile=File(...), reply_to: str=Form('')):
    if not verify_token(sender,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    receiver=(receiver or '').strip()
    if receiver.startswith('group:'):
        try: gid=int(receiver.split(':',1)[1])
        except: return {'success':False,'message':'گروه نامعتبر است.'}
        if not await asyncio.to_thread(is_group_member,gid,sender): return {'success':False,'message':'شما عضو این گروه نیستید.'}
        target_receiver=''
    else:
        gid=None
        if not await asyncio.to_thread(user_exists,receiver): return {'success':False,'message':'گیرنده وجود ندارد.'}
        target_receiver=receiver
    content=await music.read()
    if not content: return {'success':False,'message':'فایل موسیقی خالی است.'}
    if len(content)>5*1024*1024: return {'success':False,'message':'حجم موسیقی نباید بیشتر از ۵ مگابایت باشد.'}
    ctype=(music.content_type or 'audio/mpeg').lower()
    if ctype not in ALLOWED_AUDIO_TYPES: ctype='audio/mpeg'
    ext='.mp3' if ctype=='audio/mpeg' else ('.ogg' if ctype=='audio/ogg' else '.webm')
    path=f'music/{uuid.uuid4()}{ext}'
    try:
        await asyncio.to_thread(upload_storage,path,content,ctype)
        rid=int(reply_to) if reply_to else None
        mid=await asyncio.to_thread(save_message,sender,target_receiver,music.filename or '🎵 موسیقی','music',None,add_storage_prefix(path),rid,gid)
        msg=await asyncio.to_thread(get_message,mid,sender)
        asyncio.create_task(deliver_message_safe(msg))
        return {'success':True,'message':msg}
    except Exception as e:
        print('Music upload error:',repr(e)); return {'success':False,'message':f'ارسال موسیقی ناموفق بود: {type(e).__name__}'}

# ----- Saved chat -----
def get_saved_chat(username, limit=300):
    items=[]
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute("""
                SELECT m.id,m.sender,m.receiver,m.message,m.audio,m.media,m.status,
                       m.message_type,m.reply_to,m.edited,m.deleted,m.group_id,m.created_at,
                       m.forwarded_from_username,m.forwarded_from_name,m.forwarded_from_message_id,
                       sm.saved_at,
                       COALESCE(su.display_name,su.username) AS sender_display_name,
                       g.name AS group_name,
                       CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE NULL END AS other_username,
                       CASE WHEN m.group_id IS NULL AND m.sender=%s THEN COALESCE(ou.display_name,ou.username) ELSE NULL END AS other_display_name
                FROM saved_messages sm
                JOIN messages m ON m.id=sm.message_id
                LEFT JOIN users su ON su.username=m.sender
                LEFT JOIN users ou ON ou.username=CASE WHEN m.group_id IS NULL AND m.sender=%s THEN m.receiver ELSE m.sender END
                LEFT JOIN groups g ON g.id=m.group_id
                WHERE sm.username=%s
                ORDER BY sm.saved_at ASC, sm.id ASC
                LIMIT %s
            """,(username,username,username,username,limit))
            bookmarks=cur.fetchall()
            cur.execute("SELECT id,message,created_at FROM saved_chat_messages WHERE username=%s ORDER BY id ASC LIMIT %s",(username,limit))
            notes=cur.fetchall()
    for r in bookmarks:
        m=message_row_to_dict(r,saved=True)
        m['saved_chat_kind']='bookmark'; m['saved_at']=now_iso(r.get('saved_at')); m['sender_display_name']=r.get('sender_display_name') or r['sender']
        m['source_type']='group' if m.get('group_id') else 'user'; m['source_group_name']=r.get('group_name'); m['direct_other_username']=r.get('other_username')
        m['direct_other_display_name']=r.get('other_display_name') or r.get('other_username')
        m['saved_chat_mine']=m.get('sender')==username
        items.append(m)
    for r in notes:
        items.append({'id':f"note:{r['id']}",'saved_note_id':r['id'],'saved_chat_kind':'note','sender':username,'receiver':username,'message':r['message'],'message_type':'text','created_at':now_iso(r['created_at']),'status':'read','saved':False,'group_id':None,'deleted':False,'saved_chat_mine':True})
    items.sort(key=lambda x:x.get('saved_at') or x.get('created_at') or '')
    return items[-limit:]

@app.get('/api/saved-chat')
async def api_saved_chat(username: str, token: str):
    if not verify_token(username,token): return {'success':False,'messages':[]}
    return {'success':True,'messages':await asyncio.to_thread(get_saved_chat,username)}

@app.post('/api/saved-chat/send')
async def api_saved_chat_send(username: str=Form(...), token: str=Form(...), message: str=Form(...)):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    text=(message or '').strip()
    if not text or len(text)>5000: return {'success':False,'message':'متن پیام معتبر نیست.'}
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute('INSERT INTO saved_chat_messages(username,message) VALUES(%s,%s) RETURNING id,created_at',(username,text))
            row=cur.fetchone()
        c.commit()
    return {'success':True,'message':{'id':f"note:{row['id']}",'saved_note_id':row['id'],'saved_chat_kind':'note','sender':username,'receiver':username,'message':text,'message_type':'text','created_at':now_iso(row['created_at']),'status':'read','saved':False,'group_id':None,'deleted':False,'saved_chat_mine':True}}

@app.post('/api/saved-chat/delete-note')
async def api_saved_chat_delete_note(username: str=Form(...), token: str=Form(...), note_id: int=Form(...)):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    with get_db() as c:
        with c.cursor() as cur: cur.execute('DELETE FROM saved_chat_messages WHERE id=%s AND username=%s',(note_id,username))
        c.commit()
    return {'success':True}

# ----- Reporting -----
@app.post('/api/report-message')
async def api_report_message(username: str=Form(...), token: str=Form(...), message_id: int=Form(...), reason: str=Form(...)):
    if not verify_token(username,token): return {'success':False,'message':'احراز هویت ناموفق بود.'}
    row=await asyncio.to_thread(_get_message_row_with_access,message_id,username)
    if not row: return {'success':False,'message':'این پیام برای گزارش در دسترس نیست.'}
    if row['sender']==username: return {'success':False,'message':'نمی‌توانی پیام خودت را گزارش کنی.'}
    reason=(reason or '').strip() or 'محتوای نامناسب'
    if len(reason)>500: reason=reason[:500]
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute('SELECT 1 FROM message_reports WHERE reporter_username=%s AND message_id=%s AND status=\'open\'',(username,message_id))
            if cur.fetchone(): return {'success':True,'message':'گزارش قبلاً ثبت شده است.'}
            cur.execute('INSERT INTO message_reports(reporter_username,message_id,reason) VALUES(%s,%s,%s)',(username,message_id,reason))
        c.commit()
    return {'success':True,'message':'گزارش ثبت شد.'}

def _admin_ok(username,token): return username==OWNER_USERNAME and verify_token(username,token)

@app.get('/api/admin/reports')
async def api_admin_reports(username: str, token: str):
    if not _admin_ok(username,token): return {'success':False,'reports':[],'message':'دسترسی غیرمجاز.'}
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute("""
                SELECT r.id,r.message_id,r.reason,r.status,r.created_at,r.reporter_username,
                       m.sender,m.receiver,m.group_id,m.message,m.message_type,
                       COALESCE(su.display_name,su.username) AS sender_name,
                       COALESCE(ru.display_name,ru.username) AS reporter_name,
                       g.name AS group_name
                FROM message_reports r JOIN messages m ON m.id=r.message_id
                LEFT JOIN users su ON su.username=m.sender LEFT JOIN users ru ON ru.username=r.reporter_username
                LEFT JOIN groups g ON g.id=m.group_id
                ORDER BY r.created_at DESC
                LIMIT 200
            """)
            rows=cur.fetchall()
    out=[]
    for r in rows:
        out.append({k:(now_iso(v) if k=='created_at' else v) for k,v in r.items()})
    return {'success':True,'reports':out}

@app.get('/api/admin/report-history')
async def api_admin_report_history(username: str, token: str, message_id: int):
    if not _admin_ok(username,token): return {'success':False,'messages':[],'message':'دسترسی غیرمجاز.'}
    with get_db() as c:
        with c.cursor() as cur:
            cur.execute('SELECT sender,receiver,group_id FROM messages WHERE id=%s',(message_id,)); row=cur.fetchone()
    if not row: return {'success':False,'messages':[],'message':'پیام گزارش‌شده پیدا نشد.'}
    if row['group_id']:
        msgs=await asyncio.to_thread(get_group_history,row['group_id'],username)
        return {'success':True,'kind':'group','group_id':row['group_id'],'messages':msgs}
    other=row['sender'] if row['sender']!=username else row['receiver']
    msgs=await asyncio.to_thread(get_direct_history,row['sender'],other)
    return {'success':True,'kind':'user','user':other,'messages':msgs}

@app.post('/api/admin/report-status')
async def api_admin_report_status(username: str=Form(...), token: str=Form(...), report_id: int=Form(...), status: str=Form(...)):
    if not _admin_ok(username,token): return {'success':False,'message':'دسترسی غیرمجاز.'}
    if status not in {'open','reviewed','closed'}: return {'success':False,'message':'وضعیت نامعتبر.'}
    with get_db() as c:
        with c.cursor() as cur: cur.execute('UPDATE message_reports SET status=%s WHERE id=%s',(status,report_id))
        c.commit()
    return {'success':True}
