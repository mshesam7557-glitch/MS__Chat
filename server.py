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


def get_group_history(group_id):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, sender, receiver, message, audio, media, status,
                       message_type, reply_to, edited, deleted, group_id, created_at
                FROM messages WHERE group_id=%s ORDER BY id DESC LIMIT %s
            """, (group_id, MAX_HISTORY_MESSAGES))
            rows = cursor.fetchall()
    return rows_to_messages(rows)


def save_message(sender, receiver, message="", message_type="text",
                 media=None, audio=None, reply_to=None, group_id=None):
    with get_db() as connection:
        with connection.cursor() as cursor:
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
        return {"success": True, "message": "حساب با موفقیت ساخته شد."}
    except psycopg.errors.UniqueViolation:
        return {"success": False, "message": "این نام کاربری قبلاً ثبت شده است."}



def get_recent_chat_users(username):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH recent AS (
                    SELECT CASE WHEN sender=%s THEN receiver ELSE sender END AS other_user,
                           MAX(id) AS last_id
                    FROM messages
                    WHERE group_id IS NULL AND (sender=%s OR receiver=%s)
                    GROUP BY CASE WHEN sender=%s THEN receiver ELSE sender END
                ), unread AS (
                    SELECT sender AS other_user, COUNT(*) AS unread_count
                    FROM messages
                    WHERE group_id IS NULL AND receiver=%s AND status<>'read' AND deleted=FALSE
                    GROUP BY sender
                )
                SELECT u.username, u.display_name, u.avatar, u.last_seen,
                       COALESCE(unread.unread_count,0) AS unread_count
                FROM recent
                JOIN users u ON u.username=recent.other_user
                LEFT JOIN unread ON unread.other_user=u.username
                ORDER BY recent.last_id DESC
            """, (username, username, username, username, username))
            rows=cursor.fetchall()
    return [{
        "username": r["username"],
        "display_name": r["display_name"] or r["username"],
        "avatar": r["avatar"],
        "last_seen": now_iso(r["last_seen"]),
        "online": bool(connections.get(r["username"])),
        "unread_count": int(r["unread_count"] or 0),
    } for r in rows]


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
                WHERE group_id IS NOT NULL AND status<>'read' AND deleted=FALSE
                  AND sender<>%s AND EXISTS (
                    SELECT 1 FROM group_members gm WHERE gm.group_id=messages.group_id AND gm.username=%s
                  )
                GROUP BY group_id
            """, (username,username))
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


async def send_push_to_user(username, title, body):
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
        ok=await asyncio.to_thread(_push_one,obj,title,body,"/")
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
        for username in recipients:
            if not connections.get(username):
                await send_push_to_user(username, "پیام جدید در MS Chat", msg.get("message") or "📎 فایل جدید")
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
    if not connections.get(msg["receiver"]):
        await send_push_to_user(msg["receiver"], "پیام جدید در MS Chat", msg.get("message") or "📎 فایل جدید")
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
                await asyncio.to_thread(delete_storage, p)
            except Exception:
                pass
    msg = get_message(message_id)
    await notify_message_delete(msg)
    return {"success": True, "id": message_id}


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
                    "messages": await asyncio.to_thread(get_group_history, gid),
                })
                continue

            if action == "mark-group-read":
                try:
                    gid = int(data.get("group_id"))
                except (TypeError, ValueError):
                    continue
                if not await asyncio.to_thread(is_group_member, gid, username):
                    continue
                with get_db() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            UPDATE messages SET status='read'
                            WHERE group_id=%s AND sender<>%s AND status<>'read' AND deleted=FALSE
                        """, (gid, username))
                    connection.commit()
                await send_to(username, {"type":"unread","unread":await asyncio.to_thread(get_unread_counts,username)})
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
                receiver = data.get("to")
                ok = await send_call_signal(receiver, {
                    "type": "call-offer",
                    "from": username,
                    "offer": data.get("offer"),
                    "mode": data.get("mode", "audio"),
                })
                if not ok:
                    await websocket.send_json({
                        "type": "call-error",
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
