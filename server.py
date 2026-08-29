import os
import uuid
import hashlib
import secrets
import hmac
import base64
from datetime import datetime

import requests
import psycopg

from dotenv import load_dotenv
from psycopg.rows import dict_row
from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Form,
    UploadFile,
    File,
)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


# =========================================================
# LOAD ENVIRONMENT
# =========================================================

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY_TEXT = os.getenv("SECRET_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL در فایل .env پیدا نشد."
    )

if not SECRET_KEY_TEXT:
    raise RuntimeError(
        "SECRET_KEY در فایل .env پیدا نشد."
    )

if not SUPABASE_URL:
    raise RuntimeError(
        "SUPABASE_URL در فایل .env پیدا نشد."
    )

if not SUPABASE_SERVICE_KEY:
    raise RuntimeError(
        "SUPABASE_SERVICE_KEY در فایل .env پیدا نشد."
    )


SECRET_KEY = SECRET_KEY_TEXT.encode("utf-8")


# =========================================================
# APP
# =========================================================

app = FastAPI()


# =========================================================
# SETTINGS
# =========================================================

BUCKET_NAME = "voices"

MAX_AUDIO_SIZE = 10 * 1024 * 1024

SIGNED_URL_SECONDS = 3600


# =========================================================
# ONLINE CONNECTIONS
# =========================================================

connections = {}


# =========================================================
# LOCAL UPLOADS
# =========================================================

UPLOAD_FOLDER = "uploads"

os.makedirs(
    UPLOAD_FOLDER,
    exist_ok=True,
)


app.mount(
    "/uploads",
    StaticFiles(directory=UPLOAD_FOLDER),
    name="uploads",
)


# =========================================================
# DATABASE
# =========================================================

def get_db():
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=10,
    )


def init_db():
    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    display_name TEXT,
                    last_seen TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    sender TEXT NOT NULL,
                    receiver TEXT NOT NULL,
                    message TEXT,
                    audio TEXT,
                    status TEXT DEFAULT 'sent',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS display_name TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP
                """
            )

            cursor.execute(
                """
                ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS audio TEXT
                """
            )

            cursor.execute(
                """
                ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'sent'
                """
            )

        connection.commit()


init_db()


# =========================================================
# PASSWORD FUNCTIONS
# =========================================================

def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)

    password_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        200_000,
    )

    salt_text = base64.b64encode(
        salt
    ).decode("utf-8")

    hash_text = base64.b64encode(
        password_hash
    ).decode("utf-8")

    return f"{salt_text}:{hash_text}"


def verify_password(
    password: str,
    stored_hash: str,
) -> bool:
    try:
        salt_text, hash_text = stored_hash.split(":")

        salt = base64.b64decode(salt_text)
        original_hash = base64.b64decode(hash_text)

        new_hash = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            200_000,
        )

        return hmac.compare_digest(
            original_hash,
            new_hash,
        )

    except Exception:
        return False


# =========================================================
# TOKEN
# =========================================================

def create_token(username: str) -> str:
    signature = hmac.new(
        SECRET_KEY,
        username.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    return base64.urlsafe_b64encode(
        signature
    ).decode("utf-8")


def verify_token(
    username: str,
    token: str,
) -> bool:
    expected_token = create_token(username)

    return hmac.compare_digest(
        expected_token,
        token,
    )


# =========================================================
# HOME
# =========================================================

@app.get("/")
async def home():
    return FileResponse("index.html")


# =========================================================
# REGISTER
# =========================================================

@app.post("/register")
async def register(
    username: str = Form(...),
    password: str = Form(...),
):
    username = username.strip()

    if len(username) < 3:
        return {
            "success": False,
            "message": "نام کاربری باید حداقل ۳ کاراکتر باشد.",
        }

    if len(username) > 30:
        return {
            "success": False,
            "message": "نام کاربری بیش از حد طولانی است.",
        }

    if len(password) < 6:
        return {
            "success": False,
            "message": "رمز عبور باید حداقل ۶ کاراکتر باشد.",
        }

    password_hash = hash_password(password)

    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO users (
                        username,
                        password_hash,
                        display_name
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (
                        username,
                        password_hash,
                        username,
                    ),
                )

            connection.commit()

        return {
            "success": True,
            "message": "حساب با موفقیت ساخته شد.",
        }

    except psycopg.errors.UniqueViolation:
        return {
            "success": False,
            "message": "این نام کاربری قبلاً ثبت شده است.",
        }


# =========================================================
# LOGIN
# =========================================================

@app.post("/login")
async def login(
    username: str = Form(...),
    password: str = Form(...),
):
    username = username.strip()

    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    username,
                    password_hash,
                    display_name
                FROM users
                WHERE username = %s
                """,
                (username,),
            )

            user = cursor.fetchone()

    if not user:
        return {
            "success": False,
            "message": "نام کاربری یا رمز عبور اشتباه است.",
        }

    if not verify_password(
        password,
        user["password_hash"],
    ):
        return {
            "success": False,
            "message": "نام کاربری یا رمز عبور اشتباه است.",
        }

    if username in connections:
        return {
            "success": False,
            "message": "این حساب در حال حاضر وارد شده است.",
        }

    return {
        "success": True,
        "username": username,
        "display_name": user["display_name"] or username,
        "token": create_token(username),
    }


# =========================================================
# UPDATE PROFILE
# =========================================================

@app.post("/update-profile")
async def update_profile(
    username: str = Form(...),
    token: str = Form(...),
    display_name: str = Form(...),
):
    if not verify_token(
        username,
        token,
    ):
        return {
            "success": False,
            "message": "احراز هویت ناموفق بود.",
        }

    display_name = display_name.strip()

    if not display_name:
        return {
            "success": False,
            "message": "نام نمایشی نمی‌تواند خالی باشد.",
        }

    if len(display_name) > 40:
        return {
            "success": False,
            "message": "نام نمایشی بیش از حد طولانی است.",
        }

    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE users
                SET display_name = %s
                WHERE username = %s
                """,
                (
                    display_name,
                    username,
                ),
            )

        connection.commit()

    await broadcast_users()

    return {
        "success": True,
        "display_name": display_name,
    }


# =========================================================
# CHANGE PASSWORD
# =========================================================

@app.post("/change-password")
async def change_password(
    username: str = Form(...),
    token: str = Form(...),
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    if not verify_token(
        username,
        token,
    ):
        return {
            "success": False,
            "message": "احراز هویت ناموفق بود.",
        }

    if len(new_password) < 6:
        return {
            "success": False,
            "message": "رمز جدید باید حداقل ۶ کاراکتر باشد.",
        }

    if current_password == new_password:
        return {
            "success": False,
            "message": "رمز جدید باید با رمز قبلی متفاوت باشد.",
        }

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT password_hash
                FROM users
                WHERE username = %s
                """,
                (username,),
            )

            user = cursor.fetchone()

            if not user:
                return {
                    "success": False,
                    "message": "کاربر پیدا نشد.",
                }

            if not verify_password(
                current_password,
                user["password_hash"],
            ):
                return {
                    "success": False,
                    "message": "رمز فعلی اشتباه است.",
                }

            new_hash = hash_password(
                new_password
            )

            cursor.execute(
                """
                UPDATE users
                SET password_hash = %s
                WHERE username = %s
                """,
                (
                    new_hash,
                    username,
                ),
            )

        connection.commit()

    return {
        "success": True,
        "message": "رمز عبور با موفقیت تغییر کرد.",
    }


# =========================================================
# LAST SEEN
# =========================================================

def mark_last_seen(username: str):
    try:
        with get_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE users
                    SET last_seen = CURRENT_TIMESTAMP
                    WHERE username = %s
                    """,
                    (username,),
                )

            connection.commit()

    except Exception as error:
        print(
            "Last seen error:",
            error,
        )


# =========================================================
# USER INFO
# =========================================================

def get_user_info(username: str):

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    username,
                    display_name,
                    last_seen
                FROM users
                WHERE username = %s
                """,
                (username,),
            )

            user = cursor.fetchone()

    if not user:
        return None

    last_seen = user["last_seen"]

    if last_seen:
        last_seen = last_seen.isoformat(
            sep=" "
        )

    return {
        "username": user["username"],
        "display_name": (
            user["display_name"]
            or
            user["username"]
        ),
        "last_seen": last_seen,
        "online": username in connections,
    }


# =========================================================
# BROADCAST USERS
# =========================================================

async def broadcast_users():

    users = []

    for username in list(
        connections.keys()
    ):

        info = get_user_info(username)

        if info:
            users.append(info)

    disconnected = []

    for username, websocket in list(
        connections.items()
    ):

        try:
            await websocket.send_json({
                "type": "users",
                "users": users,
            })

        except Exception:
            disconnected.append(username)

    for username in disconnected:
        connections.pop(username, None)


# =========================================================
# SAVE TEXT MESSAGE
# =========================================================

def save_text_message(
    sender: str,
    receiver: str,
    message: str,
) -> int:

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO messages (
                    sender,
                    receiver,
                    message,
                    audio,
                    status
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    NULL,
                    'sent'
                )
                RETURNING id
                """,
                (
                    sender,
                    receiver,
                    message,
                ),
            )

            row = cursor.fetchone()

        connection.commit()

    return row["id"]


# =========================================================
# SAVE AUDIO MESSAGE
# =========================================================

def save_audio_message(
    sender: str,
    receiver: str,
    audio_path: str,
) -> int:

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO messages (
                    sender,
                    receiver,
                    message,
                    audio,
                    status
                )
                VALUES (
                    %s,
                    %s,
                    '',
                    %s,
                    'sent'
                )
                RETURNING id
                """,
                (
                    sender,
                    receiver,
                    audio_path,
                ),
            )

            row = cursor.fetchone()

        connection.commit()

    return row["id"]


# =========================================================
# UPDATE MESSAGE STATUS
# =========================================================

def update_message_status(
    message_id: int,
    status: str,
):
    with get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE messages
                SET status = %s
                WHERE id = %s
                """,
                (
                    status,
                    message_id,
                ),
            )

        connection.commit()


# =========================================================
# SIGNED AUDIO URL
# =========================================================

def create_signed_audio_url(
    storage_path: str,
):
    if not storage_path:
        return None

    if storage_path.startswith("/uploads/"):
        return storage_path

    if storage_path.startswith("storage:"):
        filename = storage_path[
            len("storage:")
        ]
    else:
        filename = storage_path

    url = (
        SUPABASE_URL
        + "/storage/v1/object/sign/"
        + BUCKET_NAME
        + "/"
        + filename
    )

    headers = {
        "Authorization":
            f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey":
            SUPABASE_SERVICE_KEY,
        "Content-Type":
            "application/json",
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json={
                "expiresIn":
                    SIGNED_URL_SECONDS,
            },
            timeout=20,
        )

        if response.status_code not in (200, 201):
            print(
                "Signed URL error:",
                response.status_code,
                response.text,
            )
            return None

        data = response.json()

        signed_path = (
            data.get("signedURL")
            or
            data.get("signedUrl")
            or
            data.get("signed_url")
        )

        if not signed_path:
            signed_path = data.get("path")

        if not signed_path:
            return None

        if signed_path.startswith("http"):
            return signed_path

        return (
            SUPABASE_URL
            + signed_path
        )

    except Exception as error:

        print(
            "Signed URL exception:",
            error,
        )

        return None


# =========================================================
# CHAT HISTORY
# =========================================================

def get_chat_history(
    user1: str,
    user2: str,
):

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    id,
                    sender,
                    receiver,
                    message,
                    audio,
                    status,
                    created_at
                FROM messages
                WHERE
                    (
                        sender = %s
                        AND
                        receiver = %s
                    )
                    OR
                    (
                        sender = %s
                        AND
                        receiver = %s
                    )
                ORDER BY id ASC
                """,
                (
                    user1,
                    user2,
                    user2,
                    user1,
                ),
            )

            rows = cursor.fetchall()

    result = []

    for row in rows:

        created_at = row["created_at"]

        if created_at:
            created_at = created_at.isoformat(
                sep=" "
            )

        audio_url = None

        if row["audio"]:
            audio_url = create_signed_audio_url(
                row["audio"]
            )

        result.append({
            "id": row["id"],
            "sender": row["sender"],
            "receiver": row["receiver"],
            "message": row["message"],
            "audio": audio_url,
            "status": row["status"],
            "created_at": created_at,
        })

    return result


# =========================================================
# DIRECT STORAGE UPLOAD
# =========================================================

def upload_audio_to_storage(
    filename: str,
    content: bytes,
):

    url = (
        SUPABASE_URL
        + "/storage/v1/object/"
        + BUCKET_NAME
        + "/"
        + filename
    )

    headers = {
        "Authorization":
            f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey":
            SUPABASE_SERVICE_KEY,
        "Content-Type":
            "audio/webm",
        "x-upsert":
            "false",
    }

    response = requests.post(
        url,
        headers=headers,
        data=content,
        timeout=60,
    )

    if response.status_code not in (
        200,
        201,
    ):
        raise RuntimeError(
            "Storage upload failed: "
            f"{response.status_code} "
            f"{response.text}"
        )


# =========================================================
# AUDIO UPLOAD
# =========================================================

@app.post("/upload-audio")
async def upload_audio(
    sender: str = Form(...),
    receiver: str = Form(...),
    token: str = Form(...),
    audio: UploadFile = File(...),
):

    if not verify_token(
        sender,
        token,
    ):
        return {
            "success": False,
            "message": "احراز هویت ناموفق بود.",
        }

    with get_db() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT username
                FROM users
                WHERE username = %s
                OR username = %s
                """,
                (
                    sender,
                    receiver,
                ),
            )

            users = cursor.fetchall()

    if len(users) != 2:
        return {
            "success": False,
            "message":
                "فرستنده یا گیرنده وجود ندارد.",
        }

    content = await audio.read()

    if len(content) == 0:
        return {
            "success": False,
            "message":
                "فایل صوتی خالی است.",
        }

    if len(content) > MAX_AUDIO_SIZE:
        return {
            "success": False,
            "message":
                "حجم ویس نباید بیشتر از ۱۰ مگابایت باشد.",
        }

    filename = (
        f"{uuid.uuid4()}.webm"
    )

    try:

        upload_audio_to_storage(
            filename,
            content,
        )

    except Exception as error:

        print(
            "Storage upload error:",
            error,
        )

        return {
            "success": False,
            "message":
                "آپلود ویس در Storage ناموفق بود.",
        }

    storage_path = (
        f"storage:{filename}"
    )

    message_id = save_audio_message(
        sender,
        receiver,
        storage_path,
    )

    signed_url = create_signed_audio_url(
        storage_path
    )

    if receiver in connections:

        await connections[
            receiver
        ].send_json({
            "type": "audio",
            "id": message_id,
            "from": sender,
            "audio": signed_url,
        })

        update_message_status(
            message_id,
            "delivered",
        )

    return {
        "success": True,
        "id": message_id,
        "audio": signed_url,
    }


# =========================================================
# CALL SIGNAL
# =========================================================

async def send_call_signal(
    receiver: str,
    data: dict,
):

    if receiver not in connections:
        return False

    try:

        await connections[
            receiver
        ].send_json(data)

        return True

    except Exception:

        return False


# =========================================================
# WEBSOCKET
# =========================================================

@app.websocket(
    "/chat/{username}/{token}"
)
async def chat(
    websocket: WebSocket,
    username: str,
    token: str,
):

    if not verify_token(
        username,
        token,
    ):
        await websocket.close(
            code=1008
        )
        return

    await websocket.accept()

    connections[username] = websocket

    await broadcast_users()

    try:

        while True:

            data = (
                await websocket.receive_json()
            )

            action = data.get("action")


            # =================================================
            # HISTORY
            # =================================================

            if action == "history":

                other_user = data.get("user")

                if not other_user:
                    continue

                history = get_chat_history(
                    username,
                    other_user,
                )

                await websocket.send_json({
                    "type": "history",
                    "user": other_user,
                    "messages": history,
                })

                continue


            # =================================================
            # MARK READ
            # =================================================

            if action == "mark-read":

                sender = data.get("sender")

                if not sender:
                    continue

                with get_db() as connection:
                    with connection.cursor() as cursor:

                        cursor.execute(
                            """
                            UPDATE messages
                            SET status = 'read'
                            WHERE
                                sender = %s
                                AND
                                receiver = %s
                            """,
                            (
                                sender,
                                username,
                            ),
                        )

                    connection.commit()

                if sender in connections:

                    await connections[
                        sender
                    ].send_json({
                        "type":
                            "messages-read",
                        "by":
                            username,
                    })

                continue


            # =================================================
            # MESSAGE
            # =================================================

            if action == "message":

                receiver = data.get("to")
                message = data.get("message")

                if (
                    not receiver
                    or
                    not message
                ):
                    continue

                message = message.strip()

                if not message:
                    continue

                if len(message) > 5000:

                    await websocket.send_json({
                        "type": "error",
                        "message":
                            "پیام خیلی طولانی است.",
                    })

                    continue

                with get_db() as connection:
                    with connection.cursor() as cursor:

                        cursor.execute(
                            """
                            SELECT username
                            FROM users
                            WHERE username = %s
                            """,
                            (receiver,),
                        )

                        receiver_exists = (
                            cursor.fetchone()
                        )

                if not receiver_exists:

                    await websocket.send_json({
                        "type": "error",
                        "message":
                            "این کاربر وجود ندارد.",
                    })

                    continue

                message_id = save_text_message(
                    username,
                    receiver,
                    message,
                )

                delivered = False

                if receiver in connections:

                    await connections[
                        receiver
                    ].send_json({
                        "type": "message",
                        "id": message_id,
                        "from": username,
                        "message": message,
                        "status": "delivered",
                    })

                    delivered = True

                    update_message_status(
                        message_id,
                        "delivered",
                    )

                await websocket.send_json({
                    "type": "sent",
                    "id": message_id,
                    "to": receiver,
                    "message": message,
                    "status":
                        (
                            "delivered"
                            if delivered
                            else
                            "sent"
                        ),
                })

                continue


            # =================================================
            # CALL OFFER
            # =================================================

            if action == "call-offer":

                receiver = data.get("to")
                offer = data.get("offer")

                if receiver and offer:

                    success = (
                        await send_call_signal(
                            receiver,
                            {
                                "type":
                                    "call-offer",
                                "from":
                                    username,
                                "offer":
                                    offer,
                            },
                        )
                    )

                    if not success:

                        await websocket.send_json({
                            "type":
                                "call-error",
                            "message":
                                "کاربر مورد نظر آنلاین نیست.",
                        })

                continue


            # =================================================
            # CALL ANSWER
            # =================================================

            if action == "call-answer":

                receiver = data.get("to")
                answer = data.get("answer")

                if receiver and answer:

                    await send_call_signal(
                        receiver,
                        {
                            "type":
                                "call-answer",
                            "from":
                                username,
                            "answer":
                                answer,
                        },
                    )

                continue


            # =================================================
            # ICE
            # =================================================

            if action == "ice-candidate":

                receiver = data.get("to")
                candidate = data.get("candidate")

                if receiver and candidate:

                    await send_call_signal(
                        receiver,
                        {
                            "type":
                                "ice-candidate",
                            "from":
                                username,
                            "candidate":
                                candidate,
                        },
                    )

                continue


            # =================================================
            # REJECT
            # =================================================

            if action == "call-rejected":

                receiver = data.get("to")

                if receiver:

                    await send_call_signal(
                        receiver,
                        {
                            "type":
                                "call-rejected",
                            "from":
                                username,
                        },
                    )

                continue


            # =================================================
            # END CALL
            # =================================================

            if action == "call-ended":

                receiver = data.get("to")

                if receiver:

                    await send_call_signal(
                        receiver,
                        {
                            "type":
                                "call-ended",
                            "from":
                                username,
                        },
                    )

                continue


    except WebSocketDisconnect:

        if connections.get(
            username
        ) == websocket:

            connections.pop(
                username,
                None
            )

        mark_last_seen(
            username
        )

        await broadcast_users()


    except Exception as error:

        print(
            "WebSocket error:",
            error,
        )

        if connections.get(
            username
        ) == websocket:

            connections.pop(
                username,
                None
            )

        mark_last_seen(
            username
        )

        await broadcast_users()