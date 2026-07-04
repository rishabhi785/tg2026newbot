import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import string
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import httpx
import uvicorn
import aiosqlite
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
from pydantic import BaseModel
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, ChatJoinRequestHandler, filters

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8000"))

TURSO_HTTP_URL = "https://botdb-rishabhi785.aws-ap-south-1.turso.io/v2/pipeline"
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3Nzk3MDA0NTIsImlkIjoiMDE5ZTVlNjctNzIwMS03OTQwLWI3YTUtMjUxZmI5ZTQ4YTY2IiwicmlkIjoiZGQxNGI2NWItZjI4MC00YmNjLTk5MzgtNzA4NWEwYzQ4OGViIn0.1uBpnSQhPDAfoLE8XCkhP_uQWp3i0egjA6QshsGFQxh2VrODIt07FRj4v2edrAcRwVReWqg2zKzQaZqTGoFZBA"

# ---- Turso HTTP API wrapper ----
class TursoCursor:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns
        self._idx = 0

    def _convert_row(self, raw):
        if raw is None:
            return None
        return tuple(v.get("value") if isinstance(v, dict) else v for v in raw)

    async def fetchone(self):
        if self._rows:
            return self._convert_row(self._rows[0])
        return None

    async def fetchall(self):
        return [self._convert_row(r) for r in self._rows]


class TursoConnection:
    def __init__(self):
        self._stmts = []
        self._last_cursor = TursoCursor([], [])

    async def execute(self, sql, params=()):
        self._stmts.append({"type": "execute", "stmt": {
            "sql": sql,
            "args": [{"type": "text", "value": str(p)} if p is not None else {"type": "null"} for p in params]
        }})
        self._pending_sql = sql
        self._pending_params = params
        return _PendingExec(self, sql, params)

    async def commit(self):
        await self._flush()

    async def _flush(self):
        if not self._stmts:
            return
        stmts = self._stmts[:]
        self._stmts = []
        client = await get_http_client()
        resp = await client.post(
            TURSO_HTTP_URL,
            headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
            json={"requests": stmts},
        )
        data = resp.json()
        results = data.get("results", [])
        if results:
            last = results[-1]
            if last.get("type") == "ok":
                rs = last.get("response", {}).get("result", {})
                rows = rs.get("rows", [])
                cols = rs.get("cols", [])
                self._last_cursor = TursoCursor(rows, cols)
        return self._last_cursor

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self._flush()


class _PendingExec:
    def __init__(self, conn, sql, params):
        self._conn = conn
        self._sql = sql
        self._params = params

    async def fetchone(self):
        cursor = await self._conn._flush()
        return await cursor.fetchone()

    async def fetchall(self):
        cursor = await self._conn._flush()
        return await cursor.fetchall()


def turso_connect():
    return TursoConnection()

ADMIN_ID = 8442711165

REPLIT_DOMAINS = os.getenv("REPLIT_DOMAINS", "")
MANUAL_WEBAPP_URL = "https://tg2026newbot.onrender.com/bot/verify"

if MANUAL_WEBAPP_URL:
    WEBAPP_URL = MANUAL_WEBAPP_URL
elif REPLIT_DOMAINS:
    PUBLIC_HOST = REPLIT_DOMAINS.split(",")[0].strip()
    WEBAPP_URL = f"https://{PUBLIC_HOST}/bot/verify"
else:
    WEBAPP_URL = f"http://localhost:{PORT}/bot/verify"

VSV_API_URL = "https://vsv-gateway-solutions.co.in/Api/api.php"
VSV_TOKEN = "RTCLFTJV"

bot_app_global = None

# Shared HTTP client
_http_client: httpx.AsyncClient = None

async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=15)
    return _http_client

# ===================== DATABASE =====================

async def init_db():
    async with turso_connect() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_verified INTEGER DEFAULT 0,
                device_id TEXT,
                verified_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS device_registry (
                device_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                registered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ip_registry (
                ip_address TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                registered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS persistent_device_registry (
                persistent_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                registered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_balance (
                user_id INTEGER PRIMARY KEY,
                balance REAL DEFAULT 0.0,
                referral_count INTEGER DEFAULT 0,
                last_bonus_claim TEXT,
                upi_id TEXT,
                vsv_wallet TEXT,
                email TEXT,
                mobile TEXT
            )
        """)
        # Dynamic migration for Ultra Pay column
        try:
            await db.execute("ALTER TABLE user_balance ADD COLUMN ultra_wallet TEXT")
            await db.commit()
        except:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_username TEXT NOT NULL,
                channel_link TEXT NOT NULL,
                channel_name TEXT,
                is_active INTEGER DEFAULT 1,
                channel_type TEXT DEFAULT 'public',
                added_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            await db.execute("ALTER TABLE channels ADD COLUMN channel_type TEXT DEFAULT 'public'")
            await db.commit()
        except:
            pass
        try:
            await db.execute("ALTER TABLE channels ADD COLUMN chat_id INTEGER DEFAULT NULL")
            await db.commit()
        except:
            pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                upi_id TEXT,
                vsv_wallet TEXT,
                method TEXT DEFAULT 'upi',
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                processed_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                amount REAL NOT NULL,
                user_id INTEGER NOT NULL,
                email TEXT,
                mobile TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS channel_join_requests (
                user_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                requested_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, channel_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            )
        """)
        await db.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (ADMIN_ID,))
        await db.commit()

        defaults = [
            ("refer_reward", "5"),
            ("min_withdrawal", "50"),
            ("welcome_bonus", "0"),
            ("daily_bonus", "0"),
            ("withdrawal_enabled", "1"),
            ("redeem_code_price", "10"),
            ("btn_balance", "1"),
            ("btn_refer", "1"),
            ("btn_bonus", "1"),
            ("btn_withdraw", "1"),
            ("btn_link_upi", "1"),
            ("btn_link_wallet", "1"), # generic wallet button
            ("btn_redeem", "1"),
            ("ultra_pay_enabled", "0"), # Ultra Pay defaults
            ("ultrapay_token", ""),
            ("ultrapay_key", ""),
        ]
        for key, val in defaults:
            await db.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, val))
        await db.commit()
    logger.info("Database initialized")


async def get_setting(key: str, default="0"):
    async with turso_connect() as db:
        row = await (await db.execute("SELECT value FROM bot_settings WHERE key=?", (key,))).fetchone()
    return row[0] if row else default


async def is_admin(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    async with turso_connect() as db:
        row = await (await db.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,))).fetchone()
    return row is not None


async def set_setting(key: str, value: str):
    async with turso_connect() as db:
        await db.execute("INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()


async def get_active_channels():
    async with turso_connect() as db:
        rows = await (await db.execute("SELECT id, channel_username, channel_link, channel_name, channel_type, chat_id FROM channels WHERE is_active = 1")).fetchall()
    return rows


async def check_all_channels(bot, user_id: int) -> bool:
    channels = await get_active_channels()
    if not channels:
        return True
    for ch in channels:
        ch_type = ch[4] if len(ch) > 4 else 'public'
        if ch_type == 'link_only':
            continue
        if ch_type == 'private':
            chat_id = ch[5] if len(ch) > 5 else None
            if not chat_id:
                continue
            
            is_member = False
            try:
                member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status in ["member", "administrator", "creator"]:
                    is_member = True
            except Exception as e:
                logger.error(f"Private channel check error: {e}")
            
            if not is_member:
                async with turso_connect() as db:
                    row = await (await db.execute("SELECT 1 FROM channel_join_requests WHERE user_id=? AND channel_id=?", (user_id, chat_id))).fetchone()
                    if row:
                        is_member = True  
                        
            if not is_member:
                return False
            continue
        
        try:
            member = await bot.get_chat_member(chat_id=f"@{ch[1]}", user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except Exception as e:
            logger.error(f"Channel check error {ch[1]}: {e}")
            return False
    return True

async def chat_join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    if not req:
        return
    user_id = req.from_user.id
    channel_id = req.chat.id
    
    async with turso_connect() as db:
        await db.execute(
            "INSERT OR REPLACE INTO channel_join_requests (user_id, channel_id, requested_at) VALUES (?, ?, ?)",
            (user_id, channel_id, datetime.utcnow().isoformat())
        )
        await db.commit()
        
    logger.info(f"Join request saved (Pending Approval): user={user_id} channel={channel_id}")


async def send_join_message(update, user_id: int, bot=None):
    channels = await get_active_channels()
    keyboard = []
    row = []
    for ch in channels:
        name = ch[3] or ch[1]
        row.append(InlineKeyboardButton(f"{name}", url=ch[2]))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔒CLAIM", callback_data="check_join",style="bg_success")])
    text = (
        "👑 Hey There! Welcome To Bot!!\n\n"
        "⚪️ Join The Channels Below To Continue\n\n"
        "😍 After Joining Click Claim"
    )
    try:
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        elif hasattr(update, 'edit_message_text'):
            await update.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.warning(f"send_join_message failed (user may not have started bot): {e}")


async def get_user_keyboard_async(user_id: int):
    b_bal = await get_setting("btn_balance", "1")
    b_ref = await get_setting("btn_refer", "1")
    b_bon = await get_setting("btn_bonus", "1")
    b_wit = await get_setting("btn_withdraw", "1")
    b_upi = await get_setting("btn_link_upi", "1")
    b_wal = await get_setting("btn_link_wallet", "1")
    b_red = await get_setting("btn_redeem", "1")

    buttons = []
    if b_bal == "1": buttons.append(KeyboardButton("Balance"))
    if b_ref == "1": buttons.append(KeyboardButton("Refer & Earn"))
    if b_bon == "1": buttons.append(KeyboardButton("Bonus"))
    if b_wit == "1": buttons.append(KeyboardButton("Withdraw"))
    if b_upi == "1": buttons.append(KeyboardButton("Link UPI"))
    if b_wal == "1": buttons.append(KeyboardButton("Link Wallet"))
    if b_red == "1": buttons.append(KeyboardButton("Redeem Code"))

    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]

    if await is_admin(user_id):
        rows.append([KeyboardButton("Admin Panel")])
        
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def get_admin_keyboard():
    rows = [
        [KeyboardButton("Total Users"), KeyboardButton("Withdrawal Requests")],
        [KeyboardButton("Add Channel"), KeyboardButton("Add Private Channel"), KeyboardButton("Add Link Only")],
        [KeyboardButton("Remove Channel")],
        [KeyboardButton("Update Channel"), KeyboardButton("Broadcast Message")],
        [KeyboardButton("Set Refer Reward"), KeyboardButton("Set Min Withdrawal")],
        [KeyboardButton("Set Daily Bonus")],
        [KeyboardButton("Withdraw ON/OFF"), KeyboardButton("UPI Withdraw ON/OFF")],
        [KeyboardButton("VSV Withdraw ON/OFF"), KeyboardButton("Ultra Pay ON/OFF")],
        [KeyboardButton("Link Ultra Pay API"), KeyboardButton("Manual Balance")],
        [KeyboardButton("Verification ON"), KeyboardButton("Verification OFF")],
        [KeyboardButton("Refer Earn ON"), KeyboardButton("Refer Earn OFF")],
        [KeyboardButton("Approve Withdrawal"), KeyboardButton("Reject Withdrawal")],
        [KeyboardButton("RDM Requests"), KeyboardButton("Approve RDM"), KeyboardButton("Reject RDM")],
        [KeyboardButton("Create Gift Code"), KeyboardButton("Toggle Buttons")],
        [KeyboardButton("Reset Database"), KeyboardButton("Add Admin")],
        [KeyboardButton("Remove Admin"), KeyboardButton("Admin List")],
        [KeyboardButton("Back To Menu")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


async def send_main_menu(update: Update, name: str, user_id: int):
    safe_name = str(name).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
    await update.message.reply_text(
        f"😍 Welcome, *{safe_name}*!\n\n💸 Earn Money • Refer Friends • Withdraw Instantly\n\n👇 Use button below to get started",
        reply_markup=await get_user_keyboard_async(user_id),
        parse_mode="Markdown"
    )

# --- Naya Function: Jo verification OFF hone par bhi referrer ko reward dega ---
async def process_referral_and_verify(user_id: int, bot):
    async with turso_connect() as db:
        await db.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user_id,))
        await db.commit()
        
    async with turso_connect() as db2:
        referrer_row = await (await db2.execute(
            "SELECT value FROM bot_settings WHERE key=?",
            (f"pending_referrer_{user_id}",)
        )).fetchone()

    if referrer_row:
        referrer_id_val = int(referrer_row[0])
        refer_reward = float(await get_setting("refer_reward", "5"))
        async with turso_connect() as db3:
            await db3.execute(
                "INSERT OR IGNORE INTO user_balance (user_id, balance) VALUES (?, 0.0)",
                (referrer_id_val,)
            )
            await db3.execute(
                "UPDATE user_balance SET balance = balance + ?, referral_count = referral_count + 1 WHERE user_id=?",
                (refer_reward, referrer_id_val)
            )
            await db3.execute("DELETE FROM bot_settings WHERE key=?", (f"pending_referrer_{user_id}",))
            await db3.commit()

        try:
            reward_str = f"{refer_reward:.2f}".replace(".", "\\.")
            await bot.send_message(
                chat_id=referrer_id_val,
                text=(
                    f"🎉 [User {user_id}](tg://user?id={user_id}) got invited by your URL\n"
                    f"🎁 Rs\\.{reward_str} added to your balance"
                ),
                parse_mode="MarkdownV2"
            )
        except Exception as e:
            logger.error(f"Referrer notify error: {e}")

# ===================== COMMAND HANDLERS =====================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        return

    user = update.effective_user
    context.user_data.clear()

    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0])
            if referrer_id == user.id:
                referrer_id = None
        except:
            pass

    async with turso_connect() as db:
        existing_row = await (await db.execute("SELECT user_id, is_verified FROM users WHERE user_id=?", (user.id,))).fetchone()
        is_new_user = existing_row is None
        is_verified = int(existing_row[1]) if existing_row and existing_row[1] is not None else 0

        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user.id, user.username, user.first_name)
        )
        await db.execute(
            "INSERT OR IGNORE INTO user_balance (user_id, balance) VALUES (?, 0.0)",
            (user.id,)
        )
        if is_new_user and referrer_id:
            await db.execute("DELETE FROM bot_settings WHERE key=?", (f"pending_referrer_{user.id}",))
            await db.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?)", (f"pending_referrer_{user.id}", str(referrer_id)))
        if await is_admin(user.id):
            await db.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user.id,))
            is_verified = 1
        await db.commit()

    if await is_admin(user.id):
        async with turso_connect() as dbadmin:
            await dbadmin.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user.id,))
            await dbadmin.commit()
        await send_main_menu(update, user.first_name, user.id)
        return

    is_member = await check_all_channels(context.bot, user.id)
    if not is_member:
        await send_join_message(update, user.id)
        return

    verify_on = await get_setting("verification_enabled", "1")

    if is_verified == 1 or verify_on == "0":
        # Naya Logic: Agar verify off hai, toh bypass refer claim karo
        if is_verified == 0 and verify_on == "0":
            await process_referral_and_verify(user.id, context.bot)
            
        await send_main_menu(update, user.first_name, user.id)
    else:
        keyboard = [[InlineKeyboardButton("🟢 Verify Yourself", web_app=WebAppInfo(url=WEBAPP_URL))]]
        await update.message.reply_text(
            "🔐 *Verify Yourself To Start Bot*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    channels = await get_active_channels()
    not_joined = []
    for ch in channels:
        ch_type = ch[4] if len(ch) > 4 else 'public'
        if ch_type == 'link_only':
            continue
        if ch_type == 'private':
            chat_id = ch[5] if len(ch) > 5 else None
            if not chat_id:
                continue
            
            is_member = False
            try:
                member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user.id)
                if member.status in ["member", "administrator", "creator"]:
                    is_member = True
            except Exception as e:
                logger.error(f"Private channel check error: {e}")
            
            if not is_member:
                async with turso_connect() as db:
                    row = await (await db.execute("SELECT 1 FROM channel_join_requests WHERE user_id=? AND channel_id=?", (user.id, chat_id))).fetchone()
                    if row:
                        is_member = True
                        
            if not is_member:
                not_joined.append(ch[3] or ch[1])
            continue
        
        try:
            member = await context.bot.get_chat_member(chat_id=f"@{ch[1]}", user_id=user.id)
            if member.status not in ["member", "administrator", "creator"]:
                not_joined.append(ch[3] or ch[1])
        except:
            not_joined.append(ch[3] or ch[1])

    if not_joined:
        await query.answer()
        for old_msg_id in context.user_data.get("not_joined_msg_ids", []):
            try:
                await context.bot.delete_message(chat_id=user.id, message_id=old_msg_id)
            except:
                pass
        sent = await query.message.reply_text(
            "🙆 YOU DIDN'T JOIN ALL CHANNELS"
        )
        context.user_data["not_joined_msg_ids"] = [sent.message_id]
        return
    is_member = True

    async with turso_connect() as db:
        row = await (await db.execute("SELECT is_verified FROM users WHERE user_id = ?", (user.id,))).fetchone()
        is_verified = int(row[0]) if row and row[0] is not None else 0

    verify_on = await get_setting("verification_enabled", "1")

    for old_msg_id in context.user_data.pop("not_joined_msg_ids", []):
        try:
            await context.bot.delete_message(chat_id=user.id, message_id=old_msg_id)
        except:
            pass

    if is_verified == 1 or verify_on == "0":
        # Naya Logic: Claim button dabane ke baad bypass verify aur referral reward
        if is_verified == 0 and verify_on == "0":
            await process_referral_and_verify(user.id, context.bot)
            
        try:
            await query.message.delete()
        except:
            pass
        safe_name = str(user.first_name).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
        await query.message.reply_text(
            f"😍 Welcome, *{safe_name}*!\n\n💸 Earn Money • Refer Friends • Withdraw Instantly\n\n👇 Use button below to get started",
            reply_markup=await get_user_keyboard_async(user.id),
            parse_mode="Markdown"
        )
    else:
        keyboard = [[InlineKeyboardButton("🟢 Verify Yourself", web_app=WebAppInfo(url=WEBAPP_URL))]]
        await query.edit_message_text(
            "🔐 *Verify Yourself To Start Bot*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.message.web_app_data.data
    user = update.effective_user
    context.user_data.clear()
    try:
        payload = json.loads(data)
        if payload.get("status") == "verified":
            await update.message.reply_text(
                f"✅ *VERIFIED SUCCESSFULLY!*",
                parse_mode="Markdown",
                reply_markup=await get_user_keyboard_async(user.id)
            )
            await send_main_menu(update, user.first_name, user.id)
        elif payload.get("status") == "blocked":
            await update.message.reply_text(
                "❌ *VERIFICATION FAILED*\n\nThis device is already linked to another account.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                "❌ Verification failed. Please try /start again.",
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"web_app_data error: {e}")
        await update.message.reply_text("⚠️ Something went wrong. Try /start again.")


# ===================== COMBINED MESSAGE HANDLER =====================

async def combined_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        return

    text = update.message.text
    user_id = update.effective_user.id

    admin_buttons = [
        "Total Users", "Withdrawal Requests", "Add Channel", "Add Private Channel", "Add Link Only", "Remove Channel",
        "Update Channel", "Broadcast Message", "Set Refer Reward", "Set Min Withdrawal",
        "Set Daily Bonus", "Withdraw ON/OFF", "UPI Withdraw ON/OFF", "VSV Withdraw ON/OFF",
        "Ultra Pay ON/OFF", "Link Ultra Pay API", "Manual Balance", "Approve Withdrawal", 
        "Reject Withdrawal", "Create Gift Code", "Reset Database", "Confirm Reset Database", "Back To Menu",
        "RDM Requests", "Approve RDM", "Reject RDM",
        "Verification ON", "Verification OFF", "Refer Earn ON", "Refer Earn OFF",
        "Add Admin", "Remove Admin", "Admin List", "Toggle Buttons"
    ]

    if await is_admin(user_id):
        if context.user_data.get('admin_action'):
            await handle_admin_action_input(update, context, text)
            return
        if text == "Admin Panel":
            await handle_admin_panel_menu(update, context)
            return
        if text in admin_buttons or context.user_data.get('in_admin'):
            context.user_data['in_admin'] = True
            await handle_admin_text(update, context, text)
            return

    await button_handler(update, context)


# ===================== BUTTON HANDLER =====================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        return

    text = update.message.text
    user_id = update.effective_user.id

    if await is_admin(user_id) and context.user_data.get('admin_action'):
        await handle_admin_action_input(update, context, text)
        return

    is_member = await check_all_channels(context.bot, user_id)
    if not is_member:
        await send_join_message(update, user_id)
        return

    if not await is_admin(user_id):
        verify_on = await get_setting("verification_enabled", "1")
        if verify_on == "1":
            async with turso_connect() as db:
                row = await (await db.execute("SELECT is_verified FROM users WHERE user_id = ?", (user_id,))).fetchone()
                is_verified = int(row[0]) if row and row[0] is not None else 0

            if is_verified != 1:
                keyboard = [[InlineKeyboardButton("🟢 Verify Yourself", web_app=WebAppInfo(url=WEBAPP_URL))]]
                await update.message.reply_text(
                    "🔐 *Verify Yourself To Start Bot*",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="Markdown"
                )
                return

    if text == "Balance":
        await handle_balance(update, user_id)
    elif text == "Refer & Earn":
        await handle_refer_earn(update, user_id, context)
    elif text == "Bonus":
        await handle_bonus(update, user_id)
    elif text == "Withdraw":
        await handle_withdraw(update, user_id, context)
    elif text == "Link UPI":
        context.user_data['waiting_for'] = 'upi'
        await update.message.reply_text(
            "🟢 *LINK UPI ID* — Send your UPI ID _(eg. name@upi)_",
            parse_mode="Markdown"
        )
    elif text == "Link Wallet":
        # Dynamic response based on enabled gateways
        ultra_on = await get_setting("ultra_pay_enabled", "0")
        vsv_on = await get_setting("vsv_withdrawal_enabled", "1")
        
        if ultra_on == "1" and vsv_on == "1":
            keyboard = [
                [InlineKeyboardButton("🔗 Link Ultra Pay", callback_data="link_ultra")],
                [InlineKeyboardButton("🔗 Link VSV Wallet", callback_data="link_vsv")]
            ]
            await update.message.reply_text("✨ Select the wallet you want to link:", reply_markup=InlineKeyboardMarkup(keyboard))
        elif ultra_on == "1":
            context.user_data['waiting_for'] = 'ultra_pay'
            await update.message.reply_text("💸 *LINK ULTRA PAY WALLET*\n\nSend your Ultra Pay Wallet number:", parse_mode="Markdown")
        elif vsv_on == "1":
            context.user_data['waiting_for'] = 'vsv'
            await update.message.reply_text("💳 *LINK VSV WALLET*\n\nSend your VSV Wallet number (exactly 10 digits):", parse_mode="Markdown")
        else:
            await update.message.reply_text("❌ All wallet links are currently disabled by admin.")
            
    elif text == "Redeem Code":
        await handle_redeem_code_menu(update, user_id, context)
    else:
        waiting = context.user_data.get('waiting_for')
        if waiting == 'upi':
            await handle_upi_link(update, user_id, text)
            context.user_data['waiting_for'] = None
        elif waiting == 'vsv':
            await handle_vsv_link(update, user_id, text)
            context.user_data['waiting_for'] = None
        elif waiting == 'ultra_pay':
            number = text.strip()
            async with turso_connect() as db:
                await db.execute("UPDATE user_balance SET ultra_wallet = ? WHERE user_id = ?", (number, user_id))
                await db.commit()
            await update.message.reply_text(f"✅ *ULTRA PAY WALLET LINKED!*\n\nNumber: `{number}`", parse_mode="Markdown")
            context.user_data['waiting_for'] = None
        elif waiting == 'withdraw_amount':
            await handle_withdraw_amount(update, user_id, context, text)
        elif waiting == 'redeem_buy_amount':
            await handle_redeem_buy(update, user_id, context, text)
        elif waiting == 'redeem_email':
            context.user_data['redeem_email'] = text
            context.user_data['waiting_for'] = 'redeem_mobile'
            await update.message.reply_text(
                "📱 *MOBILE NUMBER*\n\nNow send your mobile number:",
                parse_mode="Markdown"
            )
        elif waiting == 'redeem_mobile':
            await handle_redeem_finalize(update, user_id, context, text)
        elif waiting == 'redeem_use':
            await handle_redeem_use(update, user_id, text)
            context.user_data['waiting_for'] = None
        else:
            await update.message.reply_text(
                "🌟 Please use reply keyboard button",
                reply_markup=await get_user_keyboard_async(user_id)
            )


# ===================== ADMIN PANEL IN BOT =====================

async def handle_admin_panel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data['in_admin'] = True
    await update.message.reply_text(
        "⚙️ *ADMIN PANEL*\n\nWelcome, Admin! Choose an action:",
        reply_markup=get_admin_keyboard(),
        parse_mode="Markdown"
    )


async def handle_admin_action_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return

    action = context.user_data.get('admin_action')

    if text == "Back To Menu":
        context.user_data.clear()
        await send_main_menu(update, update.effective_user.first_name, user_id)
        return
    if text == "Admin Panel":
        context.user_data.clear()
        await handle_admin_panel_menu(update, context)
        return

    if action == 'add_channel':
        parts = text.split("|")
        if len(parts) < 2:
            await update.message.reply_text("⚠️ Wrong format. Send:\nChannelName|@username|https://t.me/link")
            return
        name = parts[0].strip()
        username = parts[1].strip().replace("@", "")
        link = parts[2].strip() if len(parts) > 2 else f"https://t.me/{username}"
        async with turso_connect() as db:
            await db.execute("INSERT INTO channels (channel_username, channel_link, channel_name, channel_type) VALUES (?,?,?,?)", (username, link, name, 'public'))
            await db.commit()
        await update.message.reply_text(f"✅ Public Channel Added: {name}", reply_markup=get_admin_keyboard())
        context.user_data['admin_action'] = None

    elif action == 'add_private_channel':
        parts = text.split("|")
        if len(parts) < 3:
            await update.message.reply_text("⚠️ Wrong format. Send:\nChannelName|-1001234567890|https://t.me/+invitelink\n\nChatID @userinfobot se milega.")
            return
        name = parts[0].strip()
        chat_id_str = parts[1].strip()
        link = parts[2].strip()
        try:
            chat_id = int(chat_id_str)
        except:
            await update.message.reply_text("⚠️ ChatID galat hai. Example: -1001234567890")
            return
        username = f"private_{abs(chat_id)}"
        async with turso_connect() as db:
            await db.execute(
                "INSERT INTO channels (channel_username, channel_link, channel_name, channel_type, chat_id) VALUES (?,?,?,?,?)",
                (username, link, name, 'private', chat_id)
            )
            await db.commit()
        await update.message.reply_text(
            f"✅ Private Channel Added: {name}\n\n"
            f"📌 ChatID: `{chat_id}`\n"
            f"Bot member check karega — user channel leave kare to aage nahi badh sakta.",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        context.user_data['admin_action'] = None

    elif action == 'add_link_only':
        parts = text.split("|")
        if len(parts) < 3:
            await update.message.reply_text("⚠️ Wrong format. Send:\nName|dummy|https://koi-bhi-link.com")
            return
        name = parts[0].strip()
        username = parts[1].strip().replace("@", "")
        link = parts[2].strip()
        async with turso_connect() as db:
            await db.execute("INSERT INTO channels (channel_username, channel_link, channel_name, channel_type) VALUES (?,?,?,?)", (username, link, name, 'link_only'))
            await db.commit()
        await update.message.reply_text(
            f"✅ Link Only Added: {name}\n\n"
            f"📌 Koi verify nahi hoga — sirf link show hoga user ko.",
            reply_markup=get_admin_keyboard()
        )
        context.user_data['admin_action'] = None

    elif action == 'remove_channel':
        try:
            ch_id = int(text)
            async with turso_connect() as db:
                await db.execute("UPDATE channels SET is_active=0 WHERE id=?", (ch_id,))
                await db.commit()
            await update.message.reply_text(f"✅ Channel ID {ch_id} Removed.", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send a valid channel ID number.")
        context.user_data['admin_action'] = None

    elif action == 'update_channel':
        parts = text.split("|")
        if len(parts) < 3:
            await update.message.reply_text("⚠️ Format: ID|@newusername|https://newlink")
            return
        try:
            ch_id = int(parts[0].strip())
            username = parts[1].strip().replace("@", "")
            link = parts[2].strip()
            async with turso_connect() as db:
                await db.execute("UPDATE channels SET channel_username=?, channel_link=? WHERE id=?", (username, link, ch_id))
                await db.commit()
            await update.message.reply_text(f"✅ Channel ID {ch_id} Updated.", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Invalid format.")
        context.user_data['admin_action'] = None

    elif action == 'set_refer_reward':
        try:
            val = float(text)
            await set_setting("refer_reward", str(val))
            await update.message.reply_text(f"✅ Refer Reward Set To Rs.{val}", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send a valid number.")
        context.user_data['admin_action'] = None

    elif action == 'set_min_withdrawal':
        try:
            val = float(text)
            await set_setting("min_withdrawal", str(val))
            await update.message.reply_text(f"✅ Minimum Withdrawal Set To Rs.{val}", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send a valid number.")
        context.user_data['admin_action'] = None

    elif action == 'set_welcome_bonus':
        try:
            val = float(text)
            await set_setting("welcome_bonus", str(val))
            await update.message.reply_text(f"✅ Welcome Bonus Set To Rs.{val}", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send a valid number.")
        context.user_data['admin_action'] = None

    elif action == 'set_daily_bonus':
        try:
            val = float(text)
            await set_setting("daily_bonus", str(val))
            await update.message.reply_text(f"✅ Daily Bonus Set To Rs.{val}", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send a valid number.")
        context.user_data['admin_action'] = None
        
    elif action == 'set_ultrapay_token':
        await set_setting("ultrapay_token", text.strip())
        context.user_data['admin_action'] = 'set_ultrapay_key'
        await update.message.reply_text(
            "✅ Token Saved!\n\nNow, please send the Ultra Pay API *Key*:\n_(e.g., dV71Th1npJgxAvj9s)_",
            parse_mode="Markdown"
        )
        
    elif action == 'set_ultrapay_key':
        await set_setting("ultrapay_key", text.strip())
        await set_setting("ultra_pay_enabled", "1")
        context.user_data['admin_action'] = None
        await update.message.reply_text(
            "🎉 *ULTRA PAY API SUCCESSFULLY LINKED & ENABLED!*\n\nUsers can now use Ultra Pay.",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )

    elif action == 'broadcast':
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT user_id FROM users")).fetchall()
        bot = update.get_bot()
        sent = 0
        BATCH = 25
        user_ids = [r[0] for r in rows]
        for i in range(0, len(user_ids), BATCH):
            batch = user_ids[i:i+BATCH]
            results = await asyncio.gather(
                *[bot.send_message(chat_id=uid, text=text) for uid in batch],
                return_exceptions=True
            )
            sent += sum(1 for r in results if not isinstance(r, Exception))
            await asyncio.sleep(0.5) 
        await update.message.reply_text(f"✅ Broadcast Sent To {sent} Users.", reply_markup=get_admin_keyboard())
        context.user_data['admin_action'] = None

    elif action == 'manual_balance':
        parts = text.split("|")
        if len(parts) < 2:
            await update.message.reply_text("⚠️ Format: UserID|Amount\n(Use negative to deduct)")
            return
        try:
            uid = int(parts[0].strip())
            amount = float(parts[1].strip())
            async with turso_connect() as db:
                await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, uid))
                await db.commit()
            action_word = "Added" if amount >= 0 else "Deducted"
            await update.message.reply_text(f"✅ {action_word} Rs.{abs(amount)} For User {uid}", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Invalid format.")
        context.user_data['admin_action'] = None

    elif action == 'approve_withdrawal':
        try:
            req_id = int(text)
            async with turso_connect() as db:
                req = await (await db.execute("SELECT user_id, amount, vsv_wallet, upi_id, method FROM withdrawal_requests WHERE id=? AND status='pending'", (req_id,))).fetchone()
                if not req:
                    await update.message.reply_text("⚠️ Request Not Found Or Already Processed.")
                    context.user_data['admin_action'] = None
                    return
                uid, amount, vsv_wallet, upi_id, method = req
                await db.execute("UPDATE withdrawal_requests SET status='approved', processed_at=? WHERE id=?", (datetime.utcnow().isoformat(), req_id))
                await db.commit()

            if method == 'vsv' and vsv_wallet:
                pay_url = f"{VSV_API_URL}?token={VSV_TOKEN}&paytm={vsv_wallet}&amount={amount}&comment=Withdrawal+from+bot"
                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(pay_url, timeout=15)
                    await update.message.reply_text(f"💳 Payment API Response: {resp.text[:300]}")
                except Exception as e:
                    await update.message.reply_text(f"⚠️ Payment API Error: {e}")
            else:
                await update.message.reply_text(f"✅ Approved! Pay Manually.\n\nUPI: `{upi_id}`\n\nAmount: `Rs.{amount}`", parse_mode="Markdown")

            try:
                await update.get_bot().send_message(
                    chat_id=uid,
                    text=f"✅ *WITHDRAWAL APPROVED!*\n\nYour withdrawal of Rs.{amount} has been approved!\n\nThank you! 🎉",
                    parse_mode="Markdown"
                )
            except:
                pass
        except:
            await update.message.reply_text("⚠️ Send A Valid Request ID.")
        context.user_data['admin_action'] = None
        await update.message.reply_text("Action Complete.", reply_markup=get_admin_keyboard())

    elif action == 'approve_rdm':
        try:
            rdm_id = int(text)
            async with turso_connect() as db:
                req = await (await db.execute(
                    "SELECT user_id, amount, email, mobile, code FROM redeem_codes WHERE id=? AND status='pending'",
                    (rdm_id,)
                )).fetchone()
                if not req:
                    await update.message.reply_text("⚠️ RDM Request Not Found Or Already Processed.")
                    context.user_data['admin_action'] = None
                    return
                uid, amount, email, mobile, code = req
                await db.execute(
                    "UPDATE redeem_codes SET status='approved', code=? WHERE id=?",
                    (code, rdm_id)
                )
                await db.commit()

            await update.message.reply_text(
                f"✅ RDM Request Approved!\n\n"
                f"User ID: `{uid}`\n"
                f"Amount: `Rs.{amount:.2f}`\n"
                f"Email: `{email}`\n"
                f"Mobile: `{mobile}`\n\n"
                f"Please send the redeem code to the user's email manually.",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )

            try:
                await update.get_bot().send_message(
                    chat_id=uid,
                    text=(
                        f"🎟️ *REDEEM CODE REQUEST APPROVED!*\n\n"
                        f"💰 Amount: Rs.{amount:.2f}\n"
                        f"📧 Email: {email}\n\n"
                        f"✅ Your redeem code will be sent to your email shortly!"
                    ),
                    parse_mode="Markdown"
                )
            except:
                pass
        except:
            await update.message.reply_text("⚠️ Send A Valid RDM ID.")
        context.user_data['admin_action'] = None

    elif action == 'reject_rdm':
        try:
            rdm_id = int(text)
            async with turso_connect() as db:
                req = await (await db.execute(
                    "SELECT user_id, amount, email, mobile FROM redeem_codes WHERE id=? AND status='pending'",
                    (rdm_id,)
                )).fetchone()
                if not req:
                    await update.message.reply_text("⚠️ RDM Request Not Found Or Already Processed.")
                    context.user_data['admin_action'] = None
                    return
                uid, amount, email, mobile = req
                await db.execute(
                    "UPDATE redeem_codes SET status='rejected' WHERE id=?",
                    (rdm_id,)
                )
                await db.execute(
                    "UPDATE user_balance SET balance = balance + ? WHERE user_id=?",
                    (amount, uid)
                )
                await db.commit()

            await update.message.reply_text(
                f"❌ RDM Request Rejected!\n\n"
                f"User ID: `{uid}`\n"
                f"Amount: `Rs.{amount:.2f}`\n"
                f"Email: `{email}`\n"
                f"Mobile: `{mobile}`\n\n"
                f"✅ Rs.{amount:.2f} refunded to user's balance.",
                parse_mode="Markdown",
                reply_markup=get_admin_keyboard()
            )

            try:
                await update.get_bot().send_message(
                    chat_id=uid,
                    text=(
                        f"❌ *REDEEM CODE REQUEST REJECTED!*\n\n"
                        f"💰 Amount: Rs.{amount:.2f}\n"
                        f"📧 Email: {email}\n\n"
                        f"✅ Rs.{amount:.2f} has been refunded to your wallet balance."
                    ),
                    parse_mode="Markdown"
                )
            except:
                pass
        except:
            await update.message.reply_text("⚠️ Send A Valid RDM ID.")
        context.user_data['admin_action'] = None

    elif action == 'add_admin':
        try:
            new_admin_id = int(text.strip())
            async with turso_connect() as db:
                await db.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (new_admin_id,))
                await db.commit()
            await update.message.reply_text(f"✅ User `{new_admin_id}` ab Admin hai!", parse_mode="Markdown", reply_markup=get_admin_keyboard())
        except ValueError:
            await update.message.reply_text("⚠️ Sirf User ID number bhejo.")
        context.user_data['admin_action'] = None

    elif action == 'remove_admin':
        try:
            rem_id = int(text.strip())
            if rem_id == ADMIN_ID:
                await update.message.reply_text("⚠️ Main Admin ko remove nahi kar sakte!", reply_markup=get_admin_keyboard())
            else:
                async with turso_connect() as db:
                    await db.execute("DELETE FROM admins WHERE user_id=?", (rem_id,))
                    await db.commit()
                await update.message.reply_text(f"✅ User `{rem_id}` ko Admin se hata diya.", parse_mode="Markdown", reply_markup=get_admin_keyboard())
        except ValueError:
            await update.message.reply_text("⚠️ Sirf User ID number bhejo.")
        context.user_data['admin_action'] = None

    elif action == 'create_gift_code':
        parts = text.split("|")
        try:
            amount = float(parts[0].strip())
            if len(parts) > 1:
                code = parts[1].strip().upper()
            else:
                code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            async with turso_connect() as db:
                await db.execute(
                    "INSERT INTO redeem_codes (code, amount, user_id, status) VALUES (?,?,?,?)",
                    (code, amount, ADMIN_ID, 'active')
                )
                await db.commit()
            await update.message.reply_text(
                f"✅ Gift Code Created!\n\nCode: `{code}`\nAmount: ₹{amount:.2f}\n\nShare this code with users.",
                reply_markup=get_admin_keyboard(),
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"⚠️ Error: {e}\nFormat: Amount|Code or just Amount")
        context.user_data['admin_action'] = None

    elif action == 'reject_withdrawal':
        try:
            req_id = int(text)
            async with turso_connect() as db:
                req = await (await db.execute("SELECT user_id, amount FROM withdrawal_requests WHERE id=? AND status='pending'", (req_id,))).fetchone()
                if not req:
                    await update.message.reply_text("⚠️ Request Not Found.")
                    context.user_data['admin_action'] = None
                    return
                uid, amount = req
                await db.execute("UPDATE withdrawal_requests SET status='rejected', processed_at=? WHERE id=?", (datetime.utcnow().isoformat(), req_id))
                await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, uid))
                await db.commit()
            try:
                await update.get_bot().send_message(
                    chat_id=uid,
                    text=f"❌ *WITHDRAWAL REJECTED*\n\nYour withdrawal request of Rs.{amount} has been rejected.\n💰 Amount refunded to your balance.",
                    parse_mode="Markdown"
                )
            except:
                pass
            await update.message.reply_text(f"✅ Request {req_id} Rejected And Amount Refunded.", reply_markup=get_admin_keyboard())
        except:
            await update.message.reply_text("⚠️ Send A Valid Request ID.")
        context.user_data['admin_action'] = None


async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return

    if text == "Toggle Buttons":
        keyboard = [
            [InlineKeyboardButton("Balance: " + ("✅ SHOW" if await get_setting("btn_balance", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_balance")],
            [InlineKeyboardButton("Refer & Earn: " + ("✅ SHOW" if await get_setting("btn_refer", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_refer")],
            [InlineKeyboardButton("Bonus: " + ("✅ SHOW" if await get_setting("btn_bonus", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_bonus")],
            [InlineKeyboardButton("Withdraw: " + ("✅ SHOW" if await get_setting("btn_withdraw", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_withdraw")],
            [InlineKeyboardButton("Link UPI: " + ("✅ SHOW" if await get_setting("btn_link_upi", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_link_upi")],
            [InlineKeyboardButton("Link Wallet: " + ("✅ SHOW" if await get_setting("btn_link_wallet", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_link_wallet")],
            [InlineKeyboardButton("Redeem Code: " + ("✅ SHOW" if await get_setting("btn_redeem", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_redeem")],
        ]
        await update.message.reply_text(
            "👁️ *TOGGLE USER BUTTONS*\n\nClick on any button below to hide or show it on the user's keyboard.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    elif text == "Ultra Pay ON/OFF":
        current = await get_setting("ultra_pay_enabled", "0")
        new_val = "0" if current == "1" else "1"
        await set_setting("ultra_pay_enabled", new_val)
        status = "ENABLED ✅" if new_val == "1" else "DISABLED ❌"
        await update.message.reply_text(
            f"Ultra Pay Withdrawal Is Now: {status}",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Link Ultra Pay API":
        context.user_data['admin_action'] = 'set_ultrapay_token'
        await update.message.reply_text(
            "🔗 *LINK ULTRA PAY API*\n\nPlease send the Ultra Pay API *Token*:\n_(e.g., FsYU0Gx9AeMXP...)_",
            parse_mode="Markdown"
        )

    elif text == "Total Users":
        async with turso_connect() as db:
            total = int((await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0])
            verified = int((await (await db.execute("SELECT COUNT(*) FROM users WHERE is_verified=1")).fetchone())[0])
        await update.message.reply_text(
            f"👥 *USER STATISTICS*\n\n"
            f"📊 Total Users: {total}\n"
            f"✅ Verified Users: {verified}\n"
            f"⏳ Unverified Users: {total - verified}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Withdrawal Requests":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, method, upi_id, vsv_wallet, created_at FROM withdrawal_requests WHERE status='pending' ORDER BY created_at DESC LIMIT 20"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending Withdrawal Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "📋 PENDING WITHDRAWAL REQUESTS\n\n"
        for r in rows:
            msg += f"`ID: {r[0]} | User: {r[1]} | Rs.{r[2]} | {r[3].upper()}`\n"
            if r[3] == 'upi':
                msg += f"`UPI: {r[4]}`\n"
            else:
                msg += f"`VSV/ULTRA: {r[5]}`\n"
            msg += f"`Date: {r[6][:10]}`\n\n"
        await update.message.reply_text(msg, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif text == "Add Channel":
        context.user_data['admin_action'] = 'add_channel'
        await update.message.reply_text(
            "➕ *ADD PUBLIC CHANNEL*\n\nSend channel details in this format:\n`ChannelName|@username|https://t.me/link`\n\n"
            "📌 Public channel me bot member check karega.",
            parse_mode="Markdown"
        )

    elif text == "Add Private Channel":
        context.user_data['admin_action'] = 'add_private_channel'
        await update.message.reply_text(
            "🔒 *ADD PRIVATE CHANNEL*\n\nSend channel details in this format:\n`ChannelName|-1001234567890|https://t.me/+invitelink`\n\n"
            "📌 ChatID @userinfobot se pata karo:\n"
            "1. Channel me koi message bhejo\n"
            "2. Wo message @userinfobot ko forward karo\n"
            "3. Forwarded chat ID copy karo",
            parse_mode="Markdown"
        )

    elif text == "Add Link Only":
        context.user_data['admin_action'] = 'add_link_only'
        await update.message.reply_text(
            "🔗 *ADD LINK ONLY*\n\nSend details in this format:\n`Name|dummy|https://koi-bhi-link.com`\n\n"
            "📌 Is type me bot koi bhi verify nahi karega — sirf link show hoga user ko.",
            parse_mode="Markdown"
        )

    elif text == "Remove Channel":
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT id, channel_name, channel_username FROM channels WHERE is_active=1")).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Active Channels.", reply_markup=get_admin_keyboard())
            return
        msg = "📋 ACTIVE CHANNELS\n\n"
        for r in rows:
            msg += f"ID: {r[0]} | {r[1] or r[2]} | @{r[2]}\n"
        msg += "\nSend the channel ID to remove:"
        context.user_data['admin_action'] = 'remove_channel'
        await update.message.reply_text(msg)

    elif text == "Update Channel":
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT id, channel_name, channel_username FROM channels WHERE is_active=1")).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Active Channels.", reply_markup=get_admin_keyboard())
            return
        msg = "📋 ACTIVE CHANNELS\n\n"
        for r in rows:
            msg += f"ID: {r[0]} | {r[1] or r[2]} | @{r[2]}\n"
        msg += "\nSend in format: ID|@newusername|https://newlink"
        context.user_data['admin_action'] = 'update_channel'
        await update.message.reply_text(msg)

    elif text == "Set Refer Reward":
        current = await get_setting("refer_reward", "5")
        context.user_data['admin_action'] = 'set_refer_reward'
        await update.message.reply_text(
            f"💵 *SET REFER REWARD*\n\nCurrent: Rs.{current}\n\nSend new amount:",
            parse_mode="Markdown"
        )

    elif text == "Set Min Withdrawal":
        current = await get_setting("min_withdrawal", "50")
        context.user_data['admin_action'] = 'set_min_withdrawal'
        await update.message.reply_text(
            f"🔻 *SET MINIMUM WITHDRAWAL*\n\nCurrent: Rs.{current}\n\nSend new amount:",
            parse_mode="Markdown"
        )

    elif text == "Set Daily Bonus":
        current = await get_setting("daily_bonus", "1")
        context.user_data['admin_action'] = 'set_daily_bonus'
        await update.message.reply_text(
            f"🎁 SET DAILY BONUS\n\nCurrent: Rs.{current}\n\nSend new amount:",
        )

    elif text == "Withdraw ON/OFF":
        current = await get_setting("withdrawal_enabled", "1")
        new_val = "0" if current == "1" else "1"
        await set_setting("withdrawal_enabled", new_val)
        status = "✅ ENABLED" if new_val == "1" else "❌ DISABLED"
        await update.message.reply_text(
            f"🔛 *WITHDRAWAL STATUS UPDATED*\n\nWithdrawal Is Now: {status}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Add Admin":
        context.user_data['admin_action'] = 'add_admin'
        await update.message.reply_text(
            "👤 *ADD ADMIN*\n\nUser ID bhejo jise Admin banana hai:\n_(User ID @userinfobot se milega)_",
            parse_mode="Markdown"
        )

    elif text == "Remove Admin":
        context.user_data['admin_action'] = 'remove_admin'
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT user_id FROM admins WHERE user_id != ?", (ADMIN_ID,))).fetchall()
        if not rows:
            await update.message.reply_text("ℹ️ Koi extra Admin nahi hai abhi.", reply_markup=get_admin_keyboard())
            context.user_data['admin_action'] = None
        else:
            ids = "\n".join([f"`{r[0]}`" for r in rows])
            await update.message.reply_text(
                f"👤 *CURRENT ADMINS:*\n{ids}\n\nJis ko remove karna ho uska User ID bhejo:",
                parse_mode="Markdown"
            )

    elif text == "Admin List":
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT user_id FROM admins")).fetchall()
        ids = "\n".join([f"`{r[0]}`" for r in rows])
        await update.message.reply_text(
            f"👤 *ALL ADMINS:*\n{ids}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Verification ON":
        await set_setting("verification_enabled", "1")
        await update.message.reply_text("✅ Verification Is Now ENABLED", reply_markup=get_admin_keyboard())

    elif text == "Verification OFF":
        await set_setting("verification_enabled", "0")
        await update.message.reply_text("❌ Verification Is Now DISABLED", reply_markup=get_admin_keyboard())

    elif text == "Refer Earn ON":
        await set_setting("refer_earn_enabled", "1")
        await update.message.reply_text("✅ Refer & Earn Is Now ENABLED", reply_markup=get_admin_keyboard())

    elif text == "Refer Earn OFF":
        await set_setting("refer_earn_enabled", "0")
        await update.message.reply_text("❌ Refer & Earn Is Now DISABLED", reply_markup=get_admin_keyboard())

    elif text == "Broadcast Message":
        context.user_data['admin_action'] = 'broadcast'
        await update.message.reply_text(
            "📣 *BROADCAST MESSAGE*\n\nSend the message to broadcast to all users:",
            parse_mode="Markdown"
        )

    elif text == "Manual Balance":
        context.user_data['admin_action'] = 'manual_balance'
        await update.message.reply_text(
            "✏️ *MANUAL BALANCE*\n\nSend in format:\n`UserID|Amount`\n\nExample: `123456|50`\nFor deduction: `123456|-20`",
            parse_mode="Markdown"
        )

    elif text == "Approve Withdrawal":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, method, upi_id, vsv_wallet FROM withdrawal_requests WHERE status='pending' LIMIT 10"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "📋 PENDING REQUESTS\n\n"
        for r in rows:
            payment_id = r[4] if r[3] == 'upi' else r[5]
            msg += f"`ID: {r[0]} | User: {r[1]} | Rs.{r[2]} | {r[3].upper()}`\n`UPI: {payment_id}`\n\n"
        msg += "Send request ID to approve:"
        context.user_data['admin_action'] = 'approve_withdrawal'
        await update.message.reply_text(msg, parse_mode="Markdown")

    elif text == "Reject Withdrawal":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, method FROM withdrawal_requests WHERE status='pending' LIMIT 10"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "📋 PENDING REQUESTS\n\n"
        for r in rows:
            msg += f"`ID: {r[0]} | User: {r[1]} | Rs.{r[2]} | {r[3].upper()}`\n"
        msg += "\nSend request ID to reject (amount will be refunded):"
        context.user_data['admin_action'] = 'reject_withdrawal'
        await update.message.reply_text(msg, parse_mode="Markdown")

    elif text == "RDM Requests":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, email, mobile, created_at FROM redeem_codes WHERE status='pending' AND code LIKE 'REQ-%' ORDER BY created_at DESC LIMIT 10"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending RDM Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "🎟️ PENDING RDM REQUESTS\n\n"
        for r in rows:
            msg += (
                f"`RDM ID: {r[0]}`\n"
                f"`User ID: {r[1]}`\n"
                f"`Amount: Rs.{r[2]}`\n"
                f"`Email: {r[3]}`\n"
                f"`Mobile: {r[4]}`\n"
                f"`Date: {str(r[5])[:10]}`\n\n"
            )
        msg += "Use *Approve RDM* button to approve a request."
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=get_admin_keyboard())

    elif text == "Approve RDM":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, email FROM redeem_codes WHERE status='pending' AND code LIKE 'REQ-%' ORDER BY created_at DESC LIMIT 10"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending RDM Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "🎟️ PENDING RDM REQUESTS\n\n"
        for r in rows:
            msg += f"`RDM ID: {r[0]} | User: {r[1]} | Rs.{r[2]}`\n`Email: {r[3]}`\n\n"
        msg += "Send RDM ID to approve:"
        context.user_data['admin_action'] = 'approve_rdm'
        await update.message.reply_text(msg, parse_mode="Markdown")

    elif text == "Reject RDM":
        async with turso_connect() as db:
            rows = await (await db.execute(
                "SELECT id, user_id, amount, email FROM redeem_codes WHERE status='pending' AND code LIKE 'REQ-%' ORDER BY created_at DESC LIMIT 10"
            )).fetchall()
        if not rows:
            await update.message.reply_text("✅ No Pending RDM Requests.", reply_markup=get_admin_keyboard())
            return
        msg = "🎟️ PENDING RDM REQUESTS\n\n"
        for r in rows:
            msg += f"`RDM ID: {r[0]} | User: {r[1]} | Rs.{r[2]}`\n`Email: {r[3]}`\n\n"
        msg += "Send RDM ID to reject (balance will be refunded):"
        context.user_data['admin_action'] = 'reject_rdm'
        await update.message.reply_text(msg, parse_mode="Markdown")

    elif text == "Create Gift Code":
        context.user_data['admin_action'] = 'create_gift_code'
        await update.message.reply_text(
            "🎁 CREATE GIFT CODE\n\nFormat: Amount|Code\nExample: 50|GIFT2024\n\nOr just send amount to auto-generate code:",
            parse_mode=None
        )

    elif text == "UPI Withdraw ON/OFF":
        current = await get_setting("upi_withdrawal_enabled", "1")
        new_val = "0" if current == "1" else "1"
        await set_setting("upi_withdrawal_enabled", new_val)
        status = "ENABLED" if new_val == "1" else "DISABLED"
        await update.message.reply_text(
            f"UPI Withdrawal Is Now: {status}",
            reply_markup=get_admin_keyboard()
        )

    elif text == "VSV Withdraw ON/OFF":
        current = await get_setting("vsv_withdrawal_enabled", "1")
        new_val = "0" if current == "1" else "1"
        await set_setting("vsv_withdrawal_enabled", new_val)
        status = "ENABLED" if new_val == "1" else "DISABLED"
        await update.message.reply_text(
            f"VSV Withdrawal Is Now: {status}",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Reset Database":
        await update.message.reply_text(
            "\u26a0\ufe0f ARE YOU SURE?\n\n"
            "This will DELETE all users, balances, referrals, device registrations.\n\n"
            "Channels and settings will be kept.\n\n"
            "Click Confirm Reset Database to proceed:",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("Confirm Reset Database")], [KeyboardButton("Back To Menu")]],
                resize_keyboard=True
            )
        )

    elif text == "Confirm Reset Database":
        async with turso_connect() as db:
            await db.execute("DELETE FROM users")
            await db.execute("DELETE FROM user_balance")
            await db.execute("DELETE FROM device_registry")
            await db.execute("DELETE FROM ip_registry")
            await db.execute("DELETE FROM persistent_device_registry")
            await db.execute("DELETE FROM withdrawal_requests")
            await db.execute("DELETE FROM redeem_codes")
            await db.commit()
        await update.message.reply_text(
            "\u2705 DATABASE RESET COMPLETE!\n\nAll users, balances, referrals and device data deleted.\nChannels and settings are intact.",
            reply_markup=get_admin_keyboard()
        )

    elif text == "Back To Menu":
        context.user_data.clear()
        await send_main_menu(update, update.effective_user.first_name, user_id)


# ===================== USER FEATURE HANDLERS =====================

async def handle_balance(update, user_id):
    async with turso_connect() as db:
        row = await (await db.execute("SELECT balance, referral_count FROM user_balance WHERE user_id = ?", (user_id,))).fetchone()
    balance = float(row[0]) if row and row[0] is not None else 0.0
    refs = int(row[1]) if row and row[1] is not None else 0
    await update.message.reply_text(
        f"💸 Balance: ₹{balance:.2f}\n\n"
        f"🎉 Use \'Withdraw\' Button to Withdraw The Balance!",
        parse_mode="Markdown"
    )


async def handle_refer_earn(update, user_id, context):
    refer_earn_on = await get_setting("refer_earn_enabled", "1")
    if refer_earn_on != "1":
        await update.message.reply_text("🙅 Refer & Earn currently disabled try again later")
        return
    async with turso_connect() as db:
        row = await (await db.execute("SELECT referral_count FROM user_balance WHERE user_id = ?", (user_id,))).fetchone()
    referral_count = row[0] if row else 0
    refer_reward = await get_setting("refer_reward", "5")
    bot_username = context.bot.username or "bot"
    referral_link = f"https://t.me/{bot_username}?start={user_id}"

    keyboard = [
        [
            InlineKeyboardButton("🎀 MY INVITES", callback_data=f"refer_invites_{user_id}"),
            InlineKeyboardButton("🏆 LEADERBOARD", callback_data="refer_leaderboard"),
        ],
    ]

    await update.message.reply_text(
        f"🎁 Per Invite ₹{refer_reward} UPI Cash !!\n\n"
        f"🎀 Invite Link : `{referral_link}`\n\n"
        f"🎁 Daily bonus\n\n"
        f"✅ Share Your Own Invite Link To Earn Unlimited Easy cash! 🤑",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_bonus(update, user_id):
    keyboard = [
        [InlineKeyboardButton("DAILY BONUS", callback_data=f"bonus_daily_{user_id}")],
        [InlineKeyboardButton("GIFT CODE", callback_data=f"bonus_gift_{user_id}")],
    ]
    await update.message.reply_text(
        "✨ *CHOOSE ONE:*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_withdraw(update, user_id, context):
    withdrawal_enabled = await get_setting("withdrawal_enabled", "1")
    if withdrawal_enabled == "0":
        await update.message.reply_text(
            "🔒 *WITHDRAWALS DISABLED*\n\nWithdrawals are currently disabled. Please try again later.",
            parse_mode="Markdown"
        )
        return

    async with turso_connect() as db:
        # Check ultra_wallet safely
        row = await (await db.execute("SELECT balance, upi_id, vsv_wallet, ultra_wallet FROM user_balance WHERE user_id = ?", (user_id,))).fetchone()
    balance = row[0] if row else 0.0
    upi_id = row[1] if row else None
    vsv_wallet = row[2] if row else None
    ultra_wallet = row[3] if row and len(row) > 3 else None

    min_withdrawal = float(await get_setting("min_withdrawal", "50"))

    if balance < min_withdrawal:
        await update.message.reply_text(
            f"😵 INSUFFICIENT BALANCE — Minimum Withdrawal Is Rs.{min_withdrawal:.0f}"
        )
        return

    if not upi_id and not vsv_wallet and not ultra_wallet:
        await update.message.reply_text(
            "⚠️ *PAYMENT METHOD REQUIRED*\n\nPlease link your UPI ID, VSV Wallet, or Ultra Pay first before withdrawing.",
            parse_mode="Markdown"
        )
        return

    context.user_data['withdraw_balance'] = balance
    context.user_data['withdraw_upi'] = upi_id
    context.user_data['withdraw_vsv'] = vsv_wallet
    context.user_data['withdraw_ultra'] = ultra_wallet

    upi_enabled = await get_setting("upi_withdrawal_enabled", "1")
    vsv_enabled = await get_setting("vsv_withdrawal_enabled", "1")
    ultra_enabled = await get_setting("ultra_pay_enabled", "0")

    keyboard = []
    if ultra_wallet and ultra_enabled == "1":
        keyboard.append([InlineKeyboardButton("✅ ULTRA PAY CLICK", callback_data=f"wd_ultra_{user_id}")])
    if vsv_wallet and vsv_enabled == "1":
        keyboard.append([InlineKeyboardButton("✅ VSV CLICK", callback_data=f"wd_vsv_{user_id}")])
    if upi_id and upi_enabled == "1":
        keyboard.append([InlineKeyboardButton("✅ UPI CLICK", callback_data=f"wd_upi_{user_id}")])

    if not keyboard:
        await update.message.reply_text(
            "🔒 Withdrawals for your linked methods are currently disabled by admin. Please try again later.",
        )
        return

    await update.message.reply_text(
        "✨ SELECT PAYMENT METHOD FOR WITHDRAWAL",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_withdraw_amount(update, user_id, context, text):
    try:
        amount = float(text)
    except:
        await update.message.reply_text("Please send a valid amount.")
        return

    balance = context.user_data.get('withdraw_balance', 0)
    min_withdrawal = float(await get_setting("min_withdrawal", "50"))
    upi_id = context.user_data.get('withdraw_upi')
    vsv_wallet = context.user_data.get('withdraw_vsv')
    method = context.user_data.get('withdraw_method')

    if amount < min_withdrawal:
        await update.message.reply_text(f"Minimum Withdrawal Is Rs.{min_withdrawal:.0f}")
        return
    if amount > balance:
        await update.message.reply_text(f"❌ Insufficient Balance. Your Balance: Rs.{balance:.2f}")
        return

    if not method:
        method = 'vsv' if vsv_wallet and not upi_id else 'upi'

    context.user_data['waiting_for'] = None

    if method == 'ultra':
        ultra_wallet = context.user_data.get('withdraw_ultra')
        api_token = await get_setting("ultrapay_token")
        api_key = await get_setting("ultrapay_key")
        
        if not api_token or not api_key:
            await update.message.reply_text("❌ Admin has not configured the Ultra Pay API properly.")
            return

        async with turso_connect() as db:
            await db.execute("UPDATE user_balance SET balance = balance - ? WHERE user_id=?", (amount, user_id))
            await db.commit()

        ultra_url = f"https://ultra-pay.store/APIs/api"
        params = {
            "token": api_token,
            "key": api_key,
            "paytoNumber": ultra_wallet,
            "amount": str(int(amount)),
            "comment": "Withdrawal from Bot"
        }

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(ultra_url, params=params, timeout=15)
            
            resp_text = resp.text.lower()
            if "success" in resp_text or '"status": true' in resp_text or '"status":true' in resp_text:
                await update.message.reply_text(
                    f"✅ *ULTRA PAY PAYMENT SUCCESSFUL!*\n\n💰 Amount: Rs.{amount:.2f}\n📱 Number: {ultra_wallet}",
                    parse_mode="Markdown"
                )
            else:
                async with turso_connect() as db:
                    await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                    await db.commit()
                await update.message.reply_text(f"❌ *PAYMENT FAILED!*\n\nAPI Response: {resp_text[:100]}\nYour balance has been refunded.", parse_mode="Markdown")
        except Exception as e:
            async with turso_connect() as db:
                await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                await db.commit()
            await update.message.reply_text("❌ *API ERROR!*\n\nYour balance has been refunded.", parse_mode="Markdown")

    elif method == 'vsv' and vsv_wallet:
        async with turso_connect() as db:
            await db.execute("UPDATE user_balance SET balance = balance - ? WHERE user_id=?", (amount, user_id))
            await db.commit()
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    VSV_API_URL,
                    params={
                        "token": VSV_TOKEN,
                        "paytm": vsv_wallet,
                        "amount": str(int(amount)),
                        "comment": "Withdrawal from UPI Giveaway Bot",
                    },
                    timeout=15
                )
            resp_text = resp.text.strip()
            if "success" in resp_text.lower() or resp_text == "1":
                await update.message.reply_text(
                    f"✅ *VSV WALLET PAYMENT SUCCESSFUL!*\n\n"
                    f"💰 Amount: Rs.{amount:.2f}\n"
                    f"💳 Wallet: {vsv_wallet}\n\n"
                    f"Amount has been sent to your VSV Wallet!",
                    parse_mode="Markdown"
                )
            else:
                async with turso_connect() as db:
                    await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                    await db.commit()
                await update.message.reply_text(
                    f"❌ *VSV PAYMENT FAILED!*\n\nYour balance has been refunded. Please try again.",
                    parse_mode="Markdown"
                )
        except Exception as e:
            async with turso_connect() as db:
                await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                await db.commit()
            await update.message.reply_text(
                "❌ *VSV PAYMENT ERROR!*\n\nYour balance has been refunded. Please try again.",
                parse_mode="Markdown"
            )

    elif method == 'upi' and upi_id:
        async with turso_connect() as db:
            await db.execute("UPDATE user_balance SET balance = balance - ? WHERE user_id=?", (amount, user_id))
            await db.execute(
                "INSERT INTO withdrawal_requests (user_id, amount, upi_id, vsv_wallet, method) VALUES (?,?,?,?,?)",
                (user_id, amount, upi_id, vsv_wallet, method)
            )
            await db.commit()
        try:
            admin_msg = (
                f"💸 NEW UPI WITHDRAWAL REQUEST!\n\n"
                f"User ID:\n`{user_id}`\n\n"
                f"Amount:\n`Rs.{amount:.2f}`\n\n"
                f"UPI ID:\n`{upi_id}`"
            )
            await update.get_bot().send_message(chat_id=ADMIN_ID, text=admin_msg, parse_mode="Markdown")
        except:
            pass
        await update.message.reply_text(
            f"✅ *UPI WITHDRAWAL REQUEST SUBMITTED!*\n\n"
            f"💰 Amount: Rs.{amount:.2f}\n"
            f"🏦 UPI: {upi_id}\n\n"
            f"⏳ Admin will process your request shortly.",
            parse_mode="Markdown"
        )


async def handle_upi_link(update, user_id, upi_id):
    if "@" not in upi_id or len(upi_id) < 5:
        await update.message.reply_text("⚠️ Invalid UPI ID. Format: name@upi")
        return
    async with turso_connect() as db:
        await db.execute("UPDATE user_balance SET upi_id = ? WHERE user_id = ?", (upi_id, user_id))
        await db.commit()
    await update.message.reply_text(
        f"✅ *UPI ID LINKED!*\n\n🏦 UPI: `{upi_id}`",
        parse_mode="Markdown"
    )


async def handle_vsv_link(update, user_id, vsv_number):
    vsv_number = vsv_number.strip()
    if not vsv_number.isdigit() or len(vsv_number) != 10:
        await update.message.reply_text("⚠️ Invalid VSV Wallet Number. It Must Be Exactly 10 Digits.")
        return
    async with turso_connect() as db:
        await db.execute("UPDATE user_balance SET vsv_wallet = ? WHERE user_id = ?", (vsv_number, user_id))
        await db.commit()
    await update.message.reply_text(
        f"✅ *VSV WALLET LINKED!*\n\n💳 Wallet: `{vsv_number}`",
        parse_mode="Markdown"
    )


async def handle_leaderboard(update):
    async with turso_connect() as db:
        rows = await (await db.execute(
            "SELECT b.user_id, b.referral_count FROM user_balance b JOIN users u ON b.user_id=u.user_id ORDER BY b.referral_count DESC LIMIT 13"
        )).fetchall()

    rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟", "1️⃣1️⃣", "1️⃣2️⃣", "1️⃣3️⃣"]

    if not rows:
        msg = "😍 TOP USERS WITH MOST REFERS\n\nNo data available yet!"
    else:
        msg = "😍 TOP USERS WITH MOST REFERS\n\n"
        for i, r in enumerate(rows):
            uid = str(r[0])
            masked_id = uid[:2] + "*****" + uid[-3:] if len(uid) >= 6 else uid
            rank_emoji = rank_emojis[i] if i < len(rank_emojis) else f"{i+1}."
            msg += f"{rank_emoji} Top {i+1}:\nUser ID: {masked_id}\nVerified Refers: {r[1]}\n\n"

    if hasattr(update, 'message') and update.message:
        await update.message.reply_text(msg)
    else:
        await update.reply_text(msg)


async def handle_redeem_code_menu(update, user_id, context):
    keyboard = [
        [InlineKeyboardButton("🛒 Buy Redeem Code", callback_data="redeem_buy")],
        [InlineKeyboardButton("🎁 Use Gift Code", callback_data="redeem_use")],
    ]
    await update.message.reply_text(
        "*REDEEM CODE*\n\n"
        "🛒 *Buy A Redeem Code* — Purchase a code (min Rs.10) and receive it on your email.\n\n"
        "*Use A Gift 🎁 Code* — Enter an existing code to add balance.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_redeem_buy(update, user_id, context, text):
    try:
        amount = float(text)
    except:
        await update.message.reply_text("Please Send A Valid Amount (Minimum Rs.10).")
        return

    redeem_price = float(await get_setting("redeem_code_price", "10"))
    if amount < redeem_price:
        await update.message.reply_text(f"Minimum Redeem Code Amount Is Rs.{redeem_price:.0f}")
        return

    async with turso_connect() as db:
        row = await (await db.execute("SELECT balance FROM user_balance WHERE user_id=?", (user_id,))).fetchone()
    balance = row[0] if row else 0.0

    if balance < amount:
        await update.message.reply_text(f"Insufficient Balance. Your Balance: Rs.{balance:.2f}")
        context.user_data['waiting_for'] = None
        return

    context.user_data['redeem_amount'] = amount
    context.user_data['waiting_for'] = 'redeem_email'
    await update.message.reply_text(
        "📧 *EMAIL ADDRESS*\n\nPlease send your email address to receive the redeem code:",
        parse_mode="Markdown"
    )


async def handle_redeem_finalize(update, user_id, context, mobile):
    amount = context.user_data.get('redeem_amount', 0)
    email = context.user_data.get('redeem_email', '')

    if not email or amount <= 0:
        await update.message.reply_text("⚠️ Something Went Wrong. Please Start Again.")
        context.user_data.clear()
        return

    async with turso_connect() as db:
        row = await (await db.execute("SELECT balance FROM user_balance WHERE user_id=?", (user_id,))).fetchone()
    balance = row[0] if row else 0.0
    if balance < amount:
        await update.message.reply_text("❌ Insufficient Balance.")
        context.user_data.clear()
        return

    unique_code = f"REQ-{user_id}-{int(datetime.utcnow().timestamp())}"
    async with turso_connect() as db:
        await db.execute("UPDATE user_balance SET balance = balance - ? WHERE user_id=?", (amount, user_id))
        await db.execute(
            "INSERT INTO redeem_codes (code, amount, user_id, email, mobile, status) VALUES (?,?,?,?,?,'pending')",
            (unique_code, amount, user_id, email, mobile)
        )
        await db.commit()

    async with turso_connect() as db2:
        id_row = await (await db2.execute("SELECT id FROM redeem_codes WHERE code=?", (unique_code,))).fetchone()
    numeric_id = id_row[0] if id_row else "?"
    req_id = unique_code

    context.user_data.clear()

    try:
        admin_msg = (
            f"🎟️ NEW REDEEM CODE REQUEST!\n\n"
            f"RDM ID: `{numeric_id}`\n"
            f"Request Code: `{req_id}`\n"
            f"User ID: `{user_id}`\n"
            f"Amount: `Rs.{amount:.2f}`\n"
            f"Email: `{email}`\n"
            f"Mobile: `{mobile}`\n\n"
            f"Use *Approve RDM* button and send RDM ID: `{numeric_id}` to approve."
        )
        await bot_app_global.bot.send_message(chat_id=ADMIN_ID, text=admin_msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Admin notify error: {e}")

    await update.message.reply_text(
        f"✅ *REDEEM CODE REQUEST SUBMITTED!*\n\n"
        f"💰 Amount: Rs.{amount:.2f}\n"
        f"📧 Email: {email}\n"
        f"📱 Mobile: {mobile}\n\n"
        f"⏳ Admin Will Send The Code To Your Email Shortly.",
        parse_mode="Markdown"
    )


async def handle_redeem_use(update, user_id, code):
    code = code.strip().upper()
    async with turso_connect() as db:
        row = await (await db.execute("SELECT id, amount, status FROM redeem_codes WHERE code=?", (code,))).fetchone()
        if not row:
            await update.message.reply_text("❌ Invalid Gift Code. Please check and try again.")
            return
        if row[2] == 'used':
            await update.message.reply_text("❌ This Gift Code Has Already Been Used.")
            return
        if row[2] != 'active':
            await update.message.reply_text("❌ This Gift Code Is Not Active Yet.")
            return
        amount = row[1]
        await db.execute("UPDATE redeem_codes SET status='used' WHERE id=?", (row[0],))
        await db.execute("UPDATE user_balance SET balance = balance + ? WHERE user_id=?", (amount, user_id))
        await db.commit()
    await update.message.reply_text(
        f"🎉 Gift Code Applied!\n\n💰 ₹{amount:.2f} Added To Your Balance!",
        parse_mode="Markdown"
    )


# ===================== CALLBACK QUERY HANDLER =====================

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id

    if data == "check_join":
        await check_join_callback(update, context)

    # ---- ADMIN TOGGLE BUTTONS ----
    elif data.startswith("tog_btn_"):
        key = data.replace("tog_", "")
        current_val = await get_setting(key, "1")
        new_val = "0" if current_val == "1" else "1"
        await set_setting(key, new_val)

        keyboard = [
            [InlineKeyboardButton("Balance: " + ("✅ SHOW" if await get_setting("btn_balance", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_balance")],
            [InlineKeyboardButton("Refer & Earn: " + ("✅ SHOW" if await get_setting("btn_refer", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_refer")],
            [InlineKeyboardButton("Bonus: " + ("✅ SHOW" if await get_setting("btn_bonus", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_bonus")],
            [InlineKeyboardButton("Withdraw: " + ("✅ SHOW" if await get_setting("btn_withdraw", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_withdraw")],
            [InlineKeyboardButton("Link UPI: " + ("✅ SHOW" if await get_setting("btn_link_upi", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_link_upi")],
            [InlineKeyboardButton("Link Wallet: " + ("✅ SHOW" if await get_setting("btn_link_wallet", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_link_wallet")],
            [InlineKeyboardButton("Redeem Code: " + ("✅ SHOW" if await get_setting("btn_redeem", "1") == "1" else "❌ HIDE"), callback_data="tog_btn_redeem")],
        ]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
        btn_name = key.replace('btn_', '').replace('_', ' ').title()
        status_text = "Shown" if new_val == "1" else "Hidden"
        await query.answer(f"{btn_name} Button is now {status_text}!", show_alert=True)

    elif data == "link_ultra":
        context.user_data['waiting_for'] = 'ultra_pay'
        await query.message.edit_text("💸 *LINK ULTRA PAY WALLET*\n\nSend your Ultra Pay Wallet number:", parse_mode="Markdown")
        
    elif data == "link_vsv":
        context.user_data['waiting_for'] = 'vsv'
        await query.message.edit_text("💳 *LINK VSV WALLET*\n\nSend your VSV Wallet number (exactly 10 digits):", parse_mode="Markdown")

    elif data.startswith("refer_invites_"):
        target_uid = int(data.split("_")[-1])
        async with turso_connect() as db:
            row = await (await db.execute("SELECT referral_count FROM user_balance WHERE user_id=?", (target_uid,))).fetchone()
        count = row[0] if row else 0
        await query.message.reply_text(
            f"🚀 *MY INVITES*\n\n"
            f"👥 *TOTAL VERIFIED REFERRALS:* {count}\n\n"
            f"_Keep sharing your link to earn more!_",
            parse_mode="Markdown"
        )

    elif data == "refer_leaderboard":
        await handle_leaderboard(query)

    elif data.startswith("refer_tracker_"):
        target_uid = int(data.split("_")[-1])
        async with turso_connect() as db:
            row = await (await db.execute("SELECT referral_count FROM user_balance WHERE user_id=?", (target_uid,))).fetchone()
        count = row[0] if row else 0
        refer_reward = await get_setting("refer_reward", "5")
        earned = count * float(refer_reward)
        await query.message.reply_text(
            f"👥 *REFER TRACKER*\n\n"
            f"✅ *VERIFIED REFERRALS:* {count}\n"
            f"💰 *TOTAL EARNED FROM REFERS:* RS.{earned:.2f}\n\n"
            f"_Bonus is credited after each friend verifies their device._",
            parse_mode="Markdown"
        )

    elif data.startswith("bonus_daily_"):
        target_uid = user_id
        async with turso_connect() as db:
            row = await (await db.execute("SELECT balance, last_bonus_claim FROM user_balance WHERE user_id=?", (target_uid,))).fetchone()
        balance = float(row[0]) if row and row[0] is not None else 0.0
        last_bonus = row[1] if row else None
        now = datetime.utcnow().isoformat()

        if last_bonus:
            time_diff = (datetime.utcnow() - datetime.fromisoformat(last_bonus)).total_seconds()
            if time_diff < 86400:
                hours_left = (86400 - time_diff) / 3600
                await query.message.reply_text(
                    f"⏳ *DAILY BONUS*\n\nCome back in *{hours_left:.1f} hours* to claim your daily bonus!\n\n🎁 Claim every 24 hours.",
                    parse_mode="Markdown"
                )
                return

        daily_bonus_amount = float(await get_setting("daily_bonus", "0"))

        if daily_bonus_amount <= 0:
            await query.message.reply_text(
                "⏳ *DAILY BONUS*\n\nDaily bonus has not been set by admin yet.\nPlease check back later! 🙏",
                parse_mode="Markdown"
            )
            return

        async with turso_connect() as db:
            await db.execute(
                "INSERT OR IGNORE INTO user_balance (user_id, balance) VALUES (?, 0.0)",
                (target_uid,)
            )
            await db.execute(
                "UPDATE user_balance SET balance = balance + ?, last_bonus_claim=? WHERE user_id=?",
                (daily_bonus_amount, now, target_uid)
            )
            await db.commit()
        bonus_str = f"{daily_bonus_amount:.2f}"
        await query.message.reply_text(
            f"🎁 Bonus Rs.{bonus_str} Claimed Successfully"
        )

    elif data.startswith("bonus_gift_"):
        context.user_data['waiting_for'] = 'redeem_use'
        await query.message.reply_text(
            "🎁 *ENTER GIFT CODE*\n\nSend your gift code below:",
            parse_mode="Markdown"
        )

    elif data == "redeem_buy":
        context.user_data['waiting_for'] = 'redeem_buy_amount'
        redeem_price = await get_setting("redeem_code_price", "10")
        await query.message.reply_text(
            f"🛒 *BUY REDEEM CODE*\n\nSend The Amount For The Redeem Code (Minimum Rs.{redeem_price}):",
            parse_mode="Markdown"
        )
    elif data == "redeem_use":
        context.user_data['waiting_for'] = 'redeem_use'
        await query.message.reply_text(
            "🎁 *USE Gift CODE*\n\nSend Your Gift Code:",
            parse_mode="Markdown"
        )
        
    elif data.startswith("wd_ultra_"):
        context.user_data['withdraw_method'] = 'ultra'
        context.user_data['waiting_for'] = 'withdraw_amount'
        min_withdrawal = await get_setting("min_withdrawal", "50")
        await query.message.reply_text(
            f"💸 *ULTRA PAY SELECTED*\n\nENTER YOUR AMOUNT YOU WANT TO WITHDRAW\n(EG. 10)\n\nMinimum: Rs.{min_withdrawal}",
            parse_mode="Markdown"
        )
        
    elif data.startswith("wd_upi_"):
        context.user_data['withdraw_method'] = 'upi'
        context.user_data['waiting_for'] = 'withdraw_amount'
        min_withdrawal = await get_setting("min_withdrawal", "50")
        await query.message.reply_text(
            f"🏦 *UPI SELECTED*\n\nENTER YOUR AMOUNT YOU WANT TO WITHDRAW\n(EG. 10)\n\nMinimum: Rs.{min_withdrawal}",
            parse_mode="Markdown"
        )
    elif data.startswith("wd_vsv_"):
        context.user_data['withdraw_method'] = 'vsv'
        context.user_data['waiting_for'] = 'withdraw_amount'
        min_withdrawal = await get_setting("min_withdrawal", "50")
        await query.message.reply_text(
            f"💳 *VSV SELECTED*\n\nENTER YOUR AMOUNT YOU WANT TO WITHDRAW\n(EG. 10)\n\nMinimum: Rs.{min_withdrawal}",
            parse_mode="Markdown"
        )

# ===================== VALIDATION =====================

def validate_telegram_init_data(init_data: str, bot_token: str):
    try:
        params = {}
        for item in init_data.split("&"):
            if "=" in item:
                k, v = item.split("=", 1)
                params[k] = v
        received_hash = params.pop("hash", "")
        data_check_string = "\n".join(f"{k}={unquote(v)}" for k, v in sorted(params.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(computed_hash, received_hash):
            return json.loads(unquote(params.get("user", "{}")))
        return None
    except Exception as e:
        logger.error(f"initData validation error: {e}")
        return None


# ===================== FASTAPI =====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    _http_client = httpx.AsyncClient(timeout=15)
    await init_db()
    await run_bot()
    yield
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/bot/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ===================== BOT VERIFY ENDPOINT =====================

class VerifyRequest(BaseModel):
    init_data: str
    device_id: str
    persistent_id: str = ""


@app.get("/bot/verify")
async def serve_verify_page():
    html_path = STATIC_DIR / "verify.html"
    content = html_path.read_text(encoding="utf-8")
    headers = {
        "X-Frame-Options": "ALLOWALL",
        "Content-Security-Policy": "default-src * 'unsafe-inline' 'unsafe-eval' data: blob:;",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache",
    }
    return Response(content=content, media_type="text/html", headers=headers)


@app.post("/bot/api/verify-device")
async def verify_device(payload: VerifyRequest, request: Request):
    user_data = validate_telegram_init_data(payload.init_data, BOT_TOKEN)
    if not user_data:
        raise HTTPException(status_code=403, detail="Invalid Telegram session")
    user_id = user_data.get("id")
    if not user_id:
        raise HTTPException(status_code=400, detail="User ID missing")
    device_id = payload.device_id
    persistent_id = payload.persistent_id or ""

    client_ip = request.headers.get("X-Forwarded-For", "")
    if client_ip:
        client_ip = client_ip.split(",")[0].strip()
    else:
        client_ip = request.headers.get("X-Real-IP", "")
    if not client_ip and request.client:
        client_ip = request.client.host or ""

    async with turso_connect() as db:
        row = await (await db.execute("SELECT user_id FROM device_registry WHERE device_id=?", (device_id,))).fetchone()
        if row and row[0] != user_id:
            return {"status": "blocked", "message": "Device already registered."}

        if persistent_id:
            p_row = await (await db.execute("SELECT user_id FROM persistent_device_registry WHERE persistent_id=?", (persistent_id,))).fetchone()
            if p_row and p_row[0] != user_id:
                return {"status": "blocked", "message": "Device already registered (persistent)."}

        if client_ip:
            ip_row = await (await db.execute("SELECT user_id FROM ip_registry WHERE ip_address=?", (client_ip,))).fetchone()
            if ip_row and ip_row[0] != user_id:
                return {"status": "blocked", "message": "IP already registered."}

        if not row:
            await db.execute("INSERT OR REPLACE INTO device_registry (device_id, user_id) VALUES (?, ?)", (device_id, user_id))
        if persistent_id and not (p_row if persistent_id else None):
            await db.execute("INSERT OR REPLACE INTO persistent_device_registry (persistent_id, user_id) VALUES (?, ?)", (persistent_id, user_id))
        if client_ip:
            await db.execute("INSERT OR REPLACE INTO ip_registry (ip_address, user_id) VALUES (?, ?)", (client_ip, user_id))
        now = datetime.utcnow().isoformat()

        already_verified = await (await db.execute("SELECT is_verified FROM users WHERE user_id=?", (user_id,))).fetchone()
        was_verified = int(already_verified[0]) if already_verified and already_verified[0] is not None else 0

        await db.execute(
            """INSERT INTO users (user_id, username, first_name, is_verified, device_id, verified_at)
               VALUES (?, ?, ?, 1, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET is_verified=1, device_id=excluded.device_id, verified_at=excluded.verified_at""",
            (user_id, user_data.get("username"), user_data.get("first_name"), device_id, now)
        )

        existing_balance = await (await db.execute("SELECT balance FROM user_balance WHERE user_id=?", (user_id,))).fetchone()

        if not existing_balance:
            await db.execute("INSERT OR IGNORE INTO user_balance (user_id, balance) VALUES (?,?)", (user_id, 0.0))
            await db.commit()

        referrer_to_notify = None
        referrer_reward_amount = 0.0

        await db.commit()

    if was_verified == 0:
        async with turso_connect() as db2:
            referrer_row = await (await db2.execute(
                "SELECT value FROM bot_settings WHERE key=?",
                (f"pending_referrer_{user_id}",)
            )).fetchone()

        if referrer_row:
            referrer_id_val = int(referrer_row[0])
            refer_reward = float(await get_setting("refer_reward", "5"))
            async with turso_connect() as db3:
                await db3.execute(
                    "INSERT OR IGNORE INTO user_balance (user_id, balance) VALUES (?, 0.0)",
                    (referrer_id_val,)
                )
                await db3.execute(
                    "UPDATE user_balance SET balance = balance + ?, referral_count = referral_count + 1 WHERE user_id=?",
                    (refer_reward, referrer_id_val)
                )
                await db3.execute("DELETE FROM bot_settings WHERE key=?", (f"pending_referrer_{user_id}",))
                await db3.commit()
            referrer_to_notify = referrer_id_val
            referrer_reward_amount = refer_reward


    if referrer_to_notify:
        try:
            reward_str = f"{referrer_reward_amount:.2f}".replace(".", "\\.")
            await bot_app_global.bot.send_message(
                chat_id=referrer_to_notify,
                text=(
                    f"🎉 [User {user_id}](tg://user?id={user_id}) got invited by your URL\n"
                    f"🎁 Rs\\.{reward_str} added to your balance"
                ),
                parse_mode="MarkdownV2"
            )
        except Exception as e:
            logger.error(f"Referrer notify error: {e}")

    first_name = user_data.get("first_name", "User")
    safe_first_name = str(first_name).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
    
    try:
        keyboard = await get_user_keyboard_async(user_id)
        await bot_app_global.bot.send_message(
            chat_id=user_id,
            text="✅ *Device Verified Successfully!*",
            parse_mode="Markdown"
        )
        await bot_app_global.bot.send_message(
            chat_id=user_id,
            text=f"😍 Welcome, *{safe_first_name}*!\n\n💸 Earn Money • Refer Friends • Withdraw Instantly\n\n👇 Use button below to get started",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Failed to send main menu after verification: {e}")

    return {"status": "verified", "user": {"id": user_id, "first_name": user_data.get("first_name")}}


@app.api_route("/bot/healthz", methods=["GET", "HEAD"])
async def health():
    return {"status": "ok"}


@app.post("/webhook")
async def telegram_webhook(request: Request):
    global bot_app_global
    if bot_app_global is None:
        raise HTTPException(status_code=503, detail="Bot not ready")
    data = await request.json()
    update = Update.de_json(data, bot_app_global.bot)
    await bot_app_global.process_update(update)
    return {"ok": True}


# ===================== BOT MAIN =====================

WEBHOOK_URL = "https://tg2026newbot.onrender.com/webhook"

async def run_bot():
    global bot_app_global
    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(ChatJoinRequestHandler(chat_join_request_handler))
    bot_app.add_handler(CallbackQueryHandler(callback_handler))
    bot_app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, combined_message_handler))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.bot.set_webhook(url=WEBHOOK_URL)
    bot_app_global = bot_app
    logger.info(f"Webhook set: {WEBHOOK_URL}")
    return bot_app


async def main():
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
