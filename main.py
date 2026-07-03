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
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3Nzk3MDA0NTIsImlkIjoiMDE5ZTVlNjctNzIwMS03OTQwLWI3YTUtMjUxZmI5ZTQ4YTY2IiwicmlkIjoiZGQxNGI2NWItZjI4MC00YmNjLTk5MzgtNzA4NWEwYzQ4ZjJiZjU1MzI5YWJlOSJ9.1uBpnSQhPDAfoLE8XCkhP_uQWp3i0egJA6UXsQXhsGFQXh2VrODIt07FRj4v2edrAcRwVRe"

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
            ("btn_link_wallet", "1"),
            ("btn_redeem", "1"),
            ("ultra_pay_enabled", "0"),
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
            try:
                member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    return False
            except Exception as e:
                logger.error(f"Private channel check error: {e}")
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
    logger.info(f"Join request saved: user={user_id} channel={channel_id}")


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
    keyboard.append([InlineKeyboardButton("✅ CLAIM", callback_data="check_join")])
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
    if b_bal == "1": buttons.append(KeyboardButton("💸 Balance"))
    if b_ref == "1": buttons.append(KeyboardButton("👥 Refer & Earn"))
    if b_bon == "1": buttons.append(KeyboardButton("🎁 Bonus"))
    if b_wit == "1": buttons.append(KeyboardButton("💳 Withdraw"))
    if b_upi == "1": buttons.append(KeyboardButton("🏦 Link UPI"))
    if b_wal == "1": buttons.append(KeyboardButton("💼 Link Wallet"))
    if b_red == "1": buttons.append(KeyboardButton("🎟️ Redeem Code"))

    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]

    if await is_admin(user_id):
        rows.append([KeyboardButton("⚙️ Admin Panel")])
        
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def get_admin_keyboard():
    rows = [
        [KeyboardButton("👥 Total Users"), KeyboardButton("💰 Withdrawal Requests")],
        [KeyboardButton("➕ Add Channel"), KeyboardButton("🔒 Add Private"), KeyboardButton("🔗 Add Link")],
        [KeyboardButton("❌ Remove Channel")],
        [KeyboardButton("✏️ Update Channel"), KeyboardButton("📣 Broadcast")],
        [KeyboardButton("💵 Refer Reward"), KeyboardButton("🔻 Min Withdrawal")],
        [KeyboardButton("🎁 Daily Bonus")],
        [KeyboardButton("💸 Withdraw ON/OFF"), KeyboardButton("🏦 UPI ON/OFF")],
        [KeyboardButton("💳 VSV ON/OFF"), KeyboardButton("⚡ Ultra Pay ON/OFF")],
        [KeyboardButton("🔗 Link Ultra API"), KeyboardButton("✏️ Manual Balance")],
        [KeyboardButton("✅ Verification ON"), KeyboardButton("❌ Verification OFF")],
        [KeyboardButton("✅ Refer Earn ON"), KeyboardButton("❌ Refer Earn OFF")],
        [KeyboardButton("✅ Approve"), KeyboardButton("❌ Reject")],
        [KeyboardButton("📋 RDM Requests"), KeyboardButton("✅ Approve RDM"), KeyboardButton("❌ Reject RDM")],
        [KeyboardButton("🎁 Gift Code"), KeyboardButton("👁️ Toggle Buttons")],
        [KeyboardButton("⚠️ Reset DB"), KeyboardButton("👤 Add Admin")],
        [KeyboardButton("🚫 Remove Admin"), KeyboardButton("📋 Admin List")],
        [KeyboardButton("🏠 Back To Menu")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


async def send_main_menu(update: Update, name: str, user_id: int):
    safe_name = str(name).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
    
    await update.message.reply_text(
        f"😍 Welcome, *{safe_name}*!\n\n💸 Earn Money • Refer Friends • Withdraw Instantly\n\n👇 Use button below to get started",
        reply_markup=await get_user_keyboard_async(user_id),
        parse_mode="Markdown"
    )

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
        await send_main_menu(update, user.first_name, user.id)
        return

    is_member = await check_all_channels(context.bot, user.id)
    if not is_member:
        await send_join_message(update, user.id)
        return

    verify_on = await get_setting("verification_enabled", "1")

    if is_verified == 1:
        safe_name = str(user.first_name).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
        await update.message.reply_text(
            f"😍 Welcome, *{safe_name}*!\n\n💸 Earn Money • Refer Friends • Withdraw Instantly\n\n👇 Use button below to get started",
            reply_markup=await get_user_keyboard_async(user.id),
            parse_mode="Markdown"
        )
    else:
        keyboard = [[InlineKeyboardButton("🔐 Verify Device", web_app=WebAppInfo(url=WEBAPP_URL))]]
        await update.message.reply_text(
            "🔒 *Verify Yourself*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    is_member = await check_all_channels(context.bot, user_id)
    
    if is_member:
        async with turso_connect() as db:
            await db.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user_id,))
            await db.commit()
        
        keyboard = await get_user_keyboard_async(user_id)
        await query.edit_message_text(
            "✅ *All Channels Joined!*\n\n👇 Select an option from menu",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
    else:
        await query.answer("⚠️ Please join all channels first!", show_alert=True)


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.message.web_app_data.data
    user = update.effective_user
    context.user_data.clear()
    try:
        payload = json.loads(data)
        init_data = payload.get("init_data", "")
        device_id = payload.get("device_id", "")
        persistent_id = payload.get("persistent_id", "")
        
        if not validate_telegram_init_data(init_data, BOT_TOKEN):
            await update.message.reply_text("❌ Verification failed!")
            return

        async with turso_connect() as db:
            await db.execute("INSERT OR IGNORE INTO device_registry (device_id, user_id) VALUES (?, ?)", (device_id, user.id))
            await db.execute("INSERT OR IGNORE INTO persistent_device_registry (persistent_id, user_id) VALUES (?, ?)", (persistent_id, user.id))
            await db.execute("UPDATE users SET is_verified=1, device_id=? WHERE user_id=?", (device_id, user.id))
            await db.commit()

        await update.message.reply_text("✅ *Verified!*\n\n👇 Use buttons below", reply_markup=await get_user_keyboard_async(user.id), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Web app data handler error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def combined_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    
    user_id = update.effective_user.id
    text = update.message.text

    if text == "💸 Balance":
        await handle_balance(update, user_id)
    elif text == "👥 Refer & Earn":
        await handle_refer_earn(update, user_id, context)
    elif text == "🎁 Bonus":
        await handle_bonus(update, user_id)
    elif text == "💳 Withdraw":
        await handle_withdraw(update, user_id, context)
    elif text == "🏦 Link UPI":
        context.user_data['waiting_for_upi'] = True
        await update.message.reply_text("📱 Please enter your UPI ID (e.g., name@bank):")
    elif text == "💼 Link Wallet":
        context.user_data['waiting_for_wallet'] = True
        await update.message.reply_text("📱 Please enter your wallet/phone number:")
    elif text == "🎟️ Redeem Code":
        await handle_redeem_code_menu(update, user_id, context)
    elif text == "⚙️ Admin Panel":
        if await is_admin(user_id):
            await handle_admin_panel_menu(update, context)
    elif context.user_data.get('waiting_for_upi'):
        context.user_data['waiting_for_upi'] = False
        await handle_upi_link(update, user_id, text)
    elif context.user_data.get('waiting_for_wallet'):
        context.user_data['waiting_for_wallet'] = False
        await handle_vsv_link(update, user_id, text)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data == "check_join":
        await check_join_callback(update, context)
    else:
        await callback_handler(update, context)


async def handle_admin_panel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎛️ *Admin Panel*", reply_markup=get_admin_keyboard(), parse_mode="Markdown")


async def handle_admin_action_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    
    if context.user_data.get("waiting_action") == "add_channel":
        parts = text.split("|")
        if len(parts) < 2:
            await update.message.reply_text("❌ Format: username|link|name (optional)")
            return
        username, link = parts[0].strip(), parts[1].strip()
        name = parts[2].strip() if len(parts) > 2 else username
        async with turso_connect() as db:
            await db.execute("INSERT INTO channels (channel_username, channel_link, channel_name) VALUES (?, ?, ?)", (username, link, name))
            await db.commit()
        await update.message.reply_text(f"✅ Channel {username} added!")
        context.user_data.clear()


async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    user_id = update.effective_user.id
    
    if text == "👥 Total Users":
        async with turso_connect() as db:
            row = await (await db.execute("SELECT COUNT(*) FROM users")).fetchone()
        total = row[0] if row else 0
        await update.message.reply_text(f"👥 Total Users: {total}")
    elif text == "💰 Withdrawal Requests":
        async with turso_connect() as db:
            rows = await (await db.execute("SELECT user_id, amount, method, status FROM withdrawal_requests ORDER BY created_at DESC LIMIT 10")).fetchall()
        if rows:
            msg = "📋 Recent Withdrawals:\n\n"
            for row in rows:
                msg += f"User: {row[0]} | ₹{row[1]} | {row[2]} | {row[3]}\n"
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("No withdrawal requests yet")
    elif text == "➕ Add Channel":
        context.user_data["waiting_action"] = "add_channel"
        await update.message.reply_text("📝 Enter channel details: username|link|name")
    elif text == "🏠 Back To Menu":
        keyboard = await get_user_keyboard_async(user_id)
        await update.message.reply_text("🏠 Back to main menu", reply_markup=keyboard)
    elif text == "✏️ Manual Balance":
        context.user_data["waiting_action"] = "manual_balance"
        await update.message.reply_text("👤 Enter user ID:")
    else:
        await update.message.reply_text("❌ Command not yet implemented")


async def handle_balance(update, user_id):
    async with turso_connect() as db:
        row = await (await db.execute("SELECT balance, referral_count FROM user_balance WHERE user_id=?", (user_id,))).fetchone()
    
    if row:
        balance, referrals = row[0], row[1]
        msg = f"💰 *Your Balance*\n\nBalance: ₹{balance}\nReferrals: {referrals}"
    else:
        msg = "❌ User not found"
    
    await update.message.reply_text(msg, parse_mode="Markdown")


async def handle_refer_earn(update, user_id, context):
    async with turso_connect() as db:
        row = await (await db.execute("SELECT referral_count FROM user_balance WHERE user_id=?", (user_id,))).fetchone()
    
    ref_count = row[0] if row else 0
    link = f"https://t.me/YourBotUsername?start={user_id}"
    msg = f"👥 *Refer & Earn*\n\nReferrals: {ref_count}\n\n🔗 Your Link:\n`{link}`"
    
    await update.message.reply_text(msg, parse_mode="Markdown")


async def handle_bonus(update, user_id):
    await update.message.reply_text("🎁 Bonus features coming soon!")


async def handle_withdraw(update, user_id, context):
    min_withdraw = await get_setting("min_withdrawal", "50")
    msg = f"💳 Withdraw Money\n\nMinimum: ₹{min_withdraw}\n\nEnter amount:"
    context.user_data['waiting_for_amount'] = True
    await update.message.reply_text(msg)


async def handle_withdraw_amount(update, user_id, context, text):
    try:
        amount = float(text)
        min_withdraw = float(await get_setting("min_withdrawal", "50"))
        
        async with turso_connect() as db:
            row = await (await db.execute("SELECT balance FROM user_balance WHERE user_id=?", (user_id,))).fetchone()
        
        if not row or row[0] < amount:
            await update.message.reply_text("❌ Insufficient balance!")
            return
        
        if amount < min_withdraw:
            await update.message.reply_text(f"❌ Minimum withdrawal: ₹{min_withdraw}")
            return
        
        context.user_data['pending_withdrawal'] = amount
        await update.message.reply_text(f"✅ Amount ₹{amount} confirmed. Select payment method:\n\n1️⃣ UPI\n2️⃣ Wallet")
    except:
        await update.message.reply_text("❌ Invalid amount!")


async def handle_upi_link(update, user_id, upi_id):
    async with turso_connect() as db:
        await db.execute("UPDATE user_balance SET upi_id=? WHERE user_id=?", (upi_id, user_id))
        await db.commit()
    await update.message.reply_text(f"✅ UPI ID linked: {upi_id}")


async def handle_vsv_link(update, user_id, vsv_number):
    async with turso_connect() as db:
        await db.execute("UPDATE user_balance SET vsv_wallet=? WHERE user_id=?", (vsv_number, user_id))
        await db.commit()
    await update.message.reply_text(f"✅ Wallet linked: {vsv_number}")


async def handle_leaderboard(update):
    async with turso_connect() as db:
        rows = await (await db.execute("SELECT user_id, referral_count FROM user_balance ORDER BY referral_count DESC LIMIT 10")).fetchall()
    
    if rows:
        msg = "🏆 *Leaderboard*\n\n"
        for i, row in enumerate(rows, 1):
            msg += f"{i}. User {row[0]}: {row[1]} referrals\n"
    else:
        msg = "No data yet"
    
    await update.message.reply_text(msg, parse_mode="Markdown")


async def handle_redeem_code_menu(update, user_id, context):
    await update.message.reply_text("🎁 *Redeem Code*\n\nEnter your code:", parse_mode="Markdown")
    context.user_data['waiting_for_code'] = True


async def handle_redeem_buy(update, user_id, context, text):
    await update.message.reply_text("💳 Buy codes coming soon!")


async def handle_redeem_finalize(update, user_id, context, mobile):
    await update.message.reply_text("✅ Finalization coming soon!")


async def handle_redeem_use(update, user_id, code):
    async with turso_connect() as db:
        row = await (await db.execute("SELECT amount, status FROM redeem_codes WHERE code=? AND status='pending'", (code,))).fetchone()
    
    if row:
        amount, _ = row
        async with turso_connect() as db:
            await db.execute("UPDATE redeem_codes SET status='used', user_id=? WHERE code=?", (user_id, code))
            await db.execute("UPDATE user_balance SET balance=balance+? WHERE user_id=?", (amount, user_id))
            await db.commit()
        await update.message.reply_text(f"✅ Code redeemed! +₹{amount}")
    else:
        await update.message.reply_text("❌ Invalid or already used code!")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    if data == "check_join":
        await check_join_callback(update, context)


def validate_telegram_init_data(init_data: str, bot_token: str):
    try:
        params = {}
        for item in init_data.split('&'):
            key, value = item.split('=', 1)
            params[key] = unquote(value)
        
        hash_value = params.pop('hash', '')
        
        check_string = '\n'.join(f'{k}={v}' for k, v in sorted(params.items()))
        secret_key = hmac.new(b'WebAppData', bot_token.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
        
        return computed_hash == hash_value
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


# ===================== FASTAPI =====================

async def lifespan(app: FastAPI):
    global bot_app_global
    await init_db()
    bot_app_global = Application.builder().token(BOT_TOKEN).build()
    
    bot_app_global.add_handler(CommandHandler("start", start_command))
    bot_app_global.add_handler(ChatJoinRequestHandler(chat_join_request_handler))
    bot_app_global.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, combined_message_handler))
    bot_app_global.add_handler(CallbackQueryHandler(button_handler))
    
    await bot_app_global.initialize()
    yield
    await bot_app_global.shutdown()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STATIC_DIR = Path(__file__).parent / "static"

class VerifyRequest(BaseModel):
    init_data: str
    device_id: str
    persistent_id: str = ""


@app.get("/bot/verify")
async def serve_verify_page():
    html_file = STATIC_DIR / "verify.html"
    if html_file.exists():
        return Response(content=html_file.read_text(), media_type="text/html")
    return {"error": "Verify page not found"}


@app.post("/bot/verify")
async def verify_device(payload: VerifyRequest, request: Request):
    if not validate_telegram_init_data(payload.init_data, BOT_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    async with turso_connect() as db:
        await db.execute("INSERT OR IGNORE INTO device_registry (device_id) VALUES (?)", (payload.device_id,))
        await db.commit()
    
    return {"success": True}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, bot_app_global.bot)
    await bot_app_global.process_update(update)
    return {"ok": True}


WEBHOOK_URL = "https://tg2026newbot.onrender.com/webhook"

async def run_bot():
    await bot_app_global.bot.set_webhook(WEBHOOK_URL)


async def main():
    if BOT_TOKEN:
        config = uvicorn.Config(app=app, host="0.0.0.0", port=PORT, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
    else:
        logger.error("BOT_TOKEN not set!")


if __name__ == "__main__":
    asyncio.run(main())
