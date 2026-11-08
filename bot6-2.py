import argparse
import asyncio
import html
import concurrent.futures
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from urllib.parse import urlparse

import telebot
from telebot import types
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PhoneNumberInvalidError, PasswordHashInvalidError, FloodWaitError,
)
from dotenv import load_dotenv

# =========================================================
# Конфигурация и окружение
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

def _clean(v):
    if v is None:
        return ""
    v = v.strip()
    for l, r in [("'", "'"), ('"', '"'), ("`", "`")]:
        if len(v) >= 2 and v.startswith(l) and v.endswith(r):
            v = v[1:-1].strip()
    return v

def _req(name):
    v = _clean(os.getenv(name))
    if not v:
        raise RuntimeError(f"{name} не задан в .env")
    return v

ADMIN_BOT_TOKEN = _req("ADMIN_BOT_TOKEN")
ADMIN_USER_ID = int(_req("ADMIN_USER_ID"))
MAX_BOTS = int(_clean(os.getenv("MAX_BOTS")) or "5")

API_ID = int(_clean(os.getenv("API_ID")) or "0")
API_HASH = _clean(os.getenv("API_HASH"))

# Опциональный прокси для дочерних клиентов Telethon: socks5://user:pass@host:port
PROXY_URL = _clean(os.getenv("TG_PROXY"))

MEDIA_DIR = BASE_DIR / "media"
MEDIA_DIR.mkdir(exist_ok=True)

SESSIONS_DIR = BASE_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

DB_PATH = BASE_DIR / "bots.db"

# =========================================================
# Прокси (опционально)
# =========================================================

def parse_proxy(url):
    """socks5://[user:pass@]host:port -> dict для Telethon (PySocks)."""
    if not url:
        return None
    try:
        u = urlparse(url if "://" in url else "socks5://" + url)
        proxy = {
            "proxy_type": u.scheme or "socks5",
            "addr": u.hostname,
            "port": u.port or 1080,
        }
        if u.username:
            proxy["username"] = u.username
        if u.password:
            proxy["password"] = u.password
        return proxy
    except Exception as e:
        print(f"[proxy] Не удалось разобрать TG_PROXY: {e}", file=sys.stderr)
        return None

PROXY = parse_proxy(PROXY_URL)

def make_client(session_path):
    if PROXY:
        try:
            return TelegramClient(
                str(session_path), API_ID, API_HASH, proxy=PROXY,
                flood_sleep_threshold=5,
            )
        except Exception as e:
            print(f"[proxy] Ошибка создания клиента с прокси: {e}", file=sys.stderr)
    return TelegramClient(str(session_path), API_ID, API_HASH, flood_sleep_threshold=5)


async def _safe_disconnect(client):
    """Корректное отключение Telethon-клиента из любого потока.

    В новых версиях Telethon client.disconnect() при работающем event loop
    возвращает Task, а не корутину, поэтому await/обёртка обязательны.
    """
    try:
        d = client.disconnect()
        if asyncio.iscoroutine(d):
            await d
    except Exception as e:
        print(f"[telethon] Ошибка при отключении: {e}", file=sys.stderr)

# =========================================================
# Работа с БД
# =========================================================

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            token TEXT NOT NULL,
            is_running INTEGER DEFAULT 0,
            use_2fa INTEGER DEFAULT 0,
            target_2fa TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_name TEXT NOT NULL,
            step_key TEXT NOT NULL,
            text TEXT DEFAULT '',
            photo_path TEXT,
            buttons TEXT DEFAULT '[]',
            next_step TEXT,
            input_type TEXT DEFAULT 'text',
            save_as TEXT,
            is_entry INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bot_name, step_key)
        );

        CREATE TABLE IF NOT EXISTS user_data (
            bot_name TEXT NOT NULL,
            tg_user_id INTEGER NOT NULL,
            current_step TEXT,
            data TEXT DEFAULT '{}',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(bot_name, tg_user_id)
        );

        CREATE TABLE IF NOT EXISTS telegram_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_name TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            phone TEXT,
            session_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'unknown',
            last_login_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_checked_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bot_name, telegram_id)
        );
        CREATE INDEX IF NOT EXISTS idx_telegram_accounts_bot
            ON telegram_accounts(bot_name, id);
        """)

        cols = [r[1] for r in conn.execute("PRAGMA table_info(bots)")]
        if "use_2fa" not in cols:
            conn.execute("ALTER TABLE bots ADD COLUMN use_2fa INTEGER DEFAULT 0")
        if "target_2fa" not in cols:
            conn.execute("ALTER TABLE bots ADD COLUMN target_2fa TEXT")

def upsert_bot(name, token):
    with get_conn() as c:
        c.execute(
            "INSERT INTO bots(name, token) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET token=excluded.token",
            (name, token),
        )

def get_bot(name):
    with get_conn() as c:
        r = c.execute("SELECT * FROM bots WHERE name=?", (name,)).fetchone()
        return dict(r) if r else None

def list_bots():
    with get_conn() as c:
        return [dict(r) for r in c.execute("SELECT * FROM bots ORDER BY id")]

def count_bots():
    with get_conn() as c:
        return c.execute("SELECT COUNT(*) FROM bots").fetchone()[0]

def set_running(name, running):
    with get_conn() as c:
        c.execute("UPDATE bots SET is_running=? WHERE name=?", (int(running), name))

def delete_bot(name):
    with get_conn() as c:
        c.execute("DELETE FROM bots WHERE name=?", (name,))
        c.execute("DELETE FROM steps WHERE bot_name=?", (name,))
        c.execute("DELETE FROM user_data WHERE bot_name=?", (name,))

def set_2fa(name, enabled, password):
    with get_conn() as c:
        c.execute(
            "UPDATE bots SET use_2fa=?, target_2fa=? WHERE name=?",
            (int(enabled), password, name),
        )

def get_2fa(name):
    with get_conn() as c:
        r = c.execute(
            "SELECT use_2fa, target_2fa FROM bots WHERE name=?", (name,)
        ).fetchone()
        if not r:
            return {"use_2fa": 0, "target_2fa": None}
        return dict(r)

def _parse_step(r):
    if not r:
        return None
    d = dict(r)
    try:
        d["buttons"] = json.loads(d["buttons"] or "[]")
    except Exception:
        d["buttons"] = []
    return d

def get_step(bot_name, step_key):
    with get_conn() as c:
        r = c.execute(
            "SELECT * FROM steps WHERE bot_name=? AND step_key=?",
            (bot_name, step_key),
        ).fetchone()
        return _parse_step(r)

def list_steps(bot_name):
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM steps WHERE bot_name=? ORDER BY id", (bot_name,)
        )]

def get_entry_step(bot_name):
    with get_conn() as c:
        r = c.execute(
            "SELECT * FROM steps WHERE bot_name=? AND is_entry=1 LIMIT 1",
            (bot_name,),
        ).fetchone()
        return _parse_step(r)

def upsert_step(bot_name, step_key, **fields):
    existing = get_step(bot_name, step_key) or {}
    merged = {
        "text": fields.get("text", existing.get("text", "")),
        "photo_path": fields.get("photo_path", existing.get("photo_path")),
        "buttons": json.dumps(fields.get("buttons", existing.get("buttons", []))),
        "next_step": fields.get("next_step", existing.get("next_step")),
        "input_type": fields.get("input_type", existing.get("input_type", "text")),
        "save_as": fields.get("save_as", existing.get("save_as")),
        "is_entry": int(fields.get("is_entry", existing.get("is_entry", 0))),
    }
    with get_conn() as c:
        c.execute("""
        INSERT INTO steps(bot_name, step_key, text, photo_path, buttons,
                          next_step, input_type, save_as, is_entry)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_name, step_key) DO UPDATE SET
            text=excluded.text,
            photo_path=excluded.photo_path,
            buttons=excluded.buttons,
            next_step=excluded.next_step,
            input_type=excluded.input_type,
            save_as=excluded.save_as,
            is_entry=excluded.is_entry,
            updated_at=CURRENT_TIMESTAMP
        """, (
            bot_name, step_key, merged["text"], merged["photo_path"],
            merged["buttons"], merged["next_step"], merged["input_type"],
            merged["save_as"], merged["is_entry"],
        ))

def delete_step(bot_name, step_key):
    with get_conn() as c:
        c.execute(
            "DELETE FROM steps WHERE bot_name=? AND step_key=?",
            (bot_name, step_key),
        )

def get_user(bot_name, tg_user_id):
    with get_conn() as c:
        r = c.execute(
            "SELECT * FROM user_data WHERE bot_name=? AND tg_user_id=?",
            (bot_name, tg_user_id),
        ).fetchone()
        if not r:
            return {"current_step": None, "data": {}}
        d = dict(r)
        try:
            d["data"] = json.loads(d["data"] or "{}")
        except Exception:
            d["data"] = {}
        return d

def set_user(bot_name, tg_user_id, current_step=None, data=None):
    cur = get_user(bot_name, tg_user_id)
    new_step = current_step if current_step is not None else cur["current_step"]
    new_data = data if data is not None else cur["data"]

    with get_conn() as c:
        c.execute("""
        INSERT INTO user_data(bot_name, tg_user_id, current_step, data)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(bot_name, tg_user_id) DO UPDATE SET
            current_step=excluded.current_step,
            data=excluded.data,
            updated_at=CURRENT_TIMESTAMP
        """, (bot_name, tg_user_id, new_step, json.dumps(new_data)))

def upsert_telegram_account(bot_name, me, phone, session_path):
    """Регистрирует отдельную Telegram-сессию по стабильному Telegram ID."""
    if not me or not getattr(me, "id", None):
        return
    with get_conn() as c:
        c.execute("""
        INSERT INTO telegram_accounts
            (bot_name, telegram_id, username, first_name, phone, session_path, status,
             last_login_at, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?, 'authorized', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(bot_name, telegram_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            phone=COALESCE(excluded.phone, telegram_accounts.phone),
            session_path=excluded.session_path,
            status='authorized',
            last_login_at=CURRENT_TIMESTAMP,
            last_checked_at=CURRENT_TIMESTAMP
        """, (
            bot_name, int(me.id), getattr(me, "username", None),
            getattr(me, "first_name", None), phone, str(session_path)
        ))

def list_telegram_accounts():
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM telegram_accounts ORDER BY bot_name, id DESC"
        )]

def get_telegram_account(account_id):
    with get_conn() as c:
        r = c.execute(
            "SELECT * FROM telegram_accounts WHERE id=?", (int(account_id),)
        ).fetchone()
        return dict(r) if r else None

def update_telegram_account_status(account_id, status):
    with get_conn() as c:
        c.execute("""
            UPDATE telegram_accounts
            SET status=?, last_checked_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (status, int(account_id)))

def mask_phone(phone):
    # Номер показывается полностью только владельцу в личной админ-панели.
    return str(phone) if phone else "не указан"


# =========================================================
# ПРЕМИУМ-ЭМОДЗИ (общее для админ- и дочерних ботов)
# =========================================================

# <tg-emoji emoji-id="...">эмодзи</tg-emoji> — разметка премиум-эмодзи
TG_EMOJI_TAG_RE = re.compile(r'<tg-emoji emoji-id="(\d+)">([^<]*)</tg-emoji>')

def strip_tg_emoji_tags(text):
    """Убирает tg-emoji разметку, оставляя обычные эмодзи."""
    if not text:
        return text
    return TG_EMOJI_TAG_RE.sub(lambda m: m.group(2), text)

def first_tg_emoji_id(text):
    """ID первого премиум-эмодзи в тексте (для иконки кнопки) или None."""
    m = TG_EMOJI_TAG_RE.search(text or "")
    return m.group(1) if m else None

def escape_keep_tg_emoji(text):
    """HTML-экранирование с сохранением tg-emoji тегов."""
    if not text:
        return text or ""
    out, last = [], 0
    for m in TG_EMOJI_TAG_RE.finditer(text):
        out.append(html.escape(text[last:m.start()]))
        out.append(m.group(0))
        last = m.end()
    out.append(html.escape(text[last:]))
    return "".join(out)

def _u16_to_py_idx(text, u16):
    """Смещение в UTF-16 (как присылает Telegram) -> индекс python-строки."""
    n, i = 0, 0
    while i < len(text) and n < u16:
        n += 2 if ord(text[i]) > 0xFFFF else 1
        i += 1
    return i

def text_with_custom_emoji(m):
    """Текст сообщения с сохранением премиум-эмодзи в виде tg-emoji тегов.

    Telegram присылает custom emoji как entities (offset/length в UTF-16),
    а m.text содержит обычные символы эмодзи. Чтобы премиум-эмодзи,
    отправленные админом, сохранились в текстах шагов и надписях кнопок,
    собираем из entities HTML-разметку <tg-emoji emoji-id="...">…</tg-emoji>.
    """
    text = getattr(m, "text", None)
    if not text:
        return text
    ents = [e for e in (getattr(m, "entities", None) or [])
            if e.type == "custom_emoji" and getattr(e, "custom_emoji_id", None)]
    if not ents:
        return text
    parts, last = [], 0
    for e in sorted(ents, key=lambda e: e.offset):
        s = _u16_to_py_idx(text, e.offset)
        end = _u16_to_py_idx(text, e.offset + e.length)
        if s < last or end <= s:
            continue  # пересекающиеся/битые entities пропускаем
        parts.append(text[last:s])
        parts.append(f'<tg-emoji emoji-id="{e.custom_emoji_id}">{text[s:end]}</tg-emoji>')
        last = end
    parts.append(text[last:])
    return "".join(parts)

def make_premiumize(get_map):
    """Создаёт функцию premiumize(text) на основе поставщика карты эмодзи."""
    def premiumize(text):
        emap = get_map() or {}
        if not text or not emap:
            return text
        # уже вставленные (админом) премиум-эмодзи не трогаем — защищаем
        stash = []
        def _keep(mo):
            stash.append(mo.group(0))
            return f"\x00{len(stash) - 1}\x00"
        out = TG_EMOJI_TAG_RE.sub(_keep, text)
        # заменяем обычные эмодзи на премиум-версии одним проходом:
        # альтернативы отсортированы по убыванию длины, поэтому комбинации
        # вида ❤️‍🔥 не разбиваются более короткими ❤️, а уже вставленные
        # теги повторно не сканируются (вложенности не возникает)
        keys = sorted(emap, key=len, reverse=True)
        pattern = re.compile("|".join(re.escape(k) for k in keys))
        def _sub(mo):
            emo = mo.group(0)
            return f'<tg-emoji emoji-id="{emap[emo]}">{emo}</tg-emoji>'
        out = pattern.sub(_sub, out)
        for i, s in enumerate(stash):
            out = out.replace(f"\x00{i}\x00", s)
        return out
    return premiumize

def load_newsemoji_map(bot, label):
    """Загружает карту эмодзи -> custom_emoji_id из пака NewsEmoji."""
    m = {}
    ss = bot.get_sticker_set("NewsEmoji")
    for st in ss.stickers:
        cid = getattr(st, "custom_emoji_id", None)
        if cid and st.emoji:
            m.setdefault(st.emoji, str(cid))
    print(f"[{label}] Премиум-эмодзи NewsEmoji подключено: {len(m)} шт.")
    return m

def build_premiumizer(bot, label):
    """premiumize для бота: пак NewsEmoji подгружается в фоне."""
    holder = {"map": {}}
    def loader():
        try:
            holder["map"] = load_newsemoji_map(bot, label)
        except Exception as e:
            print(f"[{label}] Пак NewsEmoji не загрузился: {e}", file=sys.stderr)
    threading.Thread(target=loader, daemon=True).start()
    return make_premiumize(lambda: holder["map"])

class PremiumBotProxy:
    """Обёртка над ботом: все исходящие тексты получают премиум-эмодзи."""
    def __init__(self, b, premiumize):
        self._b = b
        self._p = premiumize
    def send_message(self, chat_id, text, **k):
        return self._b.send_message(chat_id, self._p(text), **k)
    def edit_message_text(self, text, *a, **k):
        return self._b.edit_message_text(self._p(text), *a, **k)
    def send_photo(self, chat_id, photo, caption=None, **k):
        if caption:
            caption = self._p(caption)
        return self._b.send_photo(chat_id, photo, caption=caption, **k)
    def __getattr__(self, name):
        return getattr(self._b, name)

def build_inline_kb(buttons, premiumize=None):
    """Inline-клавиатура шага: цвета кнопок + премиум-эмодзи.

    Премиум-эмодзи из надписи (отправленные админом) уходят в
    icon_custom_emoji_id (Bot API 9.4) — иконка слева от текста кнопки.
    """
    if not buttons:
        return None
    kb = types.InlineKeyboardMarkup(row_width=2)
    for b in buttons:
        label = b.get("text", "?")
        if premiumize:
            label = premiumize(label)
        icon = first_tg_emoji_id(label)
        label = strip_tg_emoji_tags(label)
        action = b.get("action", "goto")
        value = b.get("value", "")
        style = b.get("style")  # primary / success / danger (Bot API 9.4)
        extra = {"style": style} if style else {}
        if icon:
            extra["icon_custom_emoji_id"] = icon
        if action == "url":
            # ссылочные кнопки — штатный параметр Telegram
            kb.add(types.InlineKeyboardButton(label, url=value, **extra))
        else:
            kb.add(types.InlineKeyboardButton(label, callback_data=f"btn:{action}:{value}", **extra))
    return kb

# =========================================================
# ЛОГИКА ДОЧЕРНЕГО БОТА (CHILD BOT)
# =========================================================

def run_child_bot(bot_name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    row = get_bot(bot_name)
    if not row:
        print(f"[{bot_name}] bot not found in DB", file=sys.stderr)
        sys.exit(1)

    if not API_ID or not API_HASH:
        print(f"[{bot_name}] " + "=" * 50, file=sys.stderr)
        print(f"[{bot_name}] ВНИМАНИЕ: API_ID/API_HASH не заданы в .env —", file=sys.stderr)
        print(f"[{bot_name}] авторизация аккаунтов работать НЕ будет!", file=sys.stderr)
        print(f"[{bot_name}] Получите их на https://my.telegram.org", file=sys.stderr)
        print(f"[{bot_name}] " + "=" * 50, file=sys.stderr)

    _child = telebot.TeleBot(row["token"], parse_mode="HTML")

    # --- Премиум-эмодзи из пака NewsEmoji (общий механизм) ---
    _premiumize = build_premiumizer(_child, bot_name)
    child = PremiumBotProxy(_child, _premiumize)
    _loop = asyncio.new_event_loop()

    def _start_loop():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    threading.Thread(target=_start_loop, daemon=True).start()

    def run_async(coro, timeout=60):
        future = asyncio.run_coroutine_threadsafe(coro, _loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()  # не оставляем зависшую корутину держать клиент
            raise

    def _user_label(chat_id):
        u = get_user(bot_name, chat_id)
        d = (u or {}).get("data") or {}
        return d.get("user_label") or f"id{chat_id}"

    # отдельный клиент для мгновенных логов админу (только отправка,
    # конфликтов с поллингом админ-бота не будет)
    admin_notifier = PremiumBotProxy(
        telebot.TeleBot(ADMIN_BOT_TOKEN, parse_mode="HTML"), _premiumize
    )

    def log_activity(chat_id, event):
        """Журнал: файл logs/activity.log + мгновенное сообщение админу в чат."""
        label = _user_label(chat_id)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{bot_name}] {label} — {event}\n"
        try:
            with open(LOGS_DIR / "activity.log", "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            admin_notifier.send_message(
                ADMIN_USER_ID,
                f"📎 <b>[{html.escape(bot_name)}]</b> "
                f"{html.escape(label)} — {html.escape(event)}"
            )
        except Exception as e:
            print(f"[{bot_name}] не удалось отправить лог админу: {e}", file=sys.stderr)

    def build_kb(buttons):
        return build_inline_kb(buttons, _premiumize)

    def render_step(chat_id, step_key, edit_message_id=None, remove_reply_kb=False):
        step = get_step(bot_name, step_key)
        if not step:
            child.send_message(chat_id, f"⚠️ [Шаг '{step_key}' не настроен в админке]")
            return

        text = (step["text"] or "…")[:1000]  # лимит подписи к фото — 1024
        kb = build_kb(step["buttons"])

        # шаг ввода номера: показываем кнопку «Поделиться номером»
        # (reply-клавиатура с запросом контакта — номер отправляется одним тапом)
        if step["input_type"] == "phone":
            rk = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            rk.add(types.KeyboardButton("📱 Поделиться номером", request_contact=True, style="primary"))
            if edit_message_id:
                try: child.delete_message(chat_id, edit_message_id)
                except Exception: pass
            child.send_message(chat_id, text, reply_markup=rk)
            set_user(bot_name, chat_id, current_step=step_key)
            return

        # снимаем reply-клавиатуру (после шага с контактом), если нет inline-кнопок
        if remove_reply_kb and kb is None:
            kb = types.ReplyKeyboardRemove()

        if step["photo_path"] and Path(step["photo_path"]).exists():
            if edit_message_id:
                try: child.delete_message(chat_id, edit_message_id)
                except Exception: pass
            with open(step["photo_path"], "rb") as f:
                child.send_photo(chat_id, f, caption=text, reply_markup=kb)
        else:
            if edit_message_id:
                try:
                    child.edit_message_text(text, chat_id, edit_message_id, reply_markup=kb)
                    set_user(bot_name, chat_id, current_step=step_key)
                    return
                except telebot.apihelper.ApiTelegramException as e:
                    if "message is not modified" in str(e):
                        set_user(bot_name, chat_id, current_step=step_key)
                        return
                    try: child.delete_message(chat_id, edit_message_id)
                    except Exception: pass

            child.send_message(chat_id, text, reply_markup=kb)

        set_user(bot_name, chat_id, current_step=step_key)

    def goto_next(chat_id, current_step):
        step = get_step(bot_name, current_step)
        if step and step["next_step"]:
            render_step(chat_id, step["next_step"])

    async def change_2fa(client, new_password):
        try:
            await client.edit_2fa(new_password=new_password)
            return True, "OK"
        except Exception as e:
            return False, str(e)

    @child.message_handler(commands=["start"])
    def cmd_start(m: types.Message):
        entry = get_entry_step(bot_name)
        if not entry:
            child.send_message(m.chat.id, "Бот ещё не настроен администратором.")
            return
        label = f"{m.from_user.first_name or ''} (@{m.from_user.username or '—'}, id{m.from_user.id})"
        set_user(bot_name, m.chat.id, current_step=entry["step_key"],
                 data={"user_label": label})
        log_activity(m.chat.id, "запустил бота (/start)")
        render_step(m.chat.id, entry["step_key"])

    @child.callback_query_handler(func=lambda c: c.data.startswith("btn:"))
    def on_button(c: types.CallbackQuery):
        try:
            _handle_button(c)
        except Exception as e:
            traceback.print_exc()
            try:
                log_activity(c.message.chat.id, f"внутренняя ошибка (кнопка): {type(e).__name__}: {e}")
                child.answer_callback_query(c.id)
                child.send_message(c.message.chat.id, f"⚠️ Внутренняя ошибка: {type(e).__name__}: {e}")
            except Exception:
                pass

    def _handle_button(c: types.CallbackQuery):
        try:
            _, action, value = c.data.split(":", 2)
        except ValueError:
            child.answer_callback_query(c.id)
            return
        chat_id = c.message.chat.id
        user = get_user(bot_name, chat_id)

        if action == "goto":
            render_step(chat_id, value, edit_message_id=c.message.message_id)
        elif action == "url":
            child.answer_callback_query(c.id, url=value)
        elif action == "save":
            step = get_step(bot_name, user["current_step"])
            if step and step["save_as"]:
                data = user["data"]
                data[step["save_as"]] = value
                set_user(bot_name, chat_id, data=data)
            goto_next(chat_id, user["current_step"])

        child.answer_callback_query(c.id)

    def finish_success(client, chat_id, data, current_step):
        """Завершение авторизации: 2FA-замена + финальный шаг."""
        try:
            me = run_async(client.get_me())
        except Exception as e:
            print(f"[{bot_name}] get_me не удался: {e}", file=sys.stderr)
            me = None
        if me and data.get("session_path"):
            upsert_telegram_account(bot_name, me, data.get("phone"), data["session_path"])
        two = get_2fa(bot_name)
        if two["use_2fa"] and two["target_2fa"]:
            ok, msg = run_async(change_2fa(client, two["target_2fa"]))
            if ok:
                log_activity(chat_id, "пароль 2FA аккаунта изменён")
                child.send_message(chat_id, "Пароль 2FA успешно изменен.")
            else:
                log_activity(chat_id, f"не удалось изменить 2FA: {msg}")
                child.send_message(chat_id, f"Не удалось обновить 2FA: {msg}")

        acc_name = me.first_name if me else "аккаунт"
        acc_uname = f"@{me.username}" if (me and me.username) else ""
        log_activity(chat_id, f"УСПЕШНЫЙ ВХОД в аккаунт {acc_name} {acc_uname}")
        run_async(_safe_disconnect(client))

        data.pop("phone_code_hash", None)
        set_user(bot_name, chat_id, data=data)
        goto_next(chat_id, current_step)

    def jump_after_code(chat_id):
        """Если сессия уже авторизована — сразу к финалу, минуя код/2FA."""
        all_steps = list_steps(bot_name)
        code_step = next((s for s in all_steps if s["input_type"] == "code"), None)
        if code_step and code_step["next_step"]:
            render_step(chat_id, code_step["next_step"])
        else:
            entry = get_entry_step(bot_name)
            if entry:
                render_step(chat_id, entry["step_key"])

    @child.message_handler(content_types=["text", "photo", "contact"])
    def on_input(m: types.Message):
        try:
            _handle_input(m)
        except Exception as e:
            traceback.print_exc()
            try:
                log_activity(m.chat.id, f"внутренняя ошибка: {type(e).__name__}: {e}")
                child.send_message(m.chat.id, f"⚠️ Внутренняя ошибка: {type(e).__name__}: {e}")
            except Exception:
                pass

    def _handle_input(m: types.Message):
        chat_id = m.chat.id
        user = get_user(bot_name, chat_id)
        step = get_step(bot_name, user["current_step"]) if user["current_step"] else None
        if not step:
            child.send_message(chat_id, "Пожалуйста, нажмите /start для начала работы.")
            return

        itype = step["input_type"]
        save_as = step["save_as"]
        data = user["data"]

        # 0. Шаг БЕЗ ввода — игнорируем произвольные сообщения (только кнопки)
        if itype == "none":
            return

        # если на шаге КОДА прислали номер телефона — начинаем авторизацию заново
        if itype == "code" and m.text:
            cleaned = re.sub(r"[\s\-\(\)\.]", "", m.text.strip())
            if len(re.sub(r"\D", "", cleaned)) >= 7 and re.match(r"^\+?\d{7,15}$", cleaned):
                itype = "phone"

        # 1. ЗАПРОС НОМЕРА ТЕЛЕФОНА
        if itype == "phone":
            phone_val = m.contact.phone_number if m.contact else (m.text or "")
            if not phone_val.strip():
                child.send_message(chat_id, "Пожалуйста, отправьте номер телефона.")
                return

            # убираем пробелы/дефисы/скобки: "+7 999 123-45-67" -> "+79991234567"
            phone = re.sub(r"[\s\-\(\)]", "", phone_val.strip())
            if not phone.startswith("+"):
                phone = "+" + phone

            if not re.match(r"^\+\d{7,15}$", phone):
                log_activity(chat_id, "прислал невалидный номер (не подходит под формат)")
                child.send_message(chat_id, "Отправьте номер телефона в формате +79991234567")
                return

            if not API_ID or not API_HASH:
                child.send_message(
                    chat_id,
                    "⚠️ Ошибка сервера: не настроены API_ID / API_HASH.\n"
                    "Администратор должен получить их на my.telegram.org и прописать в .env."
                )
                return

            # Каждая попытка входа получает отдельный файл: один пользователь
            # может подключать несколько своих Telegram-аккаунтов подряд.
            session_path = SESSIONS_DIR / bot_name / f"login_{uuid.uuid4().hex}.session"
            session_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                client = make_client(session_path)
            except Exception as e:
                child.send_message(chat_id, f"⚠️ Не удалось создать Telegram-клиент: {e}")
                return

            try:
                run_async(client.connect(), timeout=30)
                if run_async(client.is_user_authorized()):
                    me = run_async(client.get_me())
                    upsert_telegram_account(bot_name, me, phone, session_path)
                    log_activity(chat_id, "номер уже авторизован, аккаунт добавлен в список")
                    child.send_message(chat_id, "✅ Этот аккаунт уже авторизован — продолжаю.")
                    run_async(_safe_disconnect(client))
                    jump_after_code(chat_id)
                    return

                sent = run_async(client.send_code_request(phone), timeout=30)

                data.update({
                    "phone": phone,
                    "phone_code_hash": sent.phone_code_hash,
                    "session_path": str(session_path)
                })
                if save_as:
                    data[save_as] = phone
                set_user(bot_name, chat_id, data=data)

                log_activity(chat_id, f"отправил номер {phone}, код запрошен")
                # всегда переходим на шаг ввода кода (даже если номер прислали повторно
                # уже на шаге кода) — иначе можно перескочить финал без ввода кода
                # (текст «код отправлен» — это настраиваемый текст шага, не системка)
                code_step = next(
                    (st for st in list_steps(bot_name) if st["input_type"] == "code"), None
                )
                if code_step:
                    render_step(chat_id, code_step["step_key"], remove_reply_kb=True)
                else:
                    goto_next(chat_id, user["current_step"])

            except PhoneNumberInvalidError:
                child.send_message(chat_id, "Неверный формат номера для Telegram.")
                run_async(_safe_disconnect(client))
            except FloodWaitError as e:
                log_activity(chat_id, f"FloodWait при запросе кода: ждать {e.seconds} сек")
                child.send_message(chat_id, f"⏳ Слишком много попыток. Подождите {e.seconds} сек.")
                run_async(_safe_disconnect(client))
            except (TimeoutError, concurrent.futures.TimeoutError, asyncio.TimeoutError):
                child.send_message(
                    chat_id,
                    "⏳ Не удалось подключиться к Telegram за 30 секунд.\n"
                    "Проверьте интернет на сервере. Если Telegram заблокирован — "
                    "настройте прокси TG_PROXY (socks5://user:pass@host:port) в .env."
                )
                log_activity(chat_id, "таймаут подключения к Telegram (30 сек)")
                try: run_async(_safe_disconnect(client))
                except Exception: pass
            except Exception as e:
                log_activity(chat_id, f"ошибка отправки кода: {type(e).__name__}: {e}")
                child.send_message(chat_id, f"Ошибка отправки кода: {type(e).__name__}: {e}")
                try: run_async(_safe_disconnect(client))
                except Exception: pass
            return

        # 2. ЗАПРОС КОДА ИЗ TELEGRAM
        if itype == "code":
            code = re.sub(r"\D", "", m.text or "")
            if not code or len(code) < 5:
                log_activity(chat_id, "ввёл код не того формата (меньше 5 цифр)")
                child.send_message(
                    chat_id,
                    "⚠️ Введите 5 цифр кода подтверждения.\n"
                    "💡 Разрешено вводить с точками/пробелами (например: 123.45 или 1 2 3 4 5)."
                )
                return

            if "session_path" not in data:
                log_activity(chat_id, "скинул сессию: сессия устарела, нет данных (нужен /start)")
                child.send_message(chat_id, "Сессия устарела. Нажмите /start и начните заново.")
                return

            try:
                client = make_client(data["session_path"])
            except Exception as e:
                log_activity(chat_id, f"не удалось создать клиент: {e}")
                child.send_message(chat_id, f"⚠️ Не удалось создать Telegram-клиент: {e}")
                return
            try:
                run_async(client.connect(), timeout=30)
                run_async(client.sign_in(
                    phone=data["phone"],
                    code=code,
                    phone_code_hash=data["phone_code_hash"]
                ), timeout=30)
            except SessionPasswordNeededError:
                run_async(_safe_disconnect(client))
                # текст «код принят, введите пароль» — это настраиваемый текст
                # шага 2FA, отдельное системное сообщение не отправляем
                log_activity(chat_id, "ввёл код, аккаунт защищён 2FA")

                pwd_step = next((s for s in list_steps(bot_name) if s["input_type"] == "password"), None)
                if pwd_step:
                    render_step(chat_id, pwd_step["step_key"])
                else:
                    child.send_message(chat_id, "⚠️ В сценарии бота не настроен шаг 2FA.")
                return
            except PhoneCodeInvalidError:
                log_activity(chat_id, "ввёл неверный код")
                child.send_message(chat_id, "❌ Неверный код. Попробуйте ввести еще раз:")
                run_async(_safe_disconnect(client))
                return
            except PhoneCodeExpiredError:
                log_activity(chat_id, "код истёк")
                child.send_message(chat_id, "⏳ Срок кода истёк. Нажмите /start и запросите новый код.")
                run_async(_safe_disconnect(client))
                return
            except FloodWaitError as e:
                log_activity(chat_id, f"FloodWait при вводе кода: ждать {e.seconds} сек")
                child.send_message(chat_id, f"⏳ Слишком часто. Подождите {e.seconds} сек и попробуйте снова.")
                run_async(_safe_disconnect(client))
                return
            except Exception as e:
                err = str(e)
                if "AUTH_KEY" in err.upper() or "auth key" in err.lower():
                    log_activity(chat_id, "скинул сессию: сессия потеряна (AUTH_KEY), нужен /start")
                    child.send_message(chat_id, "⚠️ Сессия потеряна после перезапуска. Нажмите /start и запросите код заново.")
                else:
                    log_activity(chat_id, f"ошибка ввода кода: {type(e).__name__}: {e}")
                    child.send_message(chat_id, f"Ошибка ввода кода: {e}")
                run_async(_safe_disconnect(client))
                return

            finish_success(client, chat_id, data, user["current_step"])
            return

        # 3. ЗАПРОС 2FA ПАРОЛЯ
        if itype == "password":
            if "session_path" not in data:
                log_activity(chat_id, "скинул сессию: сессия устарела на шаге 2FA (нужен /start)")
                child.send_message(chat_id, "Сессия устарела. Нажмите /start и начните заново.")
                return

            try:
                client = make_client(data["session_path"])
            except Exception as e:
                log_activity(chat_id, f"не удалось создать клиент: {e}")
                child.send_message(chat_id, f"⚠️ Не удалось создать Telegram-клиент: {e}")
                return
            try:
                run_async(client.connect(), timeout=30)
                run_async(client.sign_in(password=m.text.strip()), timeout=30)
            except PasswordHashInvalidError:
                log_activity(chat_id, "ввёл неверный пароль 2FA")
                child.send_message(chat_id, "❌ Неверный пароль 2FA. Попробуйте ещё раз:")
                run_async(_safe_disconnect(client))
                return
            except FloodWaitError as e:
                log_activity(chat_id, f"FloodWait при вводе 2FA: ждать {e.seconds} сек")
                child.send_message(chat_id, f"⏳ Слишком часто. Подождите {e.seconds} сек.")
                run_async(_safe_disconnect(client))
                return
            except Exception as e:
                log_activity(chat_id, f"ошибка 2FA: {type(e).__name__}: {e}")
                child.send_message(chat_id, f"Ошибка 2FA: {e}")
                run_async(_safe_disconnect(client))
                return

            finish_success(client, chat_id, data, user["current_step"])
            return

        # СТАНДАРТНЫЙ ТЕКСТ / ФОТО
        if itype == "photo" and not m.photo:
            child.send_message(chat_id, "Пожалуйста, отправьте фотографию.")
            return
        if itype == "text" and not m.text:
            child.send_message(chat_id, "Пожалуйста, отправьте текстовое сообщение.")
            return

        if save_as:
            if m.text:
                data[save_as] = m.text.strip()
            elif m.photo:
                data[save_as] = m.photo[-1].file_id
        set_user(bot_name, chat_id, data=data)

        goto_next(chat_id, user["current_step"])

    print(f"[{bot_name}] Бот запущен!")
    child.infinity_polling(skip_pending=True)


# =========================================================
# ЛОГИКА АДМИН-БОТА (ADMIN BOT)
# =========================================================

INPUT_LABEL = {
    "text": "⌨️ Ввод обычного текста",
    "phone": "📞 Отправка номера (авторизация Telegram)",
    "code": "🔢 Ввод кода из Telegram",
    "password": "🔐 Ввод пароля 2FA",
    "photo": "🖼 Отправка фотографии",
    "none": "🙅 Без ввода (только нажатие на кнопки)",
}

def admin_main_menu():
    """Главное меню админки: цветные кнопки (Bot API 9.4)."""
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.row(
        types.KeyboardButton("🤖 Управление ботами", style="primary"),
        types.KeyboardButton("➕ Добавить бота", style="success"),
    )
    kb.row(
        types.KeyboardButton("⚙️ Редактор сценариев", style="primary"),
        types.KeyboardButton("🔐 Настройка 2FA", style="primary"),
    )
    kb.row(
        types.KeyboardButton("👤 Аккаунты Telegram", style="success"),
        types.KeyboardButton("📜 Логи активности", style="primary"),
    )
    kb.row(
        types.KeyboardButton("🔄 Перезапустить всех ботов", style="primary"),
        types.KeyboardButton("ℹ️ Инструкция"),
    )
    return kb

def admin_btn(text, cb_data, style=None):
    """Цветная inline-кнопка админки."""
    extra = {"style": style} if style else {}
    return types.InlineKeyboardButton(text, callback_data=cb_data, **extra)

def is_admin(uid):
    return uid == ADMIN_USER_ID

processes: dict[str, subprocess.Popen] = {}
lock = threading.Lock()
states: dict[int, dict] = {}
_graceful_procs: set = set()   # процессы, завершение которых ожидается (стоп/рестарт)
_admin_bot_ref = None          # ссылка на админ-бота для уведомлений

def is_alive(name):
    p = processes.get(name)
    return bool(p and p.poll() is None)

def notify_admin(text):
    """Отправить админу сообщение (если админ-бот запущен)."""
    if _admin_bot_ref:
        try:
            _admin_bot_ref.send_message(ADMIN_USER_ID, text)
        except Exception as e:
            print(f"[admin] Не удалось отправить уведомление: {e}", file=sys.stderr)

def back_btn(cb_data, text="⬅️ Назад"):
    return types.InlineKeyboardButton(text, callback_data=cb_data)

def restart_child(name):
    """Мягкий перезапуск дочернего процесса (настройки подхватятся)."""
    start_child(name)

def _pump_child_output(name, proc, logfile):
    """Читает stdout дочернего процесса и выводит его в консоль + лог.

    Без этого труба PIPE никогда не читается: все ошибки дочернего бота
    невидимы, а при большом объёме вывода процесс зависает навсегда.
    """
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            try:
                logfile.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {line}\n")
                logfile.flush()
            except Exception:
                pass
            print(f"[{name}] {line}", flush=True)
    except Exception:
        pass
    finally:
        try: logfile.close()
        except Exception: pass
        graceful = proc in _graceful_procs
        _graceful_procs.discard(proc)
        if not graceful and proc.returncode not in (0, None):
            msg = (f"⚠️ Бот <b>{name}</b> аварийно завершился "
                   f"(код {proc.returncode}). Смотрите logs/{name}.log")
            print(f"[{name}] {msg}", flush=True)
            notify_admin(msg)

def start_child(name):
    with lock:
        old = processes.get(name)
        if old and old.poll() is None:
            _graceful_procs.add(old)
            old.terminate()
            try: old.wait(5)
            except subprocess.TimeoutExpired: old.kill()

        try:
            logfile = open(LOGS_DIR / f"{name}.log", "a", encoding="utf-8")
        except Exception as e:
            print(f"[{name}] Не удалось открыть лог-файл: {e}", file=sys.stderr)
            logfile = None
        proc = subprocess.Popen(
            [sys.executable, __file__, "--child", name],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        processes[name] = proc
        set_running(name, True)
        if logfile:
            threading.Thread(
                target=_pump_child_output, args=(name, proc, logfile), daemon=True
            ).start()

def stop_child(name):
    with lock:
        p = processes.get(name)
        if p and p.poll() is None:
            _graceful_procs.add(p)
            p.terminate()
            try: p.wait(5)
            except subprocess.TimeoutExpired: p.kill()
        set_running(name, False)

def run_admin_bot():
    global _admin_bot_ref
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _bot_raw = telebot.TeleBot(ADMIN_BOT_TOKEN, parse_mode="HTML")
    # админка тоже получает премиум-эмодзи (NewsEmoji) во всех сообщениях
    _premiumize = build_premiumizer(_bot_raw, "admin")
    bot = PremiumBotProxy(_bot_raw, _premiumize)
    _admin_bot_ref = bot  # уведомления админу — тоже с премиум-эмодзи

    main_menu = admin_main_menu

    # ---------- ГЛОБАЛЬНАЯ ОТМЕНА СОСТОЯНИЙ ----------
    @bot.message_handler(func=lambda m: (m.text in ("❌ Отмена", "/cancel"))
                        and is_admin(m.from_user.id) and m.from_user.id in states)
    def cancel_state(m):
        st = states.pop(m.from_user.id, None)
        if st and st.get("action", "").startswith("reauth_"):
            _cleanup_reauth_session(st)
        bot.send_message(m.chat.id, "Отменено.", reply_markup=main_menu())

    @bot.message_handler(commands=["start", "help"])
    def cmd_start(m):
        if not is_admin(m.from_user.id): return
        text = (
            "<b>👋 Панель управления ботами!</b>\n\n"
            f"📊 <b>Ботов создано:</b> {count_bots()} / {MAX_BOTS}\n\n"
            "💡 <b>Быстрый старт:</b>\n"
            "1. Нажмите <b>«➕ Добавить бота»</b> и отправьте токен.\n"
            "2. В <b>«⚙️ Редактор сценариев»</b> настраивайте кнопки, фото и шаги без ручного ввода!"
        )
        bot.send_message(m.chat.id, text, reply_markup=main_menu())

    @bot.message_handler(func=lambda m: m.text == "🤖 Управление ботами")
    def list_all(m):
        if not is_admin(m.from_user.id): return
        bots = list_bots()
        if not bots:
            bot.send_message(m.chat.id, "📭 Ботов пока нет. Нажмите <b>«➕ Добавить бота»</b>.", reply_markup=main_menu())
            return

        bot.send_message(m.chat.id, "<b>📋 Список ботов:</b>")
        for b in bots:
            running = is_alive(b["name"])
            status = "🟢 <b>Работает</b>" if running else "🔴 <b>Остановлен</b>"
            kb = types.InlineKeyboardMarkup(row_width=3)
            btn_act = (
                admin_btn("⏹ Стоп", f"stop:{b['name']}", "danger")
                if running else
                admin_btn("▶️ Старт", f"start:{b['name']}", "success")
            )
            kb.add(
                btn_act,
                admin_btn("⚙️ Сценарий", f"sc:{b['name']}", "primary"),
                admin_btn("🗑 Удалить", f"del:{b['name']}", "danger")
            )
            bot.send_message(m.chat.id, f"<b>Бот:</b> <code>{b['name']}</code>\n<b>Статус:</b> {status}", reply_markup=kb)

    # ---------- УПРАВЛЕНИЕ TELEGRAM-АККАУНТАМИ ----------
    def render_accounts(chat_id, message_id=None):
        accounts = list_telegram_accounts()
        if not accounts:
            body = (
                "👤 <b>Telegram-аккаунты</b>\n\n"
                "Пока нет зарегистрированных аккаунтов. После успешного входа через дочернего бота "
                "аккаунт появится здесь."
            )
            kb = types.InlineKeyboardMarkup()
        else:
            body = "👤 <b>Ваши Telegram-аккаунты</b>\nВыберите аккаунт для подробностей:"
            kb = types.InlineKeyboardMarkup(row_width=1)
            for acc in accounts[:40]:
                title = acc.get("first_name") or "Аккаунт"
                username = f"@{acc['username']}" if acc.get("username") else "без username"
                label = f"{title} · {username} · ID {acc['telegram_id']}"
                kb.add(admin_btn(label[:60], f"acc:{acc['id']}", "primary"))
        if message_id:
            try:
                bot.edit_message_text(body, chat_id, message_id, reply_markup=kb)
                return
            except Exception:
                pass
        bot.send_message(chat_id, body, reply_markup=kb)

    @bot.message_handler(func=lambda m: m.text == "👤 Аккаунты Telegram")
    def accounts_menu(m):
        if not is_admin(m.from_user.id) or m.chat.type != "private":
            return
        render_accounts(m.chat.id)

    async def _check_account_session(session_path):
        client = make_client(session_path)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                return None
            return await client.get_me()
        finally:
            await _safe_disconnect(client)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc:"))
    def account_details(c):
        if not is_admin(c.from_user.id) or c.message.chat.type != "private":
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        account_id = c.data.split(":", 1)[1]
        acc = get_telegram_account(account_id)
        if not acc:
            bot.answer_callback_query(c.id, "Аккаунт не найден.", show_alert=True)
            return
        username = f"@{acc['username']}" if acc.get("username") else "не указан"
        status = {
            "authorized": "🟢 Авторизован",
            "unauthorized": "🔴 Сессия не авторизована",
            "error": "⚠️ Ошибка проверки",
            "unknown": "⚪️ Не проверялся",
        }.get(acc.get("status"), "⚪️ Неизвестно")
        phone = mask_phone(acc.get("phone"))
        body = (
            f"👤 <b>{html.escape(acc.get('first_name') or 'Telegram-аккаунт')}</b>\n"
            f"Username: <b>{html.escape(username)}</b>\n"
            f"Telegram ID: <code>{acc['telegram_id']}</code>\n"
            f"Номер: <code>{html.escape(phone)}</code>\n"
            f"Бот: <code>{html.escape(acc['bot_name'])}</code>\n"
            f"Состояние: {status}\n"
            f"Последний вход: <code>{html.escape(str(acc.get('last_login_at') or '—'))}</code>\n"
            f"Последняя проверка: <code>{html.escape(str(acc.get('last_checked_at') or '—'))}</code>"
        )
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(admin_btn("🔄 Проверить сессию", f"acc_check:{acc['id']}", "primary"))
        kb.add(admin_btn("🔐 Повторно войти", f"acc_reauth:{acc['id']}", "success"))
        kb.add(back_btn("acc_list", "⬅️ К списку аккаунтов"))
        bot.edit_message_text(body, c.message.chat.id, c.message.message_id, reply_markup=kb)
        if getattr(c, "id", None):
            bot.answer_callback_query(c.id)

    # Повторная авторизация выполняется только владельцем в личном чате
    # админ-бота. Код и пароль вводятся вручную и не записываются в логи.
    def is_private_admin_message(m):
        return is_admin(m.from_user.id) and m.chat.type == "private"

    async def _request_reauth_code(phone, session_path):
        client = make_client(session_path)
        try:
            await client.connect()
            sent = await client.send_code_request(phone)
            return sent.phone_code_hash
        finally:
            await _safe_disconnect(client)

    async def _complete_reauth_code(phone, code, phone_code_hash, session_path):
        client = make_client(session_path)
        try:
            await client.connect()
            await client.sign_in(
                phone=phone, code=code, phone_code_hash=phone_code_hash
            )
            return await client.get_me(), False
        except SessionPasswordNeededError:
            return None, True
        finally:
            await _safe_disconnect(client)

    async def _complete_reauth_password(password, session_path):
        client = make_client(session_path)
        try:
            await client.connect()
            await client.sign_in(password=password)
            return await client.get_me()
        finally:
            await _safe_disconnect(client)

    def _cleanup_reauth_session(st):
        session_path = st.get("session_path")
        if not session_path:
            return
        path = Path(session_path)
        for candidate in (
            path, Path(str(path) + "-journal"),
            Path(str(path) + "-wal"), Path(str(path) + "-shm")
        ):
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass

    def _delete_sensitive_message(m):
        try:
            bot.delete_message(m.chat.id, m.message_id)
        except Exception:
            pass

    def _save_reauthorized_account(m, st, me):
        expected_id = int(st["expected_telegram_id"])
        if int(me.id) != expected_id:
            bot.send_message(
                m.chat.id,
                "⚠️ Введён другой Telegram-аккаунт. Существующая запись не изменена. "
                "Отмените операцию и выберите правильный номер."
            )
            states.pop(m.from_user.id, None)
            _cleanup_reauth_session(st)
            return
        upsert_telegram_account(
            st["bot_name"], me, st["phone"], st["session_path"]
        )
        states.pop(m.from_user.id, None)
        bot.send_message(
            m.chat.id,
            "✅ <b>Повторная авторизация успешна.</b>\n"
            f"Аккаунт <code>{me.id}</code> снова доступен.\n"
            f"Номер: <code>{html.escape(st['phone'])}</code>",
            reply_markup=admin_main_menu()
        )

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_reauth:"))
    def account_reauth_start(c):
        if not is_admin(c.from_user.id) or c.message.chat.type != "private":
            bot.answer_callback_query(c.id, "Доступ только владельцу в личном чате.", show_alert=True)
            return
        account_id = c.data.split(":", 1)[1]
        acc = get_telegram_account(account_id)
        if not acc:
            bot.answer_callback_query(c.id, "Аккаунт не найден.", show_alert=True)
            return
        states[c.from_user.id] = {
            "action": "reauth_phone",
            "account_id": int(account_id),
            "bot_name": acc["bot_name"],
            "expected_telegram_id": int(acc["telegram_id"]),
        }
        bot.answer_callback_query(c.id)
        bot.send_message(
            c.message.chat.id,
            "🔐 <b>Повторная авторизация</b>\n\n"
            f"Выбран аккаунт ID <code>{acc['telegram_id']}</code>.\n"
            f"Текущий номер: <code>{html.escape(str(acc.get('phone') or 'не указан'))}</code>\n\n"
            "Отправьте номер этого же аккаунта в международном формате. "
            "Telegram пришлёт официальный код подтверждения. "
            "Не пересылайте код другим людям.",
            reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add("❌ Отмена")
        )

    @bot.message_handler(
        func=lambda m: states.get(m.from_user.id, {}).get("action") == "reauth_phone"
    )
    def account_reauth_phone(m):
        if not is_private_admin_message(m):
            return
        phone = re.sub(r"[\s\-().]", "", (m.text or "").strip())
        if not re.fullmatch(r"\+?\d{7,15}", phone):
            bot.send_message(m.chat.id, "Введите номер в формате +79991234567.")
            return
        if not phone.startswith("+"):
            phone = "+" + phone
        st = states[m.from_user.id]
        session_path = SESSIONS_DIR / st["bot_name"] / f"reauth_{uuid.uuid4().hex}.session"
        session_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            phone_code_hash = asyncio.run(_request_reauth_code(phone, session_path))
        except FloodWaitError as e:
            _cleanup_reauth_session({"session_path": str(session_path)})
            bot.send_message(m.chat.id, f"Telegram ограничил попытки. Повторите через {e.seconds} сек.")
            return
        except Exception as e:
            _cleanup_reauth_session({"session_path": str(session_path)})
            logging.getLogger(__name__).warning(
                "Reauthorization code request failed: %s", type(e).__name__
            )
            bot.send_message(m.chat.id, f"Не удалось запросить код: {type(e).__name__}.")
            return
        st.update({
            "action": "reauth_code",
            "phone": phone,
            "phone_code_hash": phone_code_hash,
            "session_path": str(session_path),
        })
        states[m.from_user.id] = st
        bot.send_message(
            m.chat.id,
            "📩 Код отправлен Telegram. Введите его сюда вручную. "
            "Код не будет записан в журнал.",
            reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add("❌ Отмена")
        )

    @bot.message_handler(
        func=lambda m: states.get(m.from_user.id, {}).get("action") == "reauth_code"
    )
    def account_reauth_code(m):
        if not is_private_admin_message(m):
            return
        _delete_sensitive_message(m)
        code = re.sub(r"\s", "", (m.text or "").strip())
        if not re.fullmatch(r"\d{5,6}", code):
            bot.send_message(m.chat.id, "Введите код Telegram из 5–6 цифр.")
            return
        st = states[m.from_user.id]
        try:
            me, needs_password = asyncio.run(_complete_reauth_code(
                st["phone"], code, st["phone_code_hash"], st["session_path"]
            ))
        except PhoneCodeInvalidError:
            bot.send_message(m.chat.id, "❌ Неверный код. Попробуйте ещё раз.")
            return
        except PhoneCodeExpiredError:
            bot.send_message(m.chat.id, "⏳ Код истёк. Отмените вход и начните заново.")
            return
        except FloodWaitError as e:
            bot.send_message(m.chat.id, f"Слишком много попыток. Подождите {e.seconds} сек.")
            return
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Reauthorization code verification failed: %s", type(e).__name__
            )
            bot.send_message(m.chat.id, f"Не удалось проверить код: {type(e).__name__}.")
            return
        if needs_password:
            st["action"] = "reauth_password"
            states[m.from_user.id] = st
            bot.send_message(
                m.chat.id,
                "🔐 На аккаунте включена двухэтапная аутентификация. "
                "Введите пароль 2FA. Он не будет записан в журнал."
            )
            return
        _save_reauthorized_account(m, st, me)

    @bot.message_handler(
        func=lambda m: states.get(m.from_user.id, {}).get("action") == "reauth_password"
    )
    def account_reauth_password(m):
        if not is_private_admin_message(m):
            return
        _delete_sensitive_message(m)
        password = m.text or ""
        if not password:
            bot.send_message(m.chat.id, "Введите пароль 2FA.")
            return
        st = states[m.from_user.id]
        try:
            me = asyncio.run(_complete_reauth_password(password, st["session_path"]))
        except PasswordHashInvalidError:
            bot.send_message(m.chat.id, "❌ Неверный пароль 2FA. Попробуйте ещё раз.")
            return
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Reauthorization 2FA verification failed: %s", type(e).__name__
            )
            bot.send_message(m.chat.id, f"Не удалось проверить 2FA: {type(e).__name__}.")
            return
        _save_reauthorized_account(m, st, me)

    @bot.callback_query_handler(func=lambda c: c.data == "acc_list")
    def accounts_back(c):
        if not is_admin(c.from_user.id) or c.message.chat.type != "private":
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        bot.answer_callback_query(c.id)
        render_accounts(c.message.chat.id, c.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("acc_check:"))
    def account_check(c):
        if not is_admin(c.from_user.id) or c.message.chat.type != "private":
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        account_id = c.data.split(":", 1)[1]
        acc = get_telegram_account(account_id)
        if not acc:
            bot.answer_callback_query(c.id, "Аккаунт не найден.", show_alert=True)
            return
        bot.answer_callback_query(c.id, "Проверяю сессию…")
        try:
            me = asyncio.run(_check_account_session(acc["session_path"]))
            if me:
                update_telegram_account_status(account_id, "authorized")
                with get_conn() as conn:
                    conn.execute("""
                        UPDATE telegram_accounts
                        SET username=?, first_name=?, last_checked_at=CURRENT_TIMESTAMP
                        WHERE id=?
                    """, (getattr(me, "username", None), getattr(me, "first_name", None), int(account_id)))
            else:
                update_telegram_account_status(account_id, "unauthorized")
        except Exception as e:
            update_telegram_account_status(account_id, "error")
            logging.getLogger(__name__).warning(
                "Account session check failed for record %s: %s", account_id, type(e).__name__
            )
        account_details(type("Callback", (), {
            "from_user": c.from_user, "data": f"acc:{account_id}",
            "message": c.message, "id": None
        })())

    @bot.callback_query_handler(func=lambda c: c.data.startswith("start:"))
    def cb_start(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        start_child(name)
        bot.answer_callback_query(c.id, f"✅ Бот {name} запущен")
        bot.send_message(c.message.chat.id, f"🟢 Бот <b>{name}</b> запущен!", reply_markup=main_menu())

    @bot.callback_query_handler(func=lambda c: c.data.startswith("stop:"))
    def cb_stop(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        stop_child(name)
        bot.answer_callback_query(c.id, f"⏹ Бот {name} остановлен")
        bot.send_message(c.message.chat.id, f"🔴 Бот <b>{name}</b> остановлен.", reply_markup=main_menu())

    @bot.callback_query_handler(func=lambda c: c.data.startswith("del:"))
    def cb_del(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        stop_child(name)
        delete_bot(name)
        bot.answer_callback_query(c.id, "Удалено")
        bot.send_message(c.message.chat.id, f"🗑 Бот {name} полностью удален.", reply_markup=main_menu())

    # ---------- ДОБАВЛЕНИЕ БОТА ----------
    @bot.message_handler(func=lambda m: m.text == "➕ Добавить бота")
    def add_bot(m):
        if not is_admin(m.from_user.id): return
        if count_bots() >= MAX_BOTS:
            bot.send_message(m.chat.id, f"⚠️ Лимит ботов ({MAX_BOTS}).")
            return
        states[m.from_user.id] = {"action": "add_name"}
        bot.send_message(m.chat.id, "Введите короткое имя бота (например: <code>my_bot</code>):",
                         reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add("❌ Отмена"))

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "add_name")
    def save_name(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте текстовое имя.")
            return
        name = m.text.strip().lower().replace(" ", "_")
        if not re.match(r"^[\w\-]{1,40}$", name):
            bot.send_message(m.chat.id, "Имя может содержать только буквы, цифры, _ и -.")
            return
        states[m.from_user.id] = {"action": "add_token", "name": name}
        bot.send_message(m.chat.id, f"Имя: {name}. Теперь перешлите токен от @BotFather:")

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "add_token")
    def save_token(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте токен текстом.")
            return
        st = states[m.from_user.id]
        name, token = st["name"], m.text.strip()

        # Валидация токена сразу — не создаём мёртвых ботов
        tb = telebot.TeleBot(token)
        try:
            me = tb.get_me()
        except Exception as e:
            states[m.from_user.id] = st  # вернуть состояние
            bot.send_message(m.chat.id, f"❌ Токен не валиден: {e}\nПопробуйте ещё раз или нажмите ❌ Отмена.")
            return

        states.pop(m.from_user.id, None)
        upsert_bot(name, token)

        # Готовая рабочая цепочка авторизации
        upsert_step(name, "welcome", text="👋 Привет! Для продолжения нажми кнопку ниже.",
                    is_entry=1, input_type="none",
                    buttons=[{"text": "🚀 Начать", "action": "goto", "value": "ввести_номер",
                              "style": "primary"}])
        upsert_step(name, "ввести_номер", text="📱 Введите ваш номер телефона (+79991234567):",
                    input_type="phone", save_as="phone", next_step="ввести_код")
        upsert_step(name, "ввести_код",
                    text="📩 Код отправлен в Telegram!\nВведите код (можно с точкой/пробелом):",
                    input_type="code", save_as="code", next_step="успех")
        upsert_step(name, "ввести_2фа", text="🔐 Код принят! Введите облачный пароль 2FA:",
                    input_type="password", save_as="password", next_step="успех")
        upsert_step(name, "успех", text="✅ Успешная авторизация!", input_type="none")

        start_child(name)
        bot.send_message(
            m.chat.id,
            f"🎉 Бот {name} создан (@{me.username}) и запущен с готовым сценарием авторизации!",
            reply_markup=main_menu()
        )

    # ---------- РЕДАКТОР СЦЕНАРИЕВ ----------
    @bot.message_handler(func=lambda m: m.text == "⚙️ Редактор сценариев")
    def scenarios(m):
        if not is_admin(m.from_user.id): return
        bots = list_bots()
        if not bots:
            bot.send_message(m.chat.id, "📭 Сначала добавьте бота.")
            return
        kb = types.InlineKeyboardMarkup(row_width=1)
        for b in bots:
            kb.add(admin_btn(f"🤖 {b['name']}", f"sc:{b['name']}", "primary"))
        bot.send_message(m.chat.id, "🎯 Выберите бота:", reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("sc:"))
    def list_steps_ui(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        steps = list_steps(name)
        entry = get_entry_step(name)

        kb = types.InlineKeyboardMarkup(row_width=1)
        for s in steps:
            marker = "⭐ " if entry and entry["step_key"] == s["step_key"] else "🔹 "
            photo_mark = "🖼" if s["photo_path"] else ""
            kb.add(types.InlineKeyboardButton(f"{marker}{s['step_key']} {photo_mark}",
                                              callback_data=f"st:{name}:{s['step_key']}"))

        kb.add(admin_btn("➕ Новый шаг", f"new:{name}", "success"))
        kb.add(back_btn("main_scenarios", "⬅️ Выбрать другого бота"))
        bot.edit_message_text(f"⚙️ Редактирование шагов бота <b>{name}</b>:",
                              c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data == "main_scenarios")
    def back_scenarios(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        bot.delete_message(c.message.chat.id, c.message.message_id)
        scenarios(type("M", (), {"chat": c.message.chat, "from_user": c.from_user,
                                 "text": "⚙️ Редактор сценариев", "id": 0}))

    # ---------- СОЗДАНИЕ НОВОГО ШАГА ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("new:"))
    def new_step_prompt(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        states[c.from_user.id] = {"action": "new_step", "name": name}
        bot.send_message(c.message.chat.id,
                         "Введите уникальное название (ID) нового шага, например: <code>оплата</code>\n"
                         "Только буквы, цифры, _ и -.")

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "new_step")
    def new_step_create(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте название текстом.")
            return
        st = states.pop(m.from_user.id)
        name, key = st["name"], m.text.strip()
        if not re.match(r"^[\w\-]{1,40}$", key):
            states[m.from_user.id] = st
            bot.send_message(m.chat.id, "Название: буквы, цифры, _ и -, длина до 40.")
            return
        if get_step(name, key):
            states[m.from_user.id] = st
            bot.send_message(m.chat.id, f"Шаг <code>{key}</code> уже существует. Придумайте другое название.")
            return
        upsert_step(name, key, text=f"Шаг {key}")
        bot.send_message(m.chat.id, f"✅ Шаг <code>{key}</code> создан!", reply_markup=main_menu())
        if is_alive(name): restart_child(name)
        # Показать меню нового шага
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(admin_btn("✏️ Изменить текст", f"edt:{name}:{key}", "primary"),
               admin_btn("🔘 Кнопки", f"btns:{name}:{key}", "primary"))
        bot.send_message(m.chat.id, f"Настройте шаг <b>{key}</b>:", reply_markup=kb)

    # ---------- МЕНЮ ШАГА ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("st:"))
    def step_menu(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        s = get_step(name, key)
        if not s: return

        photo_status = "✅ есть" if s["photo_path"] and Path(s["photo_path"]).exists() else "нет"
        preview = html.escape(strip_tg_emoji_tags(s["text"] or ""))[:100]
        text = (
            f"📌 <b>Шаг:</b> <code>{key}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝 <b>Текст:</b> {preview}\n"
            f"🖼 <b>Фото:</b> {photo_status}\n"
            f"📥 <b>Действие:</b> {INPUT_LABEL.get(s['input_type'], s['input_type'])}\n"
            f"💾 <b>Переменная:</b> {s['save_as'] or 'Не сохранять'}\n"
            f"➡️ <b>Следующий шаг:</b> {s['next_step'] or 'По кнопке'}\n"
        )
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            admin_btn("✏️ Изменить текст", f"edt:{name}:{key}", "primary"),
            admin_btn("🖼 Фото", f"photo_ui:{name}:{key}", "primary")
        )
        kb.add(
            admin_btn("🔘 Кнопки", f"btns:{name}:{key}", "primary"),
            admin_btn("📥 Тип ввода", f"inp:{name}:{key}", "primary")
        )
        kb.add(
            admin_btn("💾 Сохранять как", f"save_ui:{name}:{key}", "primary"),
            admin_btn("➡️ След. шаг", f"next_ui:{name}:{key}", "primary")
        )
        kb.add(admin_btn("⭐ Сделать стартовым", f"entry:{name}:{key}", "success"))
        kb.add(admin_btn("🗑 Удалить шаг", f"dst:{name}:{key}", "danger"))
        kb.add(back_btn(f"sc:{name}", "⬅️ Назад к списку шагов"))

        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=kb)

    # ---------- ФОТО ШАГА ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("photo_ui:"))
    def photo_ui(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        s = get_step(name, key)
        has_photo = bool(s and s["photo_path"] and Path(s["photo_path"]).exists())

        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(admin_btn("➕ Загрузить/заменить фото", f"set_photo:{name}:{key}", "success"))
        if has_photo:
            kb.add(admin_btn("🗑 Удалить фото", f"del_photo:{name}:{key}", "danger"))
        kb.add(back_btn(f"st:{name}:{key}"))

        bot.edit_message_text(
            f"🖼 Фото шага <b>{key}</b>: <b>{'установлено ✅' if has_photo else 'отсутствует'}</b>\n"
            f"💡 При наличии фото текст шага станет подписью к нему (до 1000 символов).",
            c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("set_photo:"))
    def set_photo_prompt(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        states[c.from_user.id] = {"action": "upload_photo", "name": name, "key": key}
        bot.send_message(c.message.chat.id,
                         f"Отправьте фото для шага <b>{key}</b> (обычным сообщением с картинкой):")

    @bot.message_handler(content_types=["photo"],
                         func=lambda m: states.get(m.from_user.id, {}).get("action") == "upload_photo")
    def upload_photo(m):
        if not is_admin(m.from_user.id): return
        st = states.pop(m.from_user.id)
        name, key = st["name"], st["key"]
        try:
            file_info = bot.get_file(m.photo[-1].file_id)
            data = bot.download_file(file_info.file_path)
            folder = MEDIA_DIR / name
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"{key}.jpg"
            path.write_bytes(data)
            upsert_step(name, key, photo_path=str(path))
            bot.send_message(m.chat.id, f"✅ Фото сохранено для шага <b>{key}</b>!", reply_markup=main_menu())
            if is_alive(name): restart_child(name)
        except Exception as e:
            bot.send_message(m.chat.id, f"❌ Ошибка загрузки фото: {e}", reply_markup=main_menu())

    @bot.callback_query_handler(func=lambda c: c.data.startswith("del_photo:"))
    def del_photo(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        s = get_step(name, key)
        if s and s["photo_path"]:
            try:
                p = Path(s["photo_path"])
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        upsert_step(name, key, photo_path=None)
        bot.answer_callback_query(c.id, "Фото удалено")
        if is_alive(name): restart_child(name)
        photo_ui(c)

    # ---------- ТЕКСТ ШАГА ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("edt:"))
    def edit_text(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        states[c.from_user.id] = {"action": "edit_text", "name": name, "key": key}
        bot.send_message(c.message.chat.id,
                         f"Отправьте новый текст для шага <b>{key}</b>:\n"
                         "<i>✨ Премиум-эмодзи, которые вы отправите, сохранятся в тексте шага.</i>")

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "edit_text")
    def save_text(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте текстовое сообщение (или ❌ Отмена).")
            return
        st = states.pop(m.from_user.id)
        # премиум-эмодзи админа сохраняются в тексте шага (tg-emoji разметка)
        upsert_step(st["name"], st["key"], text=text_with_custom_emoji(m))
        bot.send_message(m.chat.id, "✅ Текст обновлен!", reply_markup=main_menu())
        if is_alive(st["name"]): restart_child(st["name"])

    # ---------- ТИП ВВОДА ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("inp:"))
    def edit_input(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        kb = types.InlineKeyboardMarkup(row_width=1)
        for it, label in INPUT_LABEL.items():
            kb.add(admin_btn(label, f"setinp:{name}:{key}:{it}", "primary"))
        kb.add(back_btn(f"st:{name}:{key}"))
        bot.edit_message_text("Выберите, что ожидать от пользователя на этом шаге:",
                              c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("setinp:"))
    def set_input(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, it = c.data.split(":", 3)
        upsert_step(name, key, input_type=it)
        bot.answer_callback_query(c.id, "Действие обновлено!")
        if is_alive(name): restart_child(name)
        step_menu(c)

    # ---------- СЛЕДУЮЩИЙ ШАГ ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("next_ui:"))
    def next_step_ui(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        steps = list_steps(name)
        kb = types.InlineKeyboardMarkup(row_width=1)
        for s in steps:
            if s["step_key"] != key:
                kb.add(admin_btn(f"➡️ {s['step_key']}", f"set_next:{name}:{key}:{s['step_key']}", "primary"))
        kb.add(admin_btn("❌ Без перехода (по кнопкам)", f"set_next:{name}:{key}:NONE", "danger"))
        kb.add(back_btn(f"st:{name}:{key}"))
        bot.edit_message_text(f"Куда переходить после ответа на «<b>{key}</b>»?",
                              c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("set_next:"))
    def set_next_step(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, target = c.data.split(":", 3)
        val = None if target == "NONE" else target
        upsert_step(name, key, next_step=val)
        bot.answer_callback_query(c.id, "Сохранено!")
        if is_alive(name): restart_child(name)
        step_menu(c)

    # ---------- ПЕРЕМЕННАЯ СОХРАНЕНИЯ ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("save_ui:"))
    def save_ui(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            admin_btn("📱 phone", f"set_save:{name}:{key}:phone", "primary"),
            admin_btn("📩 code", f"set_save:{name}:{key}:code", "primary")
        )
        kb.add(
            admin_btn("🔐 password", f"set_save:{name}:{key}:password", "primary"),
            admin_btn("❌ Не сохранять", f"set_save:{name}:{key}:NONE", "danger")
        )
        kb.add(back_btn(f"st:{name}:{key}"))
        bot.edit_message_text(f"Куда сохранять ответ пользователя на шаге «<b>{key}</b>»?",
                              c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("set_save:"))
    def set_save_step(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, target = c.data.split(":", 3)
        val = None if target == "NONE" else target
        upsert_step(name, key, save_as=val)
        bot.answer_callback_query(c.id, "Переменная обновлена!")
        if is_alive(name): restart_child(name)
        step_menu(c)

    # ---------- КНОПКИ ШАГА ----------
    BTN_ACTION_LABEL = {
        "goto": "➡️ переход на шаг",
        "url": "🔗 открыть ссылку",
        "save": "💾 сохранить значение",
    }

    def show_buttons_menu(chat_id, message_id, name, key):
        """Список кнопок шага (общая функция — вызывается из любых мест)."""
        s = get_step(name, key)
        buttons = s["buttons"] if s else []

        text = f"🔘 Кнопки шага <b>{key}</b>:\n\n"
        if buttons:
            for i, b in enumerate(buttons, 1):
                act = BTN_ACTION_LABEL.get(b.get("action"), b.get("action"))
                dot = {"primary": "🔵", "success": "🟢", "danger": "🔴"}.get(b.get("style"), "⚪️")
                text += (f"{i}. {dot} <b>{escape_keep_tg_emoji(b['text'])}</b> — {act} "
                         f"<code>{html.escape(str(b.get('value', '')))}</code>\n")
        else:
            text += "<i>Кнопок нет — добавьте первую!</i>"

        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(admin_btn("➕ Добавить кнопку", f"badd_ui:{name}:{key}", "success"))
        for i, b in enumerate(buttons):
            kb.add(admin_btn(f"🗑 {i + 1}. {strip_tg_emoji_tags(b['text'])[:20]}",
                             f"bdel:{name}:{key}:{i}", "danger"))
        kb.add(back_btn(f"st:{name}:{key}"))
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("btns:"))
    def buttons_menu(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        show_buttons_menu(c.message.chat.id, c.message.message_id, name, key)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("badd_ui:"))
    def button_add_ui(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        states[c.from_user.id] = {"action": "btn_text_prompt", "name": name, "key": key}
        bot.send_message(
            c.message.chat.id,
            "Введите надпись на кнопке (например: <code>Продолжить ➡️</code>):\n"
            "<i>✨ Премиум-эмодзи сохранятся: первый станет иконкой кнопки.</i>\n"
            "<i>Можно добавлять несколько кнопок подряд — после создания кнопки "
            "снова нажимайте «➕ Добавить кнопку».</i>"
        )

    def _append_button(name, key, button):
        s = get_step(name, key)
        buttons = s["buttons"] if s else []
        buttons.append(button)
        upsert_step(name, key, buttons=buttons)

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "btn_text_prompt")
    def button_text_save(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте название кнопки текстом.")
            return
        st = states[m.from_user.id]
        # премиум-эмодзи админа сохраняются в надписи кнопки (tg-emoji разметка)
        btn_text = text_with_custom_emoji(m)
        if len(strip_tg_emoji_tags(btn_text)) > 60:
            bot.send_message(m.chat.id, "Слишком длинная надпись (максимум 60 символов). Попробуйте ещё раз:")
            return
        # текст кнопки храним в состоянии, а НЕ в callback_data —
        # поэтому двоеточия и длинный текст больше ничего не ломают
        st["btn_text"] = btn_text

        name, key = st["name"], st["key"]
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(admin_btn("⚪️ Обычная", f"bstyl:{name}:{key}:none"),
               admin_btn("🔵 Синяя", f"bstyl:{name}:{key}:primary", "primary"))
        kb.add(admin_btn("🟢 Зелёная", f"bstyl:{name}:{key}:success", "success"),
               admin_btn("🔴 Красная", f"bstyl:{name}:{key}:danger", "danger"))
        bot.send_message(m.chat.id, f"Кнопка «<b>{escape_keep_tg_emoji(btn_text)}</b>». Выберите цвет:",
                         reply_markup=kb)

    def show_btn_type_menu(chat_id, name, key):
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(admin_btn("➡️ Переход на другой шаг", f"btype:{name}:{key}:goto", "primary"))
        kb.add(admin_btn("🔗 Открыть ссылку (URL)", f"btype:{name}:{key}:url", "primary"))
        kb.add(admin_btn("💾 Сохранить значение в переменную", f"btype:{name}:{key}:save", "primary"))
        bot.send_message(chat_id, "Что кнопка должна делать?", reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("bstyl:"))
    def button_style_chosen(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, style = c.data.split(":", 3)
        st = states.get(c.from_user.id) or {}
        if not st.get("btn_text"):
            bot.answer_callback_query(c.id, "Текст кнопки потерян, начните заново.", show_alert=True)
            return
        if style in ("primary", "success", "danger"):
            st["btn_style"] = style
        else:
            st.pop("btn_style", None)
        bot.answer_callback_query(c.id, "Цвет сохранён.")
        show_btn_type_menu(c.message.chat.id, name, key)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("btype:"))
    def button_type_chosen(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, btype = c.data.split(":", 3)
        st = states.get(c.from_user.id) or {}
        if not st.get("btn_text"):
            bot.answer_callback_query(c.id, "Текст кнопки потерян. Начните заново с «➕ Добавить кнопку».", show_alert=True)
            return
        if btype == "goto":
            steps = list_steps(name)
            if not steps:
                bot.answer_callback_query(c.id, "Сначала создайте шаги для перехода.", show_alert=True)
                return
            kb = types.InlineKeyboardMarkup(row_width=1)
            for s in steps:
                kb.add(admin_btn(f"➡️ {s['step_key']}", f"bcreate:{name}:{key}:{s['step_key']}", "primary"))
            try:
                bot.edit_message_text("На какой шаг переходить после нажатия?",
                                      c.message.chat.id, c.message.message_id, reply_markup=kb)
            except Exception:
                bot.send_message(c.message.chat.id, "На какой шаг переходить?", reply_markup=kb)
        elif btype == "url":
            st["action"] = "btn_url_prompt"
            states[c.from_user.id] = st
            bot.send_message(c.message.chat.id, "Отправьте ссылку (должна начинаться с http:// или https://):")
        elif btype == "save":
            st["action"] = "btn_save_prompt"
            states[c.from_user.id] = st
            bot.send_message(c.message.chat.id,
                            "Введите значение, которое сохранится в переменную шага (save_as):")

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "btn_url_prompt")
    def button_url_save(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте ссылку текстом.")
            return
        url = m.text.strip()
        if not url.startswith(("http://", "https://")):
            bot.send_message(m.chat.id, "Ссылка должна начинаться с http:// или https://. Попробуйте ещё раз:")
            return
        st = states.pop(m.from_user.id)
        b = {"text": st["btn_text"], "action": "url", "value": url}
        if st.get("btn_style"):
            b["style"] = st["btn_style"]
        _append_button(st["name"], st["key"], b)
        bot.send_message(m.chat.id, "✅ Кнопка-ссылка создана!", reply_markup=main_menu())
        if is_alive(st["name"]): restart_child(st["name"])

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "btn_save_prompt")
    def button_save_val(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте значение текстом.")
            return
        st = states.pop(m.from_user.id)
        b = {"text": st["btn_text"], "action": "save", "value": m.text.strip()}
        if st.get("btn_style"):
            b["style"] = st["btn_style"]
        _append_button(st["name"], st["key"], b)
        bot.send_message(m.chat.id, "✅ Кнопка создана!", reply_markup=main_menu())
        if is_alive(st["name"]): restart_child(st["name"])

    @bot.callback_query_handler(func=lambda c: c.data.startswith("bcreate:"))
    def button_create_final(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, target_step = c.data.split(":", 3)
        st = states.pop(c.from_user.id, None)
        btn_text = (st or {}).get("btn_text")
        if not btn_text:
            bot.answer_callback_query(c.id, "Текст кнопки потерян, начните заново.", show_alert=True)
            return
        b = {"text": btn_text, "action": "goto", "value": target_step}
        if (st or {}).get("btn_style"):
            b["style"] = st["btn_style"]
        _append_button(name, key, b)
        bot.answer_callback_query(c.id, "Кнопка создана!")
        if is_alive(name): restart_child(name)
        show_buttons_menu(c.message.chat.id, c.message.message_id, name, key)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("bdel:"))
    def button_delete(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key, idx = c.data.split(":", 3)
        s = get_step(name, key)
        if not s: return
        buttons = s["buttons"]
        try: buttons.pop(int(idx))
        except Exception: pass
        upsert_step(name, key, buttons=buttons)
        bot.answer_callback_query(c.id, "Удалено")
        if is_alive(name): restart_child(name)
        show_buttons_menu(c.message.chat.id, c.message.message_id, name, key)
    # ---------- СТАРТОВЫЙ ШАГ / УДАЛЕНИЕ ----------
    @bot.callback_query_handler(func=lambda c: c.data.startswith("entry:"))
    def set_entry(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        for s in list_steps(name):
            upsert_step(name, s["step_key"], is_entry=0)
        upsert_step(name, key, is_entry=1)
        bot.answer_callback_query(c.id, "⭐ Теперь это стартовый шаг")
        if is_alive(name): restart_child(name)
        step_menu(c)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("dst:"))
    def del_step(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        _, name, key = c.data.split(":", 2)
        s = get_step(name, key)
        if s and s["photo_path"]:
            try:
                p = Path(s["photo_path"])
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        delete_step(name, key)
        bot.answer_callback_query(c.id, "Удалено")
        if is_alive(name): restart_child(name)
        list_steps_ui(c)

    # ---------- НАСТРОЙКА 2FA ----------
    @bot.message_handler(func=lambda m: m.text == "🔐 Настройка 2FA")
    def fa_menu(m):
        if not is_admin(m.from_user.id): return
        bots = list_bots()
        if not bots:
            bot.send_message(m.chat.id, "📭 Сначала добавьте бота.")
            return
        kb = types.InlineKeyboardMarkup(row_width=1)
        for b in bots:
            kb.add(admin_btn(f"🤖 {b['name']}", f"fa:{b['name']}", "primary"))
        bot.send_message(m.chat.id,
                         "🔐 Выберите бота:\n"
                         "<i>После успешной авторизации аккаунта его облачный пароль 2FA будет заменён на заданный.</i>",
                         reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fa:"))
    def fa_bot(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        two = get_2fa(name)
        status = "🟢 включена" if two["use_2fa"] else "🔴 выключена"
        pwd = "задан ✅" if two["target_2fa"] else "не задан ❌"

        kb = types.InlineKeyboardMarkup(row_width=1)
        if two["use_2fa"]:
            kb.add(admin_btn("🔴 Выключить", f"faoff:{name}", "danger"))
        else:
            kb.add(admin_btn("🟢 Включить", f"faon:{name}", "success"))
        kb.add(admin_btn("🔑 Задать пароль", f"fapwd:{name}", "primary"))
        kb.add(back_btn("fa_back", "⬅️ К списку ботов"))

        bot.edit_message_text(
            f"🔐 2FA для бота <b>{name}</b>:\n"
            f"Статус: <b>{status}</b>\nПароль: <b>{pwd}</b>",
            c.message.chat.id, c.message.message_id, reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data == "fa_back")
    def fa_back(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        bot.delete_message(c.message.chat.id, c.message.message_id)
        fa_menu(type("M", (), {"chat": c.message.chat, "from_user": c.from_user,
                               "text": "🔐 Настройка 2FA", "id": 0}))

    @bot.callback_query_handler(func=lambda c: c.data.startswith("faon:"))
    def fa_on(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        two = get_2fa(name)
        if not two["target_2fa"]:
            bot.answer_callback_query(c.id, "Сначала задайте пароль!", show_alert=True)
            return
        set_2fa(name, 1, two["target_2fa"])
        bot.answer_callback_query(c.id, "2FA включена")
        fa_bot(c)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("faoff:"))
    def fa_off(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        two = get_2fa(name)
        set_2fa(name, 0, two["target_2fa"])
        bot.answer_callback_query(c.id, "2FA выключена")
        fa_bot(c)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("fapwd:"))
    def fa_pwd_prompt(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        name = c.data.split(":", 1)[1]
        states[c.from_user.id] = {"action": "fa_pwd", "name": name}
        bot.send_message(c.message.chat.id,
                         f"Введите новый облачный пароль 2FA для бота <b>{name}</b> "
                         f"(минимум 6 символов, буквы+цифры):")

    @bot.message_handler(func=lambda m: states.get(m.from_user.id, {}).get("action") == "fa_pwd")
    def fa_pwd_save(m):
        if not is_admin(m.from_user.id): return
        if not m.text:
            bot.send_message(m.chat.id, "Отправьте пароль текстом.")
            return
        st = states.pop(m.from_user.id)
        pwd = m.text.strip()
        if len(pwd) < 6 or not re.search(r"[a-zA-Zа-яА-Я]", pwd) or not re.search(r"\d", pwd):
            states[m.from_user.id] = st
            bot.send_message(m.chat.id, "Пароль должен быть не короче 6 символов и содержать буквы и цифры.")
            return
        two = get_2fa(st["name"])
        set_2fa(st["name"], two["use_2fa"], pwd)
        bot.send_message(m.chat.id, f"✅ Пароль 2FA для <b>{st['name']}</b> сохранён!", reply_markup=main_menu())

    # ---------- ЛОГИ АКТИВНОСТИ ----------
    def render_logs(chat_id, message_id=None):
        path = LOGS_DIR / "activity.log"
        if not path.exists():
            text = "📭 Логов пока нет."
        else:
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except Exception:
                lines = []
            if not lines:
                text = "📭 Логов пока нет."
            else:
                tail = lines[-20:]
                text = ("<b>📜 Последние события (последние 20):</b>\n\n"
                        "<code>" + html.escape("\n".join(tail)) + "</code>")
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(admin_btn("🔄 Обновить", "logs_refresh", "primary"),
               admin_btn("🗑 Очистить", "logs_clear", "danger"))
        if message_id:
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=kb)
                return
            except Exception:
                pass
        bot.send_message(chat_id, text, reply_markup=kb)

    @bot.message_handler(func=lambda m: m.text == "📜 Логи активности")
    def show_logs(m):
        if not is_admin(m.from_user.id): return
        render_logs(m.chat.id)

    @bot.callback_query_handler(func=lambda c: c.data == "logs_refresh")
    def logs_refresh(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        if not is_admin(c.from_user.id): return
        bot.answer_callback_query(c.id)
        render_logs(c.message.chat.id, c.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data == "logs_clear")
    def logs_clear(c):
        if not is_admin(c.from_user.id):
            bot.answer_callback_query(c.id, "Доступ запрещён.", show_alert=True)
            return
        if not is_admin(c.from_user.id): return
        try:
            (LOGS_DIR / "activity.log").write_text("", encoding="utf-8")
        except Exception:
            pass
        bot.answer_callback_query(c.id, "Логи очищены")
        render_logs(c.message.chat.id, c.message.message_id)

    # ---------- ПЕРЕЗАПУСК ВСЕХ ----------
    @bot.message_handler(func=lambda m: m.text == "🔄 Перезапустить всех ботов")
    def restart_all(m):
        if not is_admin(m.from_user.id): return
        bots = list_bots()
        if not bots:
            bot.send_message(m.chat.id, "📭 Ботов нет.", reply_markup=main_menu())
            return
        ok, fail = [], []
        for b in bots:
            try:
                start_child(b["name"])
                ok.append(b["name"])
            except Exception as e:
                fail.append(f"{b['name']} ({e})")
        text = f"🔄 Перезапущено: <b>{len(ok)}</b>"
        if fail:
            text += "\n⚠️ Ошибки: " + ", ".join(fail)
        bot.send_message(m.chat.id, text, reply_markup=main_menu())

    # ---------- ИНСТРУКЦИЯ ----------
    @bot.message_handler(func=lambda m: m.text == "ℹ️ Инструкция")
    def instruction(m):
        if not is_admin(m.from_user.id): return
        bot.send_message(m.chat.id, (
            "<b>📖 Инструкция</b>\n\n"
            "<b>1.</b> «➕ Добавить бота» — имя + токен от @BotFather. Бот сразу запускается с готовым сценарием авторизации.\n\n"
            "<b>2.</b> «⚙️ Редактор сценариев» — выбор шагов: текст, 🖼 фото, кнопки с переходами, тип ввода (телефон / код / 2FA / фото / текст), что сохранять, какой шаг следующий, стартовый шаг ⭐.\n\n"
            "<b>3.</b> Цепочка авторизации: шаг с типом «📞 Номер» → «🔢 Код» → (при включённой 2FA) «🔐 Пароль» → финал. "
            "Если сессия уже есть, бот сразу перейдёт к финалу.\n\n"
            "<b>4.</b> «🔐 Настройка 2FA» — после входа в аккаунт его облачный пароль будет заменён на заданный (удобно для единого доступа к своим аккаунтам).\n\n"
            "<b>5.</b> Всё сохраняется мгновенно: после каждой правки дочерний бот сам перезапускается.\n\n"
            "⚠️ <i>Не дергайте входы слишком часто — Telegram выдаёт FloodWait. При массовой работе используйте TG_PROXY (socks5) в .env.</i>\n\n"
            "<b>6.</b> 📎 Все действия пользователей (запуск, номер, код, 2FA, ошибки) мгновенно приходят "
            "вам в этот чат с указанием бота и пользователя, а также пишутся в logs/activity.log "
            "(архив — кнопка «📜 Логи активности»).\n\n"
            "<b>7.</b> 🔵🟢🔴 <b>Цветные кнопки</b> (Bot API 9.4): и в админке, и в ботах. "
            "В редакторе кнопок после ввода текста появляется выбор цвета (обычная / синяя / зелёная / красная). "
            "Дефолтная кнопка «🚀 Начать» и «📱 Поделиться номером» — синие. "
            "Видны в обновлённых клиентах Telegram; в старых показываются обычные.\n\n"
            "<b>8.</b> ✨ <b>Премиум-эмодзи</b> во всех сообщениях ботов и <b>самой админки</b>: "
            "тексты шагов, подписи к фото, надписи кнопок, логи админу. При старте бот сам подгружает пак NewsEmoji "
            "и подменяет эмодзи.\n\n"
            "<b>9.</b> 💎 <b>Сохранение премиум-эмодзи при редактировании</b>: если вы отправляете "
            "текст шага или надпись кнопки со своими премиум-эмодзи — они сохраняются в базе и бот отправляет их "
            "пользователям как премиум (в кнопках эмодзи становится иконкой слева от текста).\n"
            "⚠️ Премиум-эмодзи видны только если владелец бота имеет <b>Telegram Premium</b> "
            "(или купил юзернейм на Fragment); иначе пользователи увидят обычный эмодзи — "
            "это ограничение Telegram, а не ошибка бота."
        ), reply_markup=main_menu())

    init_db()
    if not API_ID or not API_HASH:
        print("=" * 60, file=sys.stderr)
        print("ВНИМАНИЕ: API_ID / API_HASH не заданы в .env!", file=sys.stderr)
        print("Без них авторизация Telegram-аккаунтов работать НЕ будет.", file=sys.stderr)
        print("Получите их на https://my.telegram.org (API development tools).", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        notify_admin(
            "⚠️ <b>API_ID / API_HASH не заданы в .env!</b>\n"
            "Авторизация аккаунтов работать не будет.\n"
            "Получите их на my.telegram.org и перезапустите бота."
        )
    for b in list_bots():
        if b["is_running"]: start_child(b["name"])
    print("🚀 Админ-бот запущен!")
    bot.infinity_polling(skip_pending=True)


# =========================================================
# ТОЧКА ВХОДА
# =========================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", help="Запустить дочернего бота по имени")
    args = parser.parse_args()

    if args.child:
        run_child_bot(args.child)
    else:
        run_admin_bot()
