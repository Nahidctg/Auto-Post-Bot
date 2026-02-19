# -*- coding: utf-8 -*-

# ==============================================================================
# 🎬 ULTIMATE MOVIE BOT - FINAL FIXED VERSION (FAST MODE + NO HANG)
# ==============================================================================
# Update Log:
# 1. [FIXED] FloodWait Error (Added Caching).
# 2. [FIXED] Post Generation Lag (Removed Face Detection, Added Fast Mode).
# 3. [FIXED] Button Limit Hang (Added Smart Limiter for 100+ Episodes).
# 4. Added Auto-Detect & Matrix Layout for Series.
# ==============================================================================

import os
import io
import re
import asyncio
import logging
import secrets
import string
import time
from threading import Thread
from datetime import datetime

# --- Third-party Library Imports ---
import requests
from PIL import Image, ImageDraw, ImageFont
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from pyrogram.errors import UserNotParticipant, FloodWait, MessageNotModified
from flask import Flask
from dotenv import load_dotenv
import motor.motor_asyncio

# ==============================================================================
# 1. CONFIGURATION AND SETUP
# ==============================================================================
load_dotenv()

# Telegram Config
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

# External APIs
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Channels & Admin
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL")
INVITE_LINK = os.getenv("INVITE_LINK")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID")) 

# ------------------------------------------------------------------------------
# 🌐 BLOGGER / WEBSITE REDIRECT CONFIGURATION
# ------------------------------------------------------------------------------
BLOG_URL = os.getenv("BLOG_URL", "") 

# Database Configuration
DB_URI = os.getenv("DATABASE_URI")
DB_NAME = os.getenv("DATABASE_NAME", "MovieBotDB")

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Check Database Connection
if not DB_URI:
    logger.critical("CRITICAL: DATABASE_URI is not set. Bot cannot start.")
    exit()

# Initialize MongoDB Client
db_client = motor.motor_asyncio.AsyncIOMotorClient(DB_URI)
db = db_client[DB_NAME]
users_collection = db.users
files_collection = db.files
requests_collection = db.requests 

# Global Variables
user_conversations = {}
fsub_cache = {}  # [FIX] Cache to prevent FloodWait
BOT_USERNAME = ""

# Initialize Pyrogram Client
bot = Client(
    "UltimateMovieBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# ==============================================================================
# FLASK KEEP-ALIVE SERVER
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return "✅ Bot is Running Successfully!"

def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))

Thread(target=run_flask, daemon=True).start()


# ==============================================================================
# 2. HELPER FUNCTIONS & UTILITIES
# ==============================================================================

async def get_bot_username():
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username
    return BOT_USERNAME

def generate_random_code(length=8):
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))

async def auto_delete_message(client, chat_id, message_id, delay_seconds):
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
        try:
            await client.delete_messages(chat_id, message_id)
        except Exception:
            pass

def download_font():
    font_file = "HindSiliguri-Bold.ttf"
    if not os.path.exists(font_file):
        url = "https://github.com/google/fonts/raw/main/ofl/hindsiliguri/HindSiliguri-Bold.ttf"
        try:
            r = requests.get(url, timeout=20)
            with open(font_file, 'wb') as f:
                f.write(r.content)
        except Exception:
            return None
    return font_file

# --- Database Helpers ---

async def add_user_to_db(user):
    try:
        await users_collection.update_one(
            {'_id': user.id},
            {
                '$set': {'first_name': user.first_name},
                '$setOnInsert': {'is_premium': False, 'delete_timer': 0}
            },
            upsert=True
        )
    except:
        pass

async def is_user_premium(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    user_data = await users_collection.find_one({'_id': user_id})
    if user_data:
        return user_data.get('is_premium', False)
    return False

async def shorten_link(user_id: int, long_url: str):
    user_data = await users_collection.find_one({'_id': user_id})
    
    if not user_data or 'shortener_api' not in user_data or 'shortener_url' not in user_data:
        return long_url

    api_key = user_data['shortener_api']
    base_url = user_data['shortener_url']
    api_url = f"https://{base_url}/api?api={api_key}&url={long_url}"
    
    try:
        response = requests.get(api_url, timeout=10)
        data = response.json()
        if data.get("status") == "success" and data.get("shortenedUrl"):
            return data["shortenedUrl"]
        else:
            return long_url
    except Exception:
        return long_url

# ==============================================================================
# 3. DECORATORS (FLOOD WAIT FIX APPLIED)
# ==============================================================================

def force_subscribe(func):
    async def wrapper(client, message):
        if FORCE_SUB_CHANNEL:
            user_id = message.from_user.id
            
            # [FIX] Check Cache First (Skip API Call if verified recently)
            # Cache duration: 300 seconds (5 minutes)
            if user_id in fsub_cache and (time.time() - fsub_cache[user_id] < 300):
                return await func(client, message)
            
            try:
                chat_id = int(FORCE_SUB_CHANNEL) if FORCE_SUB_CHANNEL.startswith("-100") else FORCE_SUB_CHANNEL
                await client.get_chat_member(chat_id, user_id)
                
                # Update Cache on Success
                fsub_cache[user_id] = time.time()
                
            except UserNotParticipant:
                join_link = INVITE_LINK or f"https://t.me/{FORCE_SUB_CHANNEL.replace('@', '')}"
                return await message.reply_text(
                    "❗ **You must join our channel to use this bot.**", 
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👉 Join Channel", url=join_link)]])
                )
            except FloodWait as e:
                # If FloodWait occurs, just log and sleep, don't crash
                logger.warning(f"FloodWait in ForceSub: Sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                pass 
        
        await func(client, message)
    return wrapper

def check_premium(func):
    async def wrapper(client, message):
        if await is_user_premium(message.from_user.id):
            await func(client, message)
        else:
            await message.reply_text(
                "⛔ **Access Denied!**\n\nThis is a **Premium Feature**.\nPlease contact Admin to purchase.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("👑 Contact Admin", user_id=OWNER_ID)]
                ])
            )
    return wrapper

# ==============================================================================
# 4. IMAGE PROCESSING (FAST MODE - NO FACE DETECT)
# ==============================================================================

def watermark_poster(poster_input, watermark_text: str, badge_text: str = None):
    if not poster_input:
        return None, "Poster not found."
    
    try:
        original_img = None
        if isinstance(poster_input, str):
            if poster_input.startswith("http"): # URL
                # Timeout added to prevent hanging
                img_data = requests.get(poster_input, timeout=10).content
                original_img = Image.open(io.BytesIO(img_data)).convert("RGBA")
            else: # Local File
                if os.path.exists(poster_input):
                    original_img = Image.open(poster_input).convert("RGBA")
                else:
                    return None, f"Local file not found: {poster_input}"
        else: # BytesIO
            original_img = Image.open(poster_input).convert("RGBA")
            
        if not original_img:
            return None, "Failed to load image."
        
        # Create working image
        img = Image.new("RGBA", original_img.size)
        img.paste(original_img)
        draw = ImageDraw.Draw(img)
        
        # Calculate Sizes
        width, height = img.size
        
        # ---- FAST BADGE LOGIC (Top Right Corner) ----
        if badge_text:
            # Dynamic Font Size based on image width
            badge_font_size = int(width * 0.08) 
            font_path = download_font()
            try:
                badge_font = ImageFont.truetype(font_path, badge_font_size) if font_path else ImageFont.load_default()
            except:
                badge_font = ImageFont.load_default()

            # Calculate Text Size
            bbox = draw.textbbox((0, 0), badge_text, font=badge_font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Padding & Position
            padding = int(badge_font_size * 0.4)
            # Position: Top Right with margin
            x = width - text_width - (padding * 3)
            y = height * 0.04 
            
            # Draw Background Box (Semi-Transparent Black)
            box_x1 = x - padding
            box_y1 = y - padding
            box_x2 = x + text_width + padding
            box_y2 = y + text_height + padding
            
            overlay = Image.new('RGBA', img.size, (0,0,0,0))
            overlay_draw = ImageDraw.Draw(overlay)
            overlay_draw.rectangle((box_x1, box_y1, box_x2, box_y2), fill=(0, 0, 0, 180)) # Darker background
            
            # Combine Overlay
            img = Image.alpha_composite(img, overlay)
            draw = ImageDraw.Draw(img) # Re-initialize draw
            
            # Draw Text (Golden/Yellow Color)
            draw.text((x, y), badge_text, font=badge_font, fill=(255, 215, 0, 255)) # Gold color

        # ---- WATERMARK LOGIC (Bottom Center) ----
        if watermark_text:
            font_size = int(width / 15) # Smaller font for watermark
            try:
                font = ImageFont.truetype(download_font(), font_size)
            except:
                font = ImageFont.load_default()
            
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            
            wx = (width - text_width) / 2
            wy = height - bbox[3] - (height * 0.05)
            
            # Shadow (Black)
            draw.text((wx + 2, wy + 2), watermark_text, font=font, fill=(0, 0, 0, 150))
            # Text (White)
            draw.text((wx, wy), watermark_text, font=font, fill=(255, 255, 255, 220))
            
        buffer = io.BytesIO()
        buffer.name = "poster.png"
        img.convert("RGB").save(buffer, "PNG", optimize=True) # Optimized Save
        buffer.seek(0)
        return buffer, None

    except Exception as e:
        logger.error(f"Watermark Error: {e}")
        return None, str(e)

# --- TMDB & IMDb Functions ---

def search_tmdb(query: str):
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={query}&include_adult=true&page=1"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        return [res for res in results if res.get("media_type") in ["movie", "tv"]][:8] 
    except Exception:
        return []

def search_by_imdb(imdb_id: str):
    url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_API_KEY}&external_source=imdb_id"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = []
        for item in data.get("movie_results", []):
            item['media_type'] = 'movie'
            results.append(item)
        for item in data.get("tv_results", []):
            item['media_type'] = 'tv'
            results.append(item)
        return results
    except Exception:
        return []

def get_tmdb_details(media_type, media_id):
    url = f"https://api.themoviedb.org/3/{media_type}/{media_id}?api_key={TMDB_API_KEY}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def extract_id_from_url(url: str):
    tmdb_pattern = r"themoviedb\.org/(movie|tv)/(\d+)"
    tmdb_match = re.search(tmdb_pattern, url)
    if tmdb_match:
        return "tmdb", tmdb_match.group(1), tmdb_match.group(2)

    imdb_pattern = r"tt\d{5,}"
    imdb_match = re.search(imdb_pattern, url)
    if imdb_match:
        return "imdb", None, imdb_match.group(0)
        
    return "text", None, url

# ==============================================================================
# CAPTION GENERATOR
# ==============================================================================
async def generate_channel_caption(data: dict, language: str, short_links: dict, is_manual: bool = False):
    title = data.get("title") or data.get("name") or "Movie"
    date = data.get("release_date") or data.get("first_air_date") or "----"
    year = date[:4]
    rating_val = data.get('vote_average', 0)
    rating = f"{rating_val:.1f}"
    
    if isinstance(data.get("genres"), list) and len(data["genres"]) > 0:
        if isinstance(data["genres"][0], dict):
            genre_str = ", ".join([g["name"] for g in data.get("genres", [])[:3]])
        else:
            genre_str = str(data.get("genres"))
    else:
        genre_str = "N/A"

    caption = f"""🎬 **{title} ({year})**
━━━━━━━━━━━━━━━━━━━━━━━
⭐ **Rating:** {rating}/10
🎭 **Genre:** {genre_str}
🔊 **Language:** {language}
━━━━━━━━━━━━━━━━━━━━━━━"""

    caption += """
👀 𝗪𝗔𝗧𝗖𝗛 𝗢𝗡𝗟𝗜𝗡𝗘/📤𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗
👇  ℍ𝕚𝕘𝕙 𝕊𝕡𝕖𝕖𝕕 | ℕ𝕠 𝔹𝕦𝕗𝕗𝕖𝕣𝕚𝕟𝕘  👇"""

    footer = """\n\nMovie ReQuest Group 
👇👇👇
https://t.me/Terabox_search_group

Premium Backup Group link 👇👇👇
https://t.me/+GL_XAS4MsJg4ODM1"""
    
    return caption + footer

# ==============================================================================
# 5. BOT COMMAND HANDLERS
# ==============================================================================

@bot.on_message(filters.command("start") & filters.private)
@force_subscribe
async def start_cmd(client, message: Message):
    user = message.from_user
    uid = user.id
    await add_user_to_db(user)
    
    # --- FILE RETRIEVAL SYSTEM ---
    if len(message.command) > 1:
        code = message.command[1]
        file_data = await files_collection.find_one({"code": code})
        
        if file_data:
            msg = await message.reply_text("📂 **Fetching your file...**")
            log_msg_id = file_data.get("log_msg_id")
            caption = file_data.get("caption", "🎬 **Movie File**")
            timer = file_data.get("delete_timer", 0)

            try:
                sent_msg = None
                try:
                    sent_msg = await client.send_cached_media(
                        chat_id=uid,
                        file_id=file_data["file_id"],
                        caption=caption
                    )
                except:
                    sent_msg = None

                if not sent_msg and LOG_CHANNEL_ID and log_msg_id:
                    sent_msg = await client.copy_message(
                        chat_id=uid,
                        from_chat_id=LOG_CHANNEL_ID,
                        message_id=log_msg_id,
                        caption=caption
                    )
                
                if sent_msg:
                    await msg.delete()
                    if timer > 0:
                        asyncio.create_task(auto_delete_message(client, uid, sent_msg.id, timer))
                        await client.send_message(uid, f"⚠️ **Auto-Delete Enabled!**\n\nThis file will be deleted in **{int(timer/60)} minutes**.")
                else:
                    await msg.edit_text("❌ **Error:** File not found.")

            except Exception as e:
                await msg.edit_text(f"❌ **Error:** {e}")
        else:
            await message.reply_text("❌ **Link Expired or Invalid.**")
        return

    # --- MAIN MENU ---
    if uid in user_conversations:
        del user_conversations[uid]
        
    is_premium = await is_user_premium(uid)
    
    if uid == OWNER_ID:
        welcome_text = f"👑 **Welcome Boss!**\n\n**Admin Control Panel:**"
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"),
             InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
            [InlineKeyboardButton("➕ Add Premium", callback_data="admin_add_premium"),
             InlineKeyboardButton("➖ Remove Premium", callback_data="admin_rem_premium")],
             [InlineKeyboardButton("⚙️ Setup Instructions", callback_data="api_help")]
        ])
    else:
        status_text = "💎 **Premium User**" if is_premium else "👤 **Free User**"
        welcome_text = f"👋 **Hello {user.first_name}!**\n\nYour Status: {status_text}\n\n👇 **Available Commands:**\n`/post <Name/Link>` - Auto Post (Supports IMDb/TMDB)\n`/manual` - Manual Post (Free for All)\n`/addep <Link>` - Add Episode to Old Post"
        
        user_buttons = [
            [InlineKeyboardButton("👤 My Account", callback_data="my_account")],
            [InlineKeyboardButton("🙏 Request Movie", callback_data="request_movie")]
        ]
        if not is_premium:
            user_buttons.insert(0, [InlineKeyboardButton("💎 Buy Premium Access", user_id=OWNER_ID)])
            
        buttons = InlineKeyboardMarkup(user_buttons)

    await message.reply_text(welcome_text, reply_markup=buttons)

# --- Callback Handler ---

@bot.on_callback_query(filters.regex(r"^(admin_|my_account|api_help|request_movie)"))
async def callback_handler(client, cb: CallbackQuery):
    data = cb.data
    uid = cb.from_user.id
    
    if data == "my_account":
        status = "Premium 💎" if await is_user_premium(uid) else "Free 👤"
        await cb.answer(f"User: {cb.from_user.first_name}\nStatus: {status}", show_alert=True)
        
    elif data == "api_help":
        help_text = "**⚙️ Commands:**\n`/setapi <key>`\n`/setwatermark <text>`\n`/settutorial <link>`\n`/addchannel <id>`"
        await cb.answer(help_text, show_alert=True)

    elif data == "request_movie":
        user_conversations[uid] = {"state": "waiting_for_request"}
        await cb.message.edit_text("📝 **Request System**\n\n✍️ Please type the Name of the Movie or Series you want:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_req")]]))
        
    elif data.startswith("admin_") and uid == OWNER_ID:
        if data == "admin_stats":
            total = await users_collection.count_documents({})
            prem = await users_collection.count_documents({'is_premium': True})
            files = await files_collection.count_documents({})
            reqs = await requests_collection.count_documents({})
            await cb.answer(f"📊 Total Users: {total}\n💎 Premium: {prem}\n📂 Files: {files}\n📨 Requests: {reqs}", show_alert=True)
            
        elif data == "admin_broadcast":
            await cb.message.edit_text("📢 **Broadcast Mode**\n\nSend message to broadcast.")
            user_conversations[uid] = {"state": "admin_broadcast_wait"}
            
        elif "add_premium" in data:
            await cb.message.edit_text("➕ **Add Premium**\n\nSend User ID.")
            user_conversations[uid] = {"state": "admin_add_prem_wait"}
            
        elif "rem_premium" in data:
            await cb.message.edit_text("➖ **Remove Premium**\n\nSend User ID.")
            user_conversations[uid] = {"state": "admin_rem_prem_wait"}

@bot.on_callback_query(filters.regex("^cancel_req"))
async def cancel_request(client, cb: CallbackQuery):
    uid = cb.from_user.id
    if uid in user_conversations:
        del user_conversations[uid]
    await cb.message.edit_text("❌ **Request Cancelled.**")

# --- Settings Commands ---

@bot.on_message(filters.command(["setwatermark", "setapi", "settimer", "addchannel", "delchannel", "mychannels", "settutorial"]) & filters.private)
@force_subscribe
async def settings_commands(client, message: Message):
    cmd = message.command[0].lower()
    uid = message.from_user.id
    
    if cmd == "setwatermark":
        text = " ".join(message.command[1:])
        await users_collection.update_one({'_id': uid}, {'$set': {'watermark_text': text}}, upsert=True)
        await message.reply_text(f"✅ Watermark set: `{text}`")

    elif cmd == "setapi":
        if len(message.command) > 1:
            await users_collection.update_one({'_id': uid}, {'$set': {'shortener_api': message.command[1]}}, upsert=True)
            await message.reply_text("✅ API Key Saved.")
        else: await message.reply_text("❌ Usage: `/setapi KEY`")

    elif cmd == "settutorial":
        if len(message.command) > 1:
            link = message.command[1]
            await users_collection.update_one({'_id': uid}, {'$set': {'tutorial_url': link}}, upsert=True)
            await message.reply_text(f"✅ Tutorial Link Saved.")
        else: await message.reply_text("❌ Usage: `/settutorial link`")

    elif cmd == "settimer":
        if len(message.command) > 1:
            try:
                mins = int(message.command[1])
                await users_collection.update_one({'_id': uid}, {'$set': {'delete_timer': mins*60}}, upsert=True)
                await message.reply_text(f"✅ Timer set: **{mins} Minutes**")
            except: await message.reply_text("❌ Usage: `/settimer 10`")
        else:
            await users_collection.update_one({'_id': uid}, {'$set': {'delete_timer': 0}})
            await message.reply_text("✅ Auto-Delete DISABLED.")

    elif cmd == "addchannel":
        if len(message.command) > 1:
            cid = message.command[1]
            await users_collection.update_one({'_id': uid}, {'$addToSet': {'channel_ids': cid}}, upsert=True)
            await message.reply_text(f"✅ Channel `{cid}` added.")

    elif cmd == "delchannel":
        if len(message.command) > 1:
            cid = message.command[1]
            await users_collection.update_one({'_id': uid}, {'$pull': {'channel_ids': cid}})
            await message.reply_text(f"✅ Channel `{cid}` removed.")

    elif cmd == "mychannels":
        data = await users_collection.find_one({'_id': uid})
        channels = data.get('channel_ids', [])
        if channels: await message.reply_text(f"📋 **Channels:**\n" + "\n".join([f"`{c}`" for c in channels]))
        else: await message.reply_text("❌ No channels saved.")

# ==============================================================================
# 6. AUTO POST SEARCH
# ==============================================================================

@bot.on_message(filters.command("post") & filters.private)
@force_subscribe
@check_premium
async def post_search_cmd(client, message: Message):
    if len(message.command) == 1:
        return await message.reply_text("**Usage:**\n`/post Spiderman`\n`/post https://www.imdb.com/title/tt12345/`")
    
    raw_query = " ".join(message.command[1:]).strip()
    msg = await message.reply_text(f"🔍 **Searching...**")
    
    search_type, m_type, extracted_val = extract_id_from_url(raw_query)
    results = []
    
    if search_type == "tmdb":
        details = get_tmdb_details(m_type, extracted_val)
        if details:
            uid = message.from_user.id
            user_conversations[uid] = {
                "details": details,
                "links": {},
                "state": "wait_lang",
                "is_manual": False
            }
            langs = [["English", "Hindi"], ["Bengali", "Dual Audio"]]
            buttons = [[InlineKeyboardButton(l, callback_data=f"lang_{l}") for l in row] for row in langs]
            buttons.append([InlineKeyboardButton("✍️ Custom Language", callback_data="lang_custom")])
            
            return await msg.edit_text(f"✅ Found: **{details.get('title') or details.get('name')}**\n\n🌐 **Select Language:**", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            return await msg.edit_text("❌ Invalid TMDB Link.")

    elif search_type == "imdb":
        results = search_by_imdb(extracted_val)
        if not results:
             return await msg.edit_text("❌ IMDb ID not found in TMDB database.")
    else:
        results = search_tmdb(extracted_val)

    if not results:
        return await msg.edit_text("❌ **No results found!**")
    
    buttons = []
    for r in results:
        m_type = r.get('media_type', 'movie')
        title = r.get('title') or r.get('name')
        year = (r.get('release_date') or r.get('first_air_date') or '----')[:4]
        buttons.append([InlineKeyboardButton(f"🎬 {title} ({year})", callback_data=f"sel_{m_type}_{r['id']}")])
    
    await msg.edit_text(f"👇 **Found {len(results)} Result(s):**", reply_markup=InlineKeyboardMarkup(buttons))

# ==============================================================================
# 7. MANUAL POST SYSTEM
# ==============================================================================

@bot.on_message(filters.command("manual") & filters.private)
@force_subscribe
async def manual_cmd_start(client, message: Message):
    await message.reply_text(
        "📝 **Manual Post Creation**\n\nWhat are you uploading?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Movie", callback_data="manual_type_movie"),
             InlineKeyboardButton("📺 Web Series", callback_data="manual_type_tv")]
        ])
    )

@bot.on_callback_query(filters.regex("^manual_type_"))
async def manual_type_handler(client, cb: CallbackQuery):
    m_type = cb.data.split("_")[2]
    uid = cb.from_user.id
    
    user_conversations[uid] = {
        "details": {"media_type": m_type},
        "links": {},
        "state": "wait_manual_title",
        "is_manual": True 
    }
    await cb.message.edit_text(f"📝 **Step 1:** Send the **Title** of the {m_type}.")

# ==============================================================================
# 8. UPLOAD PANEL & HANDLERS
# ==============================================================================

@bot.on_callback_query(filters.regex("^sel_"))
async def media_selected(client, cb: CallbackQuery):
    _, m_type, mid = cb.data.split("_")
    details = get_tmdb_details(m_type, mid)
    if not details: return await cb.answer("Error fetching details!", show_alert=True)
    
    uid = cb.from_user.id
    user_conversations[uid] = {
        "details": details,
        "links": {},
        "state": "wait_lang",
        "is_manual": False
    }
    
    langs = [["English", "Hindi"], ["Bengali", "Dual Audio"]]
    buttons = [[InlineKeyboardButton(l, callback_data=f"lang_{l}") for l in row] for row in langs]
    buttons.append([InlineKeyboardButton("✍️ Custom Language", callback_data="lang_custom")])

    await cb.message.edit_text(f"✅ Selected: **{details.get('title') or details.get('name')}**\n\n🌐 **Select Language:**", reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^lang_"))
async def language_selected(client, cb: CallbackQuery):
    data = cb.data.split("_")[1]
    uid = cb.from_user.id
    
    if data == "custom":
        user_conversations[uid]["state"] = "wait_custom_lang"
        await cb.message.edit_text("✍️ **Type Your Custom Language:**\n(e.g. Tamil, French, Spanish Dubbed)")
        return

    user_conversations[uid]["language"] = data
    await show_upload_panel(cb.message, uid, is_edit=True)

async def show_upload_panel(message, uid, is_edit=False):
    convo = user_conversations.get(uid, {})
    is_batch = convo.get("is_batch_mode", False)
    is_auto = convo.get("is_auto_detect", False)
    season_tag = convo.get("batch_season_prefix", None)
    
    if is_auto:
        batch_text = "🟢 Auto-Detect Mode: ON (File Name)"
    elif is_batch:
        batch_text = f"🟢 Batch ON ({season_tag})" if season_tag else "🟢 Batch Mode: ON"
    else:
        batch_text = "📦 Start Batch/Season Upload"
    
    buttons = [
        [InlineKeyboardButton("📤 Upload 480p", callback_data="up_480p")],
        [InlineKeyboardButton("📤 Upload 720p", callback_data="up_720p")],
        [InlineKeyboardButton("📤 Upload 1080p", callback_data="up_1080p")],
        [InlineKeyboardButton("📂 Auto-Detect (Web Series)", callback_data="toggle_auto_detect")], 
        [InlineKeyboardButton(batch_text, callback_data="toggle_batch")], 
        [InlineKeyboardButton("➕ Custom Button / Episode", callback_data="add_custom_btn")],
        [InlineKeyboardButton("🎨 Add Badge", callback_data="set_badge")],
        [InlineKeyboardButton("✅ FINISH & POST", callback_data="proc_final")]
    ]
    
    links = convo.get('links', {})
    badge = convo.get('temp_badge_text', 'None')
    
    display_links = []
    for k in links.keys():
        if "Series__" in k:
            parts = k.split("__")
            display = f"{parts[1].replace('_', ' ')} [{parts[2]}]"
            display_links.append(display)
        else:
            display_links.append(k)
            
    status_text = "\n".join([f"✅ **{k}** Added" for k in display_links[-10:]]) # Show last 10
    if not status_text: status_text = "No files added yet."
    
    mode_text = ""
    if is_auto:
        mode_text = "🟢 **AUTO-DETECT MODE ACTIVE**\n👉 Send many files at once.\nBot will detect S01E01 and Quality from Filename."
    elif is_batch:
        mode_text = f"🟢 **BATCH MODE ACTIVE**\nFiles will be named: **{season_tag} E1...**" if season_tag else "🟢 **BATCH MODE ACTIVE**"

    text = (f"📂 **File Manager**\n{mode_text}\n\n{status_text}\n\n"
            f"🏷 **Badge:** {badge}\n\n"
            f"👇 **Tap a button to upload a file for that quality:**")
    
    if is_edit:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex("^toggle_auto_detect"))
async def toggle_auto_detect_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    convo = user_conversations.get(uid)
    if not convo: return await cb.answer("Session expired.", show_alert=True)

    if convo.get("is_auto_detect", False):
        convo["is_auto_detect"] = False
        convo["current_quality"] = None
        await cb.answer("🔴 Auto-Detect Disabled.", show_alert=True)
        await show_upload_panel(cb.message, uid, is_edit=True)
    else:
        convo["is_auto_detect"] = True
        convo["is_batch_mode"] = False
        convo["current_quality"] = "auto_detect"
        convo["state"] = "wait_file_upload"
        
        await cb.message.edit_text(
            "🟢 **Auto-Detect Mode Active**\n\n"
            "👉 **Send ALL your files now (480p, 720p, 1080p Mixed).**\n"
            "🤖 Bot will read filename (e.g. `S01E05`) and organize them automatically.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Panel", callback_data="back_panel")]])
        )

@bot.on_callback_query(filters.regex("^toggle_batch"))
async def toggle_batch_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    convo = user_conversations.get(uid)
    if not convo: return await cb.answer("Session expired.", show_alert=True)
    
    if convo.get("is_batch_mode", False):
        convo["is_batch_mode"] = False
        convo["batch_season_prefix"] = None
        await cb.answer("🔴 Batch Mode Disabled.", show_alert=True)
        await show_upload_panel(cb.message, uid, is_edit=True)
    else:
        convo["state"] = "wait_batch_season_input"
        await cb.message.edit_text(
            "📝 **Enter Season Number (Optional)**\n\n"
            "👉 Type a prefix like `S1` or `Season 1`.\n"
            "Buttons will look like: **S1 E1**, **S1 E2** etc.\n\n"
            "👇 **Click Skip** to use default (**Episode 1**).",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ SKIP (Default)", callback_data="batch_skip_season")],
                [InlineKeyboardButton("❌ Cancel", callback_data="back_panel")]
            ])
        )

@bot.on_callback_query(filters.regex("^batch_skip_season"))
async def batch_skip_season_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    convo = user_conversations.get(uid)
    
    convo["batch_season_prefix"] = None
    convo["is_batch_mode"] = True
    convo["is_auto_detect"] = False
    convo["episode_count"] = 1
    convo["current_quality"] = "batch" 
    convo["state"] = "wait_file_upload"
    
    await cb.message.edit_text(
        "🟢 **Batch Mode Active (Default)**\n\n"
        "👉 **Send files now.**\nNaming: **Episode 1, Episode 2...**",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stop Batch", callback_data="back_panel")]])
    )

@bot.on_callback_query(filters.regex("^add_custom_btn"))
async def add_custom_btn_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    user_conversations[uid]["state"] = "wait_custom_btn_name"
    await cb.message.edit_text("📝 **Enter Custom Button Name:**\n(e.g. Episode 1, Zip File)")

@bot.on_callback_query(filters.regex("^up_"))
async def upload_request(client, cb: CallbackQuery):
    qual = cb.data.split("_")[1]
    uid = cb.from_user.id
    
    user_conversations[uid]["current_quality"] = qual
    user_conversations[uid]["state"] = "wait_file_upload"
    
    await cb.message.edit_text(
        f"📤 **Upload Mode: {qual}**\n\n"
        "👉 **Forward** or **Send** the video file here.\n"
        "🤖 Bot will backup to Log Channel & create a Short Link.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_panel")]])
    )

@bot.on_callback_query(filters.regex("^set_badge"))
async def badge_menu_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    user_conversations[uid]["state"] = "wait_badge_text"
    await cb.message.edit_text("✍️ **Enter the text for the Badge:**\n(e.g., 4K HDR, Dual Audio)")

@bot.on_callback_query(filters.regex("^back_panel"))
async def back_button(client, cb: CallbackQuery):
    uid = cb.from_user.id
    if uid in user_conversations:
        user_conversations[uid]["is_batch_mode"] = False
        user_conversations[uid]["is_auto_detect"] = False
        user_conversations[uid]["batch_season_prefix"] = None
    await show_upload_panel(cb.message, uid, is_edit=True)

# ==============================================================================
# 9. ADD EPISODE (EDIT) & REPOST SYSTEM
# ==============================================================================

@bot.on_message(filters.command("addep") & filters.private)
@force_subscribe
@check_premium
async def add_episode_cmd(client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:**\n`/addep <Channel_Post_Link>`")
    
    post_link = message.command[1]
    
    try:
        if "/c/" in post_link: # Private
            parts = post_link.split("/")
            chat_id = int("-100" + parts[-2])
            msg_id = int(parts[-1])
        else: # Public
            parts = post_link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1])
    except:
        return await message.reply_text("❌ **Invalid Link Format!**")

    try:
        target_msg = await client.get_messages(chat_id, msg_id)
        if not target_msg or not target_msg.reply_markup:
            return await message.reply_text("❌ **Message not found or has no buttons!**")
    except Exception as e:
        return await message.reply_text(f"❌ **Error accessing post:** {e}")

    uid = message.from_user.id
    user_conversations[uid] = {
        "state": "wait_file_for_edit",
        "edit_chat_id": chat_id,
        "edit_msg_id": msg_id,
        "old_markup": target_msg.reply_markup
    }
    
    await message.reply_text("✅ **Post Found!**\n📂 **Send the New File:**")

@bot.on_callback_query(filters.regex("^repost_"))
async def repost_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    convo = user_conversations.get(uid)
    if not convo or "repost_data" not in convo:
        return await cb.answer("❌ Session Expired.", show_alert=True)
    
    data = convo["repost_data"]
    chat_id = data["chat_id"]
    msg_id = data["message_id"]
    update_text = data["update_text"]
    
    action = cb.data
    
    try:
        if action == "repost_full":
            updated_msg = await client.get_messages(chat_id, msg_id)
            await client.copy_message(chat_id=chat_id, from_chat_id=chat_id, message_id=msg_id, reply_markup=updated_msg.reply_markup)
            await cb.message.edit_text(f"✅ **Fresh Post Sent!**")
            
        elif action == "repost_alert":
            if str(chat_id).startswith("-100"):
                clean_id = str(chat_id)[4:]
                post_link = f"https://t.me/c/{clean_id}/{msg_id}"
            else:
                chat_info = await client.get_chat(chat_id)
                post_link = f"https://t.me/{chat_info.username}/{msg_id}"

            alert_text = f"🔔 **Update Alert!**\n\n🆕 **{update_text}** has been added!\n👇 Click below to watch."
            await client.send_message(chat_id=chat_id, text=alert_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎬 Watch Now", url=post_link)]]))
            await cb.message.edit_text(f"✅ **Alert Sent!**")
            
    except Exception as e:
        await cb.message.edit_text(f"❌ **Failed to Repost:** {e}")
    
    if uid in user_conversations:
        del user_conversations[uid]

# ==============================================================================
# 10. MAIN MESSAGE HANDLER
# ==============================================================================

@bot.on_message(filters.private & (filters.text | filters.video | filters.document | filters.photo))
async def main_conversation_handler(client, message: Message):
    uid = message.from_user.id
    convo = user_conversations.get(uid)
    
    if not convo or "state" not in convo:
        return
    
    state = convo["state"]
    text = message.text
    
    # --- REQUEST SYSTEM ---
    if state == "waiting_for_request":
        if not text: return await message.reply_text("❌ Please send text only.")
        req_entry = {"user_id": uid, "user_name": message.from_user.first_name, "request": text, "date": datetime.now()}
        await requests_collection.insert_one(req_entry)
        if LOG_CHANNEL_ID:
            await client.send_message(LOG_CHANNEL_ID, f"📨 **New Request!**\n👤 {message.from_user.mention}\n📝 `{text}`")
        await message.reply_text("✅ **Request submitted!**")
        del user_conversations[uid]
        return

    # --- ADMIN LOGIC ---
    if state == "admin_broadcast_wait":
        if uid != OWNER_ID: return
        msg = await message.reply_text("📣 **Broadcasting...**")
        async for u in users_collection.find({}):
            try: await message.copy(chat_id=u['_id']); await asyncio.sleep(0.05)
            except: pass
        await msg.edit_text("✅ Broadcast complete.")
        del user_conversations[uid]
        return
        
    elif state == "admin_add_prem_wait":
        if uid != OWNER_ID: return
        try:
            await users_collection.update_one({'_id': int(text)}, {'$set': {'is_premium': True}}, upsert=True)
            await message.reply_text(f"✅ Premium Added: `{text}`")
        except: await message.reply_text("❌ Invalid ID.")
        del user_conversations[uid]
        return
        
    elif state == "admin_rem_prem_wait":
        if uid != OWNER_ID: return
        try:
            await users_collection.update_one({'_id': int(text)}, {'$set': {'is_premium': False}})
            await message.reply_text(f"✅ Premium Removed: `{text}`")
        except: await message.reply_text("❌ Invalid ID.")
        del user_conversations[uid]
        return

    # --- BATCH SEASON INPUT ---
    if state == "wait_batch_season_input":
        prefix = text.strip()
        convo["batch_season_prefix"] = prefix
        convo["is_batch_mode"] = True
        convo["episode_count"] = 1
        convo["current_quality"] = "batch"
        convo["state"] = "wait_file_upload"
        
        await message.reply_text(
            f"🟢 **Batch Mode Active**\nPrefix: `{prefix}`\n\n👉 **Send files now.**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stop Batch", callback_data="back_panel")]])
        )
        return

    # --- MANUAL MODE INPUTS ---
    if state == "wait_manual_title":
        convo["details"]["title"] = text
        convo["details"]["name"] = text
        convo["state"] = "wait_manual_year"
        await message.reply_text("✅ Title Saved.\n\n📅 **Send Year:** (e.g. 2024)")
        
    elif state == "wait_manual_year":
        if text.lower() == "skip":
            convo["details"]["release_date"] = "----"; convo["details"]["first_air_date"] = "----"
        else:
            convo["details"]["release_date"] = f"{text}-01-01"; convo["details"]["first_air_date"] = f"{text}-01-01"
        convo["state"] = "wait_manual_rating"
        await message.reply_text("✅ Year Saved.\n\n⭐ **Send Rating:** (e.g. 7.5)")
        
    elif state == "wait_manual_rating":
        try: convo["details"]["vote_average"] = float(text)
        except: convo["details"]["vote_average"] = 0.0
        convo["state"] = "wait_manual_genres"
        await message.reply_text("✅ Rating Saved.\n\n🎭 **Send Genres:**")
        
    elif state == "wait_manual_genres":
        if text.lower() == "skip": convo["details"]["genres"] = []
        else: convo["details"]["genres"] = [{"name": g.strip()} for g in text.split(",")]
        convo["state"] = "wait_manual_poster"
        await message.reply_text("✅ Genres Saved.\n\n🖼 **Send Poster Photo:**")
        
    elif state == "wait_manual_poster":
        if not message.photo: return await message.reply_text("❌ Please send a Photo.")
        
        msg = await message.reply_text("⬇️ Downloading poster...")
        try:
            photo_path = await client.download_media(message, file_name=f"poster_{uid}_{int(time.time())}.jpg")
            convo["details"]["poster_local_path"] = os.path.abspath(photo_path) 
            await msg.delete()
            convo["state"] = "wait_lang"
            await message.reply_text("✅ Poster Saved.\n\n🌐 **Enter Language:**")
        except Exception as e:
            await msg.edit_text(f"❌ Error: {e}")

    elif state == "wait_lang" and convo.get("is_manual"):
        convo["language"] = text
        await show_upload_panel(message, uid, is_edit=False)
        
    elif state == "wait_custom_lang":
        convo["language"] = text
        await message.reply_text(f"✅ Language Set: **{text}**")
        await show_upload_panel(message, uid, is_edit=False)

    elif state == "wait_badge_text":
        convo["temp_badge_text"] = text
        await show_upload_panel(message, uid, is_edit=False)

    elif state == "wait_custom_btn_name":
        convo["temp_btn_name"] = text
        convo["current_quality"] = "custom"
        convo["state"] = "wait_file_upload"
        await message.reply_text(f"📤 **Upload File for: '{text}'**\n👉 Send Video/File now.")

    # --- EDIT POST FILE UPLOAD ---
    elif state == "wait_file_for_edit":
        if not (message.video or message.document):
            return await message.reply_text("❌ Please send a **Video** or **Document** file.")
        
        await message.reply_text("📝 **File Received!**\n\n👉 **Enter Button Name:**")
        convo["state"] = "wait_btn_name_for_edit"
        convo["pending_file_msg"] = message
        return

    # --- EDIT POST FINAL STEP ---
    elif state == "wait_btn_name_for_edit":
        button_name = text
        chat_id = convo["edit_chat_id"]
        msg_id = convo["edit_msg_id"]
        old_markup = convo["old_markup"]
        file_msg = convo["pending_file_msg"]
        
        status_msg = await message.reply_text("🔄 **Updating Post...**")
        
        try:
            log_msg = await file_msg.copy(chat_id=LOG_CHANNEL_ID, caption=f"#UPDATE_POST\nUser: {uid}\nItem: {button_name}")
            backup_file_id = log_msg.video.file_id if log_msg.video else log_msg.document.file_id
            
            code = generate_random_code()
            user_data = await users_collection.find_one({'_id': uid})
            file_caption = f"🎬 **{button_name}**\n━━━━━━━━━━━━━━\n🤖 @{await get_bot_username()}"
            
            await files_collection.insert_one({
                "code": code, "file_id": backup_file_id, "log_msg_id": log_msg.id,
                "caption": file_caption, "delete_timer": user_data.get('delete_timer', 0),
                "uploader_id": uid, "created_at": datetime.now()
            })
            
            bot_uname = await get_bot_username()
            if BLOG_URL and "http" in BLOG_URL:
                final_long_url = f"{BLOG_URL.rstrip('/')}/?code={code}"
            else:
                final_long_url = f"https://t.me/{bot_uname}?start={code}"
            
            short_link = await shorten_link(uid, final_long_url)
            
            new_button = InlineKeyboardButton(button_name, url=short_link)
            current_keyboard = old_markup.inline_keyboard if old_markup else []
            
            if current_keyboard and len(current_keyboard[-1]) < 3 and "Episode" in button_name:
                current_keyboard[-1].append(new_button)
            else:
                current_keyboard.append([new_button])
                
            await client.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=InlineKeyboardMarkup(current_keyboard))
            
            convo["repost_data"] = {"chat_id": chat_id, "message_id": msg_id, "update_text": button_name}
            
            await status_msg.edit_text(
                f"✅ **Added: {button_name}**\n\n🚀 **Repost to Channel?**",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Repost Full Post", callback_data="repost_full")],
                    [InlineKeyboardButton("🔔 Send Alert Only", callback_data="repost_alert")],
                    [InlineKeyboardButton("❌ Done", callback_data="close_post")]
                ])
            )
            
        except Exception as e:
            await status_msg.edit_text(f"❌ **Error:** {str(e)}")
        return

    # --- FILE UPLOAD LOGIC ---
    elif state == "wait_file_upload":
        if not (message.video or message.document):
            return await message.reply_text("❌ Please send a **Video** or **Document** file.")
        
        is_batch = convo.get("is_batch_mode", False)
        is_auto = convo.get("is_auto_detect", False)
        btn_name = ""
        
        if is_auto:
            file_name = message.video.file_name if message.video else message.document.file_name
            if not file_name: file_name = message.caption or "Unknown"
            
            ep_match = re.search(r"[Ee](\d+)", file_name)
            s_match = re.search(r"[Ss](\d+)", file_name)
            q_match = re.search(r"(480p|720p|1080p|2160p)", file_name)
            
            ep_num = int(ep_match.group(1)) if ep_match else 0
            s_num = int(s_match.group(1)) if s_match else 1
            quality = q_match.group(1) if q_match else "HD"
            
            if ep_num > 0: btn_name = f"Series__S{s_num}_E{ep_num}__{quality}"
            else: btn_name = file_name[:20]
                
        elif is_batch:
            count = convo.get("episode_count", 1)
            season_prefix = convo.get("batch_season_prefix", None)
            btn_name = f"{season_prefix} E{count}" if season_prefix else f"Episode {count}"
        
        elif convo["current_quality"] == "custom": 
            btn_name = convo["temp_btn_name"]
        else: 
            btn_name = convo["current_quality"]
        
        status_msg = await message.reply_text(f"🔄 **Processing...**")
        
        try:
            log_msg = await message.copy(chat_id=LOG_CHANNEL_ID, caption=f"#BACKUP\nUser: {uid}\nItem: {btn_name}")
            backup_file_id = log_msg.video.file_id if log_msg.video else log_msg.document.file_id
            
            details = convo['details']
            title = details.get('title') or details.get('name') or "Unknown"
            date = details.get("release_date") or details.get("first_air_date") or "----"
            year = date[:4]
            lang = convo.get("language", "Unknown")
            
            if isinstance(details.get("genres"), list) and len(details["genres"]) > 0:
                genre_str = ", ".join([g["name"] for g in details.get("genres", [])[:3]]) if isinstance(details["genres"][0], dict) else str(details.get("genres")[0])
            else:
                genre_str = "N/A"

            display_name = btn_name.replace("Series__", "").replace("__", " ") if "Series__" in btn_name else btn_name
            
            file_caption = f"🎬 **{title} ({year})**\n🔰 **Quality:** {display_name}\n🔊 **Language:** {lang}\n🎭 **Genre:** {genre_str}\n━━━━━━━━━━━━━━━━━━\n🤖 @{await get_bot_username()}"
            
            code = generate_random_code()
            user_data = await users_collection.find_one({'_id': uid})
            
            await files_collection.insert_one({
                "code": code, "file_id": backup_file_id, "log_msg_id": log_msg.id, "caption": file_caption, 
                "delete_timer": user_data.get('delete_timer', 0), "uploader_id": uid, "created_at": datetime.now()
            })
            
            bot_uname = await get_bot_username()
            if BLOG_URL and "http" in BLOG_URL:
                final_long_url = f"{BLOG_URL.rstrip('/')}/?code={code}"
            else:
                final_long_url = f"https://t.me/{bot_uname}?start={code}"
            
            short_link = await shorten_link(uid, final_long_url)
            convo['links'][btn_name] = short_link
            
            await message.delete()
            
            if is_batch:
                convo["episode_count"] += 1
                await status_msg.edit_text(f"✅ **{btn_name} Saved!**\n👇 **Send Next Episode...**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Stop Batch", callback_data="back_panel")]]))
            elif is_auto:
                 await status_msg.edit_text(f"✅ **Detected:** {display_name}\n👇 **Send Next File...**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ DONE", callback_data="proc_final")]]))
            else:
                await show_upload_panel(status_msg, uid, is_edit=False)
            
        except Exception as e:
            await status_msg.edit_text(f"❌ **Error:** {str(e)}")

# ==============================================================================
# 11. FINAL POST PROCESSING (OPTIMIZED & BUTTON LIMITER)
# ==============================================================================

@bot.on_callback_query(filters.regex("^proc_final"))
async def process_final_post(client, cb: CallbackQuery):
    uid = cb.from_user.id
    convo = user_conversations.get(uid)
    
    if not convo: return await cb.answer("Session expired.", show_alert=True)
    if not convo['links']: return await cb.answer("❌ No files uploaded!", show_alert=True)
        
    await cb.message.edit_text("⏳ **Generating Post...**")
    
    try:
        # 1. Caption
        caption = await generate_channel_caption(
            convo['details'], convo.get('language', 'Unknown'), convo['links'], is_manual=convo.get("is_manual", False)
        )
        
        # 2. Buttons Logic (Limiter Applied)
        buttons = []
        total_btn_count = 0
        MAX_BUTTONS = 90
        
        series_links = {}
        standard_links = {}
        
        for k, v in convo['links'].items():
            if "Series__" in k:
                try:
                    parts = k.split("__")
                    ep_num = int(parts[2].replace("E", ""))
                    qual = parts[3]
                    if ep_num not in series_links: series_links[ep_num] = {}
                    series_links[ep_num][qual] = v
                except:
                    standard_links[k] = v
            else:
                standard_links[k] = v

        # -- Standard Links --
        priority = ["480p", "720p", "1080p"]
        temp_row = []
        
        def std_sort_key(k):
            if k in priority: return priority.index(k)
            nums = re.findall(r'\d+', k)
            if nums: return 100 + int(nums[-1])
            return 300
        
        for qual in sorted(standard_links.keys(), key=std_sort_key):
            if total_btn_count >= MAX_BUTTONS: break
            link = standard_links[qual]
            btn_text = qual
            if qual in priority: btn_text = f"📥 Download {qual}"
            elif "Episode" in qual: btn_text = qual.replace("Episode", "Ep")
            
            if qual in priority:
                if temp_row: buttons.append(temp_row); total_btn_count += len(temp_row); temp_row = []
                buttons.append([InlineKeyboardButton(btn_text, url=link)])
                total_btn_count += 1
            else:
                temp_row.append(InlineKeyboardButton(btn_text, url=link))
                if len(temp_row) == 3: buttons.append(temp_row); total_btn_count += 3; temp_row = []
        
        if temp_row: buttons.append(temp_row); total_btn_count += len(temp_row)
        
        # -- Series Links (Matrix) --
        if series_links:
            for ep in sorted(series_links.keys(), key=int):
                if total_btn_count >= MAX_BUTTONS:
                    buttons.append([InlineKeyboardButton(f"⚠️ Limit Reached (Ep {ep}+)", callback_data="ignore")])
                    break
                
                row = []
                row.append(InlineKeyboardButton(f"🎬 Ep {ep}", callback_data="ignore"))
                
                quals = series_links[ep]
                if "480p" in quals: row.append(InlineKeyboardButton("480p", url=quals["480p"]))
                if "720p" in quals: row.append(InlineKeyboardButton("720p", url=quals["720p"]))
                if "1080p" in quals: row.append(InlineKeyboardButton("1080p", url=quals["1080p"]))
                
                for q, l in quals.items():
                    if q not in ["480p", "720p", "1080p"]:
                         if len(row) < 5: row.append(InlineKeyboardButton(q, url=l))
                
                buttons.append(row)
                total_btn_count += len(row)
        
        # Tutorial
        user_data = await users_collection.find_one({'_id': uid})
        if user_data.get('tutorial_url'):
            buttons.append([InlineKeyboardButton("ℹ️ How to Download", url=user_data['tutorial_url'])])
        
        # 3. Poster Processing
        details = convo['details']
        poster_input = None
        if details.get('poster_local_path') and os.path.exists(details['poster_local_path']):
            poster_input = details['poster_local_path']
        elif details.get('poster_path'):
            poster_input = f"https://image.tmdb.org/t/p/w500{details['poster_path']}"
            
        poster_buffer, error = watermark_poster(poster_input, user_data.get('watermark_text'), convo.get('temp_badge_text'))
        
        if not poster_buffer: return await cb.message.edit_text(f"❌ Image Error: {error}")
        
        poster_buffer.seek(0)
        preview_msg = await client.send_photo(
            chat_id=uid, photo=poster_buffer, caption=caption, reply_markup=InlineKeyboardMarkup(buttons)
        )

        await cb.message.delete()
        
        convo['final_post_data'] = {'file_id': preview_msg.photo.file_id, 'caption': caption, 'buttons': buttons}
        
        channels = user_data.get('channel_ids', [])
        channel_btns = []
        if channels:
            for cid in channels:
                channel_btns.append([InlineKeyboardButton(f"📢 Post to: {cid}", callback_data=f"sndch_{cid}")])
        else:
            await client.send_message(uid, "⚠️ **No Channels Saved!** Add using `/addchannel <id>`.")
        
        channel_btns.append([InlineKeyboardButton("✅ DONE / CLOSE", callback_data="close_post")])
        await client.send_message(uid, "👇 **Select Channel to Publish:**", reply_markup=InlineKeyboardMarkup(channel_btns))

    except Exception as e:
        logger.error(f"Post Gen Error: {e}")
        await cb.message.edit_text(f"❌ **Error:** {str(e)}")

@bot.on_callback_query(filters.regex("^sndch_"))
async def send_to_channel_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    target_cid = cb.data.split("_")[1]
    convo = user_conversations.get(uid)
    if not convo: return await cb.answer("Session expired.", show_alert=True)
    
    data = convo['final_post_data']
    try:
        await client.send_photo(chat_id=int(target_cid), photo=data['file_id'], caption=data['caption'], reply_markup=InlineKeyboardMarkup(data['buttons']))
        await cb.answer(f"✅ Posted to {target_cid}", show_alert=True)
    except Exception as e:
        await cb.answer(f"❌ Failed: {e}", show_alert=True)

@bot.on_callback_query(filters.regex("^close_post"))
async def close_post_handler(client, cb: CallbackQuery):
    uid = cb.from_user.id
    if uid in user_conversations:
        local_path = user_conversations[uid].get('details', {}).get('poster_local_path')
        if local_path and os.path.exists(local_path):
            try: os.remove(local_path)
            except: pass     
        del user_conversations[uid]
    await cb.message.delete()
    await cb.answer("✅ Session Closed.", show_alert=True)

if __name__ == "__main__":
    logger.info("🚀 Bot is starting...")
    bot.run()
