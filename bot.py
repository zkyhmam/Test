# -*- coding: utf-8 -*-
import re
import asyncio
import os
import subprocess
import json
import time
import uuid
import math
import logging
from pyrogram import Client, filters, types
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

# --- Configuration ---
API_ID = 25713843 # Replace with your API_ID if different
API_HASH = "311352d08811e7f5136dfb27f71014c1" # Replace with your API_HASH if different
BOT_TOKEN = "7368701753:AAEPtxx3ZmDusfgC72tx-kbgYNsblDyoJBg" # Replace with your BOT_TOKEN if different
ADMIN_ID = 6988696258 # Replace with your ADMIN_ID if different

ADMIN_LINK = "https://t.me/Zaky1million" # Your admin link
CHANNEL_LINK = "https://t.me/theasiaworld" # Your channel link

# --- Bot Instance ---
app = Client("caption_editor_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- In-memory storage ---
user_data = {
    ADMIN_ID: {
        "state": "idle",
        "structure": {},
        "start_season": 1,
        "start_episode": 1,
        "current_season": 1,
        "current_episode": 1,
        "show_name": "",
        "last_prompt_message_id": None,
        "message_buffer": [],
        "processing_lock": asyncio.Lock(),
        "thumb_file_id": None,
        "watermark_text": None,
        "caption_format": "basic", # Default format
        "show_year": None,
    }
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logging.getLogger("pyrogram").setLevel(logging.WARNING) # Reduce pyrogram verbosity

# --- Helper Functions ---

async def delete_last_prompt(chat_id):
    if chat_id in user_data and user_data[chat_id].get("last_prompt_message_id"):
        try:
            await app.delete_messages(chat_id, user_data[chat_id]["last_prompt_message_id"])
        except Exception as e:
            logging.warning(f"Could not delete message {user_data[chat_id]['last_prompt_message_id']}: {e}")
        finally:
            if chat_id in user_data:
                 user_data[chat_id]["last_prompt_message_id"] = None

def parse_season_episode_format(text):
    if not text: return None
    match = re.fullmatch(r"S(\d+)E(\d+)", text.strip(), re.IGNORECASE)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        if season > 0 and episode > 0:
            return season, episode
    return None

def parse_structure_string(structure_str):
    if not structure_str: return None
    parsed_structure = {}
    parts = re.findall(r"S\d+E\d+", structure_str.upper())
    season_episode_pairs = []
    for part in parts:
        parsed = parse_season_episode_format(part)
        if not parsed: continue
        season_episode_pairs.append(parsed)
    if not season_episode_pairs: return None
    season_episode_pairs.sort(key=lambda x: x[0])
    last_ep_count = None
    max_processed_season = 0
    processed_seasons = set()
    for season, episodes in season_episode_pairs:
        if episodes <= 0: return None
        if season in processed_seasons:
             logging.warning(f"Duplicate definition for S{season}. Using last one.")
        if last_ep_count is not None:
            for s_gap in range(max_processed_season + 1, season):
                 if s_gap not in parsed_structure:
                     parsed_structure[s_gap] = last_ep_count
        parsed_structure[season] = episodes
        processed_seasons.add(season)
        last_ep_count = episodes
        max_processed_season = max(max_processed_season, season)
    if max_processed_season > 0:
         parsed_structure['default'] = parsed_structure[max_processed_season]
    elif last_ep_count is not None:
        parsed_structure['default'] = last_ep_count
    else: return None
    min_defined_season = min(s[0] for s in season_episode_pairs) if season_episode_pairs else 0
    if min_defined_season > 1:
        first_defined_ep_count = parsed_structure[min_defined_season]
        for s_gap in range(1, min_defined_season):
            if s_gap not in parsed_structure:
                parsed_structure[s_gap] = first_defined_ep_count
    if 'default' not in parsed_structure and 1 in parsed_structure:
        parsed_structure['default'] = parsed_structure[1]
    if not parsed_structure or 'default' not in parsed_structure:
        logging.error("Could not determine default episode count.")
        return None
    return parsed_structure

def get_episodes_for_season(structure, season):
    if not structure or 'default' not in structure:
        logging.warning(f"Structure/default missing for season {season}. Returning 0.")
        return 0
    return structure.get(season, structure['default'])

def format_caption_basic(show_name, season, episode):
    return f"{show_name} S{season:02d}E{episode:02d}"

# New function to get quality string based on height
def get_quality_string(height):
    if not height or height <= 0:
        return "Unknown Quality"

    if height <= 360:
        quality_p = "360p"
        quality_desc = "SD 😊♥️"
    elif height <= 480:
        quality_p = "480p"
        quality_desc = "SD 😊♥️"
    elif height <= 540:
        quality_p = "540p"
        quality_desc = "HD 😊♥️"
    elif height <= 720:
        quality_p = "720p"
        quality_desc = "HD 😊♥️"
    else: # >= 1080p or slightly above 720p
        quality_p = "1080p"
        quality_desc = "FHD 😊♥️"

    return f"{quality_p} - {quality_desc}"

async def send_prompt(chat_id, text, state_to_set, reply_markup=None):
    """Sends prompt with ForceReply or InlineKeyboard, disabling preview."""
    await delete_last_prompt(chat_id)
    is_force_reply = not reply_markup # Assume ForceReply if no markup provided

    try:
        # Always send the main prompt text first
        sent_message = await app.send_message(
            chat_id,
            text,
            reply_markup=types.ForceReply(selective=True) if is_force_reply else None,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

        # If it's an inline keyboard, send it as a reply to the prompt text
        prompt_msg_to_store_id = sent_message.id # Default to the main message ID
        if not is_force_reply and reply_markup:
             keyboard_message = await app.send_message(
                 chat_id,
                 "اختر من الخيارات:", # Generic text for keyboard message
                 reply_markup=reply_markup,
                 reply_to_message_id=sent_message.id,
                 disable_web_page_preview=True
             )
             prompt_msg_to_store_id = keyboard_message.id # Store the keyboard message ID to delete later

        if chat_id in user_data:
            user_data[chat_id]["last_prompt_message_id"] = prompt_msg_to_store_id
            user_data[chat_id]["state"] = state_to_set
    except Exception as e:
        logging.error(f"Error sending prompt to {chat_id}: {e}")
        if chat_id in user_data:
            user_data[chat_id]["state"] = "idle"

# --- Decorators ---
def admin_only(func):
    async def wrapper(client, message):
        if message.from_user and message.from_user.id == ADMIN_ID:
            if ADMIN_ID not in user_data: # Initialize if missing (robustness)
                 user_data[ADMIN_ID] = {
                    "state": "idle", "structure": {}, "start_season": 1, "start_episode": 1,
                    "current_season": 1, "current_episode": 1, "show_name": "",
                    "last_prompt_message_id": None, "message_buffer": [],
                    "processing_lock": asyncio.Lock(), "thumb_file_id": None,
                    "watermark_text": None, "caption_format": "basic", "show_year": None,
                 }
            await func(client, message)
        elif message.from_user:
            logging.info(f"Ignoring message from non-admin user: {message.from_user.id}")
        else:
             logging.info("Ignoring message with no sender information.")
    return wrapper

def admin_only_callback(func):
     async def wrapper(client, callback_query):
        if callback_query.from_user and callback_query.from_user.id == ADMIN_ID:
            if ADMIN_ID not in user_data: # Initialize if missing
                 user_data[ADMIN_ID] = {
                    "state": "idle", "structure": {}, "start_season": 1, "start_episode": 1,
                    "current_season": 1, "current_episode": 1, "show_name": "",
                    "last_prompt_message_id": None, "message_buffer": [],
                    "processing_lock": asyncio.Lock(), "thumb_file_id": None,
                    "watermark_text": None, "caption_format": "basic", "show_year": None,
                 }
            await func(client, callback_query)
        elif callback_query.from_user:
             try:
                await callback_query.answer("أنت غير مصرح لك.", show_alert=True)
             except Exception as e:
                 logging.warning(f"Error answering callback for non-admin {callback_query.from_user.id}: {e}")
        else:
             logging.info("Ignoring callback with no sender information.")
             try: await callback_query.answer()
             except: pass
     return wrapper

# --- Progress Helpers ---
def humanbytes(size: float) -> str:
    if not size: return "0 B"
    power = 1024; n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) - 1: # Prevent index error
        size /= power; n += 1
    return f"{size:.2f} {power_labels[n]}"

def time_formatter(seconds: float) -> str:
    if seconds is None or seconds < 0: return "---"
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = (((str(days) + "d, ") if days else "") +
           ((str(hours) + "h, ") if hours else "") +
           ((str(minutes) + "m, ") if minutes else "") +
           ((str(seconds) + "s") if seconds else ""))
    return tmp.strip(', ') if tmp else "0s"

# Stores message_id, last update time, etc.
progress_trackers = {}

async def progress_callback(current, total, client: Client, chat_id: int, message_id: int, action_title: str, file_name: str, unique_key: str):
    """Callback for download/upload progress with new English format and linked bar."""
    global progress_trackers
    now = time.time()

    if unique_key not in progress_trackers:
        progress_trackers[unique_key] = {
            "start_time": now, "last_update_time": now,
            "last_current_bytes": current, "message_id": message_id
        }

    tracker = progress_trackers[unique_key]
    # Throttle updates: enforce >= 1 second
    if now - tracker["last_update_time"] < 1.0 and current != total:
        return

    if total > 0:
        percentage = (current / total) * 100
        elapsed_time = now - tracker["start_time"]
        speed = (current - tracker["last_current_bytes"]) / (now - tracker["last_update_time"]) if (now - tracker["last_update_time"]) > 0 else 0
        eta_seconds = ((total - current) / speed) if speed > 0 else 0

        current_f = humanbytes(current)
        total_f = humanbytes(total)
        speed_f = f"{humanbytes(speed)}/s" if speed > 0 else "---"
        eta_f = time_formatter(eta_seconds) if eta_seconds > 0 and eta_seconds != float('inf') else "---"
        safe_filename = file_name.replace("<", "<").replace(">", ">")
        icon = "🚀" if action_title == "Downloading" else "💠" # Use 💠 for upload

        # Generate linked progress bar
        bar_length = 20
        filled_length = int(bar_length * percentage / 100)
        filled_part = '▣' * filled_length
        empty_part = '▢' * (bar_length - filled_length)
        # Embed link only in the filled part
        linked_filled_part = f'<a href="{ADMIN_LINK}">{filled_part}</a>' if filled_part else ""
        progress = linked_filled_part + empty_part

        text = (
            f"<b>{icon} Try To {action_title}... ⚡</b>\n\n" # English title
            f"<code>{progress}</code>\n\n"
            f"🔗 <b>Size :</b> {current_f} | {total_f}\n"
            f"⏳ <b>Done :</b> {percentage:.2f}%\n"
            f"🚀 <b>Speed :</b> {speed_f}\n"
            f"⏰ <b>ETA :</b> {eta_f}"
        )

        try:
            await client.edit_message_text(
                chat_id=chat_id, message_id=tracker["message_id"],
                text=text, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True # Disable preview
            )
            tracker["last_update_time"] = now
            tracker["last_current_bytes"] = current
        except FloodWait as e:
             logging.warning(f"Flood wait of {e.value}s during progress update for {unique_key}. Sleeping.")
             await asyncio.sleep(e.value + 1.0) # Wait out floodwait + 1s buffer
             # After waiting, force tracker update time to allow immediate next edit attempt
             tracker["last_update_time"] = time.time()
             tracker["last_current_bytes"] = current # Assume bytes didn't change during sleep
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                 logging.warning(f"Could not edit progress message {tracker['message_id']}: {e}")

    if current == total:
        if unique_key in progress_trackers:
            del progress_trackers[unique_key]

async def watermark_progress_spinner(client: Client, chat_id: int, message_id: int, original_filename: str):
    """Displays a spinning progress indicator with link for watermark."""
    start_time = time.time()
    interval = 1.0  # Minimum interval
    spinner_len = 7
    frame_index = 0

    try:
        while True:
            elapsed_time = time.time() - start_time
            elapsed_f = time_formatter(elapsed_time)

            # Create spinner bar with moving linked character
            bar_list = ['▢'] * spinner_len
            current_pos = frame_index % spinner_len
            bar_list[current_pos] = f'<a href="{ADMIN_LINK}">▣</a>' # Linked character
            spinner_bar = "".join(bar_list)

            safe_filename = original_filename.replace("<", "<").replace(">", ">")
            text = (
                f"<b>🖌️ Applying Watermark...</b>\n"
                # f"<code>{safe_filename}</code>\n\n" # Optional: Keep filename if needed
                f"⚙️ <b>Progress:</b> <code>{spinner_bar}</code>\n"
                f"⏱️ <b>Elapsed:</b> {elapsed_f}\n"
                # f"<i>(This can take time...)</i>" # Optional comment
            )

            next_update_time = time.time() + interval # Calculate when next update should happen
            try:
                await client.edit_message_text(
                    chat_id=chat_id, message_id=message_id,
                    text=text, parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True # Disable preview
                )
                # Reset interval on success IF it was increased due to flood wait
                # Keep it at 1.0 otherwise
                interval = 1.0
            except FloodWait as e:
                logging.warning(f"Flood wait of {e.value}s during watermark spinner. Increasing interval.")
                wait_duration = e.value + 1.0 # Wait flood + 1s buffer
                interval = wait_duration # Set next interval to wait out flood
                next_update_time = time.time() + wait_duration
            except asyncio.CancelledError:
                 logging.info("Watermark spinner task cancellation received.")
                 break
            except Exception as e:
                if "MESSAGE_NOT_MODIFIED" not in str(e):
                    logging.warning(f"Error updating watermark spinner: {e}")

            # Sleep until the next update time, respecting the potentially increased interval
            sleep_duration = max(0, next_update_time - time.time())
            await asyncio.sleep(sleep_duration)
            frame_index += 1

    except asyncio.CancelledError:
         logging.info("Watermark spinner task cancelled during sleep.")
    finally:
         logging.info("Watermark spinner task finished.")

# --- Metadata Extraction ---
async def get_video_metadata(file_path: str) -> dict | None:
    if not os.path.exists(file_path):
        logging.error(f"get_video_metadata: File not found: {file_path}")
        return None
    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', file_path]
    try:
        process = await asyncio.create_subprocess_exec(
            *command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logging.error(f"ffprobe error for {file_path}: {stderr.decode('utf-8', 'ignore')}")
            return None
        metadata = json.loads(stdout.decode('utf-8', 'ignore'))
        duration = 0; width = 0; height = 0
        if 'format' in metadata and 'duration' in metadata['format']:
            try: duration = int(float(metadata['format']['duration']))
            except: pass
        if 'streams' in metadata:
            for stream in metadata['streams']:
                if stream.get('codec_type') == 'video':
                    try:
                        width = int(stream.get('width', 0))
                        height = int(stream.get('height', 0))
                        if duration == 0 and 'duration' in stream:
                             try: duration = int(float(stream['duration']))
                             except: pass
                    except: pass
                    break # Use first video stream
        # Return dict even if some values are 0, handle downstream
        return {'duration': duration, 'width': width, 'height': height}
    except FileNotFoundError:
        logging.error("ffprobe command not found. Is ffmpeg installed and in PATH?")
        return None
    except Exception as e:
        logging.exception(f"Unexpected error in get_video_metadata for {file_path}: {e}")
        return None

# --- Command Handlers ---

@app.on_message(filters.command("start") & filters.private)
@admin_only
async def start_command(client, message):
    await delete_last_prompt(message.chat.id)
    await message.reply_text(
        f"مرحباً أيها الأدمن! 👋\n\n"
        f"أنا بوت تعديل كابشن الفيديو.\n"
        f"اختر أحد الأوضاع لبدء العمل:\n\n"
        f"<b>الأوامر المتاحة:</b>\n"
        f"/start - عرض هذه الرسالة\n"
        f"/new - 🚶 بدء إعداد مهمة جديدة خطوة بخطوة (تحديد هيكل، بداية، اسم)\n"
        f"/auto <code>[اسم] [هيكل] [بداية]</code> - ⚡️ إعداد سريع لمهمة تسلسلية\n"
        f"/auto2 - 🔎 بدء وضع الاستخراج التلقائي (اسم، غلاف، علامة، تنسيق)\n" # Updated description
        f"/cancel - 🛑 إلغاء أي مهمة جارية وإعادة البوت لوضع الخمول\n\n"
        f"<b>ملاحظة:</b> استخدم /cancel لإيقاف أي عملية قبل بدء واحدة جديدة.",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

@app.on_message(filters.command("new") & filters.private)
@admin_only
async def new_command(client, message):
    await delete_last_prompt(message.chat.id)
    user_data[ADMIN_ID].update({
        "state": "await_structure", "structure": {}, "start_season": 1, "start_episode": 1,
        "current_season": 1, "current_episode": 1, "show_name": "", "last_prompt_message_id": None,
        "message_buffer": [], "thumb_file_id": None, "watermark_text": None,
        "caption_format": "basic", "show_year": None, # Reset all task data
    })
    prompt_text = (
        f"🎬 <b>مهمة جديدة (/new): تحديد هيكل المواسم</b>\n\n"
        f"أرسل هيكل عدد الحلقات لكل موسم.\n"
        f"<b>التنسيق:</b> <code>S<موسم>E<حلقات></code> (مثال: <code>S1E12</code> أو <code>S1E10-S2E20</code>)\n"
    )
    await send_prompt(message.chat.id, prompt_text, "await_structure")

@app.on_message(filters.command("auto") & filters.private)
@admin_only
async def auto_command(client, message):
    await delete_last_prompt(message.chat.id)
    text_parts = message.text.split(maxsplit=1)
    if len(text_parts) < 2:
        await message.reply_text("❌ <b>خطأ:</b> الأمر /auto يحتاج وسائط.", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
        return
    args_part = text_parts[1]
    start_match = re.search(r"S(\d+)E(\d+)$", args_part.strip(), re.IGNORECASE)
    if not start_match:
        await message.reply_text("❌ <b>خطأ:</b> لم أجد تنسيق نقطة البداية (<code>S<رقم>E<رقم></code>) في نهاية الأمر.", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
        return
    start_str = start_match.group(0)
    start_point = parse_season_episode_format(start_str)
    if not start_point:
         await message.reply_text(f"❌ <b>خطأ:</b> تنسيق نقطة البداية '<code>{start_str}</code>' غير صالح.", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
         if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
         return
    start_season, start_episode = start_point
    before_start = args_part[:start_match.start()].strip()
    structure_match = re.search(r"S\d+E\d+", before_start, re.IGNORECASE)
    if not structure_match:
         await message.reply_text("❌ <b>خطأ:</b> لم أجد تنسيق هيكل المواسم (<code>S<رقم>E<رقم>...</code>) قبل نقطة البداية.", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
         if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
         return
    potential_structure_part = before_start[structure_match.start():].strip()
    structure = parse_structure_string(potential_structure_part)
    if structure is None:
         await message.reply_text(f"❌ <b>خطأ: تنسيق هيكل المواسم غير صالح.</b>\nالجزء المحلل: <code>{potential_structure_part}</code>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
         if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
         return
    name = before_start[:structure_match.start()].strip()
    if not name:
         await message.reply_text("❌ <b>خطأ:</b> لم أتمكن من تحديد اسم المسلسل.", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
         if ADMIN_ID in user_data: user_data[ADMIN_ID]["state"] = "idle"
         return
    user_data[ADMIN_ID].update({
        "state": "processing", "structure": structure, "start_season": start_season,
        "start_episode": start_episode, "current_season": start_season, "current_episode": start_episode,
        "show_name": name, "last_prompt_message_id": None, "message_buffer": [],
        "thumb_file_id": None, "watermark_text": None, "caption_format": "basic", "show_year": None, # Reset others
    })
    await message.reply_text(
        f"✅ <b>تم الحفظ بنجاح باستخدام /auto!</b>\n\n"
        f"<b>اسم المسلسل:</b> {name}\n"
        f"<b>هيكل المواسم:</b> تم تحليله (الافتراضي: {structure.get('default', 'N/A')}).\n"
        f"<b>سأبدأ التسمية من:</b> S{start_season:02d}E{start_episode:02d}\n\n"
        f"الآن يمكنك إرسال ملفات الفيديو لي (وضع التسمية التسلسلية).",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

@app.on_message(filters.command("auto2") & filters.private)
@admin_only
async def auto2_command(client, message):
    await delete_last_prompt(message.chat.id)
    user_data[ADMIN_ID].update({
        "state": "await_name_auto2", "structure": {}, "start_season": 1, "start_episode": 1,
        "current_season": 1, "current_episode": 1, "show_name": "", "last_prompt_message_id": None,
        "message_buffer": [], "thumb_file_id": None, "watermark_text": None,
        "caption_format": "basic", "show_year": None, # Reset all
    })
    prompt_text = (
        f"🎬 <b>وضع الاستخراج التلقائي (/auto2)</b>\n\n"
        f"🏷️ <b>الخطوة 1: اسم المسلسل</b>\n"
        f"أرسل اسم المسلسل (مثال: <code>The Blacklist</code>)"
    )
    await send_prompt(message.chat.id, prompt_text, "await_name_auto2")

@app.on_message(filters.command("cancel") & filters.private)
@admin_only
async def cancel_command(client, message):
    chat_id = message.chat.id
    await delete_last_prompt(chat_id)
    if ADMIN_ID in user_data:
        user_data[ADMIN_ID].update({
            "state": "idle", "structure": {}, "start_season": 1, "start_episode": 1,
            "current_season": 1, "current_episode": 1, "show_name": "",
            "last_prompt_message_id": None, "message_buffer": [],
            "thumb_file_id": None, "watermark_text": None,
            "caption_format": "basic", "show_year": None, # Reset all task data
        })
        logging.info(f"Task cancelled by user {ADMIN_ID}. State reset to idle.")
        await message.reply_text("❌ تم إلغاء العملية الحالية بنجاح.\nالبوت الآن في وضع الخمول.", disable_web_page_preview=True)
    else:
        await message.reply_text("لم يتم العثور على بيانات للمستخدم.", disable_web_page_preview=True)

# --- Callback Handlers ---

@app.on_callback_query(filters.regex("^cancel_step$"))
@admin_only_callback
async def cancel_step_callback(client, callback_query: types.CallbackQuery):
    chat_id = callback_query.message.chat.id
    try: await callback_query.message.delete()
    except Exception as e: logging.warning(f"Could not delete keyboard message via callback: {e}")
    # Try deleting the original prompt the keyboard replied to
    if callback_query.message.reply_to_message:
        try: await app.delete_messages(chat_id, callback_query.message.reply_to_message.id)
        except Exception as e: logging.warning(f"Could not delete original prompt via callback cancel: {e}")

    if ADMIN_ID in user_data:
        user_data[ADMIN_ID].update({
            "state": "idle", "structure": {}, "start_season": 1, "start_episode": 1,
            "current_season": 1, "current_episode": 1, "show_name": "",
            "last_prompt_message_id": None, "message_buffer": [],
            "thumb_file_id": None, "watermark_text": None,
            "caption_format": "basic", "show_year": None, # Reset all task data
        })
    try:
        await callback_query.answer("تم إلغاء الخطوة والعودة للخمول.", show_alert=False)
        await app.send_message(chat_id, "❌ تم إلغاء إعداد المهمة. يمكنك البدء من جديد.", disable_web_page_preview=True)
    except Exception as e:
        logging.warning(f"Error answering cancel callback or sending confirmation: {e}")

@app.on_callback_query(filters.regex("^set_format_(basic|new)$"))
@admin_only_callback
async def caption_format_callback(client: Client, callback_query: types.CallbackQuery):
    chat_id = callback_query.message.chat.id
    chosen_format = callback_query.data.split("_")[-1] # "basic" or "new"

    if ADMIN_ID not in user_data or user_data[ADMIN_ID].get("state") != "await_caption_format_auto2":
        await callback_query.answer("الحالة غير متوقعة، حاول البدء من جديد.", show_alert=True)
        return

    user_data[ADMIN_ID]["caption_format"] = chosen_format
    await delete_last_prompt(chat_id) # Delete the keyboard message
    # Try deleting the message the keyboard replied to
    if callback_query.message.reply_to_message:
        try: await app.delete_messages(chat_id, callback_query.message.reply_to_message.id)
        except Exception as e: logging.warning(f"Could not delete format prompt text: {e}")

    await callback_query.answer(f"تم اختيار التنسيق: {chosen_format}")

    if chosen_format == "new":
        # Prompt for year
        prompt_text = (
            f"✅ <b>تم اختيار التنسيق الجديد.</b>\n\n"
            f"📅 <b>الخطوة التالية: سنة الإنتاج</b>\n\n"
            f"أرسل سنة إنتاج المسلسل (مثال: <code>2023</code>)"
        )
        await send_prompt(chat_id, prompt_text, "await_year_auto2")
    else: # Basic format chosen
        user_data[ADMIN_ID]["state"] = "processing_auto2" # Ready to process
        show_name = user_data[ADMIN_ID].get("show_name", "N/A")
        watermark = user_data[ADMIN_ID].get("watermark_text", "N/A")
        await app.send_message(
            chat_id,
            f"🎉 <b>اكتمل إعداد مهمة /auto2!</b>\n\n"
            f"<b>الاسم:</b> {show_name}\n"
            f"<b>الغلاف:</b> تم الحفظ ✅\n"
            f"<b>العلامة:</b> <code>{watermark}</code> ✅\n"
            f"<b>التنسيق:</b> أساسي ✅\n\n"
            f"✅ جاهز لاستقبال الفيديوهات.",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )


# --- Main Message Handler ---

@app.on_message(filters.private)
@admin_only
async def handle_messages(client: Client, message: types.Message):
    if message.text and message.text.startswith('/'):
        return

    if ADMIN_ID not in user_data:
        logging.error("Admin data not found in handle_messages.")
        return

    admin_state = user_data[ADMIN_ID].get("state", "idle")
    chat_id = message.chat.id

    is_reply_to_bot_prompt = False
    original_prompt_msg_id = None
    last_prompt_keyboard_msg_id = user_data[ADMIN_ID].get("last_prompt_message_id")

    if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.is_self:
        is_prompt_target = False
        # Check if replying to the keyboard msg itself (which has last_prompt_message_id)
        if message.reply_to_message.id == last_prompt_keyboard_msg_id:
             # The original text prompt is the one the keyboard replied to
             if message.reply_to_message.reply_to_message:
                 original_prompt_msg_id = message.reply_to_message.reply_to_message.id
             is_prompt_target = True
        # Check if replying to the force reply msg directly
        elif message.reply_to_message.reply_markup and isinstance(message.reply_to_message.reply_markup, types.ForceReply):
            original_prompt_msg_id = message.reply_to_message.id
            is_prompt_target = True

        # Only consider it a reply to *our* prompt if one of the above is true
        if is_prompt_target:
            is_reply_to_bot_prompt = True


    # --- State Machine for Configuration ---
    if admin_state in ["await_structure", "await_start", "await_name", "await_name_auto2", "await_thumb_auto2", "await_watermark_auto2", "await_year_auto2"]: # Added await_year_auto2
        if is_reply_to_bot_prompt:
            # --- /new: Structure ---
            if admin_state == "await_structure" and message.text:
                structure = parse_structure_string(message.text)
                if structure:
                    user_data[ADMIN_ID]["structure"] = structure
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    prompt_text = (f"✅ <b>هيكل المواسم محفوظ.</b>\n\n"
                                   f"🗓️ <b>الخطوة 2: نقطة البداية</b> (<code>S1E1</code>)")
                    await send_prompt(chat_id, prompt_text, "await_start")
                else:
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    error_text = "❌ <b>خطأ: تنسيق الهيكل غير صالح.</b> أعد المحاولة:"
                    await send_prompt(chat_id, error_text, "await_structure")

            # --- /new: Start Point ---
            elif admin_state == "await_start" and message.text:
                start_point = parse_season_episode_format(message.text)
                if start_point:
                    s_season, s_episode = start_point
                    user_data[ADMIN_ID].update({"start_season": s_season, "start_episode": s_episode,
                                                "current_season": s_season, "current_episode": s_episode})
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    prompt_text = (f"✅ <b>نقطة البداية S{s_season:02d}E{s_episode:02d} محفوظة.</b>\n\n"
                                   f"🏷️ <b>الخطوة 3: اسم المسلسل</b>")
                    await send_prompt(chat_id, prompt_text, "await_name")
                else:
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    error_text = "❌ <b>خطأ: تنسيق نقطة البداية غير صالح.</b> أعد المحاولة:"
                    await send_prompt(chat_id, error_text, "await_start")

            # --- /new: Show Name ---
            elif admin_state == "await_name" and message.text:
                show_name = message.text.strip()
                if show_name:
                    user_data[ADMIN_ID]["show_name"] = show_name
                    user_data[ADMIN_ID]["state"] = "processing"
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    start_s = user_data[ADMIN_ID]['start_season']
                    start_e = user_data[ADMIN_ID]['start_episode']
                    await app.send_message(chat_id, f"🎉 <b>اكتمل إعداد /new!</b>\n"
                                           f"<b>الاسم:</b> {show_name}\n<b>البداية:</b> S{start_s:02d}E{start_e:02d}\n"
                                           f"✅ جاهز للمعالجة التسلسلية.",
                                           parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                else:
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    error_text = "❌ <b>خطأ: اسم المسلسل مطلوب.</b> أعد المحاولة:"
                    await send_prompt(chat_id, error_text, "await_name")

            # --- /auto2: Show Name ---
            elif admin_state == "await_name_auto2" and message.text:
                show_name = message.text.strip()
                if show_name:
                    user_data[ADMIN_ID]["show_name"] = show_name
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    prompt_text = (f"✅ <b>اسم المسلسل: {show_name}</b>\n\n"
                                   f"🖼️ <b>الخطوة 2: صورة الغلاف</b>")
                    await send_prompt(chat_id, prompt_text, "await_thumb_auto2")
                else:
                     await delete_last_prompt(chat_id)
                     try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                     except Exception: pass
                     error_text = "❌ <b>خطأ: اسم المسلسل مطلوب.</b> أعد المحاولة:"
                     await send_prompt(chat_id, error_text, "await_name_auto2")

            # --- /auto2: Thumbnail ---
            elif admin_state == "await_thumb_auto2":
                 if message.photo:
                    thumb_file_id = message.photo.file_id
                    user_data[ADMIN_ID]["thumb_file_id"] = thumb_file_id
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    prompt_text = (
                        f"✅ <b>صورة الغلاف محفوظة.</b>\n\n"
                        f"🖋️ <b>الخطوة 3: نص العلامة المائية</b>\n\n"
                        f"أرسل النص للعلامة المائية (مثل اسم قناتك)."
                    )
                    await send_prompt(chat_id, prompt_text, "await_watermark_auto2")
                 elif message.text :
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    error_text = "❌ لم ترسل صورة. أرسل صورة الغلاف:"
                    await send_prompt(chat_id, error_text, "await_thumb_auto2")

            # --- /auto2: Watermark ---
            elif admin_state == "await_watermark_auto2":
                if message.text:
                    watermark_text = message.text.strip()
                    if watermark_text:
                        user_data[ADMIN_ID]["watermark_text"] = watermark_text
                        await delete_last_prompt(chat_id)
                        try:
                            if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                            await message.delete()
                        except Exception: pass

                        # Now ask for caption format using buttons
                        prompt_text = (
                            f"✅ <b>العلامة المائية:</b> <code>{watermark_text}</code>\n\n"
                            f"🎨 <b>الخطوة 4: تنسيق الكابشن</b>\n\n"
                            f"اختر تنسيق الكابشن المطلوب للفيديوهات:"
                        )
                        markup = types.InlineKeyboardMarkup([
                            [types.InlineKeyboardButton("📝 تنسيق أساسي (اسم SXXEXX)", callback_data="set_format_basic")],
                            [types.InlineKeyboardButton("✨ تنسيق جديد (متعدد الأسطر)", callback_data="set_format_new")],
                            [types.InlineKeyboardButton("⛔️ إلغاء", callback_data="cancel_step")] # Keep cancel option
                        ])
                        await send_prompt(chat_id, prompt_text, "await_caption_format_auto2", reply_markup=markup)

                    else:
                        await delete_last_prompt(chat_id)
                        try:
                            if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                            await message.delete()
                        except Exception: pass
                        error_text = "❌ <b>خطأ: نص العلامة المائية مطلوب.</b> أعد المحاولة:"
                        await send_prompt(chat_id, error_text, "await_watermark_auto2")
                else:
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass
                    error_text = "❌ لم ترسل نصًا. أرسل نص العلامة المائية:"
                    await send_prompt(chat_id, error_text, "await_watermark_auto2")

            # --- /auto2: Year (for New Format) ---
            elif admin_state == "await_year_auto2" and message.text:
                year_text = message.text.strip()
                # Basic validation: check if it's roughly a 4-digit number
                if year_text.isdigit() and len(year_text) == 4:
                    user_data[ADMIN_ID]["show_year"] = year_text
                    user_data[ADMIN_ID]["state"] = "processing_auto2" # Ready to process
                    await delete_last_prompt(chat_id)
                    try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                    except Exception: pass

                    show_name = user_data[ADMIN_ID].get("show_name", "N/A")
                    watermark = user_data[ADMIN_ID].get("watermark_text", "N/A")
                    await app.send_message(
                        chat_id,
                        f"🎉 <b>اكتمل إعداد مهمة /auto2!</b>\n\n"
                        f"<b>الاسم:</b> {show_name}\n"
                        f"<b>الغلاف:</b> تم الحفظ ✅\n"
                        f"<b>العلامة:</b> <code>{watermark}</code> ✅\n"
                        f"<b>التنسيق:</b> جديد (سنة: {year_text}) ✅\n\n"
                        f"✅ جاهز لاستقبال الفيديوهات.",
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                else:
                     await delete_last_prompt(chat_id)
                     try:
                        if original_prompt_msg_id: await app.delete_messages(chat_id, original_prompt_msg_id)
                        await message.delete()
                     except Exception: pass
                     error_text = f"❌ <b>خطأ:</b> '<code>{year_text}</code>' ليست سنة صالحة (أدخل 4 أرقام).\nأعد إرسال سنة الإنتاج:"
                     await send_prompt(chat_id, error_text, "await_year_auto2")


        # --- Handle non-reply or non-text replies during config ---
        elif not is_reply_to_bot_prompt and message.text :
             await message.reply_text("يرجى الرد على رسالة الطلب السابقة لإكمال الإعداد.", quote=True, disable_web_page_preview=True)
        elif (message.photo or message.video or message.document or message.sticker):
             # Delete unexpected media silently if not a reply to prompt
             if not is_reply_to_bot_prompt:
                try: await message.delete()
                except Exception as e: logging.warning(f"Could not delete unexpected media {message.id} during state {admin_state}: {e}")
             # If it *is* a reply, but wrong type (e.g. photo reply to text prompt)
             elif admin_state not in ["await_thumb_auto2"]: # Allow photo reply only for thumb state
                  await message.reply_text("الرجاء إرسال النوع المطلوب للرد (عادةً نص).", quote=True, disable_web_page_preview=True)


    # --- Processing: Sequential Mode ---
    elif admin_state == "processing":
        if message.video:
            if "message_buffer" not in user_data[ADMIN_ID]: user_data[ADMIN_ID]["message_buffer"] = []
            user_data[ADMIN_ID]["message_buffer"].append(message)
            if "processing_lock" not in user_data[ADMIN_ID]: user_data[ADMIN_ID]["processing_lock"] = asyncio.Lock()

            if not user_data[ADMIN_ID]["processing_lock"].locked():
                async with user_data[ADMIN_ID]["processing_lock"]:
                    user_data[ADMIN_ID]["message_buffer"].sort(key=lambda m: m.id)
                    while user_data[ADMIN_ID]["message_buffer"]:
                        if user_data[ADMIN_ID].get("state") != "processing":
                            logging.warning("State changed during sequential processing. Clearing buffer.")
                            user_data[ADMIN_ID]["message_buffer"].clear(); break

                        video_message = user_data[ADMIN_ID]["message_buffer"].pop(0)
                        msg_id = video_message.id
                        current_s = user_data[ADMIN_ID].get("current_season")
                        current_e = user_data[ADMIN_ID].get("current_episode")
                        show_name = user_data[ADMIN_ID].get("show_name")
                        structure = user_data[ADMIN_ID].get("structure")

                        if None in [current_s, current_e, show_name, structure] or 'default' not in structure:
                            logging.error(f"Invalid state data for sequential processing (msg {msg_id}).")
                            print(f"Edit: Failed validation for msg {msg_id}, Status: Error")
                            try:
                                await app.send_message(ADMIN_ID, f"⚠️ خطأ داخلي: بيانات /auto أو /new غير صالحة (فيديو {msg_id}).", disable_web_page_preview=True)
                                user_data[ADMIN_ID]["state"] = "idle"
                            except Exception as e_notify: logging.error(f"Error sending state error notification: {e_notify}")
                            user_data[ADMIN_ID]["message_buffer"].clear(); break

                        new_caption = format_caption_basic(show_name, current_s, current_e)
                        status = "Error"
                        try:
                            await client.copy_message(chat_id=chat_id, from_chat_id=chat_id,
                                                      message_id=video_message.id, caption=new_caption)
                            await video_message.delete()
                            status = "Done"

                            episodes_in_current_season = get_episodes_for_season(structure, current_s)
                            if episodes_in_current_season > 0 and current_e < episodes_in_current_season:
                                user_data[ADMIN_ID]["current_episode"] += 1
                            elif episodes_in_current_season > 0:
                                user_data[ADMIN_ID]["current_season"] += 1
                                user_data[ADMIN_ID]["current_episode"] = 1
                            else:
                                logging.error(f"Season {current_s} has 0 episodes. Stopping.")
                                print(f"Edit: {new_caption}, Status: Error - Season has 0 episodes")
                                await app.send_message(ADMIN_ID, f"⚠️ خطأ: الموسم {current_s} به 0 حلقات.", disable_web_page_preview=True)
                                user_data[ADMIN_ID]["state"] = "idle"; user_data[ADMIN_ID]["message_buffer"].clear(); break
                            await asyncio.sleep(0.3)
                        except Exception as e:
                            logging.exception(f"Error processing video {msg_id} sequentially.")
                            print(f"Edit: Failed for original msg {msg_id}, Status: Error")
                            print(f"Error Details: {e}")
                            try: await video_message.delete()
                            except Exception: pass
                        finally:
                            if status == "Done": print(f"Edit: {new_caption}, Status: Done")
        elif not message.video and admin_state == "processing":
             try: await message.delete()
             except Exception as e: logging.warning(f"Could not delete non-video message {message.id} in state {admin_state}: {e}")

    # --- Processing: Auto2 Mode ---
    elif admin_state == "processing_auto2":
        if message.video:
            if "processing_lock" not in user_data[ADMIN_ID]: user_data[ADMIN_ID]["processing_lock"] = asyncio.Lock()

            async with user_data[ADMIN_ID]["processing_lock"]:
                if user_data[ADMIN_ID].get("state") != "processing_auto2":
                    logging.warning("State changed during auto2 processing. Skipping.")
                    return

                video_message = message
                msg_id = video_message.id
                show_name = user_data[ADMIN_ID].get("show_name")
                thumb_file_id = user_data[ADMIN_ID].get("thumb_file_id")
                watermark_text = user_data[ADMIN_ID].get("watermark_text")
                caption_format_choice = user_data[ADMIN_ID].get("caption_format", "basic")
                show_year = user_data[ADMIN_ID].get("show_year")
                original_caption = video_message.caption or ""
                original_filename = video_message.video.file_name if video_message.video.file_name else f"video_{msg_id}"

                # --- Validations ---
                if not show_name: logging.error(f"Show name missing in auto2 mode (msg {msg_id})."); print(f"Edit: Failed valid msg {msg_id}, Status: Error - Missing Name"); user_data[ADMIN_ID]["state"] = "idle"; return
                if not thumb_file_id: logging.error(f"Thumb ID missing in auto2 mode (msg {msg_id})."); print(f"Edit: Failed valid msg {msg_id}, Status: Error - Missing Thumb"); user_data[ADMIN_ID]["state"] = "idle"; return
                if not watermark_text: logging.error(f"Watermark missing in auto2 mode (msg {msg_id})."); print(f"Edit: Failed valid msg {msg_id}, Status: Error - Missing Watermark"); user_data[ADMIN_ID]["state"] = "idle"; return
                if caption_format_choice == "new" and not show_year: logging.error(f"Year missing for new format (msg {msg_id})."); print(f"Edit: Failed valid msg {msg_id}, Status: Error - Missing Year"); user_data[ADMIN_ID]["state"] = "idle"; return
                # --- End Validations ---

                status = "Error"
                download_path = None
                thumb_download_path = None
                watermarked_path = None
                progress_message = None
                spinner_task = None
                unique_suffix = str(uuid.uuid4())[:6]
                base_filename = f"{msg_id}_{unique_suffix}"
                temp_dir = "./downloads"
                if not os.path.exists(temp_dir): os.makedirs(temp_dir)
                download_key = f"dl_{base_filename}"
                upload_key = f"ul_{base_filename}"

                font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
                if not os.path.exists(font_path):
                    potential_paths = ["/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf", "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf", "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf", "/system/fonts/Roboto-Regular.ttf"]
                    for p_path in potential_paths:
                        if os.path.exists(p_path): font_path = p_path; logging.info(f"Using fallback font: {font_path}"); break
                font_exists = os.path.exists(font_path)
                if not font_exists: logging.warning(f"No watermark font found. Skipping watermark for {msg_id}.")

                try:
                    progress_message = await client.send_message(chat_id, f"🚀 Preparing... <code>{original_filename}</code>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)

                    logging.info(f"[{base_filename}] Downloading thumbnail...")
                    thumb_download_path = await client.download_media(thumb_file_id, file_name=os.path.join(temp_dir, f"thumb_{base_filename}.jpg"))
                    if not thumb_download_path or not os.path.exists(thumb_download_path): raise Exception("Failed to download thumbnail.")
                    logging.info(f"[{base_filename}] Thumbnail downloaded.")

                    logging.info(f"[{base_filename}] Downloading video...")
                    download_path = await client.download_media(
                        video_message, file_name=os.path.join(temp_dir, f"video_{base_filename}.mp4"),
                        progress=progress_callback, progress_args=(client, chat_id, progress_message.id, "Downloading", original_filename, download_key)) # English action
                    if not download_path or not os.path.exists(download_path): raise Exception("Failed to download video.")
                    logging.info(f"[{base_filename}] Video downloaded.")

                    logging.info(f"[{base_filename}] Extracting metadata...")
                    metadata = await get_video_metadata(download_path)
                    duration = metadata.get('duration', 0) if metadata else 0
                    width = metadata.get('width', 0) if metadata else 0
                    height = metadata.get('height', 0) if metadata else 0
                    logging.info(f"[{base_filename}] Metadata: D={duration}, W={width}, H={height}")

                    video_to_upload = download_path
                    watermark_applied = False
                    if font_exists and watermark_text:
                        logging.info(f"[{base_filename}] Starting watermark process...")
                        if progress_message:
                            spinner_task = asyncio.create_task(watermark_progress_spinner(client, chat_id, progress_message.id, original_filename))

                        watermarked_path = os.path.join(temp_dir, f"wm_{base_filename}.mp4")
                        escaped_watermark = watermark_text.replace("'", "'\\\\\\''").replace(":", "\\\\:")
                        fontsize = 20; x_margin = 10; y_margin = 10
                        fontcolor = "white@0.8"; boxcolor = "black@0.5"; boxborderw = 5
                        drawtext_filter = (f"drawtext=text='{escaped_watermark}':x={x_margin}:y={y_margin}:fontsize={fontsize}:fontcolor='{fontcolor}':box=1:boxcolor='{boxcolor}':boxborderw={boxborderw}:fontfile='{font_path}'")
                        ffmpeg_command = ['ffmpeg', '-hide_banner', '-loglevel', 'warning', '-i', download_path, '-vf', drawtext_filter, '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23', '-c:a', 'copy', '-y', watermarked_path]

                        logging.info(f"[{base_filename}] Executing ffmpeg: {' '.join(ffmpeg_command)}")
                        ffmpeg_start_time = time.time()
                        process = await asyncio.create_subprocess_exec(*ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        stdout, stderr = await process.communicate()
                        ffmpeg_end_time = time.time()
                        ffmpeg_duration = ffmpeg_end_time - ffmpeg_start_time

                        if spinner_task and not spinner_task.done(): spinner_task.cancel() # Cancel right after ffmpeg finishes

                        stdout_str = stdout.decode('utf-8', 'ignore').strip()
                        stderr_str = stderr.decode('utf-8', 'ignore').strip()

                        if process.returncode == 0:
                            logging.info(f"[{base_filename}] ffmpeg completed in {ffmpeg_duration:.2f}s.")
                            if stderr_str: logging.warning(f"[{base_filename}] ffmpeg stderr (warnings):\n{stderr_str}")
                            video_to_upload = watermarked_path; watermark_applied = True
                        else:
                            logging.error(f"[{base_filename}] ffmpeg FAILED (Code: {process.returncode}) in {ffmpeg_duration:.2f}s.")
                            if stderr_str: logging.error(f"[{base_filename}] ffmpeg stderr:\n{stderr_str}")
                            if progress_message:
                                 try: await progress_message.edit_text(f"<b>⚠️ Error applying watermark!</b>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                                 except Exception: pass
                            await asyncio.sleep(3)
                    elif not font_exists: logging.warning(f"[{base_filename}] Font not found, skipping watermark.")
                    else: logging.warning(f"[{base_filename}] Watermark text empty, skipping.")

                    # --- Generate Caption based on choice ---
                    final_caption = ""
                    extracted_info = extract_season_episode_from_caption(original_caption)
                    season_num = extracted_info[0] if extracted_info else 0
                    episode_num = extracted_info[1] if extracted_info else 0

                    if caption_format_choice == "new":
                         if season_num > 0 and episode_num > 0 and show_year:
                             quality_str = get_quality_string(height)
                             arrow1 = f'<a href="{CHANNEL_LINK}">➦</a>'
                             arrow2 = f'<a href="{CHANNEL_LINK}">➨</a>'
                             arrow3 = f'<a href="{CHANNEL_LINK}">➥</a>'
                             final_caption = (
                                 f"{arrow1} S{season_num:02d}E{episode_num:02d}\n"
                                 f"{arrow2} {show_name} ({show_year})\n"
                                 f"{arrow3} {quality_str}"
                             )
                         else:
                             logging.warning(f"[{base_filename}] Missing data for new format (S/E:{extracted_info}, Year:{show_year}). Falling back to basic.")
                             final_caption = format_caption_basic(show_name, season_num, episode_num) if extracted_info else show_name
                    else: # Basic format
                         final_caption = format_caption_basic(show_name, season_num, episode_num) if extracted_info else show_name
                    # --- End Caption Generation ---


                    upload_action_text = "Uploading processed video" if watermark_applied else "Uploading original video"
                    logging.info(f"[{base_filename}] {upload_action_text}...")
                    if progress_message: # Update before upload starts
                        try: await progress_message.edit_text(f"⬆️ Preparing upload: <code>{original_filename}</code>...", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        except Exception: pass

                    await client.send_video(
                        chat_id=chat_id, video=video_to_upload, caption=final_caption,
                        thumb=thumb_download_path, duration=duration, width=width, height=height,
                        progress=progress_callback,
                        progress_args=(client, chat_id, progress_message.id, "Uploading", original_filename, upload_key), # English action
                        parse_mode=ParseMode.HTML, # Needed if caption has HTML links
                        disable_web_page_preview=True # Disable preview for caption links too
                    )
                    status = "Done"
                    logging.info(f"[{base_filename}] Video upload completed.")

                    await asyncio.sleep(1.0)
                    await video_message.delete()

                except Exception as e:
                    logging.exception(f"[{base_filename}] Error during overall processing of video {msg_id}: {e}")
                    print(f"Edit: Failed for original msg {msg_id}, Status: Error")
                    print(f"Error Details: {e}")
                    try: await video_message.delete()
                    except Exception: pass
                    if spinner_task and not spinner_task.done(): spinner_task.cancel() # Cancel spinner on general error

                finally:
                    # Ensure spinner is awaited/cleaned up if it was started
                    if spinner_task:
                        if not spinner_task.done(): spinner_task.cancel()
                        try: await spinner_task
                        except asyncio.CancelledError: pass # Expected
                        except Exception as e_spin_final: logging.error(f"[{base_filename}] Error awaiting final spinner cancel: {e_spin_final}")

                    if progress_message:
                        try: await progress_message.delete()
                        except Exception: pass

                    files_to_delete = [download_path, thumb_download_path, watermarked_path]
                    for f_path in files_to_delete:
                        if f_path and os.path.exists(f_path):
                             try: os.remove(f_path); logging.info(f"[{base_filename}] Cleaned temp: {os.path.basename(f_path)}")
                             except OSError as rm_err: logging.error(f"[{base_filename}] Failed remove {f_path}: {rm_err}")

                    if download_key in progress_trackers: del progress_trackers[download_key]
                    if upload_key in progress_trackers: del progress_trackers[upload_key]
                    if status == "Done":
                        print(f"Edit: {final_caption.splitlines()[0] if final_caption else 'N/A'}, Status: Done") # Log first line

        elif not message.video and admin_state == "processing_auto2":
             try: await message.delete()
             except Exception as e: logging.warning(f"Could not delete non-video message {message.id} in state {admin_state}: {e}")

    # --- Idle State ---
    elif admin_state == "idle":
        if message.video:
             await message.reply_text("البوت في وضع الخمول. استخدم أمر بدء.", disable_web_page_preview=True)
        elif message.text or message.photo or message.document or message.sticker:
             try: await message.delete() # Delete stray messages
             except Exception: pass

# --- Main Execution ---
async def set_bot_commands():
    try:
        await app.set_bot_commands([
            types.BotCommand("start", "▶️ بدء وعرض المساعدة"),
            types.BotCommand("new", "🚶 إعداد مهمة (خطوة بخطوة)"),
            types.BotCommand("auto", "⚡️ إعداد سريع (تسلسلي)"),
            types.BotCommand("auto2", "🔎 إعداد سريع (استخراج + غلاف + علامة)"),
            types.BotCommand("cancel", "🛑 إلغاء المهمة الحالية")
        ])
        logging.info("Bot commands updated successfully.")
    except Exception as e:
        logging.error(f"Failed to set bot commands: {e}")

async def main():
    logging.info("Starting bot...")
    async with app:
        if ADMIN_ID not in user_data: # Initialize if completely missing
            user_data[ADMIN_ID] = {
                "state": "idle", "structure": {}, "start_season": 1, "start_episode": 1,
                "current_season": 1, "current_episode": 1, "show_name": "",
                "last_prompt_message_id": None, "message_buffer": [],
                "processing_lock": asyncio.Lock(), "thumb_file_id": None,
                "watermark_text": None, "caption_format": "basic", "show_year": None,
            }
        await set_bot_commands()
        bot_info = await app.get_me()
        logging.info(f"Bot started as @{bot_info.username} (ID: {bot_info.id})")
        logging.info(f"Admin ID: {ADMIN_ID}")
        logging.info("Bot is listening...")
        await asyncio.Event().wait() # Keep running

if __name__ == "__main__":
    try:
        # Register handlers before running
        # Commands are handled by decorators
        # Need to explicitly add callback handlers
        app.add_handler(CallbackQueryHandler(cancel_step_callback, filters.regex("^cancel_step$")))
        app.add_handler(CallbackQueryHandler(caption_format_callback, filters.regex("^set_format_(basic|new)$")))
        # Message handler is handled by decorator

        app.run(main()) # Use app.run() for better lifecycle management
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
    except Exception as e:
        logging.critical(f"Critical error starting or running bot: {e}", exc_info=True)
    finally:
        logging.info("Bot shutting down...")
