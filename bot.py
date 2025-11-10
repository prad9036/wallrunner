import json
import random
import logging
import asyncio
import os
import httpx
from telethon import TelegramClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from urllib.parse import urlparse
import hashlib
from PIL import Image
import imagehash
import base64
from datetime import datetime
from dotenv import load_dotenv
import signal
import aiofiles
import aiofiles.os as async_os


# --- Load environment first ---
load_dotenv()

print("API_ID env:", os.getenv("API_ID"))
print("API_HASH env length:", len(os.getenv("API_HASH") or ""))
print("BOT_TOKEN starts with:", (os.getenv("BOT_TOKEN") or "")[:10])

# --- Config ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SESSION = "wallpaper_bot"
DATA_FILE = "wallpapers.json"

# --- Bot Groups ---
try:
    from bot_config import BOT_GROUPS
    logging.info("Loaded BOT_GROUPS from bot_config.py.")
except ImportError:
    logging.warning("bot_config.py not found. Using default BOT_GROUPS.")
    BOT_GROUPS = {
        "group_1": {"id": -1002370505230, "categories": ["anime", "cars", "nature"], "interval_seconds": 60},
        "group_2": {"id": -1002392972227, "categories": ["flowers", "abstract", "space"], "interval_seconds": 300},
    }

SIMILARITY_THRESHOLD = 5
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

shutdown_requested = False
ACTIVE_TASKS = set()


def handle_shutdown():
    global shutdown_requested
    shutdown_requested = True
    logging.info("Shutdown requested. Waiting for ongoing wallpaper posts to complete...")


# --- Load & Save JSON ---
async def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    async with aiofiles.open(DATA_FILE, "r", encoding="utf-8") as f:
        content = await f.read()
        if not content.strip():
            return []
        return json.loads(content)


async def save_data(data):
    """Save data safely, converting datetime and Telethon objects to serializable forms."""
    def default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        try:
            return str(obj)
        except Exception:
            return None

    async with aiofiles.open(DATA_FILE, "w", encoding="utf-8") as f:
        serialized = json.dumps(data, ensure_ascii=False, indent=2, default=default_serializer)
        await f.write(serialized)


# --- Hashing ---
def calculate_hashes(filepath):
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(block)
        sha256 = sha256_hash.hexdigest()
        p_hash = str(imagehash.average_hash(Image.open(filepath)))
        return sha256, p_hash
    except Exception as e:
        logging.error(f"Error calculating hashes for {filepath}: {e}")
        return None, None


async def check_image_hashes_in_data(data, sha256, p_hash):
    max_diff = 64
    new_hash = imagehash.hex_to_hash(p_hash)

    for item in data:
        existing_sha = item.get("sha256")
        existing_phash = item.get("phash")

        if existing_sha == sha256:
            return "skipped", {"reason": "Duplicate", "details": {"type": "SHA256_match"}}

        if existing_phash:
            try:
                db_hash = imagehash.hex_to_hash(existing_phash)
                diff = new_hash - db_hash
                if diff < SIMILARITY_THRESHOLD:
                    similarity_percentage = ((max_diff - diff) / max_diff) * 100
                    return "skipped", {
                        "reason": "Similar",
                        "details": {
                            "type": "p_hash_match",
                            "diff": diff,
                            "similarity_percentage": round(similarity_percentage, 2)
                        }
                    }
            except Exception:
                continue
    return "proceed", None


# --- Update JSON entry ---
async def update_wallpaper_status(data, jpg_url, status, reasons=None, sha256=None, phash=None, tg_response=None):
    for item in data:
        if item["image_url"] == jpg_url:
            item["status"] = status
            if sha256:
                item["sha256"] = sha256
            if phash:
                item["phash"] = phash
            if reasons:
                item["reasons"] = reasons
            if tg_response:
                item["tg_response"] = tg_response
            break
    await save_data(data)


# --- Download Image ---
async def download_image(url, filename):
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(url)
            r.raise_for_status()
            async with aiofiles.open(filename, "wb") as f:
                await f.write(r.content)
        return filename
    except Exception as e:
        logging.warning(f"Download failed for {url}: {e}")
        return None


# --- Fetch Random Wallpaper ---
async def get_random_wallpaper(data, categories):
    pending = [w for w in data if w.get("status", "pending") == "pending" and w["category"] in categories]
    return random.choice(pending) if pending else None


# --- Send Wallpaper to Group ---
async def send_wallpaper_to_group(client, data, config):
    if shutdown_requested:
        logging.info(f"Skipping wallpaper send for group {config['id']} due to shutdown request.")
        return

    task = asyncio.current_task()
    ACTIVE_TASKS.add(task)
    try:
        group_id = config["id"]
        categories = config["categories"]
        wallpaper = await get_random_wallpaper(data, categories)
        if not wallpaper:
            logging.info(f"No new wallpapers found for categories {categories} for group {group_id}.")
            return

        jpg_url = wallpaper["image_url"]
        tags = wallpaper["tags"]
        caption = " ".join([f"#{t.replace(' ', '')}" for t in tags]) if tags else "#wallpaper"
        category = wallpaper.get("category", "wallpaper")
        filename = f"{category}_{random.randint(1000,9999)}_{os.path.basename(urlparse(jpg_url).path)}"

        path = await download_image(jpg_url, filename)
        if not path:
            reasons = {"reason": "Download failed"}
            await update_wallpaper_status(data, jpg_url, "failed", reasons)
            return

        sha256, phash = calculate_hashes(path)
        if not sha256 or not phash:
            reasons = {"reason": "Hashing failed"}
            await update_wallpaper_status(data, jpg_url, "failed", reasons)
            await async_os.remove(path)
            return

        status_check, reasons = await check_image_hashes_in_data(data, sha256, phash)
        if status_check == "skipped":
            await update_wallpaper_status(data, jpg_url, "skipped", reasons, sha256, phash)
            await async_os.remove(path)
            return

        try:
            preview_response = await client.send_file(group_id, path, caption=caption, force_document=False)
            await asyncio.sleep(5)
            hd_response = await client.send_file(group_id, path, caption="HD Download", force_document=True)
            tg_response = {"preview": preview_response.to_dict(), "hd": hd_response.to_dict()}
            reasons = {"reason": "Success"}
            await update_wallpaper_status(data, jpg_url, "posted", reasons, sha256, phash, tg_response)
            logging.info(f"Posted wallpaper {jpg_url} to group {group_id}")
        except Exception as telegram_e:
            reasons = {"reason": "Telegram upload failed", "details": str(telegram_e)}
            await update_wallpaper_status(data, jpg_url, "failed", reasons)
            logging.error(f"Telegram upload failed for {jpg_url}: {telegram_e}")
        finally:
            if os.path.exists(path):
                await async_os.remove(path)
    finally:
        ACTIVE_TASKS.discard(task)


# --- Main ---
async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_shutdown)
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown)

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)

    data = await load_data()
    if not data:
        logging.critical("No wallpapers.json found or it's empty.")
        return

    scheduler = AsyncIOScheduler()
    for group_name, config in BOT_GROUPS.items():
        scheduler.add_job(
            send_wallpaper_to_group,
            'interval',
            args=[client, data, config],
            seconds=config['interval_seconds'],
            id=f'job_{group_name}',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60
        )
    scheduler.start()
    logging.info("Wallpaper bot started (JSON mode).")

    while not shutdown_requested:
        await asyncio.sleep(1)

    if ACTIVE_TASKS:
        logging.info(f"Waiting for {len(ACTIVE_TASKS)} active wallpaper tasks to finish...")
        await asyncio.gather(*ACTIVE_TASKS)

    await client.disconnect()
    logging.info("All tasks completed. Exiting.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")
