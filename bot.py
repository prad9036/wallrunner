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
from datetime import datetime
from dotenv import load_dotenv
import signal
import aiofiles
import aiofiles.os as async_os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure

# --- Load environment ---
load_dotenv()

# --- Verbose logging setup ---
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("wallbot")

# --- Config ---
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("WALLBOT_TOKEN", "")
SESSION = "wallpaper_bot"

MONGO_URI = os.getenv("FIREBASE_MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "prdp"
COLLECTION_NAME = "wallpapers"

log.info("Initializing Mongo client...")
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]
log.info(f"MongoDB target: {MONGO_URI}/{DB_NAME}.{COLLECTION_NAME}")

# --- Bot Groups ---
try:
    from bot_config import BOT_GROUPS
    log.info("Loaded BOT_GROUPS from bot_config.py")
except ImportError:
    log.warning("bot_config.py not found. Using default BOT_GROUPS.")
    BOT_GROUPS = {
        "group_1": {"id": -1002370505230, "categories": ["anime", "cars", "nature"], "interval_seconds": 60},
        "group_2": {"id": -1002392972227, "categories": ["flowers", "abstract", "space"], "interval_seconds": 300},
    }

# --- Globals ---
SIMILARITY_THRESHOLD = 5
shutdown_requested = False
ACTIVE_TASKS = set()


def handle_shutdown():
    global shutdown_requested
    shutdown_requested = True
    log.warning("Shutdown requested. Waiting for ongoing tasks to finish...")


# --- MongoDB Setup ---
async def ensure_indexes():
    logging.info("Checking MongoDB indexes (safe mode)...")
    try:
        existing = await collection.index_information()
        logging.info(f"Existing indexes: {list(existing.keys())}")
    except Exception as e:
        logging.warning(f"Could not list indexes: {e}")
        existing = {}

    for field, unique in [
        ("image_url", True),
        ("wallpaper_url", True),
        ("status", False),
        ("category", False),
    ]:
        try:
            if field not in existing:
                logging.info(f"Creating index on '{field}' (unique={unique})...")
                await collection.create_index(field, unique=unique)
                logging.info(f"Index created on '{field}'.")
            else:
                logging.info(f"Index on '{field}' already exists, skipping.")
        except Exception as e:
            logging.warning(f"Skipping index '{field}' due to: {e}")

    logging.info("MongoDB index setup complete (safe mode).")


# --- Hashing ---
def calculate_hashes(filepath):
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(block)
        sha256 = sha256_hash.hexdigest()
        p_hash = str(imagehash.average_hash(Image.open(filepath)))
        log.debug(f"Computed hashes for {filepath}: sha256={sha256[:8]}..., phash={p_hash}")
        return sha256, p_hash
    except Exception as e:
        log.error(f"Hashing error for {filepath}: {e}")
        return None, None


async def check_image_hashes_in_data(sha256, p_hash):
    max_diff = 64
    new_hash = imagehash.hex_to_hash(p_hash)

    if await collection.find_one({"sha256": sha256}):
        log.info("Duplicate SHA256 detected.")
        return "skipped", {"reason": "Duplicate"}

    async for item in collection.find({"phash": {"$exists": True}}):
        try:
            db_hash = imagehash.hex_to_hash(item["phash"])
            diff = new_hash - db_hash
            if diff < SIMILARITY_THRESHOLD:
                similarity = ((max_diff - diff) / max_diff) * 100
                log.info(f"Similar image found (diff={diff}, {similarity:.1f}% similar)")
                return "skipped", {"reason": "Similar", "diff": diff, "similarity": round(similarity, 1)}
        except Exception:
            continue
    return "proceed", None


# --- Mongo helpers ---
async def get_random_wallpaper(categories):
    log.info(f"Fetching random pending wallpaper for {categories}")
    pipeline = [
        {"$match": {"status": "pending", "category": {"$in": categories}}},
        {"$sample": {"size": 1}},
    ]
    async for doc in collection.aggregate(pipeline):
        log.info(f"Selected wallpaper: {doc.get('image_url', 'N/A')}")
        return doc
    log.info("No pending wallpapers found.")
    return None


async def update_wallpaper_status(jpg_url, status, reasons=None, sha256=None, phash=None, tg_response=None):
    log.info(f"Updating status for {jpg_url[:80]} → {status}")
    update_doc = {"$set": {"status": status}}
    if sha256:
        update_doc["$set"]["sha256"] = sha256
    if phash:
        update_doc["$set"]["phash"] = phash
    if reasons:
        update_doc["$set"]["reasons"] = reasons
    if tg_response:
        update_doc["$set"]["tg_response"] = tg_response
    await collection.update_one({"image_url": jpg_url}, update_doc)


# --- Download Image ---
async def download_image(url, filename):
    log.info(f"Downloading image: {url}")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(url)
            r.raise_for_status()
            async with aiofiles.open(filename, "wb") as f:
                await f.write(r.content)
        log.info(f"Saved {filename}")
        return filename
    except Exception as e:
        log.warning(f"Download failed for {url}: {e}")
        return None


# --- Send Wallpaper to Telegram Group ---
async def send_wallpaper_to_group(client, config):
    if shutdown_requested:
        log.warning(f"Skipping send for group {config['id']} (shutdown in progress)")
        return

    task = asyncio.current_task()
    ACTIVE_TASKS.add(task)
    try:
        group_id = config["id"]
        categories = config["categories"]
        log.info(f"Running job for group {group_id} with categories {categories}")
        wallpaper = await get_random_wallpaper(categories)
        if not wallpaper:
            return

        jpg_url = wallpaper["image_url"]
        tags = wallpaper.get("tags", [])
        caption = " ".join([f"#{t.replace(' ', '')}" for t in tags]) if tags else "#wallpaper"
        category = wallpaper.get("category", "wallpaper")
        filename = f"{category}_{random.randint(1000,9999)}_{os.path.basename(urlparse(jpg_url).path)}"

        path = await download_image(jpg_url, filename)
        if not path:
            await update_wallpaper_status(jpg_url, "failed", {"reason": "Download failed"})
            return

        sha256, phash = calculate_hashes(path)
        if not sha256 or not phash:
            await update_wallpaper_status(jpg_url, "failed", {"reason": "Hashing failed"})
            await async_os.remove(path)
            return

        status_check, reasons = await check_image_hashes_in_data(sha256, phash)
        if status_check == "skipped":
            await update_wallpaper_status(jpg_url, "skipped", reasons, sha256, phash)
            await async_os.remove(path)
            return

        try:
            log.info(f"Sending wallpaper to Telegram group {group_id}...")
            preview = await client.send_file(group_id, path, caption=caption, force_document=False)
            await asyncio.sleep(5)
            hd = await client.send_file(group_id, path, caption="HD Download", force_document=True)
            await update_wallpaper_status(jpg_url, "posted", {"reason": "Success"}, sha256, phash, {"preview": preview.to_dict(), "hd": hd.to_dict()})
            log.info(f"Successfully posted wallpaper {jpg_url}")
        except Exception as e:
            await update_wallpaper_status(jpg_url, "failed", {"reason": "Telegram upload failed", "details": str(e)})
            log.error(f"Telegram error for {jpg_url}: {e}")
        finally:
            if os.path.exists(path):
                await async_os.remove(path)
    finally:
        ACTIVE_TASKS.discard(task)


# --- Main ---
async def main():
    log.info("===== WALLRUNNER BOT STARTING =====")

    log.info("Step 1: Connecting to MongoDB...")
    await ensure_indexes()
    log.info("MongoDB connection verified.")

    log.info("Step 2: Initializing Telegram client...")
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    log.info(f"Connected as Telegram bot: @{me.username} (ID: {me.id})")

    log.info("Step 3: Setting up scheduler...")
    scheduler = AsyncIOScheduler()
    for name, cfg in BOT_GROUPS.items():
        log.info(f"Adding job: {name} -> group={cfg['id']}, interval={cfg['interval_seconds']}s")
        scheduler.add_job(
            send_wallpaper_to_group,
            "interval",
            args=[client, cfg],
            seconds=cfg["interval_seconds"],
            id=f"job_{name}",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )

    scheduler.start()
    log.info("Scheduler started. Running initial jobs immediately...")

    initial_tasks = [send_wallpaper_to_group(client, cfg) for cfg in BOT_GROUPS.values()]
    await asyncio.gather(*initial_tasks)

    log.info("Bot is fully running and awaiting next intervals.")
    while not shutdown_requested:
        await asyncio.sleep(1)

    if ACTIVE_TASKS:
        log.info(f"Waiting for {len(ACTIVE_TASKS)} active tasks to finish...")
        await asyncio.gather(*ACTIVE_TASKS)

    await client.disconnect()
    log.info("Bot shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Program interrupted by user.")
