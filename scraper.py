import requests
from bs4 import BeautifulSoup
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://4kwallpapers.com"
DATA_FILE = "wallpapers.json"
MAX_PAGE_WORKERS = 50     # how many pages scraped concurrently
MAX_DETAIL_WORKERS = 100   # how many wallpapers per page scraped concurrently

session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0 (GitHub Actions bot)"


def sanitize_tags(raw_tags):
    tags = []
    for t in raw_tags.split(","):
        t = t.strip()
        t = re.sub(r"\s+", "_", t)
        t = re.sub(r"[^A-Za-z0-9_]", "_", t)
        if t:
            tags.append(t)
    return tags


def get_highest_image(url):
    """Fetch wallpaper page and return highest resolution JPG or PNG URL."""
    try:
        html = session.get(url, timeout=10).text
    except Exception:
        return None

    matches = re.findall(r'/images/wallpapers/[^"]+\.(?:jpe?g|png)', html, re.IGNORECASE)
    if not matches:
        return None
    full_urls = [BASE_URL + m for m in matches]
    best = None
    best_pixels = 0
    for u in full_urls:
        m = re.search(r'-(\d+)x(\d+)-\d+\.', u)
        if m:
            w, h = map(int, m.groups())
            pixels = w * h
            if pixels > best_pixels:
                best_pixels = pixels
                best = u
    return best


def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def already_in_data(data, wallpaper_url, image_url):
    for item in data:
        if item["wallpaper_url"] == wallpaper_url or item["image_url"] == image_url:
            return True
    return False


def fetch_wallpaper_details(href):
    """Fetch metadata for a single wallpaper page."""
    wallpaper_url = href if href.startswith("http") else BASE_URL + href
    category = wallpaper_url.split("/")[3]
    try:
        html2 = session.get(wallpaper_url, timeout=10).text
        soup2 = BeautifulSoup(html2, "html.parser")
        meta = soup2.find("meta", {"name": "keywords"})
        tags = sanitize_tags(meta["content"]) if meta else []
        image_url = get_highest_image(wallpaper_url)
        if not image_url:
            return None
        return {
            "category": category,
            "wallpaper_url": wallpaper_url,
            "image_url": image_url,
            "tags": tags,
        }
    except Exception as e:
        print(f"[ERROR] {wallpaper_url}: {e}")
        return None


def scrape_page(page_num, existing_data):
    """Scrape a single listing page and return a list of new wallpapers."""
    url = BASE_URL if page_num == 1 else f"{BASE_URL}/?page={page_num}"
    print(f"=== Scraping {url} ===")
    try:
        html = session.get(url, timeout=10).text
    except Exception as e:
        print(f"[ERROR] Failed to fetch page {url}: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    links = [a["href"] for a in soup.select("a.wallpapers__canvas_image")]

    new_items = []
    with ThreadPoolExecutor(max_workers=MAX_DETAIL_WORKERS) as executor:
        futures = [executor.submit(fetch_wallpaper_details, href) for href in links]
        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue
            if already_in_data(existing_data, result["wallpaper_url"], result["image_url"]):
                continue
            new_items.append(result)
            print(f"Added: {result['wallpaper_url']}")
    return new_items


def main():
    data = load_data()
    page = 1
    total_new = 0
    consecutive_skips = 0

    while True:
        # process 10 pages concurrently
        page_batch = [page + i for i in range(MAX_PAGE_WORKERS)]
        print(f"\n>>> Processing pages {page}–{page + MAX_PAGE_WORKERS - 1}")

        all_new = []
        with ThreadPoolExecutor(max_workers=MAX_PAGE_WORKERS) as executor:
            futures = {executor.submit(scrape_page, p, data): p for p in page_batch}
            for future in as_completed(futures):
                new_items = future.result()
                if new_items:
                    all_new.extend(new_items)

        if not all_new:
            consecutive_skips += 1
            if consecutive_skips >= 3:
                print("No new wallpapers after several batches. Stopping.")
                break
        else:
            consecutive_skips = 0
            total_new += len(all_new)
            data.extend(all_new)
            save_data(data)
            print(f"Saved {len(all_new)} new wallpapers.")

        page += MAX_PAGE_WORKERS

    print(f"\n=== Done! Total new wallpapers: {total_new} ===")


if __name__ == "__main__":
    main()
