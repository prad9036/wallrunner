# scraper.py
import requests
from bs4 import BeautifulSoup
import json
import re
import os

BASE_URL = "https://4kwallpapers.com"
DATA_FILE = "wallpapers.json"


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
    html = requests.get(url).text
    # look for both .jpg/.jpeg and .png
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


def scrape_page(data, page_url, consecutive_skips):
    html = requests.get(page_url).text
    soup = BeautifulSoup(html, "html.parser")
    links = [a["href"] for a in soup.select("a.wallpapers__canvas_image")]

    for href in links:
        wallpaper_url = href if href.startswith("http") else BASE_URL + href
        category = wallpaper_url.split("/")[3]

        html2 = requests.get(wallpaper_url).text
        soup2 = BeautifulSoup(html2, "html.parser")
        meta = soup2.find("meta", {"name": "keywords"})
        tags = sanitize_tags(meta["content"]) if meta else []

        image_url = get_highest_image(wallpaper_url)
        if not image_url:
            continue

        if already_in_data(data, wallpaper_url, image_url):
            consecutive_skips += 1
            if consecutive_skips >= 50:
                print("50 consecutive matches found. Terminating.")
                return consecutive_skips, False
            continue

        consecutive_skips = 0
        item = {
            "category": category,
            "wallpaper_url": wallpaper_url,
            "image_url": image_url,
            "tags": tags,
        }
        data.append(item)
        print(f"Added: {wallpaper_url}")

    return consecutive_skips, True


def main():
    data = load_data()
    page = 1
    consecutive_skips = 0

    while True:
        url = BASE_URL if page == 1 else f"{BASE_URL}/?page={page}"
        print(f"\n=== Scraping {url} ===")
        consecutive_skips, keep_going = scrape_page(data, url, consecutive_skips)
        save_data(data)
        if not keep_going:
            break
        page += 1


if __name__ == "__main__":
    main()
