import subprocess
import time
import os
import sys
import hashlib
from bot_config import BOT_GROUPS


def file_hash(path):
    """Compute a quick hash of a file for change detection."""
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_next_delay(bot_groups):
    """Return the smallest interval in seconds."""
    return min(group["interval_seconds"] for group in bot_groups.values())


def commit_and_push_if_changed():
    """Commit wallpapers.json if it changed."""
    repo = os.getenv("GITHUB_REPOSITORY")
    if not repo:
        print("⚠️ Not running inside GitHub Actions — skipping commit.")
        return

    print("📤 Checking for changes in wallpapers.json...")
    subprocess.run(["git", "config", "--global", "user.email", "bot@githubactions.local"], check=False)
    subprocess.run(["git", "config", "--global", "user.name", "GitHub Actions Bot"], check=False)

    subprocess.run(["git", "add", "wallpapers.json"], check=False)
    diff_check = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff_check.returncode != 0:
        print("🔄 Changes detected — committing and pushing...")
        subprocess.run(["git", "commit", "-m", "Auto-update wallpapers.json"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✅ Changes pushed successfully.")
    else:
        print("🟢 No changes to commit.")


def main():
    print("🚀 Running wallpaper bot once...")

    old_hash = file_hash("wallpapers.json")

    # Run bot
    result = subprocess.run([sys.executable, "bot.py"], check=False)
    print(f"Bot exited with code {result.returncode}")

    # Compare file hash to detect changes
    new_hash = file_hash("wallpapers.json")
    if old_hash != new_hash:
        commit_and_push_if_changed()
    else:
        print("🟢 wallpapers.json unchanged — skipping commit.")

    # Determine next run delay
    delay = get_next_delay(BOT_GROUPS)
    print(f"Next scheduled run (from config): {delay} seconds")

    if delay <= 600:
        print(f"🕒 Delay ≤ 10 min ({delay}s). Sleeping until next run...")
        time.sleep(delay)
        print("🔁 Restarting bot automatically...")
        subprocess.run([sys.executable, "bot.py"], check=False)
        commit_and_push_if_changed()
    else:
        print(f"🕓 Delay > 10 min. Triggering next GitHub Action run in {delay} seconds...")
        repo = os.getenv("GITHUB_REPOSITORY")
        workflow = os.getenv("GITHUB_WORKFLOW")
        token = os.getenv("GITHUB_TOKEN")

        if not all([repo, workflow, token]):
            print("⚠️ Missing GitHub environment vars — cannot auto-trigger.")
            sys.exit(0)

        cmd = (
            f"nohup bash -c 'sleep {delay}; "
            f"gh workflow run \"{workflow}\" --repo \"{repo}\" "
            f"--ref \"$(git rev-parse HEAD)\" > /dev/null 2>&1 &'"
        )
        print(f"Scheduling next run with: {cmd}")
        subprocess.run(cmd, shell=True, check=False)
        print("✅ Next workflow run scheduled.")


if __name__ == "__main__":
    main()
