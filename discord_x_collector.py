import re
import json
import os
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp



DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")     
GUILD_ID = 1347581046442688553           
CHANNEL_ID = 1353336758808477736        

SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY")

MAX_UNIQUE_TWEETS = 100


IGNORED_ROLE_NAME = "Super OG"

IGNORED_TWITTER_SCREEN_NAME = "billions_ntwk"  
IGNORED_URL_SUBSTRING = "billions_ntwk"        

OUTPUT_FILE = "tweets_from_discord.json"

DISCORD_API_BASE = "https://discord.com/api/v9"
DISCORD_PAGE_LIMIT = 100
DISCORD_REQUEST_DELAY = 0.35
REQUEST_TIMEOUT = 25
MAX_RETRIES = 6

MAX_CONCURRENCY = 6
BATCH_SIZE = 200
RATE_LIMIT_SLEEP = 5  # seconds
SOCIALDATA_BASE = "https://api.socialdata.tools/twitter/tweets/"



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("collector")



def save_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def safe_int(v: Any) -> int:
    return int(v) if isinstance(v, (int, float)) else 0



def normalize_discord_text(text: str) -> str:
    return re.sub(r"<(https?://[^>]+)>", r"\1", text or "")

def extract_links(text: str) -> List[str]:
    text = normalize_discord_text(text)
    return re.findall(r"https?://\S+", text)

def is_tweet_url(url: str) -> bool:
    u = url.lower()
    return (("twitter.com/" in u) or ("x.com/" in u)) and ("/status/" in u)

def extract_tweet_id(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        parts = parsed.path.strip("/").split("/")
        if "status" not in parts:
            return None
        tid = parts[parts.index("status") + 1]
        tid = tid.split("?")[0].split("#")[0]
        return tid if tid else None
    except Exception:
        return None

def canonicalize_url(url: str) -> str:
    return (url or "").split("?")[0].split("#")[0]

def should_ignore_tweet_link(url: str) -> bool:
    """
    Игнорируем, если:
    - в URL есть подстрока billions_ntwk
    - или username в пути ссылки == billions_ntwk (x.com/<username>/status/..)
    """
    u = (url or "").strip()
    low = u.lower()

    if IGNORED_URL_SUBSTRING.lower() in low:
        return True

    try:
        parsed = urlparse(u)
        parts = parsed.path.strip("/").split("/")
        # ожидаем: [username, "status", tweet_id]
        if len(parts) >= 3 and parts[1].lower() == "status":
            username = parts[0].lower()
            if username == IGNORED_URL_SUBSTRING.lower():
                return True
    except Exception:
        pass

    return False

def discord_message_link(guild_id: int, channel_id: int, message_id: str) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"



def pick_tweet_images(tweet: Dict[str, Any]) -> List[str]:
    images: List[str] = []

    ext = tweet.get("extended_entities") or {}
    media = ext.get("media") or []
    for m in media:
        if isinstance(m, dict) and m.get("type") in ("photo", "image"):
            u = m.get("media_url_https") or m.get("media_url") or m.get("url")
            if u:
                images.append(u)

    ent = tweet.get("entities") or {}
    media2 = ent.get("media") or []
    for m in media2:
        if isinstance(m, dict) and m.get("type") in ("photo", "image"):
            u = m.get("media_url_https") or m.get("media_url") or m.get("url")
            if u:
                images.append(u)

    media3 = tweet.get("media") or []
    if isinstance(media3, list):
        for m in media3:
            if isinstance(m, dict) and m.get("type") in ("photo", "image"):
                u = m.get("media_url_https") or m.get("media_url") or m.get("url")
                if u:
                    images.append(u)

    seen = set()
    out = []
    for u in images:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# DISCORD API


async def discord_get_json(session: aiohttp.ClientSession, url: str) -> Any:
    retries = 0
    while True:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as r:
                data = await r.json(content_type=None)

                if r.status == 429:
                    retry_after = 2.0
                    if isinstance(data, dict):
                        retry_after = float(data.get("retry_after", 2))
                    log.warning(f"Discord 429 — sleep {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue

                if r.status >= 500:
                    retries += 1
                    if retries >= MAX_RETRIES:
                        raise RuntimeError(f"Discord server error {r.status}: {data}")
                    await asyncio.sleep(2)
                    continue

                if r.status != 200:
                    raise RuntimeError(f"Discord error {r.status}: {data}")

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries += 1
            if retries >= MAX_RETRIES:
                raise
            log.warning(f"Discord request error: {e} — retry in 2s")
            await asyncio.sleep(2)

async def discord_get_roles_map(session: aiohttp.ClientSession, guild_id: int) -> Dict[str, str]:
    url = f"{DISCORD_API_BASE}/guilds/{guild_id}/roles"
    try:
        roles = await discord_get_json(session, url)
    except Exception as e:
        log.warning(f"Cannot fetch guild roles (no perms?): {e}")
        return {}

    m: Dict[str, str] = {}
    if isinstance(roles, list):
        for r in roles:
            rid = str(r.get("id") or "")
            name = r.get("name") or ""
            if rid:
                m[rid] = name
    return m

async def discord_get_member_role_ids(session: aiohttp.ClientSession, guild_id: int, user_id: str) -> List[str]:
    url = f"{DISCORD_API_BASE}/guilds/{guild_id}/members/{user_id}"
    try:
        member = await discord_get_json(session, url)
        roles = member.get("roles") if isinstance(member, dict) else []
        return [str(x) for x in roles] if isinstance(roles, list) else []
    except Exception:
        return []

def has_ignored_role(role_names: List[str], ignored_role_name: str) -> bool:
    target = ignored_role_name.strip().lower()
    return any((r or "").strip().lower() == target for r in role_names)


# COLLECT jobs + sources


async def collect_last_unique_tweets_from_discord(
    discord_token: str,
    guild_id: int,
    channel_id: int,
    max_unique: int,
    ignored_role_name: str
) -> Tuple[List[Tuple[str, str]], Dict[str, List[Dict[str, Any]]]]:
    """
    Собираем max_unique уникальных tweet_id.
    Правила:
    - если у автора сообщения есть роль Super OG -> игнорируем все твит-ссылки из этого сообщения
    - если ссылка содержит 'billions_ntwk' в URL / username -> игнорируем эту ссылку
    """

    headers = {
        "authorization": discord_token,
        "user-agent": "Mozilla/5.0"
    }

    seen_tweet_ids = set()
    jobs: List[Tuple[str, str]] = []                  
    sources_map: Dict[str, List[Dict[str, Any]]] = {} 

    last_id: Optional[int] = None

    async with aiohttp.ClientSession(headers=headers) as session:
        roles_map = await discord_get_roles_map(session, guild_id)

        member_roles_cache: Dict[str, Tuple[List[str], List[str]]] = {}

        while len(seen_tweet_ids) < max_unique:
            await asyncio.sleep(DISCORD_REQUEST_DELAY)

            if last_id is None:
                url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages?limit={DISCORD_PAGE_LIMIT}"
            else:
                url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages?before={last_id}&limit={DISCORD_PAGE_LIMIT}"

            page = await discord_get_json(session, url)
            if not page:
                break

            for msg in page:
                content = msg.get("content") or ""
                links = extract_links(content)

                tweet_links: List[Tuple[str, str]] = []
                for link in links:
                    if not is_tweet_url(link):
                        continue

                    canon = canonicalize_url(link)

                    if should_ignore_tweet_link(canon):
                        continue

                    tid = extract_tweet_id(canon)
                    if tid:
                        tweet_links.append((tid, canon))

                if not tweet_links:
                    continue

                author = msg.get("author") or {}
                author_id = str(author.get("id") or "")

                if author_id and author_id in member_roles_cache:
                    role_ids, role_names = member_roles_cache[author_id]
                else:
                    role_ids = await discord_get_member_role_ids(session, guild_id, author_id) if author_id else []
                    role_names = [roles_map.get(rid, rid) for rid in role_ids]
                    member_roles_cache[author_id] = (role_ids, role_names)

                if has_ignored_role(role_names, ignored_role_name):
                    continue

                discord_username = author.get("username", "")
                discriminator = author.get("discriminator")
                if discriminator and discriminator != "0":
                    discord_display = f"{discord_username}#{discriminator}"
                else:
                    discord_display = discord_username

                msg_id = str(msg.get("id") or "")
                source = {
                    "guild_id": guild_id,
                    "channel_id": channel_id,
                    "message_id": msg_id,
                    "message_url": discord_message_link(guild_id, channel_id, msg_id) if msg_id else "",
                    "author_username": discord_display,
                    "author_id": author_id,
                    "author_roles": role_names,
                    "author_role_ids": role_ids,
                    "content": content,
                }

                for tid, canonical in tweet_links:
                    sources_map.setdefault(tid, []).append(source)

                    if tid not in seen_tweet_ids:
                        seen_tweet_ids.add(tid)
                        jobs.append((tid, canonical))

                        if len(seen_tweet_ids) >= max_unique:
                            break

                if len(seen_tweet_ids) >= max_unique:
                    break

            last_id = min(int(m["id"]) for m in page)
            log.info(f"Collected unique tweets: {len(seen_tweet_ids)}/{max_unique} | jobs: {len(jobs)}")

    return jobs, sources_map



async def fetch_tweet_socialdata(
    session: aiohttp.ClientSession,
    tweet_id: str,
    sem: asyncio.Semaphore
) -> Optional[Dict[str, Any]]:
    url = f"{SOCIALDATA_BASE}{tweet_id}"
    retries = 0

    async with sem:
        while True:
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as r:
                    if r.status == 429:
                        log.warning(f"SocialData 429 — sleep {RATE_LIMIT_SLEEP}s")
                        await asyncio.sleep(RATE_LIMIT_SLEEP)
                        continue

                    if r.status in (403, 404):
                        return None

                    if r.status >= 500:
                        retries += 1
                        if retries >= MAX_RETRIES:
                            return None
                        await asyncio.sleep(3)
                        continue

                    r.raise_for_status()
                    return await r.json(content_type=None)

            except (aiohttp.ClientError, asyncio.TimeoutError):
                retries += 1
                if retries >= MAX_RETRIES:
                    return None
                await asyncio.sleep(3)

# Build final JSON record

def build_tweet_record(
    tweet_id: str,
    tweet_url: str,
    tweet: Dict[str, Any],
    discord_sources: List[Dict[str, Any]]
) -> Dict[str, Any]:
    user = tweet.get("user") or {}
    twitter_screen = user.get("screen_name") or user.get("username") or ""
    twitter_name = user.get("name") or ""

    return {
        "tweet_id": str(tweet.get("id") or tweet_id),
        "tweet_url": tweet_url,
        "discord_sources": discord_sources,
        "twitter": {
            "author_screen_name": twitter_screen,
            "author_name": twitter_name,
            "author_profile_image": user.get("profile_image_url_https") or user.get("profile_image_url"),
            "likes": safe_int(tweet.get("favorite_count")),
            "views": safe_int(tweet.get("views_count")),
            "comments": safe_int(tweet.get("reply_count")),
            "retweets": safe_int(tweet.get("retweet_count")),
            "images": pick_tweet_images(tweet),
            "text": tweet.get("full_text") or tweet.get("text") or ""
        }
    }


async def main():
    if not DISCORD_TOKEN or DISCORD_TOKEN == "YOUR_DISCORD_TOKEN":
        raise RuntimeError("Заполни DISCORD_TOKEN в конфиге.")
    if not SOCIALDATA_API_KEY or SOCIALDATA_API_KEY == "YOUR_SOCIALDATA_BEARER":
        raise RuntimeError("Заполни SOCIALDATA_API_KEY в конфиге.")
    if not GUILD_ID or GUILD_ID == 123456789012345678:
        raise RuntimeError("Заполни GUILD_ID в конфиге (ID сервера Discord).")

    log.info(
        f"Collecting {MAX_UNIQUE_TWEETS} latest unique tweet links from Discord "
        f"(ignore role '{IGNORED_ROLE_NAME}', ignore tweet author '{IGNORED_TWITTER_SCREEN_NAME}', "
        f"ignore links containing '{IGNORED_URL_SUBSTRING}')..."
    )

    jobs, sources_map = await collect_last_unique_tweets_from_discord(
        DISCORD_TOKEN, GUILD_ID, CHANNEL_ID, MAX_UNIQUE_TWEETS, IGNORED_ROLE_NAME
    )

    if not jobs:
        log.warning("No tweet links found (after filters).")
        save_json(OUTPUT_FILE, [])
        return

    log.info(f"Unique tweet IDs collected: {len(jobs)}")

    out: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    headers = {"Authorization": f"Bearer {SOCIALDATA_API_KEY}"}

    async with aiohttp.ClientSession(headers=headers) as session:
        for i in range(0, len(jobs), BATCH_SIZE):
            batch = jobs[i:i + BATCH_SIZE]

            async def process_one(tid: str, url: str):
                tweet = await fetch_tweet_socialdata(session, tid, sem)
                if not tweet:
                    return

                user = tweet.get("user") or {}
                screen = (user.get("screen_name") or user.get("username") or "").lower()
                if screen == IGNORED_TWITTER_SCREEN_NAME.lower():
                    return

                out.append(build_tweet_record(
                    tweet_id=tid,
                    tweet_url=url,
                    tweet=tweet,
                    discord_sources=sources_map.get(tid, [])
                ))

            await asyncio.gather(*(process_one(tid, url) for tid, url in batch))
            log.info(f"Processed: {min(i + BATCH_SIZE, len(jobs))}/{len(jobs)} | output: {len(out)}")

    out_by_id = {str(r.get("tweet_id")): r for r in out}
    ordered: List[Dict[str, Any]] = []
    for tid, url in jobs:
        rec = out_by_id.get(str(tid))
        if not rec:
            rec = next((x for x in out if x.get("tweet_url") == url), None)
        if rec:
            ordered.append(rec)

    save_json(OUTPUT_FILE, ordered)
    log.info(f"Saved: {OUTPUT_FILE} | records: {len(ordered)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
