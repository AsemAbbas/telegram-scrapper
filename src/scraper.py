import asyncio
import json
from datetime import datetime, timezone

from telethon import TelegramClient
from telethon.tl import functions, types
from telethon.tl.types import (
    MessageEntityUrl, MessageEntityTextUrl, MessageEntityBold,
    MessageEntityItalic, MessageEntityCode, MessageEntityPre,
    MessageEntityMention, MessageEntityHashtag, MessageEntityCashtag,
    MessageEntityBotCommand, MessageEntityEmail, MessageEntityPhone,
    MessageEntityStrike, MessageEntityUnderline, MessageEntitySpoiler,
    MessageEntityBlockquote, MessageEntityMentionName,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaGeo,
    MessageMediaGeoLive, MessageMediaContact, MessageMediaPoll,
    MessageMediaVenue, MessageMediaWebPage, MessageMediaDice,
    MessageMediaGame, MessageMediaInvoice, MessageMediaUnsupported,
    DocumentAttributeFilename, DocumentAttributeVideo,
    DocumentAttributeAudio, DocumentAttributeSticker,
    DocumentAttributeAnimated, DocumentAttributeCustomEmoji,
    DocumentAttributeImageSize,
    PeerChannel, PeerChat, PeerUser,
)

from .config import TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME

BATCH_SIZE = 200


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def classify_media(msg):
    media = getattr(msg, "media", None)
    if media is None:
        return None

    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc is None:
            return "document"
        mime = doc.mime_type or ""
        mtype = "document"
        is_voice = getattr(media, "voice", False)
        is_round = getattr(media, "round", False)
        for attr in doc.attributes or []:
            if isinstance(attr, DocumentAttributeVideo):
                mtype = "round_video" if (is_round or attr.round_message) else "video"
            elif isinstance(attr, DocumentAttributeAudio):
                mtype = "voice" if (is_voice or attr.voice) else "audio"
            elif isinstance(attr, DocumentAttributeSticker):
                mtype = "sticker"
            elif isinstance(attr, DocumentAttributeAnimated):
                mtype = "animated_sticker"
        if mime == "video/mp4" and mtype == "document":
            mtype = "gif"
        elif mime.startswith("image/") and mtype == "document":
            mtype = "image"
        return mtype
    if isinstance(media, (MessageMediaGeo, MessageMediaGeoLive)):
        return "geo"
    if isinstance(media, MessageMediaVenue):
        return "venue"
    if isinstance(media, MessageMediaContact):
        return "contact"
    if isinstance(media, MessageMediaPoll):
        return "poll"
    if isinstance(media, MessageMediaWebPage):
        return "webpage"
    if isinstance(media, MessageMediaDice):
        return "dice"
    if isinstance(media, MessageMediaGame):
        return "game"
    if isinstance(media, MessageMediaInvoice):
        return "invoice"
    return "unknown"


def extract_entities(msg):
    entities = getattr(msg, "entities", None) or []
    text = msg.text or ""
    result = {
        "urls": [],
        "mentions": [],
        "hashtags": [],
    }
    for e in entities:
        offset, length = e.offset, e.length
        fragment = text[offset:offset + length] if text else ""
        if isinstance(e, MessageEntityUrl):
            result["urls"].append(fragment)
        elif isinstance(e, MessageEntityTextUrl):
            result["urls"].append(e.url)
        elif isinstance(e, MessageEntityMention):
            result["mentions"].append(fragment)
        elif isinstance(e, MessageEntityMentionName):
            result["mentions"].append(f"{fragment}(id:{e.user_id})")
        elif isinstance(e, MessageEntityHashtag):
            result["hashtags"].append(fragment)
    return result


def extract_forward_info(msg):
    fwd = getattr(msg, "fwd_from", None)
    if not fwd:
        return {}
    info = {"fwd_from_name": getattr(fwd, "from_name", None)}
    from_id = getattr(fwd, "from_id", None)
    if isinstance(from_id, PeerChannel):
        info["fwd_from_id"] = f"channel:{from_id.channel_id}"
    elif isinstance(from_id, PeerUser):
        info["fwd_from_id"] = f"user:{from_id.user_id}"
    elif isinstance(from_id, PeerChat):
        info["fwd_from_id"] = f"chat:{from_id.chat_id}"
    return info


class CustomEmojiResolver:
    def __init__(self, client):
        self.client = client
        self.cache = {}

    async def label_for(self, document_id):
        if document_id in self.cache:
            return self.cache[document_id]
        try:
            docs = await self.client(functions.messages.GetCustomEmojiDocumentsRequest([document_id]))
            if docs:
                for attr in getattr(docs[0], "attributes", []) or []:
                    if isinstance(attr, DocumentAttributeCustomEmoji):
                        self.cache[document_id] = attr.alt or f"[custom:{document_id}]"
                        return self.cache[document_id]
        except Exception:
            pass
        self.cache[document_id] = f"[custom:{document_id}]"
        return self.cache[document_id]


async def summarize_reactions(client, reactions_obj):
    if not reactions_obj or not getattr(reactions_obj, "results", None):
        return None, 0
    resolver = getattr(client, "_emoji_resolver", None)
    if not resolver:
        resolver = CustomEmojiResolver(client)
        client._emoji_resolver = resolver
    parts = []
    total = 0
    for rc in reactions_obj.results:
        count = getattr(rc, "count", 0) or 0
        total += count
        r = getattr(rc, "reaction", None)
        try:
            if isinstance(r, types.ReactionEmoji):
                label = r.emoticon
            elif isinstance(r, types.ReactionCustomEmoji):
                label = await resolver.label_for(r.document_id)
            else:
                label = "[other]"
        except Exception:
            label = "[?]"
        parts.append(f"{label}:{count}")
    return ", ".join(parts), total


# ═══════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ═══════════════════════════════════════════════════════════════

async def scrape_channel(client, channel_name, from_date, to_date):
    """Scrape a single channel and return list of row dicts."""
    entity = await client.get_entity(channel_name)
    username = getattr(entity, "username", None) or str(channel_name)
    title = getattr(entity, "title", username)

    print(f"{'='*50}")
    print(f"  {title} (@{username})")
    print(f"  Range: {from_date.strftime('%Y-%m-%d %H:%M')} -> {to_date.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")

    # Fetch messages
    all_messages = []
    offset_id = 0
    batch_num = 0

    while True:
        batch_num += 1
        try:
            kwargs = {"limit": BATCH_SIZE}
            if offset_id == 0:
                kwargs["offset_date"] = to_date
            else:
                kwargs["offset_id"] = offset_id
            msgs = await client.get_messages(entity, **kwargs)
        except Exception as e:
            print(f"  Batch error: {e}, retrying...")
            await asyncio.sleep(3)
            continue

        if not msgs:
            break

        done = False
        for m in msgs:
            if m.date.astimezone(timezone.utc) < from_date:
                done = True
                break
            all_messages.append(m)

        offset_id = msgs[-1].id
        if batch_num % 5 == 0:
            print(f"  Batch {batch_num}: {len(all_messages)} msgs so far")
        if done:
            break
        await asyncio.sleep(0.3)

    print(f"  Fetched {len(all_messages)} messages")
    if not all_messages:
        return []

    # Process messages
    scrape_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = []

    for msg in all_messages:
        dt_utc = msg.date.astimezone(timezone.utc)
        mtype = classify_media(msg)
        ent = extract_entities(msg)
        reactions_str, reactions_sum = await summarize_reactions(
            client, getattr(msg, "reactions", None)
        )
        fwd_info = extract_forward_info(msg)

        row = {
            "scrape_date": scrape_date,
            "channel": title,
            "username": f"@{username}",
            "msg_id": msg.id,
            "date_utc": dt_utc.strftime("%Y-%m-%d"),
            "time_utc": dt_utc.strftime("%H:%M:%S"),
            "text": (msg.text or "").replace("\r", "")[:50000],
            "views": getattr(msg, "views", None) or 0,
            "forwards": getattr(msg, "forwards", None) or 0,
            "replies_count": (
                getattr(msg.replies, "replies", None)
                if getattr(msg, "replies", None) else 0
            ) or 0,
            "reactions": reactions_str or "",
            "reactions_sum": reactions_sum,
            "media_type": mtype or "",
            "urls": " | ".join(ent["urls"]) if ent["urls"] else "",
            "url_count": len(ent["urls"]),
            "hashtags": " | ".join(ent["hashtags"]) if ent["hashtags"] else "",
            "mentions": " | ".join(ent["mentions"]) if ent["mentions"] else "",
            "is_forward": bool(fwd_info),
            "fwd_from_name": fwd_info.get("fwd_from_name", ""),
            "post_link": f"https://t.me/{username}/{msg.id}",
        }
        rows.append(row)

    return rows


async def run_scraper(channels_config):
    """
    Main entry point. Connects to Telegram, scrapes all channels, returns all rows.
    channels_config: list of dicts with keys: name, from_date, to_date
    """
    client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
    await client.start(phone=TG_PHONE)
    me = await client.get_me()
    print(f"Logged in as: {me.first_name}")

    all_rows = []

    for ch in channels_config:
        try:
            rows = await scrape_channel(
                client,
                ch["name"],
                ch["from_date"],
                ch["to_date"],
            )
            all_rows.extend(rows)
            print(f"  -> {len(rows)} rows from {ch['name']}")
        except Exception as e:
            print(f"  ERROR scraping {ch['name']}: {e}")

    await client.disconnect()
    print(f"\nTotal: {len(all_rows)} rows from {len(channels_config)} channels")
    return all_rows
