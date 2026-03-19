"""
Telegram Scraper Web GUI
A simple Flask web interface for the Telegram scraper.
"""
import asyncio
import json
import os
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit

from src.config import load_channels, TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME, SHEET_ID
from src.sheets import push_to_sheets

from telethon import TelegramClient
from telethon.tl import types

app = Flask(__name__)
app.config['SECRET_KEY'] = 'telegram-scraper-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state
scrape_status = {
    "running": False,
    "progress": 0,
    "current_channel": "",
    "messages": [],
    "results": [],
    "error": None
}


def log_message(msg, msg_type="info"):
    """Send a log message to the frontend."""
    scrape_status["messages"].append({"type": msg_type, "text": msg, "time": datetime.now().strftime("%H:%M:%S")})
    socketio.emit('log', {"type": msg_type, "text": msg, "time": datetime.now().strftime("%H:%M:%S")})


async def scrape_channel_simple(client, channel_name, from_date, to_date, log_fn):
    """Simplified scraper that works within the web context."""
    entity = await client.get_entity(channel_name)
    username = getattr(entity, "username", None) or str(channel_name)
    title = getattr(entity, "title", username)
    
    log_fn(f"Channel: {title} (@{username})")
    
    all_messages = []
    offset_id = 0
    batch_num = 0
    BATCH_SIZE = 200
    
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
            log_fn(f"Batch error: {e}, retrying...", "warning")
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
        if batch_num % 3 == 0:
            log_fn(f"  Fetched {len(all_messages)} messages so far...")
        if done:
            break
        await asyncio.sleep(0.3)
    
    log_fn(f"  Total: {len(all_messages)} messages")
    
    # Process messages into rows
    scrape_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    rows = []
    
    for msg in all_messages:
        dt_utc = msg.date.astimezone(timezone.utc)
        
        # Simple reactions summary
        reactions_str = ""
        reactions_sum = 0
        if hasattr(msg, 'reactions') and msg.reactions and hasattr(msg.reactions, 'results'):
            parts = []
            for rc in msg.reactions.results:
                count = getattr(rc, "count", 0) or 0
                reactions_sum += count
                r = getattr(rc, "reaction", None)
                if isinstance(r, types.ReactionEmoji):
                    parts.append(f"{r.emoticon}:{count}")
                else:
                    parts.append(f"[r]:{count}")
            reactions_str = ", ".join(parts)
        
        # Extract URLs, mentions, hashtags
        urls, mentions, hashtags = [], [], []
        for e in (getattr(msg, "entities", None) or []):
            text = msg.text or ""
            fragment = text[e.offset:e.offset + e.length] if text else ""
            etype = type(e).__name__
            if "Url" in etype:
                urls.append(getattr(e, 'url', fragment))
            elif "Mention" in etype:
                mentions.append(fragment)
            elif "Hashtag" in etype:
                hashtags.append(fragment)
        
        # Media type
        media = getattr(msg, "media", None)
        mtype = ""
        if media:
            mtype = type(media).__name__.replace("MessageMedia", "").lower()
        
        # Forward info
        fwd = getattr(msg, "fwd_from", None)
        is_forward = bool(fwd)
        fwd_from_name = getattr(fwd, "from_name", "") if fwd else ""
        
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
            "replies_count": (getattr(msg.replies, "replies", None) if getattr(msg, "replies", None) else 0) or 0,
            "reactions": reactions_str,
            "reactions_sum": reactions_sum,
            "media_type": mtype,
            "urls": " | ".join(urls) if urls else "",
            "url_count": len(urls),
            "hashtags": " | ".join(hashtags) if hashtags else "",
            "mentions": " | ".join(mentions) if mentions else "",
            "is_forward": is_forward,
            "fwd_from_name": fwd_from_name,
            "post_link": f"https://t.me/{username}/{msg.id}",
        }
        rows.append(row)
    
    return rows


async def run_scraper_coroutine(channels_config, push_to_sheet, log_fn):
    """Main async scraper coroutine."""
    log_fn(f"Starting scraper for {len(channels_config)} channel(s)...")
    
    log_fn("Connecting to Telegram...")
    client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
    await client.start(phone=TG_PHONE)
    me = await client.get_me()
    log_fn(f"Logged in as: {me.first_name}", "success")
    
    all_rows = []
    total_channels = len(channels_config)
    
    for i, ch in enumerate(channels_config):
        scrape_status["current_channel"] = ch["name"]
        progress = int((i / total_channels) * 100)
        scrape_status["progress"] = progress
        socketio.emit('status', {"running": True, "progress": progress, "channel": ch["name"]})
        
        log_fn(f"Scraping: {ch['name']} (last {ch['hours_back']}h)...")
        
        try:
            rows = await scrape_channel_simple(client, ch["name"], ch["from_date"], ch["to_date"], log_fn)
            all_rows.extend(rows)
            
            scrape_status["results"].append({
                "channel": ch["name"],
                "messages": len(rows),
                "hours_back": ch["hours_back"]
            })
            
            log_fn(f"  ✓ {len(rows)} messages from {ch['name']}", "success")
            socketio.emit('channel_done', {"channel": ch["name"], "count": len(rows)})
            
        except Exception as e:
            log_fn(f"  ✗ Error scraping {ch['name']}: {str(e)}", "error")
            scrape_status["results"].append({
                "channel": ch["name"],
                "messages": 0,
                "error": str(e)
            })
    
    await client.disconnect()
    
    # Push to Google Sheets
    if push_to_sheet and all_rows and SHEET_ID:
        log_fn("Pushing to Google Sheets...")
        try:
            push_to_sheets(all_rows)
            log_fn(f"Pushed {len(all_rows)} rows to Google Sheets", "success")
        except Exception as e:
            log_fn(f"Error pushing to Sheets: {str(e)}", "error")
    elif not all_rows:
        log_fn("No messages to push", "warning")
    elif not SHEET_ID:
        log_fn("SHEET_ID not configured, skipping Google Sheets", "warning")
    
    return all_rows


def run_scraper_async(channels_config, push_to_sheet=True):
    """Run the scraper in a background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    all_rows = []
    
    try:
        scrape_status["running"] = True
        scrape_status["progress"] = 0
        scrape_status["messages"] = []
        scrape_status["results"] = []
        scrape_status["error"] = None
        
        socketio.emit('status', {"running": True, "progress": 0})
        
        all_rows = loop.run_until_complete(
            run_scraper_coroutine(channels_config, push_to_sheet, log_message)
        )
        
        scrape_status["progress"] = 100
        log_message(f"Done! Total: {len(all_rows)} messages from {len(channels_config)} channel(s)", "success")
        
    except Exception as e:
        scrape_status["error"] = str(e)
        log_message(f"Error: {str(e)}", "error")
    
    finally:
        scrape_status["running"] = False
        socketio.emit('status', {"running": False, "progress": 100})
        socketio.emit('done', {"total": len(all_rows), "results": scrape_status["results"]})
        loop.close()


@app.route('/')
def index():
    """Main page."""
    channels = load_channels()
    return render_template('index.html', 
                         channels=channels,
                         sheet_id=SHEET_ID,
                         status=scrape_status)


@app.route('/api/channels', methods=['GET'])
def get_channels():
    """Get current channel configuration."""
    channels = load_channels()
    return jsonify(channels)


@app.route('/api/channels', methods=['POST'])
def save_channels():
    """Save channel configuration."""
    data = request.json
    channels_path = Path(__file__).parent / "channels.json"
    
    config = {
        "default_hours": data.get("default_hours", 24),
        "channels": data.get("channels", [])
    }
    
    with open(channels_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    return jsonify({"success": True})


@app.route('/api/scrape', methods=['POST'])
def start_scrape():
    """Start a scrape job."""
    if scrape_status["running"]:
        return jsonify({"error": "Scraper is already running"}), 400
    
    data = request.json or {}
    push_to_sheet = data.get("push_to_sheet", True)
    
    # Load channels and prepare config
    channels = load_channels()
    if not channels:
        return jsonify({"error": "No channels configured"}), 400
    
    # Start scraper in background thread
    thread = threading.Thread(target=run_scraper_async, args=(channels, push_to_sheet))
    thread.daemon = True
    thread.start()
    
    return jsonify({"success": True, "message": "Scraper started"})


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current scraper status."""
    return jsonify(scrape_status)


if __name__ == '__main__':
    print("=" * 50)
    print("  Telegram Scraper Web GUI")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 50)
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
