"""
Run this ONCE locally to authenticate your Telegram account.
It will create a session file that you then base64-encode for GitHub Actions.

Usage:
    1. Copy .env.example to .env and fill in your Telegram credentials
    2. Run: python auth_session.py
    3. Enter the code Telegram sends you
    4. The session file will be created
    5. Base64 encode it for GitHub:
       python -c "import base64; print(base64.b64encode(open('scraper_session.session','rb').read()).decode())"
    6. Save that output as the TG_SESSION GitHub Secret
"""
import asyncio
from telethon import TelegramClient
from src.config import TG_API_ID, TG_API_HASH, TG_PHONE, TG_SESSION_NAME


async def main():
    if not TG_API_ID or not TG_API_HASH:
        print("ERROR: Set TG_API_ID and TG_API_HASH in .env first")
        return

    client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
    await client.start(phone=TG_PHONE)
    me = await client.get_me()
    print(f"Authenticated as: {me.first_name} (@{me.username})")
    print(f"Session file created: {TG_SESSION_NAME}.session")
    print("\nNext step: base64 encode the session file for GitHub Actions:")
    print(f'  python -c "import base64; print(base64.b64encode(open(\'{TG_SESSION_NAME}.session\',\'rb\').read()).decode())"')
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
