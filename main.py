"""
Telegram Daily Scraper -> Google Sheets
Run this script to scrape channels and push data to Google Sheets.
"""
import asyncio
import sys

from src.config import load_channels
from src.scraper import run_scraper
from src.sheets import push_to_sheets


async def main():
    print("=" * 50)
    print("  Telegram Daily Scraper")
    print("=" * 50)

    # Load channel config
    channels = load_channels()
    if not channels:
        print("No channels configured in channels.json")
        sys.exit(1)

    print(f"\nChannels to scrape: {len(channels)}")
    for ch in channels:
        print(f"  - {ch['name']} (last {ch['hours_back']}h)")

    # Scrape
    print("\n--- SCRAPING ---")
    all_rows = await run_scraper(channels)

    # Push to Google Sheets
    print("\n--- PUSHING TO GOOGLE SHEETS ---")
    push_to_sheets(all_rows)

    print("\n--- DONE ---")


if __name__ == "__main__":
    asyncio.run(main())
