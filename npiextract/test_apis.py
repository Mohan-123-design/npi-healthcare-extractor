# test_apis.py - Test both API connections

import os
from dotenv import load_dotenv
load_dotenv()

print("=" * 60)
print("  API CONNECTION TESTER")
print("=" * 60)

# Test 1: Check API keys exist
print("\n1. Checking API keys in .env file...")

ant_key = os.getenv("SCRAPINGANT_API_KEY", "")
wai_key = os.getenv("WEBSCRAPINGAI_API_KEY", "")

print(f"   ScrapingAnt key: {'✅ Found (' + ant_key[:8] + '...)' if ant_key and ant_key != 'your_scrapingant_key_here' else '❌ MISSING'}")
print(f"   WebScraping.AI key: {'✅ Found (' + wai_key[:8] + '...)' if wai_key and wai_key != 'your_webscraping_ai_key_here' else '❌ MISSING'}")

# Test 2: DNS Resolution
print("\n2. Testing DNS resolution...")
import socket

apis_to_test = {
    "api.scrapingant.com": "ScrapingAnt",
    "api.webscraping.ai": "WebScraping.AI",
    "api.scraping.ai": "OLD WRONG endpoint (should fail)",
}

for domain, name in apis_to_test.items():
    try:
        ip = socket.gethostbyname(domain)
        print(f"   ✅ {domain} → {ip} ({name})")
    except socket.gaierror:
        print(f"   ❌ {domain} → DOES NOT EXIST ({name})")

# Test 3: Actual API calls
print("\n3. Testing actual API calls...")
import requests

# Test ScrapingAnt
if ant_key and ant_key != "your_scrapingant_key_here":
    print("\n   Testing ScrapingAnt...")
    try:
        resp = requests.get(
            "https://api.scrapingant.com/v2/general",
            headers={"x-api-key": ant_key},
            params={"url": "https://httpbin.org/get", "browser": "false"},
            timeout=30,
        )
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"   ✅ ScrapingAnt is WORKING! (Response: {len(resp.text)} chars)")
        elif resp.status_code == 401:
            print(f"   ❌ INVALID API KEY")
        elif resp.status_code == 402:
            print(f"   ❌ NO CREDITS REMAINING")
        else:
            print(f"   ⚠ Unexpected: {resp.text[:200]}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

# Test WebScraping.AI
if wai_key and wai_key != "your_webscraping_ai_key_here":
    print("\n   Testing WebScraping.AI...")
    try:
        resp = requests.get(
            "https://api.webscraping.ai/html",
            params={
                "api_key": wai_key,
                "url": "https://httpbin.org/get",
                "js": "false",
                "timeout": 10000,
            },
            timeout=30,
        )
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"   ✅ WebScraping.AI is WORKING! (Response: {len(resp.text)} chars)")
        elif resp.status_code == 401:
            print(f"   ❌ INVALID API KEY")
        elif resp.status_code == 402:
            print(f"   ❌ NO CREDITS")
        else:
            print(f"   ⚠ Unexpected: {resp.text[:200]}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "=" * 60)
print("  TEST COMPLETE")
print("=" * 60)