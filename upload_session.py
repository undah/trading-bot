"""
Run this locally after deploying to Railway to upload your TradingView session.
Usage: python upload_session.py https://your-railway-url.railway.app
"""
import sys
import json
import requests

if len(sys.argv) < 2:
    print("Usage: python upload_session.py <railway-url>")
    sys.exit(1)

url = sys.argv[1].rstrip("/")

with open("tv_session.json", "r") as f:
    session_data = json.load(f)

response = requests.post(f"{url}/upload-session", json={"session_data": session_data})
print(response.json())
