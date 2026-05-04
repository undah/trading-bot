import os
import base64
import urllib.parse
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from playwright.sync_api import sync_playwright
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

BASE_CHART_URL = os.getenv("BASE_CHART_URL", "https://www.tradingview.com/chart/GoEcqSyG/")
SESSION_FILE = "tv_session.json"

SYMBOL_MAP = {
    "EURUSD": "FX:EURUSD",
    "GBPUSD": "FX:GBPUSD",
    "USDJPY": "FX:USDJPY",
    "USDCHF": "FX:USDCHF",
    "AUDUSD": "FX:AUDUSD",
    "USDCAD": "FX:USDCAD",
    "NZDUSD": "FX:NZDUSD",
    "EURGBP": "FX:EURGBP",
    "EURJPY": "FX:EURJPY",
    "GBPJPY": "FX:GBPJPY",
    "XAUUSD": "OANDA:XAUUSD",
    "XAGUSD": "OANDA:XAGUSD",
    "US30":   "TVC:US30",
    "NAS100": "TVC:NDQ",
}

class RunRequest(BaseModel):
    pair: Optional[str] = None

def take_screenshot(pair: Optional[str] = None) -> str:
    symbol = SYMBOL_MAP.get((pair or "").upper())
    if symbol:
        chart_url = f"{BASE_CHART_URL}?symbol={urllib.parse.quote(symbol)}"
    else:
        chart_url = os.getenv("CHART_URL", f"{BASE_CHART_URL}?symbol=TVC%3ANDQ")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=SESSION_FILE,
            viewport={"width": 1600, "height": 900}
        )
        page = context.new_page()
        page.goto(chart_url)
        page.wait_for_selector(".chart-container", timeout=15000)
        page.wait_for_timeout(3000)
        screenshot_path = "chart.png"
        page.screenshot(path=screenshot_path)
        browser.close()
        return screenshot_path

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

@app.post("/run")
def run_screenshot(body: RunRequest = RunRequest()):
    try:
        if not os.path.exists(SESSION_FILE):
            raise HTTPException(status_code=400, detail="tv_session.json not found. Upload it first.")

        path = take_screenshot(body.pair)

        with open(path, "rb") as f:
            img_base64 = base64.b64encode(f.read()).decode("utf-8")

        return JSONResponse({
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "image_base64": img_base64,
            "pair": body.pair,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-session")
async def upload_session(request: dict):
    try:
        session_data = request.get("session_data")
        if not session_data:
            raise HTTPException(status_code=400, detail="session_data required")
        import json
        with open(SESSION_FILE, "w") as f:
            json.dump(session_data, f)
        return {"status": "session updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
