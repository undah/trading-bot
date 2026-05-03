import os
import base64
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from playwright.sync_api import sync_playwright
from datetime import datetime

app = FastAPI()

CHART_URL = os.getenv("CHART_URL", "https://www.tradingview.com/chart/GoEcqSyG/?symbol=TVC%3ANDQ")
SESSION_FILE = "tv_session.json"

def take_screenshot():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            storage_state=SESSION_FILE,
            viewport={"width": 1600, "height": 900}
        )
        page = context.new_page()
        page.goto(CHART_URL)
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
def run_screenshot():
    try:
        if not os.path.exists(SESSION_FILE):
            raise HTTPException(status_code=400, detail="tv_session.json not found. Upload it first.")
        
        path = take_screenshot()
        
        with open(path, "rb") as f:
            img_bytes = f.read()
        img_base64 = base64.b64encode(img_bytes).decode("utf-8")
        
        return JSONResponse({
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "image_base64": img_base64
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-session")
async def upload_session(request: dict):
    """Endpoint to update the TradingView session file"""
    try:
        session_data = request.get("session_data")
        if not session_data:
            raise HTTPException(status_code=400, detail="session_data required")
        
        with open(SESSION_FILE, "w") as f:
            import json
            json.dump(session_data, f)
        
        return {"status": "session updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
