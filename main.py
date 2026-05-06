import os
import base64
import urllib.parse
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, time as dtime
from typing import Optional, List

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from playwright.sync_api import sync_playwright
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_SERVICE_KEY", "")
BASE_CHART_URL = os.getenv("BASE_CHART_URL", "https://www.tradingview.com/chart/GoEcqSyG/")
SESSION_FILE   = "tv_session.json"

SYMBOL_MAP = {
    "EURUSD": "FX:EURUSD",  "GBPUSD": "FX:GBPUSD",   "USDJPY": "FX:USDJPY",
    "USDCHF": "FX:USDCHF",  "AUDUSD": "FX:AUDUSD",   "USDCAD": "FX:USDCAD",
    "NZDUSD": "FX:NZDUSD",  "EURGBP": "FX:EURGBP",   "EURJPY": "FX:EURJPY",
    "GBPJPY": "FX:GBPJPY",  "XAUUSD": "OANDA:XAUUSD", "XAGUSD": "OANDA:XAGUSD",
    "US30":   "TVC:US30",   "NAS100": "TVC:NDQ",
}

DAY_MAP = {
    "Mon": "mon", "Tue": "tue", "Wed": "wed", "Thu": "thu",
    "Fri": "fri", "Sat": "sat", "Sun": "sun",
}

SESSION_WINDOWS = {
    "london":   (dtime(8,  0), dtime(12, 0)),
    "new_york": (dtime(13, 0), dtime(17, 0)),
}

# ── In-memory config ──────────────────────────────────────────────────────────

current_config: dict = {
    "enabled": False,
    "schedule_mode": "interval",
    "interval_minutes": 15,
    "fixed_time": None,
    "days": [],
    "sessions": ["always"],
    "pairs": ["EURUSD"],
}

scheduler = AsyncIOScheduler(timezone="UTC")

# ── Supabase helpers ──────────────────────────────────────────────────────────

def _sb_headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

async def fetch_config_from_supabase() -> dict | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/screenshot_config",
            headers=_sb_headers(),
            params={"order": "updated_at.desc", "limit": "1"},
            timeout=10,
        )
        if r.status_code == 200:
            rows = r.json()
            return rows[0] if rows else None
    return None

async def write_log(status: str, image_b64: str | None, reason: str | None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    payload = {
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "image_base64": image_b64,
        "reason": reason,
    }
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{SUPABASE_URL}/rest/v1/screenshot_log",
                headers=_sb_headers(),
                json=payload,
                timeout=15,
            )
            if r.status_code not in (200, 201, 204):
                log.warning(f"write_log HTTP {r.status_code}: {r.text[:200]}")
            else:
                log.info(f"write_log OK ({r.status_code}) — status={status} reason={payload.get('reason','')!r}")
    except Exception as e:
        log.warning(f"Failed to write log: {e}")

# ── Screenshot ────────────────────────────────────────────────────────────────

def _take_screenshot_sync(pair: str | None) -> bytes:
    symbol = SYMBOL_MAP.get((pair or "").upper())
    chart_url = (
        f"{BASE_CHART_URL}?symbol={urllib.parse.quote(symbol)}"
        if symbol else
        f"{BASE_CHART_URL}?symbol=TVC%3ANDQ"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            storage_state=SESSION_FILE,
            viewport={"width": 1280, "height": 720},
        )
        page = ctx.new_page()
        page.goto(chart_url)
        page.wait_for_selector(".chart-container", timeout=15000)
        page.wait_for_timeout(3000)

        # ── Zoom to today only ────────────────────────────────────────────────
        # Press 'D' to jump to daily view, then use keyboard shortcut to
        # reset zoom and fit today's session into the viewport.
        # TradingView shortcut: Alt+R resets zoom, then we use the date range
        # bar at the bottom to select "1D" (today only).
        try:
            # Click the "1D" range button at the bottom of the chart
            one_day_btn = page.locator('button[data-value="1D"]').first
            if one_day_btn.is_visible(timeout=3000):
                one_day_btn.click()
                page.wait_for_timeout(1500)
            else:
                # Fallback: use keyboard shortcut Alt+R to reset zoom then
                # press Left arrow to shift view to show only today
                page.keyboard.press("Alt+r")
                page.wait_for_timeout(1000)
        except Exception:
            # If neither works, just take the screenshot as-is
            pass

        page.wait_for_timeout(1500)
        img_bytes = page.screenshot(type="jpeg", quality=75)
        browser.close()
    return img_bytes


# ── Session check ─────────────────────────────────────────────────────────────

def _in_session(sessions: list) -> bool:
    if "always" in sessions:
        return True
    now = datetime.now(timezone.utc).time()
    for s in sessions:
        if s in SESSION_WINDOWS:
            start, end = SESSION_WINDOWS[s]
            if start <= now <= end:
                return True
    return False

# ── Scheduled job ─────────────────────────────────────────────────────────────

async def run_scheduled():
    cfg = current_config
    if not cfg.get("enabled"):
        return

    if not _in_session(cfg.get("sessions", ["always"])):
        log.info("Skipping — outside session window")
        await write_log("skipped", None, "Outside session window")
        return

    if not os.path.exists(SESSION_FILE):
        log.error("tv_session.json missing — cannot run")
        await write_log("error", None, "tv_session.json not found on server")
        return

    pairs = cfg.get("pairs") or ["EURUSD"]
    log.info(f"Scheduled capture: {pairs}")

    for pair in pairs:
        try:
            img_b64 = await capture(pair)
            await write_log("success", img_b64, f"Scheduled — {pair}")
            log.info(f"  ✓ {pair}")
        except Exception as e:
            log.error(f"  ✗ {pair}: {e}")
            await write_log("error", None, f"Scheduled — {pair}: {str(e)[:200]}")

# ── Scheduler management ──────────────────────────────────────────────────────

def apply_schedule(cfg: dict):
    global current_config
    current_config = {k: cfg[k] for k in [
        "enabled", "schedule_mode", "interval_minutes",
        "fixed_time", "days", "sessions", "pairs",
    ] if k in cfg}

    if scheduler.get_job("screenshot"):
        scheduler.remove_job("screenshot")

    if not cfg.get("enabled"):
        log.info("Scheduler disabled")
        return

    mode = cfg.get("schedule_mode", "interval")

    if mode == "interval":
        minutes = cfg.get("interval_minutes") or 15
        trigger = IntervalTrigger(minutes=minutes, timezone="UTC")
        log.info(f"Scheduler: every {minutes}m")
    else:
        ft = cfg.get("fixed_time") or "09:00"
        hour, minute = ft.split(":")
        days = cfg.get("days") or []
        dow = ",".join(DAY_MAP[d] for d in days if d in DAY_MAP) or "*"
        trigger = CronTrigger(hour=int(hour), minute=int(minute), day_of_week=dow, timezone="UTC")
        log.info(f"Scheduler: fixed {ft} UTC on {dow}")

    scheduler.add_job(run_scheduled, trigger=trigger, id="screenshot", replace_existing=True)

def _next_run() -> str | None:
    job = scheduler.get_job("screenshot")
    return job.next_run_time.isoformat() if job and job.next_run_time else None

# ── App lifespan ──────────────────────────────────────────────────────────────

def _restore_session_from_env():
    """Write tv_session.json from TV_SESSION_B64 env var if the file is missing."""
    session_b64 = os.getenv("TV_SESSION_B64", "")
    if not session_b64:
        return
    if os.path.exists(SESSION_FILE):
        return
    try:
        data = base64.b64decode(session_b64).decode("utf-8")
        with open(SESSION_FILE, "w") as f:
            f.write(data)
        log.info("tv_session.json restored from TV_SESSION_B64 env var")
    except Exception as e:
        log.warning(f"Failed to restore session from env: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    _restore_session_from_env()
    scheduler.start()
    try:
        cfg = await fetch_config_from_supabase()
        if cfg:
            apply_schedule(cfg)
            log.info(f"Config loaded from Supabase — enabled={cfg.get('enabled')}")
        else:
            log.info("No config in Supabase — scheduler idle")
    except Exception as e:
        log.warning(f"Could not load config on startup: {e}")
    yield
    scheduler.shutdown()

# ── FastAPI ───────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

class RunRequest(BaseModel):
    pair: Optional[str] = None

class ScheduleConfig(BaseModel):
    enabled: bool
    schedule_mode: str = "interval"
    interval_minutes: Optional[int] = 15
    fixed_time: Optional[str] = None
    days: List[str] = []
    sessions: List[str] = ["always"]
    pairs: List[str] = ["EURUSD"]

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scheduler_enabled": current_config.get("enabled", False),
        "next_run": _next_run(),
    }

@app.get("/schedule")
def get_schedule():
    return {**current_config, "next_run": _next_run()}

@app.post("/schedule")
async def update_schedule(body: ScheduleConfig):
    apply_schedule(body.model_dump())
    return {"status": "ok", "next_run": _next_run()}

@app.post("/run")
async def run_single(body: RunRequest = RunRequest()):
    if not os.path.exists(SESSION_FILE):
        raise HTTPException(status_code=400, detail="tv_session.json not found.")
    try:
        img_b64 = await capture(body.pair)
        await write_log("success", img_b64, f"Manual — {body.pair or 'default'}")
        return JSONResponse({
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "image_base64": img_b64,
            "pair": body.pair,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run-all")
async def run_all():
    if not os.path.exists(SESSION_FILE):
        raise HTTPException(status_code=400, detail="tv_session.json not found.")
    pairs = current_config.get("pairs") or ["EURUSD"]
    results = []
    for pair in pairs:
        try:
            img_b64 = await capture(pair)
            await write_log("success", img_b64, f"Manual — {pair}")
            results.append({"pair": pair, "status": "success"})
        except Exception as e:
            await write_log("error", None, f"Manual — {pair}: {str(e)[:200]}")
            results.append({"pair": pair, "status": "error", "detail": str(e)})
    failed = [r for r in results if r["status"] == "error"]
    if failed and len(failed) == len(results):
        raise HTTPException(status_code=500, detail=f"All pairs failed: {failed[0].get('detail')}")
    return {"status": "done", "results": results}

@app.post("/upload-session")
async def upload_session(request: dict):
    import json
    try:
        session_data = request.get("session_data")
        if not session_data:
            raise HTTPException(status_code=400, detail="session_data required")
        with open(SESSION_FILE, "w") as f:
            json.dump(session_data, f)
        return {"status": "session updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
