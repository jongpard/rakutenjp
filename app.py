# -*- coding: utf-8 -*-
"""
Rakuten JP · Beauty(100939) Daily Ranking Top160
- ScraperAPI (JP, render=true) with throttle detection, session rotation, exponential backoff
- Fallback to Playwright (headless Chromium) to force-load 80 items per page
- Saves CSV to data/, debug HTML to data/debug/
- Posts Slack Top10 (delta shown as '-') and uploads CSV to Google Drive (if secrets set)
"""

import os, re, sys, time, random, traceback, datetime as dt
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ========= Env & Paths =========
KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).strftime("%Y-%m-%d")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DBG_DIR = os.path.join(DATA_DIR, "debug")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

# Ranking URLs (1~80, 81~160)
BASE = "https://ranking.rakuten.co.jp/daily/100939/"
PAGE_URLS = [BASE, BASE + "p=2/"]

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))

# Secrets & Options
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/121.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
}

THROTTLE_SIGNATURES = [
    "アクセスが集中",  # 접속 집중 안내
    "ご迷惑をおかけしまして誠に申し訳ございません",
]

# ========= Small utils =========
def log(msg: str):
    print(msg, flush=True)

def is_throttled(html: str) -> bool:
    if not html:
        return True
    return any(sig in html for sig in THROTTLE_SIGNATURES)

def only_digits(s: str):
    if not s:
        return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group()) if m else None

def clean_price(txt: str):
    if not txt:
        return None
    m = re.search(r"[\d,]+", txt)
    return int(m.group().replace(",", "")) if m else None

# ========= ScraperAPI (render) with throttle handling =========
def scraper_get(url: str, dbg_name: str, max_retry: int = 6, base_wait: float = 1.2) -> str:
    if not SCRAPERAPI_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")

    last_err = None
    for attempt in range(1, max_retry + 1):
        session_id = f"rk-{int(time.time()*1000)}-{random.randint(1000,9999)}"
        params = {
            "api_key": SCRAPERAPI_KEY,
            "url": url,
            "country_code": "jp",
            "render": "true",
            "retry_404": "true",
            "keep_headers": "true",
            "device_type": "desktop",
            "session_number": session_id,
            "wait_for": "div.rnkRanking_after4box",
            "wait_time": "5000",  # ms
        }
        try:
            r = requests.get("https://api.scraperapi.com/", params=params, headers=HEADERS, timeout=60)
            r.raise_for_status()
            html = r.text
            # save debug
            open(os.path.join(DBG_DIR, f"{dbg_name}_try{attempt}.html"), "w", encoding="utf-8").write(html or "")

            if not is_throttled(html) and len(html) > 2000:
                return html

            wait = min(2 ** attempt, 20) + random.uniform(0.3, 0.9)
            log(f"[WARN] throttled (try {attempt}/{max_retry}, session={session_id}) → sleep {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            last_err = e
            wait = min(2 ** attempt, 20) + random.uniform(0.3, 0.9)
            log(f"[WARN] ScraperAPI 예외 (try {attempt}/{max_retry}): {e} → sleep {wait:.1f}s")
            time.sleep(wait)

    if last_err:
        raise last_err
    raise RuntimeError("ScraperAPI throttled")

# ========= Playwright fallback =========
def render_with_playwright(url: str, max_wait_ms: int = 18000) -> str:
    from playwright.sync_api import sync_playwright
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"
        ])
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="ja-JP"
        )
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(url, wait_until="domcontentloaded")

        # scroll to load
        start = time.time()
        last_h = 0
        while time.time() - start < max_wait_ms / 1000.0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.6)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h
        time.sleep(1.0)
        html = page.content()
        context.close()
        browser.close()
    return html

# ========= Parsing =========
CARD_SEL = ",".join([
    "div.rnkRanking_after4box",
    "div.rnkRanking_box",
    "li.rnkRanking_item",
    "li.rnkRankingList__item",
])
RANK_SELS  = [".rnkRanking_rank", ".rnkRanking_dispRank", "[class*='Rank']", "[class*='rank']"]
NAME_SELS  = [".rnkRanking_itemName a", "a.rnkRanking_itemName", ".itemName a", "a[href*='item.rakuten.co.jp']", "a"]
PRICE_SELS = [".rnkRanking_price", ".price", "[class*='price']"]
SHOP_SELS  = [".rnkRanking_shop a", ".rnkRanking_shop", "[class*='shop'] a"]

STOPWORDS = ["楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営","店","本店","支店",
             "楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"]

def brand_from_shop(shop: str) -> str:
    if not shop:
        return ""
    b = shop
    for w in STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"[【】\[\]（）()]", "", b)
    b = re.sub(r"\s{2,}", " ", b).strip(" -_·|·")
    return b.strip()

def pick_one(el, sels):
    for s in sels:
        f = el.select_one(s)
        if f:
            return f
    return None

def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for card in soup.select(CARD_SEL):
        rk_el = pick_one(card, RANK_SELS)
        if not rk_el:
            continue
        rank = only_digits(rk_el.get_text(strip=True))
        if not rank:
            continue

        a = pick_one(card, NAME_SELS)
        name = a.get_text(strip=True) if a else ""
        href = a.get("href") if a and a.has_attr("href") else ""
        if href and href.startswith("/"):
            href = urljoin("https://ranking.rakuten.co.jp", href)
        if href and href.startswith("//"):
            href = "https:" + href

        pr_el = pick_one(card, PRICE_SELS)
        price = clean_price(pr_el.get_text(strip=True)) if pr_el else None

        sh = pick_one(card, SHOP_SELS)
        shop = sh.get_text(strip=True) if sh else ""
        brand = brand_from_shop(shop)

        if name:
            rows.append({
                "rank": rank, "name": name, "price": price,
                "shop": shop, "brand": brand, "url": href
            })
    return rows

# ========= Slack =========
def build_slack(df: pd.DataFrame) -> str:
    lines = [f"*Rakuten Japan · 뷰티 Top{min(MAX_RANK, len(df))} ({TODAY})*",
             "", "*🏆 Top10 (raw 제품명)*"]
    for _, r in df.sort_values("rank").head(10).iterrows():
        price = f" — ¥{int(r['price']):,}" if pd.notna(r.get("price")) else ""
        lines.append(f"{int(r['rank']):>3}위 | - | {r['name']}{price}")
    lines += ["", "*↔ 랭크 인&아웃*", "0개의 제품이 인&아웃 되었습니다."]
    return "\n".join(lines)

def slack_post(text: str):
    if not SLACK_WEBHOOK_URL:
        log("[INFO] Slack 미설정 → 전송 생략")
        return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20)
        r.raise_for_status()
        log("[INFO] Slack 전송 OK")
    except Exception as e:
        log(f"[WARN] Slack 전송 실패: {e}")

# ========= Google Drive =========
def upload_gdrive(local_path: str):
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        log("[INFO] 드라이브 업로드 생략(시크릿 미설정)")
        return
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        meta = {"name": os.path.basename(local_path), "parents": [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
        service.files().create(body=meta, media_body=media, fields="id").execute()
        log("[INFO] 드라이브 업로드 OK")
    except Exception as e:
        log(f"[ERROR] 드라이브 업로드 실패: {e}")
        traceback.print_exc()

# ========= Collector =========
def collect() -> pd.DataFrame:
    all_rows = []
    for i, url in enumerate(PAGE_URLS, start=1):
        log(f"[GET] {url}")
        ok = False

        # 1) ScraperAPI(렌더) 우선
        try:
            html = scraper_get(url, dbg_name=f"p{i}")
            if is_throttled(html):
                log("[WARN] ScraperAPI throttled 화면")
            rows = parse_page(html)
            log(f"[parse] p{i}: {len(rows)}")
            all_rows.extend(rows)
            ok = len(rows) >= 60  # 60 이상이면 충분
        except Exception as e:
            log(f"[WARN] ScraperAPI 실패: {e}")

        # 2) 부족하면 Playwright 폴백 한 번
        if not ok:
            try:
                pw_html = render_with_playwright(url, max_wait_ms=18000)
                open(os.path.join(DBG_DIR, f"p{i}_pw.html"), "w", encoding="utf-8").write(pw_html or "")
                if is_throttled(pw_html):
                    log("[WARN] Playwright도 throttled 화면")
                rows = parse_page(pw_html)
                log(f"[parse/pw] p{i}: {len(rows)}")
                all_rows.extend(rows)
            except Exception as e:
                log(f"[ERROR] Playwright 폴백 실패: {e}")

        # 페이지 간 휴식(차단 완화)
        time.sleep(1.2)

    df = pd.DataFrame(all_rows)
    if df.empty:
        log("[ERROR] 수집 0건 — 전량 차단됨. data/debug/* 열어 확인")
        sys.exit(1)

    # 중복·정렬·Cut
    df = df.drop_duplicates(subset=["rank"]).sort_values("rank")
    df = df[df["rank"] <= MAX_RANK].reset_index(drop=True)
    log(f"[INFO] 수집 개수: {len(df)}")
    return df

def main():
    log("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    df = collect()

    # CSV 저장
    csv_path = os.path.join(DATA_DIR, f"라쿠텐재팬_뷰티_랭킹_{TODAY}.csv")
    df[["rank","name","price","shop","brand","url"]].to_csv(csv_path, index=False, encoding="utf-8-sig")
    log(f"[INFO] CSV 저장: {csv_path}")

    # Slack
    slack_post(build_slack(df))

    # Google Drive
    upload_gdrive(csv_path)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[오류] {e}")
        traceback.print_exc()
        sys.exit(1)
