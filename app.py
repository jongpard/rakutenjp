# -*- coding: utf-8 -*-
"""
Rakuten JP · Beauty(100939) Daily Ranking Top160
- ScraperAPI (JP, render=true) with throttle detection, session rotation, exponential backoff
- Fallback to Playwright to force-load 80 items/page
- Robust parser including TOP3 boxes (1~3위) + normal list (4위~)
- Brand normalization: aggregator shops (e.g., 楽天24) are NOT brands
- Slack Top10 (raw + (ko) optional), I/O section fixed text
- Google Drive upload
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
SLACK_TRANSLATE_JA2KO = os.getenv("SLACK_TRANSLATE_JA2KO", "1") == "1"

# Naver Papago (선택)
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "qA98WCnxWFvx_odn1fKc")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "G_kRiRAk7z")

# Google Drive
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

# ========= Translation (Papago) =========
from functools import lru_cache
@lru_cache(maxsize=4096)
def translate_ja_ko(text: str) -> str:
    if not SLACK_TRANSLATE_JA2KO or not text:
        return ""
    try:
        url = "https://openapi.naver.com/v1/papago/n2mt"
        headers = {"X-Naver-Client-Id": NAVER_CLIENT_ID, "X-Naver-Client-Secret": NAVER_CLIENT_SECRET}
        data = {"source": "ja", "target": "ko", "text": text[:4900]}
        r = requests.post(url, headers=headers, data=data, timeout=5)
        r.raise_for_status()
        return r.json()["message"]["result"]["translatedText"]
    except Exception:
        return ""

# ========= ScraperAPI (render) with throttle handling =========
def scraper_get(url: str, dbg_name: str, max_retry: int = 6) -> str:
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
            "wait_for": "div.rnkRanking_after4box,div.rnkRanking_top3box",
            "wait_time": "5000",
        }
        try:
            r = requests.get("https://api.scraperapi.com/", params=params, headers=HEADERS, timeout=60)
            r.raise_for_status()
            html = r.text
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

# ========= Brand normalize =========
AGGREGATOR_SHOPS = {
    "楽天24", "Rakuten24", "Rakuten 24", "ケンコーコム", "爽快ドラッグ", "LOHACO",
}
STOPWORDS = [
    "楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営",
    "店","本店","支店","楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE",
]
def _clean_brand(b: str) -> str:
    if not b: return ""
    b = re.sub(r"[【】\[\]（）()]", "", b)
    for w in STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"\s{2,}", " ", b).strip(" -_·|·")
    b = re.sub(r"\d+$", "", b).strip()
    if not b or re.fullmatch(r"\d+", b) or len(b) < 2:
        return ""
    return b
def _brand_from_name(name: str) -> str:
    if not name: return ""
    m = re.search(r"【\s*公式\s*】\s*([^\s\|｜/／]+)", name)
    if m: return _clean_brand(m.group(1))
    m = re.search(r"【\s*([^\s\|｜/／]+)\s*】", name)
    if m: return _clean_brand(m.group(1))
    tok = re.split(r"[｜\|/／\s]+", name.strip())[0]
    return _clean_brand(tok)
def extract_brand(name: str, shop: str) -> str:
    shop = (shop or "").strip()
    if any(a in shop for a in AGGREGATOR_SHOPS):
        return _brand_from_name(name) or ""
    b = re.split(r"(?:公式|オフィシャル|ショップ|ストア|直営|楽天市場店)", shop)[0].strip()
    b = _clean_brand(b)
    if b: return b
    return _brand_from_name(name)

# ========= Parsing =========
# TOP3 전용 + 일반 리스트 모두 커버
TOP3_SEL = ",".join([
    "#rnkRankingTop3 .rnkRanking_top3box",
    "div.rnkRanking_top3box",
])
CARD_SEL = ",".join([
    "div.rnkRanking_after4box",
    "div.rnkRanking_box",
    "li.rnkRanking_item",
    "li.rnkRankingList__item",
])

def pick_one(el, sels):
    for s in sels:
        f = el.select_one(s)
        if f: return f
    return None

def find_rank_text(scope):
    # rank 텍스트를 다양한 위치에서 탐색 (예: "1位", "3 位")
    cand = scope.select("[class*='Rank'], .rnkRanking_dispRank, .rnkRanking_rank, [id*='Rank']")
    for c in cand:
        t = c.get_text(" ", strip=True)
        m = re.search(r"(\d+)\s*位", t)
        if m:
            return int(m.group(1))
    # 최후의 보루: scope 내 전체 텍스트에서 패턴 찾기
    t = scope.get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*位", t)
    return int(m.group(1)) if m else None

def parse_cards(scope):
    items = []
    # 이름/URL
    a = scope.select_one(".rnkRanking_itemName a") or scope.select_one("a.rnkRanking_itemName")
    if not a:
        # 이미지 링크가 메인 링크인 경우
        a = scope.select_one(".rnkRanking_image a") or scope.select_one("a[href*='item.rakuten.co.jp']")
    name = a.get_text(strip=True) if a else ""
    url = a.get("href") if a and a.has_attr("href") else ""
    if url and url.startswith("//"): url = "https:" + url
    if url and url.startswith("/"):  url = urljoin("https://ranking.rakuten.co.jp", url)

    # 가격/샵
    price_el = scope.select_one(".rnkRanking_price") or scope.select_one(".price")
    shop_el  = scope.select_one(".rnkRanking_shop a") or scope.select_one(".rnkRanking_shop")
    price = clean_price(price_el.get_text(strip=True)) if price_el else None
    shop  = shop_el.get_text(strip=True) if shop_el else ""

    # 랭크
    rank = find_rank_text(scope)
    if rank and name:
        items.append({"rank":rank,"name":name,"price":price,"shop":shop,"url":url})
    return items

def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # 1) TOP3 먼저
    for box in soup.select(TOP3_SEL):
        rows.extend(parse_cards(box))

    # 2) 일반 리스트
    for card in soup.select(CARD_SEL):
        rows.extend(parse_cards(card))

    # 3) 중복 제거 + 정렬
    # URL을 키로 1차, rank로 2차 보정
    tmp = {}
    for it in rows:
        key = (it["url"] or "") + f"#{it['rank']}"
        tmp[key] = it
    rows = list(tmp.values())
    rows.sort(key=lambda x: x["rank"])
    return rows

# ========= Slack =========
def build_slack(df: pd.DataFrame) -> str:
    lines = [f"*Rakuten Japan · 뷰티 Top{min(MAX_RANK, len(df))} ({TODAY})*",
             "", "*🏆 Top10 (raw 제품명)*"]
    for _, r in df.sort_values("rank").head(10).iterrows():
        price = f" — ¥{int(r['price']):,}" if pd.notna(r.get("price")) else ""
        raw = r["name"]
        ko  = translate_ja_ko(raw)
        link = f"<{r['url']}|{raw}>"
        ko_part = f" ({ko})" if ko else ""
        lines.append(f"{int(r['rank']):>3}위 | {link}{ko_part}{price}")
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
        # 1) ScraperAPI(렌더)
        try:
            html = scraper_get(url, dbg_name=f"p{i}")
            if is_throttled(html):
                log("[WARN] ScraperAPI throttled 화면")
            rows = parse_page(html)
            log(f"[parse] p{i}: {len(rows)}")
            all_rows.extend(rows)
            ok = len(rows) >= 60
        except Exception as e:
            log(f"[WARN] ScraperAPI 실패: {e}")

        # 2) 부족하면 Playwright 폴백
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

        time.sleep(1.0)  # 페이지 간 딜레이

    if not all_rows:
        log("[ERROR] 수집 0건 — 전량 차단됨. data/debug/* 확인")
        sys.exit(1)

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["rank"]).sort_values("rank")
    df = df[df["rank"] <= MAX_RANK].reset_index(drop=True)

    # 브랜드 최종 정리
    df["brand"] = df.apply(lambda r: extract_brand(r["name"], r["shop"]), axis=1)

    log(f"[INFO] 수집 개수: {len(df)}")
    return df

def main():
    log("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    df = collect()

    csv_path = os.path.join(DATA_DIR, f"라쿠텐재팬_뷰티_랭킹_{TODAY}.csv")
    df[["rank","name","price","shop","brand","url"]].to_csv(csv_path, index=False, encoding="utf-8-sig")
    log(f"[INFO] CSV 저장: {csv_path}")

    slack_post(build_slack(df))
    upload_gdrive(csv_path)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[오류] {e}")
        traceback.print_exc()
        sys.exit(1)
