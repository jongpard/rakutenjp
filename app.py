# app.py — Rakuten Japan Beauty Ranking (Top160) with ScraperAPI + Playwright fallback
import os, re, sys, time, json, math, traceback, datetime as dt
import requests
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pandas as pd

# ---- 환경 ----
TZ = os.getenv("TZ", "Asia/Seoul")
TODAY = dt.datetime.now().strftime("%Y-%m-%d")

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))       # 80/160 설정
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_TRANSLATE_JA2KO = os.getenv("SLACK_TRANSLATE_JA2KO", "1") == "1"

# ScraperAPI
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "")

# Google Drive OAuth
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "")

# 기본 URL (뷰티 일간 랭킹)
BASE = "https://ranking.rakuten.co.jp/daily/100939/"

# 저장 폴더
os.makedirs("data", exist_ok=True)
os.makedirs("data/debug", exist_ok=True)

def log(msg): print(msg, flush=True)

# ------------------- 공통 유틸 -------------------
def only_digits(s):
    if not s: return None
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group()) if m else None

def clean_price(txt):
    if not txt: return None
    # 예) "￥1,980" → 1980
    m = re.search(r'[\d,]+', txt)
    return int(m.group().replace(',', '')) if m else None

def fetch_scraperapi(url, render=True, country="jp", wait_ms=4000, retry=2, timeout=30):
    if not SCRAPERAPI_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPERAPI_KEY,
        "url": url,
        "country_code": country,
        "render": "true" if render else "false",
        "wait": str(wait_ms),
    }
    last = None
    for i in range(retry+1):
        try:
            r = requests.get("https://api.scraperapi.com/", params=params, timeout=timeout)
            last = r
            r.raise_for_status()
            return r.text
        except Exception as e:
            if i == retry:
                if last is not None:
                    log(f"[HTTP] {last.status_code} {last.text[:200]}")
                raise
            time.sleep(1.2 + 0.8*i)
    return None

def parse_page(html, page_idx):
    soup = BeautifulSoup(html, "lxml")
    # 핵심 아이템 블록 (초기 20개 + 렌더 시 대부분 80개)
    # 관찰된 공통 셀렉터
    cards = soup.select("div.rnkRanking_after4box")
    items = []
    for c in cards:
        rank_txt = c.select_one(".rnkRanking_rank")
        name_a = c.select_one(".rnkRanking_itemName a")
        price_el = c.select_one(".rnkRanking_price")
        shop_el  = c.select_one(".rnkRanking_shop a") or c.select_one(".rnkRanking_shop")

        rank = only_digits(rank_txt.get_text(strip=True)) if rank_txt else None
        name = name_a.get_text(strip=True) if name_a else None
        href = name_a["href"].strip() if name_a and name_a.has_attr("href") else None
        if href and href.startswith("//"):
            href = "https:" + href
        price = clean_price(price_el.get_text(strip=True) if price_el else "")

        shop = shop_el.get_text(strip=True) if shop_el else ""
        # 브랜드 추정: "공식|ショップ|store|shop|楽天" 제거
        brand_guess = re.sub(r"(公式|オフィシャル|ショップ|store|Shop|楽天|直営|専門店|本店|支店)", "", shop, flags=re.I).strip()

        if rank and name:
            items.append({
                "rank": rank,
                "name": name,
                "price": price,
                "brand_guess": brand_guess,
                "shop": shop,
                "url": href,
                "page": page_idx
            })
    return items, len(cards)

# ------------------- Playwright 폴백 -------------------
def render_with_playwright(url, max_wait=12000, scroll_target=80):
    from playwright.sync_api import sync_playwright
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            locale="ja-JP"
        )
        page = context.new_page()
        page.set_default_timeout(20000)

        page.goto(url, wait_until="domcontentloaded")
        # 스크롤로 추가 로딩 유도
        last_height = 0
        start = time.time()
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
            height = page.evaluate("document.body.scrollHeight")
            if height == last_height: break
            last_height = height
            if time.time() - start > max_wait/1000.0: break

        # 80개가 보일 만큼 한번 더 기다리기
        time.sleep(1.2)
        html = page.content()
        context.close()
        browser.close()
    return html

# ------------------- Slack -------------------
def post_to_slack(title, df):
    if not SLACK_WEBHOOK_URL:
        log("[INFO] Slack 전송 생략(미설정)")
        return
    lines = []
    lines.append(f"*{title}*")
    lines.append("")
    # Top10: raw 제품명 그대로
    lines.append("🏆 *Top10 (raw 제품명)*")
    for _, row in df.sort_values("rank").head(10).iterrows():
        delta = row.get("delta")
        delta_txt = "-"
        if pd.notna(delta):
            if delta > 0: delta_txt = f"↑{int(delta)}"
            elif delta < 0: delta_txt = f"↓{abs(int(delta))}"
            else: delta_txt = "-"
        price_txt = f"— ¥{row['price']:,}" if pd.notna(row['price']) else ""
        lines.append(f"{int(row['rank'])}위 | {row['name']}{price_txt}")

    payload = {"text": "\n".join(lines)}
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        r.raise_for_status()
        log("[INFO] Slack 전송 OK")
    except Exception as e:
        log(f"[WARN] Slack 전송 실패: {e}")

# ------------------- Google Drive 업로드 -------------------
def upload_to_gdrive(filepath, filename, folder_id):
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and folder_id):
        log("[INFO] 드라이브 업로드 생략(시크릿 미설정)")
        return
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        media = MediaFileUpload(filepath, mimetype="text/csv", resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        created = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        log(f"[INFO] 드라이브 업로드 OK: {filename} (id={created.get('id')})")
    except Exception as e:
        log(f"[ERROR] 드라이브 업로드 실패: {e}")
        traceback.print_exc()

# ------------------- 메인 수집 -------------------
def collect():
    # 대상 페이지 구성 (1-80, 81-160 …)
    pages = [BASE, BASE + "p=2/"]
    if MAX_RANK > 160:
        pages.append(BASE + "p=3/")
    if MAX_RANK > 240:
        pages.append(BASE + "p=4/")

    all_rows = []
    page_counts = []
    used_playwright = False

    for idx, url in enumerate(pages, start=1):
        log(f"[GET] {url}")
        html_raw = None
        ok = False

        # 1) ScraperAPI 렌더 우선
        try:
            html_raw = fetch_scraperapi(url, render=True, country="jp", wait_ms=4500, retry=1)
            open(f"data/debug/p{idx}_scrapi.html", "w", encoding="utf-8").write(html_raw or "")
            rows, visible = parse_page(html_raw, idx)
            page_counts.append(visible)
            all_rows.extend(rows)
            # 80 미만이면 폴백
            if visible < 60:
                raise RuntimeError(f"render snapshot too small: {visible}")
            ok = True
        except Exception as e:
            log(f"[WARN] ScraperAPI 렌더 실패/부족: {e}")

        # 2) 폴백: Playwright
        if not ok:
            try:
                used_playwright = True
                html_pw = render_with_playwright(url, max_wait=15000)
                open(f"data/debug/p{idx}_pw.html", "w", encoding="utf-8").write(html_pw or "")
                rows, visible = parse_page(html_pw, idx)
                page_counts[-1:] = [visible] if page_counts else [visible]
                all_rows.extend(rows)
            except Exception as e:
                log(f"[ERROR] Playwright 렌더 실패: {e}")

    # 정렬/중복제거
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["rank"]).sort_values("rank")
    # TopN 컷
    df = df[df["rank"] <= MAX_RANK].copy()
    log(f"[INFO] 수집 개수: {len(df)} (페이지 가시카드: {page_counts})")
    # 기대치 체크
    expected = min(MAX_RANK, 80 * len(pages))
    if len(df) < min(80, expected) // 2:
        log("[경고] 수집 수가 기대보다 너무 적습니다. data/debug/* 확인 바람.")

    # 파일 저장
    out_name = f"라쿠텐재팬_뷰티_랭킹_{TODAY}.csv"
    out_path = os.path.join("data", out_name)
    df[["rank","name","price","brand_guess","shop","url"]].to_csv(out_path, index=False, encoding="utf-8-sig")
    log(f"[INFO] CSV 저장: {out_path}")

    return df, out_path, out_name

def main():
    try:
        df, path, name = collect()

        # 슬랙
        title = f"Rakuten Japan · 뷰티 Top{min(MAX_RANK, len(df))} ({TODAY})"
        post_to_slack(title, df)

        # 구글드라이브 업로드
        upload_to_gdrive(path, name, GDRIVE_FOLDER_ID)

    except Exception as e:
        log(f"[오류] {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    log('[INFO] 라쿠텐 뷰티 랭킹 수집 시작')
    main()
