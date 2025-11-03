# -*- coding: utf-8 -*-
import os, re, time, datetime as dt
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ===== ScraperAPI =====
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
if not SCRAPERAPI_KEY:
    raise RuntimeError("SCRAPERAPI_KEY 시크릿이 필요합니다.")
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"
COMMON_PARAMS = {
    "api_key": SCRAPERAPI_KEY,
    "country_code": "jp",
    "render": "true",
    "retry_404": "true",
    "keep_headers": "true",
    "device_type": "desktop",
    "session_number": "rakutenjp-1",
    # 렌더가 준비될 때까지 대기(카드가 나타날 때까지)
    "wait_for": "div.rnkRanking_after4box,div.rnkRanking_box,li.rnkRanking_item,li.rnkRankingList__item",
    "wait_time": "4000",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
}

# ===== Paths =====
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DBG_DIR  = os.path.join(DATA_DIR, "debug")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

# ===== Ranking URLs =====
BASE = "https://ranking.rakuten.co.jp"
CAT = "100939"
URLS = [f"{BASE}/daily/{CAT}/", f"{BASE}/daily/{CAT}/p=2/"]
MAX_ITEMS = int(os.getenv("RAKUTEN_MAX_RANK", "160"))

# ===== Slack =====
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
def slack_post(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack 미설정] 메시지 생략")
        return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:200])
    except Exception as e:
        print("[Slack 예외]", e)

def kst_today_str():
    return (dt.datetime.utcnow() + dt.timedelta(hours=9)).strftime("%Y-%m-%d")

def scraper_get(url: str, dbg_name: str) -> str:
    params = dict(COMMON_PARAMS)
    params["url"] = url
    for _ in range(2):
        r = requests.get(SCRAPER_ENDPOINT, params=params, headers=HEADERS, timeout=60)
        if r.status_code == 200 and len(r.text) > 2000:
            html = r.text
            open(os.path.join(DBG_DIR, f"{dbg_name}.html"), "w", encoding="utf-8").write(html)
            return html
        time.sleep(1.2)
    r.raise_for_status()
    return r.text

CARD_SEL = ",".join([
    "div.rnkRanking_after4box",
    "div.rnkRanking_box",
    "li.rnkRanking_item",
    "li.rnkRankingList__item",
])
RANK_SELS  = [".rnkRanking_dispRank",".rnkRanking_rank","[class*='Rank']","[class*='rank']"]
NAME_SELS  = [".rnkRanking_itemName a","a.rnkRanking_itemName",".itemName a","a[href*='item.rakuten.co.jp']", "a"]
PRICE_SELS = [".rnkRanking_price",".price","[class*='price']"]
SHOP_SELS  = [".rnkRanking_shop a",".rnkRanking_shop","[class*='shop'] a"]

STOPWORDS = ["楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営","店","本店","支店",
             "楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"]

def brand_from_shop(shop: str) -> str:
    if not shop: return ""
    b = shop
    for w in STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"[【】\[\]（）()]", "", b)
    b = re.sub(r"\s{2,}", " ", b).strip(" -_·|·")
    return b.strip()

def pick_one(el, sels):
    for s in sels:
        f = el.select_one(s)
        if f: return f
    return None

def clean_price(txt: str):
    if not txt: return None
    d = re.sub(r"[^\d]", "", txt)
    return int(d) if d else None

def parse_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for card in soup.select(CARD_SEL):
        rk_el = pick_one(card, RANK_SELS)
        if not rk_el: continue
        m = re.search(r"\d+", rk_el.get_text(strip=True))
        if not m: continue
        rank = int(m.group())

        a = pick_one(card, NAME_SELS)
        name = a.get_text(strip=True) if a else ""
        href = a.get("href") if a and a.has_attr("href") else ""
        if href and href.startswith("/"): href = urljoin(BASE, href)

        pr_el = pick_one(card, PRICE_SELS)
        price = clean_price(pr_el.get_text(strip=True)) if pr_el else None

        sh = pick_one(card, SHOP_SELS)
        shop = sh.get_text(strip=True) if sh else ""
        brand = brand_from_shop(shop)

        if name:
            rows.append({"rank": rank, "name": name, "price": price, "url": href, "shop": shop, "brand": brand})
    return rows

def collect() -> list[dict]:
    all_rows = []
    for i, url in enumerate(URLS, start=1):
        print(f"[GET] {url}")
        html = scraper_get(url, dbg_name=f"p{i}")
        rows = parse_page(html)
        print(f"[parse] p{i}: {len(rows)}")
        all_rows.extend(rows)
        time.sleep(0.8)

    all_rows = [r for r in all_rows if r.get("rank")]
    all_rows.sort(key=lambda r: r["rank"])
    # 동일 rank 중복 가드
    uniq = {}
    for r in all_rows: uniq[r["rank"]] = r
    return [uniq[k] for k in sorted(uniq.keys())][:MAX_ITEMS]

def save_csv(rows: list[dict]) -> str:
    path = os.path.join(DATA_DIR, f"라쿠텐재팬_뷰티_랭킹_{kst_today_str()}.csv")
    pd.DataFrame(rows, columns=["rank","name","price","url","shop","brand"]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )
    return path

def build_slack(rows: list[dict]) -> str:
    lines = [f"*Rakuten Japan · 뷰티 Top{len(rows)} ({kst_today_str()})*",
             "", "*🏆 Top10 (raw 제품명)*"]
    for r in rows[:10]:
        price = f"￥{r['price']:,}" if r.get("price") else "￥0"
        lines.append(f"{r['rank']:>3}위 | - | {r['name']} — {price}")
    lines += ["", "*↔ 랭크 인&아웃*", "0개의 제품이 인&아웃 되었습니다."]
    return "\n".join(lines)

def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    rows = collect()
    print(f"[INFO] 수집 개수: {len(rows)}")
    if len(rows) < 80:
        print("[경고] 수집 수가 80 미만입니다. data/debug/p*.html로 DOM 확인 바랍니다.")

    csv_path = save_csv(rows)
    print("[INFO] CSV 저장:", csv_path)

    slack_post(build_slack(rows))
    print("[INFO] Slack 전송 OK (미설정이면 생략)")

if __name__ == "__main__":
    main()
