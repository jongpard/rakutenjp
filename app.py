# app.py — Rakuten JP Beauty Daily Top200 via ScraperAPI (JP, render)
import os, re, time, datetime as dt
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://ranking.rakuten.co.jp"
CATEGORY_ID = "100939"               # 美容・コスメ・香水
MAX_ITEMS = 200
TIMEOUT = 60

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
if not SCRAPERAPI_KEY:
    raise RuntimeError("SCRAPERAPI_KEY 시크릿이 필요합니다.")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
}

def scraper_get(url: str, render: bool = True) -> str:
    """ScraperAPI(일본, 렌더링)로 HTML 가져오기 + 간단 재시도."""
    params = {
        "api_key": SCRAPERAPI_KEY,
        "url": url,
        "country_code": "jp",
        "render": "true" if render else "false",
        "retry_404": "true",
        "keep_headers": "true",
    }
    for i in range(3):
        r = requests.get("https://api.scraperapi.com/", params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200 and len(r.text) > 1000:
            return r.text
        time.sleep(1.2)
    r.raise_for_status()  # 마지막 응답 에러 내보내기
    return r.text

def page_url(page:int) -> str:
    return f"{BASE}/daily/{CATEGORY_ID}/" if page == 1 else f"{BASE}/daily/{CATEGORY_ID}/p={page}/"

def clean_price(txt: str):
    if not txt: return None
    d = re.sub(r"[^\d]", "", txt)
    return int(d) if d else None

BRAND_STOPWORDS = [
    "楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営","店","本店","支店",
    "楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"
]
def brand_from_shop(shop: str) -> str:
    if not shop: return ""
    b = shop
    for w in BRAND_STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"[【】\[\]（）()]", "", b)
    b = re.sub(r"\s{2,}", " ", b).strip(" -_·|·")
    return b.strip()

def parse_items(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    cards = soup.select("div.rnkRanking_after4box")
    for c in cards:
        # rank
        rk_el = c.select_one(".rnkRanking_dispRank")
        if not rk_el: 
            continue
        rk_txt = rk_el.get_text(strip=True)
        m = re.search(r"\d+", rk_txt)
        if not m: 
            continue
        rank = int(m.group())

        # name & url
        a = c.select_one(".rnkRanking_itemName a")
        name = a.get_text(strip=True) if a else ""
        href = a["href"] if a and a.has_attr("href") else ""
        if href and href.startswith("/"):
            href = urljoin(BASE, href)

        # price
        pr_el = c.select_one(".rnkRanking_price")
        price = clean_price(pr_el.get_text(strip=True)) if pr_el else None

        # shop & brand
        shop_a = c.select_one(".rnkRanking_shop a")
        shop = shop_a.get_text(strip=True) if shop_a else ""
        brand = brand_from_shop(shop)

        rows.append({
            "rank": rank,
            "name": name,
            "price": price,
            "url": href,
            "shop": shop,
            "brand": brand,
        })
    return rows

def collect_top200() -> list[dict]:
    all_rows: list[dict] = []
    page = 1
    while len(all_rows) < MAX_ITEMS and page <= 3:  # p1~p3까지만(최대 240위)
        url = page_url(page)
        print(f"[GET] {url}")
        html = scraper_get(url, render=True)  # ← 403 우회 핵심
        # 디버그 저장(원하면 주석 해제)
        # open(f"data_debug_p{page}.html", "w", encoding="utf-8").write(html)

        rows = parse_items(html)
        print(f"[parse] p{page}: {len(rows)}")
        if not rows:
            break
        all_rows.extend(rows)
        page += 1
        time.sleep(0.8)

    # 정렬/상한
    all_rows = [r for r in all_rows if r.get("rank")]
    all_rows.sort(key=lambda r: r["rank"])
    return all_rows[:MAX_ITEMS]

def save_csv(rows: list[dict]) -> str:
    kst = dt.datetime.utcnow() + dt.timedelta(hours=9)
    fname = f"라쿠텐재팬_뷰티_랭킹_{kst.strftime('%Y-%m-%d')}.csv"
    df = pd.DataFrame(rows, columns=["rank","name","price","url","shop","brand"])
    df.to_csv(fname, index=False, encoding="utf-8-sig")
    return fname

def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    rows = collect_top200()
    print(f"[INFO] 수집 개수: {len(rows)}")
    if not rows:
        raise RuntimeError("파싱 0건 — 렌더링/크레딧/셀렉터 확인 필요")
    csv_path = save_csv(rows)
    print("[INFO] CSV 저장:", csv_path)

if __name__ == "__main__":
    main()
