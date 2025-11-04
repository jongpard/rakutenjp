import csv, re, sys, time, datetime as dt
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

BASE = "https://ranking.rakuten.co.jp"
CATEGORY_ID = "100939"            # 美容・コスメ・香水
MAX_ITEMS = 200
TIMEOUT = 25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

def clean_price(text: str) -> int | None:
    if not text: return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

# 상점명에서 브랜드 추출(가볍게) — 너무 공격적으로 지우지 않음
BRAND_STOPWORDS = [
    "楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営",
    "店","本店","支店","楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"
]
def brand_from_shop(shop: str) -> str:
    if not shop: return ""
    b = shop.strip()
    for w in BRAND_STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    # 괄호·공백 정리
    b = re.sub(r"[【】\[\]（）()]", "", b)
    b = re.sub(r"\s{2,}", " ", b).strip(" -_·|·")
    return b.strip()

def parse_items(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for box in soup.select("div.rnkRanking_after4box"):
        # rank
        rank_el = box.select_one(".rnkRanking_dispRank")
        if not rank_el: 
            continue
        rank_txt = rank_el.get_text(strip=True)
        rank = int(re.sub(r"[^\d]", "", rank_txt)) if rank_txt else None

        # name + url
        name_a = box.select_one(".rnkRanking_itemName a")
        name = name_a.get_text(strip=True) if name_a else ""
        url  = name_a["href"] if name_a and name_a.has_attr("href") else ""

        # price
        price_el = box.select_one(".rnkRanking_price")
        price = clean_price(price_el.get_text()) if price_el else None

        # shop & brand
        shop_a = box.select_one(".rnkRanking_shop a")
        shop = shop_a.get_text(strip=True) if shop_a else ""
        brand = brand_from_shop(shop)

        items.append({
            "rank": rank,
            "name": name,
            "price": price,
            "url": url if url.startswith("http") else urljoin(BASE, url),
            "shop": shop,
            "brand": brand,
        })
    return items

def fetch_page(page:int) -> str:
    # p=1은 파라미터 없이 접근 (라쿠텐 구조)
    url = f"{BASE}/daily/{CATEGORY_ID}/" if page==1 else f"{BASE}/daily/{CATEGORY_ID}/p={page}/"
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.text

def collect_top200() -> list[dict]:
    all_rows: list[dict] = []
    page = 1
    while len(all_rows) < MAX_ITEMS:
        html = fetch_page(page)
        rows = parse_items(html)
        if not rows:
            # 더 이상 파싱 안 되면 중단
            break
        all_rows.extend(rows)
        page += 1
        # 과한 요청 방지
        time.sleep(0.8)
        # 400위까지 페이지가 있으므로 안전장치
        if page > 6: 
            break
    # 정렬·상한
    all_rows = [r for r in all_rows if r.get("rank")]
    all_rows.sort(key=lambda r: r["rank"])
    return all_rows[:MAX_ITEMS]

def save_csv(rows:list[dict]) -> str:
    kst_today = dt.datetime.utcnow() + dt.timedelta(hours=9)
    fname = f"라쿠텐재팬_뷰티_랭킹_{kst_today.strftime('%Y-%m-%d')}.csv"
    fields = ["rank","name","price","url","shop","brand"]
    with open(fname, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return fname

def main():
    print('[INFO] 라쿠텐 뷰티 랭킹 수집 시작')
    rows = collect_top200()
    print(f'[INFO] 수집 개수: {len(rows)}')
    if not rows:
        print('[ERROR] 파싱 0건 — 선택자 확인 필요')
        sys.exit(1)
    csv_path = save_csv(rows)
    print(f'[INFO] CSV 저장: {csv_path}')
    # === 여기서부터는 네가 이미 쓰는 업로드/슬랙 로직 호출 ===
    # upload_to_gdrive(csv_path)  # 기존 함수 유지
    # post_to_slack(...)          # 기존 함수 유지

if __name__ == "__main__":
    main()
