import os, time, math, json, requests
from bs4 import BeautifulSoup

BASE = "https://ranking.rakuten.co.jp/daily/100939/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
}

def fetch_page(page:int=1) -> BeautifulSoup:
    url = BASE if page == 1 else f"{BASE}p={page}/"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_items(soup: BeautifulSoup):
    items = []
    # 모든 랭크 블록에서 공통적으로 사용되는 두 요소:
    # - 순위: .rnkRanking_dispRank
    # - 상품명 링크: .rnkRanking_itemName a
    for name_a in soup.select("div.rnkRanking_itemName a"):
        # 가장 가까운 컨테이너에서 순위/가격/샵/리뷰를 찾음
        container = name_a.find_parent().find_parent()  # itemName -> upperbox -> 그 위
        # 안전장치: 상위로 넉넉히 탐색
        for _ in range(5):
            if container and container.select_one(".rnkRanking_dispRank"):
                break
            container = container.parent if container else None
        if not container:
            continue

        rank_tag = container.select_one(".rnkRanking_dispRank")
        price_tag = container.select_one(".rnkRanking_price")
        shop_a = container.select_one(".rnkRanking_shop a")

        rank_txt = rank_tag.get_text(strip=True) if rank_tag else ""
        # "81位" 처럼 들어오므로 숫자만 추출
        rank = int("".join([c for c in rank_txt if c.isdigit()])) if rank_txt else None

        items.append({
            "rank": rank,
            "title_ja": name_a.get_text(strip=True),
            "url": name_a["href"],
            "price": price_tag.get_text(strip=True) if price_tag else "",
            "shop": shop_a.get_text(strip=True) if shop_a else "",
        })
    # 랭크 기준 정렬 및 중복 제거
    dedup = {it["rank"]: it for it in items if it["rank"] is not None}
    return [dedup[k] for k in sorted(dedup.keys())]

def collect_top(n_items=160, max_pages=13):
    results = []
    page = 1
    while len(results) < n_items and page <= max_pages:
        soup = fetch_page(page)
        page_items = parse_items(soup)
        results.extend([it for it in page_items if it["rank"] not in {x["rank"] for x in results}])
        page += 1
        time.sleep(0.5)  # 예의상 살짝 딜레이
    # 원하는 개수만
    results = sorted(results, key=lambda x: x["rank"])[:n_items]
    return results

# --- 번역 (DeepL 또는 Google Cloud, 없으면 원문 유지) ---
import requests

def translate_ja_to_ko(texts):
    deepl_key = os.getenv("DEEPL_API_KEY")
    gcloud_key = os.getenv("GOOGLE_API_KEY")
    if deepl_key:
        url = "https://api-free.deepl.com/v2/translate"
        data = []
        for t in texts:
            data.append(("text", t))
        resp = requests.post(url, data=data + [("target_lang","KO"),("source_lang","JA")],
                             headers={"Authorization": f"DeepL-Auth-Key {deepl_key}"}, timeout=20)
        resp.raise_for_status()
        return [tr["text"] for tr in resp.json()["translations"]]
    elif gcloud_key:
        url = f"https://translation.googleapis.com/language/translate/v2?key={gcloud_key}"
        payload = {"q": texts, "source":"ja", "target":"ko", "format":"text"}
        resp = requests.post(url, json=payload, timeout=20)
        resp.raise_for_status()
        return [tr["translatedText"] for tr in resp.json()["data"]["translations"]]
    else:
        # 키 없으면 그대로 반환
        return texts

# --- 슬랙 전송(웹훅) ---
def post_to_slack(items, webhook_url, title="Rakuten Japan 뷰티 랭킹"):
    # 상위 10개만 본문에 표시 + 나머지는 요약
    top = items[:10]
    rest_count = max(0, len(items)-10)
    # 번역 준비
    to_translate = [f'{it["title_ja"]}' for it in top]
    ko = translate_ja_to_ko(to_translate)

    lines = [f"*{title}*"]
    for i, it in enumerate(top):
        line = f'{it["rank"]}. {it["title_ja"]}\n   ▶ {ko[i]}\n   💴 {it["price"]} | 🏬 {it["shop"]} | <{it["url"]}|상품링크>'
        lines.append(line)
    if rest_count:
        lines.append(f"… 그리고 {rest_count}개 항목 더 수집됨.")

    payload = {"text": "\n".join(lines)}
    r = requests.post(webhook_url, json=payload, timeout=15)
    r.raise_for_status()

if __name__ == "__main__":
    # 원하는 개수만큼 수집 (예: 1~160위)
    items = collect_top(n_items=160)
    # CSV 저장 예시
    import csv, datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d")
    fname = f"rakuten_beauty_{ts}.csv"
    with open(fname, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank","title_ja","price","shop","url"])
        w.writeheader()
        w.writerows(items)

    # 슬랙 전송 (환경변수 SLACK_WEBHOOK_URL 사용)
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        post_to_slack(items, webhook_url=webhook, title=f"Rakuten Japan 뷰티 랭킹 {ts}")
    print(f"[INFO] 수집 완료: {len(items)}개, 파일: {fname}")
