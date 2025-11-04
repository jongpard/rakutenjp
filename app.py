# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty(100939) Daily Rank 1~160
- ScraperAPI(JP, render=true)로 p=1,2만 수집 (절대 160 넘지 않음)
- TOP3 + 이후 통합 파싱 (상품명 a 기준, 가까운 조상에서 랭크/가격/샵 추출)
- rank_int/price_int 정규화 → 누락/정렬/비교 안정화
- CSV 저장 + (옵션) Slack TOP10 (일본어+한국어 1줄)
"""

import os, re, io, time, traceback, datetime as dt
from typing import List, Dict, Optional
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# ---------------- 기본 설정 ----------------
KST = dt.timezone(dt.timedelta(hours=9))
def kst_now(): return dt.datetime.now(KST)
def today_str(): return kst_now().strftime("%Y-%m-%d")
def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

DATA_DIR = "data"; os.makedirs(DATA_DIR, exist_ok=True)

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))  # ← 꼭 160 유지
SAVE_DEBUG = os.getenv("RAKUTEN_SAVE_DEBUG", "1") in ("1","true","True")
DO_TRANSLATE = os.getenv("SLACK_TRANSLATE_JA2KO", "1") in ("1","true","True")

BASE = "https://ranking.rakuten.co.jp/daily/100939/"
PAGE_URLS = [BASE, BASE + "p=2/"]  # 1~80, 81~160만

FNAME = lambda d: f"라쿠텐재팬_뷰티_랭킹_{d}.csv"

# ---------------- ScraperAPI ----------------
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9,ko;q=0.8"
}

def scraper_get(url: str) -> str:
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPER_KEY,
        "url": url,
        "country_code": "jp",
        "render": "true",
        "retry_404": "true",
        "keep_headers": "true",
    }
    r = requests.get(SCRAPER_ENDPOINT, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text

# ---------------- 파싱 ----------------
YEN_RE = re.compile(r"([0-9,]+)\s*円")
RANK_TXT_RE = re.compile(r"(\d+)\s*位")
BRAND_STOPWORDS = [
    "楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営",
    "店","本店","支店","楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"
]

def brand_from_shop(shop: str) -> str:
    b = clean(shop)
    for w in BRAND_STOPWORDS:
        b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"[【】\[\]（）()]", "", b)
    return b.strip(" -_·|·")

def find_rank_in_block(block: BeautifulSoup) -> Optional[int]:
    # 1) 대표 클래스
    el = block.select_one(".rnkRanking_dispRank, .rank, .rnkRanking_rank")
    if el:
        m = RANK_TXT_RE.search(el.get_text(" ", strip=True) or "")
        if m: return int(m.group(1))
    # 2) 전체 텍스트
    txt = block.get_text(" ", strip=True)
    m = RANK_TXT_RE.search(txt or "")
    if m: return int(m.group(1))
    # 3) 이미지 alt
    img = block.select_one("img[alt*='位']")
    if img:
        m = RANK_TXT_RE.search(img.get("alt") or "")
        if m: return int(m.group(1))
    return None

def nearest_item_block(a: BeautifulSoup) -> Optional[BeautifulSoup]:
    cur = a
    for _ in range(10):
        if not cur: break
        if find_rank_in_block(cur) is not None:
            return cur
        cur = cur.parent
    return a.find_parent()

def parse_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []
    seen_ranks = set()

    for a in soup.select("div.rnkRanking_itemName a"):
        block = nearest_item_block(a)
        if not block: continue

        rank = find_rank_in_block(block)
        if not rank or rank in seen_ranks: 
            continue
        seen_ranks.add(rank)

        name = clean(a.get_text())
        href = re.sub(r"[?#].*$", "", (a.get("href") or "").strip())

        pr_el = block.select_one(".rnkRanking_price")
        pr_txt = clean(pr_el.get_text()) if pr_el else ""
        pm = YEN_RE.search(pr_txt); price = int(pm.group(1).replace(",", "")) if pm else np.nan

        shop_a = block.select_one(".rnkRanking_shop a")
        shop = clean(shop_a.get_text()) if shop_a else ""
        brand = brand_from_shop(shop)

        rows.append({"rank": rank, "product_name": name, "price": price,
                     "url": href, "shop": shop, "brand": brand})
    rows.sort(key=lambda r: r["rank"])
    return rows

def fetch_rank160() -> pd.DataFrame:
    merged: List[Dict] = []
    for i, url in enumerate(PAGE_URLS, 1):
        html = scraper_get(url)
        if SAVE_DEBUG:
            open(f"data/debug/rakuten_p{i}.html","w",encoding="utf-8").write(html)
        merged.extend(parse_page(html))
        time.sleep(0.5)
    df = pd.DataFrame(merged)
    return df

# ---------------- 정규화/안전 처리 ----------------
def extract_int_first(s):
    if pd.isna(s): return np.nan
    m = re.search(r"\d+", str(s)); return int(m.group()) if m else np.nan

def parse_price_val(s):
    if pd.isna(s): return np.nan
    ds = re.findall(r"\d+", str(s)); return int("".join(ds)) if ds else np.nan

def normalize_top160(df_raw: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if df_raw.empty: return df_raw
    df = df_raw.copy()
    df.insert(0, "date", date_str)
    df["rank_int"]  = df["rank"].apply(extract_int_first)
    df["price_int"] = df["price"].apply(parse_price_val)

    # 유효 랭크만 (1~160), 랭크 기준 유니크
    df = df[df["rank_int"].between(1, 160, inclusive="both")]
    df = df.sort_values("rank_int").drop_duplicates(subset=["rank_int"], keep="first")
    # 혹시 더 많아도 절대 160 넘기지 않음
    df = df.head(160).reset_index(drop=True)

    # 최종 CSV 포맷
    out = df[["date","rank_int","product_name","price_int","url","shop","brand"]].rename(
        columns={"rank_int":"rank", "price_int":"price"}
    )
    return out

# ---------------- 번역/슬랙 ----------------
def translate_ja2ko_batch(texts: List[str]) -> List[str]:
    if not DO_TRANSLATE or not texts: return ["" for _ in texts]
    # 1차 googletrans
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(texts, src="ja", dest="ko")
        return [getattr(r, "text", "") or "" for r in (res if isinstance(res, list) else [res])]
    except Exception as e:
        print("[번역 경고] googletrans 실패:", e)
    # 2차 deep-translator
    try:
        from deep_translator import GoogleTranslator
        gt = GoogleTranslator(source="ja", target="ko")
        return [gt.translate(t) if t else "" for t in texts]
    except Exception as e2:
        print("[번역 경고] deep-translator 실패:", e2)
        return ["" for _ in texts]

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[Slack 미설정] 생략"); return
    try:
        r = requests.post(url, json={"text": text}, timeout=25)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:300])
    except Exception as e:
        print("[Slack 예외]", e)

def build_slack_message(df_today: pd.DataFrame, date_str: str) -> str:
    lines = [f"*Rakuten Japan 뷰티 랭킹 160 — {date_str}*", "", "*TOP 10*"]
    t10 = df_today.head(10)
    ja = t10["product_name"].astype(str).tolist()
    ko = translate_ja2ko_batch(ja)
    for i, (_, r) in enumerate(t10.iterrows()):
        link = f"<{r['url']}|{slack_escape(r['product_name'])}>"
        price = f"￥{int(r['price']):,}" if pd.notnull(r['price']) else "￥0"
        lines.append(f"{int(r['rank'])}. {link} — {price}")
        if ko[i]: lines.append(f"    ▶ {slack_escape(ko[i])}")
    return "\n".join(lines)

# ---------------- 메인 ----------------
def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    raw = fetch_rank160()
    print(f"[INFO] 원시 수집: {len(raw)} rows")

    date_s = today_str()
    top160 = normalize_top160(raw, date_s)
    print(f"[INFO] 정규화 후: {len(top160)} rows (<=160)")

    # CSV 저장
    fname = FNAME(date_s)
    top160.to_csv(os.path.join(DATA_DIR, fname), index=False, encoding="utf-8-sig")
    print("[INFO] CSV 저장:", fname)

    # Slack (옵션)
    msg = build_slack_message(top160, date_s)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try: slack_post(f"*라쿠텐 수집 실패*\n```\n{e}\n```")
        except: pass
        raise
