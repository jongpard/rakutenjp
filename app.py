# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty(100939) Rank 1~160
- 1차: ScraperAPI render=true
- 2차: ScraperAPI render=false (보강)
- 두 패스 병합 → 빈 랭크 보강(특히 TOP3/중간 누락)
- 정규화(rank_int/price_int) 후 1~160 고정
- CSV + Slack(TOP10 ja+ko)
"""

import os, re, io, time, traceback, datetime as dt
from typing import List, Dict, Optional
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# ---------------- 공통/경로 ----------------
KST = dt.timezone(dt.timedelta(hours=9))
def kst_now(): return dt.datetime.now(KST)
def today_str(): return kst_now().strftime("%Y-%m-%d")
def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

DATA_DIR = "data"
DBG_DIR = os.path.join(DATA_DIR, "debug")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

MAX_RANK = 160  # 고정
SAVE_DEBUG = os.getenv("RAKUTEN_SAVE_DEBUG", "1") in ("1","true","True")
DO_TRANSLATE = os.getenv("SLACK_TRANSLATE_JA2KO", "1") in ("1","true","True")

BASE = "https://ranking.rakuten.co.jp/daily/100939/"
PAGE_URLS = [BASE, BASE + "p=2/"]  # 딱 1~160만

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

def scraper_get(url: str, render: bool) -> str:
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPER_KEY,
        "url": url,
        "country_code": "jp",
        "render": "true" if render else "false",
        "retry_404": "true",
        "keep_headers": "true",
    }
    r = requests.get(SCRAPER_ENDPOINT, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text

# ---------------- 파싱 ----------------
YEN_RE       = re.compile(r"([0-9,]+)\s*円")
RANK_TXT_RE  = re.compile(r"(\d+)\s*位")
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

def fetch_pass(render_flag: bool, pass_tag: str) -> pd.DataFrame:
    merged: List[Dict] = []
    for idx, url in enumerate(PAGE_URLS, 1):
        html = scraper_get(url, render=render_flag)
        if SAVE_DEBUG:
            open(os.path.join(DBG_DIR, f"rakuten_p{idx}_{pass_tag}.html"), "w", encoding="utf-8").write(html)
        merged.extend(parse_page(html))
        time.sleep(0.5)
    return pd.DataFrame(merged)

# ---------------- 정규화/병합 ----------------
def extract_int_first(s):
    if pd.isna(s): return np.nan
    m = re.search(r"\d+", str(s)); return int(m.group()) if m else np.nan

def parse_price_val(s):
    if pd.isna(s): return np.nan
    ds = re.findall(r"\d+", str(s)); return int("".join(ds)) if ds else np.nan

def normalize_df(df_raw: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if df_raw.empty: return df_raw
    df = df_raw.copy()
    df.insert(0, "date", date_str)
    df["rank_int"]  = df["rank"].apply(extract_int_first)
    df["price_int"] = df["price"].apply(parse_price_val)
    # 1~160만, 랭크 유니크
    df = df[df["rank_int"].between(1, 160, inclusive="both")]
    df = df.sort_values("rank_int").drop_duplicates(subset=["rank_int"], keep="first")
    return df

def merge_best(primary: pd.DataFrame, backup: pd.DataFrame) -> pd.DataFrame:
    """
    primary 우선, 부족한 랭크는 backup에서 보충
    """
    if primary is None or primary.empty:
        return backup.copy()
    if backup is None or backup.empty:
        return primary.copy()

    A = primary.set_index("rank_int", drop=False)
    B = backup.set_index("rank_int", drop=False)

    ranks = list(range(1, 161))
    picked = []
    for r in ranks:
        if r in A.index:
            picked.append(A.loc[r])
        elif r in B.index:
            picked.append(B.loc[r])
    out = pd.DataFrame(picked)
    out = out.reset_index(drop=True)
    # 혹시 모를 중복 제거 (rank_int 기준)
    out = out.drop_duplicates(subset=["rank_int"], keep="first").sort_values("rank_int")
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
    # 1차: render=true
    p1 = fetch_pass(render_flag=True, pass_tag="r1")
    p1 = normalize_df(p1, today_str())

    # 2차: render=false (보강)
    p0 = fetch_pass(render_flag=False, pass_tag="r0")
    p0 = normalize_df(p0, today_str())

    # 병합 (primary 우선)
    merged = merge_best(p1, p0)

    # 최종 1~160 고정
    merged = merged[merged["rank_int"].between(1,160, inclusive="both")]
    merged = merged.sort_values("rank_int").drop_duplicates(subset=["rank_int"], keep="first").head(160)
    # 최종 CSV 포맷
    out = merged[["date","rank_int","product_name","price_int","url","shop","brand"]].rename(
        columns={"rank_int":"rank","price_int":"price"}
    ).reset_index(drop=True)

    print(f"[INFO] 최종 건수: {len(out)} (<=160)")
    fname = FNAME(today_str())
    out.to_csv(os.path.join(DATA_DIR, fname), index=False, encoding="utf-8-sig")
    print("[INFO] CSV 저장:", fname)

    # Slack
    msg = build_slack_message(out, today_str())
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
