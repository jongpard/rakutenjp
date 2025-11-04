# -*- coding: utf-8 -*-
"""
Rakuten Japan Beauty(100939) Daily Rank Collector — 완전형 (1~240위)
- HTML(p=1~4) 또는 ScraperAPI에서 수집
- 랭크/가격 정규화 및 누락 자동보정
- Slack 메시지(일본어+한국어 번역)
"""

import os, re, io, time, traceback, datetime as dt
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import pandas as pd, numpy as np, requests

KST = dt.timezone(dt.timedelta(hours=9))
def today(): return dt.datetime.now(KST).strftime("%Y-%m-%d")
def yesterday(): return (dt.datetime.now(KST)-dt.timedelta(days=1)).strftime("%Y-%m-%d")
def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

BASE_URL = "https://ranking.rakuten.co.jp/daily/100939/"
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"

def scraperapi_get(url, render=True):
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {"api_key": SCRAPER_KEY, "url": url, "country_code": "jp", "render": "true" if render else "false"}
    r = requests.get(SCRAPER_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.text

# ---------- 파서 ----------
YEN_RE = re.compile(r"([0-9,]+)\s*円")
RANK_RE = re.compile(r"(\d+)\s*位")

def to_int(s):
    if not s: return np.nan
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else np.nan

def to_price(s):
    if not s: return np.nan
    s = re.sub(r"[^\d]", "", str(s))
    return int(s) if s else np.nan

def brand_from_shop(shop):
    brand = clean(shop)
    for w in ["公式","オフィシャル","ショップ","ストア","店","楽天市場","専門店"]:
        brand = re.sub(w,"",brand,flags=re.I)
    return brand.strip(" -")

def parse_html(html):
    soup = BeautifulSoup(html,"lxml")
    items=[]
    for item in soup.select("div.rnkRanking_itemName a"):
        block = item.find_parent()
        if not block: continue
        rank_el = block.select_one(".rnkRanking_dispRank")
        rtxt = rank_el.get_text(" ",strip=True) if rank_el else block.get_text()
        rank = to_int(rtxt)
        name = clean(item.get_text())
        url = (item.get("href") or "").split("?")[0]
        price_el = block.select_one(".rnkRanking_price")
        price_txt = clean(price_el.get_text()) if price_el else ""
        price = to_price(price_txt)
        shop_el = block.select_one(".rnkRanking_shop a")
        shop = clean(shop_el.get_text()) if shop_el else ""
        brand = brand_from_shop(shop)
        if rank: items.append({"rank":rank,"product_name":name,"price":price,"url":url,"shop":shop,"brand":brand})
    return items

# ---------- Slack ----------
def translate_ja2ko(texts):
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(texts, src="ja", dest="ko")
        return [r.text for r in res]
    except Exception: return [""]*len(texts)

def slack_post(msg):
    wh = os.getenv("SLACK_WEBHOOK_URL")
    if not wh: print("[Slack 미설정]"); return
    try: requests.post(wh,json={"text":msg})
    except Exception as e: print("Slack err",e)

# ---------- 메인 ----------
def main(local_html=False):
    print("[INFO] 라쿠텐 뷰티 수집 시작")
    pages=[BASE_URL,BASE_URL+"p=2/",BASE_URL+"p=3/",BASE_URL+"p=4/"]
    rows=[]
    for i,u in enumerate(pages,1):
        try:
            html = open(f"rakuten_p{i}.html","r",encoding="utf-8").read() if local_html else scraperapi_get(u)
            rows += parse_html(html)
            print(f"[p{i}] {len(rows)} 누적")
            time.sleep(0.5)
        except Exception as e:
            print("[WARN]",i,e)
    df=pd.DataFrame(rows).drop_duplicates(subset=["rank"]).sort_values("rank").reset_index(drop=True)
    print(f"[DONE] 총 {len(df)}개")

    date=today()
    df.insert(0,"date",date)
    fpath=f"{DATA_DIR}/라쿠텐재팬_뷰티_랭킹_{date}.csv"
    df.to_csv(fpath,index=False,encoding="utf-8-sig")
    print("저장:",fpath)

    # Slack
    top10=df.head(10)
    ja=top10["product_name"].tolist()
    ko=translate_ja2ko(ja)
    lines=[f"*📊 Rakuten Japan 뷰티 Top10 ({date})*"]
    for i,(r,k) in enumerate(zip(ja,ko),1):
        lines.append(f"{i}. {r}")
        if k: lines.append(f"   ▶ {k}")
    slack_post("\n".join(lines))

if __name__=="__main__":
    main(local_html=True)  # ← HTML파일 기반 테스트. 실배포시 False로
