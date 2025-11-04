# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty (100939) Daily Rank 1~160
- 페이지 1(1~80) + 페이지 2(81~160)만 수집 → 절대 160 초과 X
- Playwright로 렌더/스크롤 대기(다중 셀렉터, 재시도, 혼잡 화면 감지)
- 실패 시 ScraperAPI render=true → 부족 랭크는 render=false로 보강
- 가격은 '...円'만 집계(리뷰/개수 숫자 미포함)
- Slack TOP10: 일본어 + 한국어 1줄 (googletrans → deep_translator 폴백)
- 디버그 HTML: data/debug/rakuten_p{1,2}_{pw|r1|r0}.html 저장(옵션)
"""

import os, re, io, time, math, traceback, datetime as dt
from typing import List, Dict, Optional, Tuple
import requests
import pandas as pd
import numpy as np
from dataclasses import dataclass
from bs4 import BeautifulSoup

# ----------------------------- 기본/경로 -----------------------------
KST = dt.timezone(dt.timedelta(hours=9))
def today_str(): return dt.datetime.now(KST).strftime("%Y-%m-%d")
def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

DATA_DIR = "data"
DBG_DIR  = os.path.join(DATA_DIR, "debug")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

MAX_RANK = 160  # 요구사항 고정
SAVE_DEBUG = os.getenv("RAKUTEN_SAVE_DEBUG", "1").lower() in ("1","true","yes")

BASE_P1 = "https://ranking.rakuten.co.jp/daily/100939/"
BASE_P2 = "https://ranking.rakuten.co.jp/daily/100939/p=2/"
PAGES = [(BASE_P1, 80), (BASE_P2, 80)]

# ----------------------------- 텍스트/가격 -----------------------------
OFFICIAL_PAT   = re.compile(r"^\s*(公式|公式ショップ|公式ストア)\s*", re.I)
BRACKETS_PAT   = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
YEN_AMOUNT_RE  = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
JP_CHAR_RE     = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")

def remove_official(s: str) -> str:
    return OFFICIAL_PAT.sub("", clean(s or ""))

def contains_ja(s: str) -> bool:
    return bool(JP_CHAR_RE.search(s or ""))

def parse_yen_block(txt: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    amounts = [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(txt or "") if m.group(1) != "0"]
    sale = min(amounts) if amounts else None
    orig = max(amounts) if len(amounts) >= 2 else None
    pct  = None
    if sale and orig and orig > sale:
        pct = int(math.floor((1 - sale/orig) * 100))
    return sale, orig, pct

# ----------------------------- 번역 (Qoo10 폴백) -----------------------------
def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    if os.getenv("SLACK_TRANSLATE_JA2KO", "1").lower() not in ("1","true","yes"):
        print("[번역] OFF"); return ["" for _ in lines]
    texts = [clean(x) for x in lines]
    ja = [t for t in texts if contains_ja(t)]
    if not ja: return ["" for _ in texts]

    trans = {}
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(ja, src="ja", dest="ko")
        outs = [getattr(r, "text", "") or "" for r in (res if isinstance(res, list) else [res])]
        trans = dict(zip(ja, outs))
    except Exception as e:
        print("[번역 경고] googletrans 실패:", e)
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source='ja', target='ko')
            trans = {t: gt.translate(t) for t in ja}
        except Exception as e2:
            print("[번역 경고] deep_translator 실패:", e2)
            return ["" for _ in texts]

    return [trans.get(t, "") if contains_ja(t) else "" for t in texts]

# ----------------------------- Slack -----------------------------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        print("[Slack 미설정] 메시지 미전송\n", text)
        return
    try:
        r = requests.post(url, json={"text": text}, timeout=25)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:300])
    except Exception as e:
        print("[Slack 예외]", e)

# ----------------------------- Playwright 수집 -----------------------------
MAIN_SELECTORS = [
    "#rnkRankingMain", ".rnkRankingMain", ".rnkRanking_box",
    ".rnkRanking_list", ".rnkRanking_itemName"
]

def _any_selector(page) -> str:
    for s in MAIN_SELECTORS:
        try:
            if page.query_selector(s): return s
        except: pass
    return ""

def _render_and_collect(url: str, expect: int) -> List[Dict]:
    """다중 셀렉터 대기 + 스크롤 로딩 + 혼잡 화면 재시도."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 1000},
            locale="ja-JP", timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,ko;q=0.8"},
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()

        last_err = None
        for attempt in range(3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try: page.wait_for_load_state("networkidle", timeout=30_000)
                except PWTimeout: pass

                # 혼잡 화면이면 리로드
                if "アクセスが集中" in (page.content()[:6000]):
                    time.sleep(3)
                    page.reload(wait_until="domcontentloaded", timeout=60_000)
                    try: page.wait_for_load_state("networkidle", timeout=20_000)
                    except PWTimeout: pass

                # 다중 셀렉터 대기(최대 60s)
                found = ""
                deadline = time.time() + 60
                while time.time() < deadline and not found:
                    found = _any_selector(page)
                    if not found: time.sleep(0.5)
                if not found:
                    raise PWTimeout(f"ranking container not found (tried {MAIN_SELECTORS})")

                # 충분히 로드될 때까지 스크롤(최대 45s)
                t0 = time.time(); last = -1
                while True:
                    n = page.eval_on_selector_all(
                        "a[href*='item.rakuten.co.jp/'], a[href*='/item/']",
                        "els => els.length"
                    )
                    if n >= expect: break
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(0.7)
                    if n == last: time.sleep(0.7)
                    last = n
                    if time.time() - t0 > 45: break

                # 카드 추출
                data = page.evaluate("""
                    () => {
                      const rows = [];
                      const cards = Array.from(document.querySelectorAll('li, .rnkRanking_item'));
                      for (const el of cards) {
                        // rank
                        let r = 0;
                        const rk = el.querySelector('.rankNo, .rnkRankBadge, .rnkRanking_rank, .rank, .rnkRanking_dispRank');
                        if (rk) {
                          const m = (rk.textContent||'').match(/\\d+/); if (m) r = parseInt(m[0],10);
                        }
                        const a = el.querySelector('a[href*="item.rakuten.co.jp/"], a[href*="/item/"]');
                        if (!a) continue;
                        const href = a.href;
                        const name = (a.textContent||'').replace(/\\s+/g,' ').trim();
                        let shop = '';
                        const shopEl = el.querySelector('.rnkRanking_shop, .rnkTop_shop, .shop, .rnkRanking_shop a');
                        if (shopEl) shop = (shopEl.textContent||'').replace(/\\s+/g,' ').trim();
                        const block = (el.innerText||'').replace(/\\s+/g,' ').trim();
                        rows.push({rank:r, href, name, shop, block});
                      }
                      return rows.filter(x => x.rank > 0);
                    }
                """)
                if SAVE_DEBUG:
                    open(os.path.join(DBG_DIR, f"rakuten_{'p1' if url.endswith('/100939/') else 'p2'}_pw.html"),
                         "w", encoding="utf-8").write(page.content())
                ctx.close(); browser.close()
                return data
            except Exception as e:
                last_err = e
                time.sleep(2)
                try: page.close()
                except: pass
                page = ctx.new_page()

        ctx.close(); browser.close()
        if last_err: raise last_err
        return []
# ----------------------------- ScraperAPI 폴백 -----------------------------
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"

def scraperapi_get(url: str, render: bool=True) -> str:
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPER_KEY, "url": url, "country_code": "jp",
        "render": "true" if render else "false", "retry_404": "true"
    }
    r = requests.get(SCRAPER_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.text

def parse_bs(html: str, tag: str) -> List[Dict]:
    if SAVE_DEBUG:
        open(os.path.join(DBG_DIR, tag), "w", encoding="utf-8").write(html)
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for el in soup.select("li, .rnkRanking_item"):
        rk = el.select_one(".rankNo, .rnkRankBadge, .rnkRanking_rank, .rank, .rnkRanking_dispRank")
        if not rk: continue
        m = re.search(r"\d+", rk.get_text(" ", strip=True))
        if not m: continue
        rank = int(m.group())
        a = el.select_one("a[href*='item.rakuten.co.jp/'], a[href*='/item/']")
        if not a: continue
        href = a.get("href", "")
        name = clean(a.get_text())
        shop_el = el.select_one(".rnkRanking_shop, .rnkTop_shop, .shop, .rnkRanking_shop a")
        shop = clean(shop_el.get_text()) if shop_el else ""
        block = clean(el.get_text(" ", strip=True))
        rows.append({"rank":rank,"href":href,"name":name,"shop":shop,"block":block})
    rows.sort(key=lambda x: x["rank"])
    return rows

# ----------------------------- 수집/정규화 -----------------------------
@dataclass
class Item:
    rank: int
    brand: str
    product_name: str
    price: Optional[int]
    orig_price: Optional[int]
    discount_percent: Optional[int]
    url: str
    shop: str

def fetch_rakuten_top160() -> List[Item]:
    rows: List[Dict] = []

    # 1) Playwright 우선
    try:
        for url, expect in PAGES:
            part = _render_and_collect(url, expect)
            part = sorted(part, key=lambda x: x["rank"])[:80]   # 과다 수집 방지
            rows.extend(part)
    except Exception as e:
        print("[WARN] Playwright 실패, ScraperAPI 폴백:", e)
        tmp = []
        # 2) ScraperAPI render=true
        for i, (url, _) in enumerate(PAGES, 1):
            h1 = scraperapi_get(url, render=True)
            tmp.extend(parse_bs(h1, f"rakuten_p{i}_r1.html"))
        # render=false로 부족 랭크 보강
        got = {r["rank"] for r in tmp}
        if len(got) < 160:
            for i, (url, _) in enumerate(PAGES, 1):
                h0 = scraperapi_get(url, render=False)
                tmp.extend(parse_bs(h0, f"rakuten_p{i}_r0.html"))
        rows = sorted(tmp, key=lambda x: x["rank"])

    # 랭크 중복/범위 보정
    by_rank = {}
    for r in rows:
        if 1 <= r["rank"] <= MAX_RANK and r["rank"] not in by_rank:
            by_rank[r["rank"]] = r
    fixed = [by_rank[k] for k in sorted(by_rank.keys())]

    items: List[Item] = []
    for r in fixed:
        sale, orig, pct = parse_yen_block(r["block"])
        items.append(Item(
            rank=int(r["rank"]),
            brand=remove_official(r.get("shop") or ""),
            product_name=remove_official(r.get("name") or ""),
            price=sale, orig_price=orig, discount_percent=pct,
            url=r["href"], shop=r.get("shop") or ""
        ))
    return [x for x in items if 1 <= x.rank <= MAX_RANK]

def to_df(items: List[Item], date_str: str) -> pd.DataFrame:
    df = pd.DataFrame([{
        "date": date_str,
        "rank": it.rank,
        "product_name": it.product_name,
        "price": it.price,
        "url": it.url,
        "shop": it.shop,
        "brand": it.brand,
        "orig_price": it.orig_price,
        "discount_percent": it.discount_percent,
    } for it in items])
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
    return df.sort_values("rank")

# ----------------------------- Slack 메시지 -----------------------------
def build_slack_message(df_today: pd.DataFrame, date_str: str) -> str:
    t10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    lines = [f"*Rakuten Japan 뷰티 랭킹 160 — {date_str}*","", "*TOP 10*"]
    ja = []
    base = []
    for _, r in t10.iterrows():
        nm = BRACKETS_PAT.sub("", clean(f'{r.get("brand","")} {r.get("product_name","")}'))
        ja.append(nm)
        price_s = f'¥{int(r.get("price")):,}' if pd.notnull(r.get("price")) else "¥0"
        base.append(f'{int(r["rank"])}. <{r["url"]}|{slack_escape(nm)}> — {price_s}')
    kos = translate_ja_to_ko_batch(ja)
    for i, ln in enumerate(base):
        lines.append(ln)
        if kos[i]: lines.append(kos[i])
    return "\n".join(lines)

# ----------------------------- main -----------------------------
def run_rakuten_job():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    items = fetch_rakuten_top160()
    print(f"[INFO] 최종 건수: {len(items)} (<= {MAX_RANK})")
    d = today_str()
    df = to_df(items, d)
    fn = f"라쿠텐재팬_뷰티_랭킹_{d}.csv"
    df.to_csv(os.path.join(DATA_DIR, fn), index=False, encoding="utf-8-sig")
    print("[INFO] CSV 저장:", fn)
    msg = build_slack_message(df, d)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        run_rakuten_job()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try: slack_post(f"*라쿠텐 뷰티 랭킹 실패*\n```\n{e}\n```")
        except: pass
        raise
