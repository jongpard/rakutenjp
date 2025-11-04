# -*- coding: utf-8 -*-
import os, re, io, time, math, traceback
import datetime as dt
import pandas as pd
import requests
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

# -------- 기본 설정 --------
RAKUTEN_DAILY_P1 = "https://ranking.rakuten.co.jp/daily/100939/"
RAKUTEN_DAILY_P2 = "https://ranking.rakuten.co.jp/daily/100939/p=2/"
MAX_RANK = 160  # <- 요구사항: 160까지만

OFFICIAL_PAT = re.compile(r"^\s*(公式|公式ショップ|公式ストア)\s*", re.I)
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
YEN_AMOUNT_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")

def clean(s): return re.sub(r"\s+", " ", (s or "")).strip()
def remove_official(s: str) -> str: return OFFICIAL_PAT.sub("", clean(s or ""))

def parse_yen(text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    # 텍스트에서 '...円'만 모아 최솟값=sale, 최댓값=orig, 퍼센트는 계산
    amts = [int(m.group(1).replace(",", "")) for m in YEN_AMOUNT_RE.finditer(text or "") if m.group(1) != "0"]
    sale = min(amts) if amts else None
    orig = max(amts) if len(amts) >= 2 else None
    pct  = None
    if sale and orig and orig > sale:
        pct = int(math.floor((1 - sale/orig) * 100))
    return sale, orig, pct

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

# -------- 번역 (Qoo10 스타일 폴백) --------
JP_CHAR_RE = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
def contains_ja(s): return bool(JP_CHAR_RE.search(s or ""))

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "1").lower() in ("1","true","yes")
    texts = [clean(x) for x in lines]
    if not flag: 
        print("[번역 경고] 번역 OFF"); return ["" for _ in texts]
    ja = [t for t in texts if contains_ja(t)]
    if not ja: return ["" for _ in texts]
    # googletrans → deep_translator 폴백
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
            gt = DT(source='ja', target='ko'); trans = {t: gt.translate(t) for t in ja}
        except Exception as e2:
            print("[번역 경고] deep_translator 실패:", e2)
            return ["" for _ in texts]
    return [trans.get(t, "") if contains_ja(t) else "" for t in texts]

# -------- Playwright 렌더 & 파싱 --------
def _pw():
    from playwright.sync_api import sync_playwright
    return sync_playwright()

def _render_and_collect(url: str, expect: int) -> List[Dict]:
    """
    페이지 로딩이 매우 느려도 끝까지 기다려서 카드 데이터만 추출.
    expect=80 이면 최소 80개 보일 때까지 스크롤/대기 반복.
    """
    with _pw() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox","--disable-dev-shm-usage"
            ],
        )
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 1000},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7"},
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=25_000)
        except: pass

        # 랭킹 컨테이너 등장까지
        page.wait_for_selector("#rnkRankingMain", timeout=30_000)
        # 느린 이미지/비동기 보정: 80개 노출될 때까지 스크롤 다운 반복 (최대 30초)
        t0 = time.time()
        last = 0
        while True:
            n = page.eval_on_selector_all("#rnkRankingMain li, #rnkRankingMain .rnkRanking_item", "els => els.length")
            if n >= expect: break
            # 조금 더 내려보기
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.6)
            if n == last: time.sleep(0.6)
            last = n
            if time.time() - t0 > 30: break

        # 상단(1~3위) 광고/배너 혼입 방지: 랭킹 블록 내부에서 앵커만 추출
        data = page.evaluate("""
            () => {
              const box = document.querySelector('#rnkRankingMain');
              if (!box) return [];
              const rows = [];
              const cards = box.querySelectorAll('li, .rnkRanking_item');
              for (const el of cards) {
                // 랭크 번호
                let r = 0;
                const rk = el.querySelector('.rankNo, .rnkRankBadge, .rnkRanking_rank, .rank');
                if (rk) {
                  const m = (rk.textContent||'').match(/\\d+/);
                  if (m) r = parseInt(m[0], 10);
                }
                // 상품 링크
                const a = el.querySelector('a[href*="item.rakuten.co.jp/"], a[href*="/item/"]');
                if (!a) continue;
                const href = a.href;
                const name = (a.textContent||'').replace(/\\s+/g,' ').trim();

                // 샵/브랜드(상점 링크/텍스트)
                let shop = '';
                const shopEl = el.querySelector('.rnkRanking_shop, .rnkTop_shop, .shop, .rnkRanking_shop a');
                if (shopEl) shop = (shopEl.textContent||'').replace(/\\s+/g,' ').trim();

                // 가격 텍스트(카드 전체)
                const block = (el.innerText||'').replace(/\\s+/g,' ').trim();
                rows.push({rank:r, href, name, shop, block});
              }
              // 중복/광고 제거 + 랭크 있는 것만
              return rows.filter(x => x.rank > 0);
            }
        """)
        ctx.close(); browser.close()
        return data

def fetch_rakuten_top160() -> List[Item]:
    # 페이지1(1~80), 페이지2(81~160)
    rows = []
    for url, expect in [(RAKUTEN_DAILY_P1, 80), (RAKUTEN_DAILY_P2, 80)]:
        part = _render_and_collect(url, expect=expect)
        # 안전장치: 과하게 많이 나오면 상위 80개만 자름
        part_sorted = sorted(part, key=lambda x: x["rank"])[:80]
        rows.extend(part_sorted)

    # 랭크 중복/누락 정리(간혹 페이지 이동 시 꼬임 방지)
    by_rank = {}
    for r in rows:
        if 1 <= r["rank"] <= MAX_RANK and r["rank"] not in by_rank:
            by_rank[r["rank"]] = r
    fixed = [by_rank[k] for k in sorted(by_rank.keys())]

    items: List[Item] = []
    for r in fixed:
        sale, orig, pct = parse_yen(r["block"])
        brand = remove_official(r.get("shop") or "")
        title = remove_official(r.get("name") or "")
        items.append(Item(
            rank = int(r["rank"]),
            brand = brand,
            product_name = title,
            price = sale,
            orig_price = orig,
            discount_percent = pct,
            url = r["href"],
            shop = r.get("shop") or ""
        ))
    # 최종 방어: 160 초과 금지
    return [x for x in items if 1 <= x.rank <= MAX_RANK]

# -------- DataFrame / Slack --------
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
    # rank 형 안전화
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
    return df.sort_values("rank")

def build_slack_lines(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> str:
    # TOP10 + 번역 1줄
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    base = []
    ja_names = []
    for _, r in top10.iterrows():
        name = BRACKETS_PAT.sub("", clean(f'{r.get("brand","")} {r.get("product_name","")}'))
        ja_names.append(name)
        price_s = f'¥{int(r.get("price")):,}' if pd.notnull(r.get("price")) else "¥0"
        base.append(f'{int(r["rank"])}. <{r["url"]}|{name}> — {price_s}')

    kos = translate_ja_to_ko_batch(ja_names)
    lines = ["*Rakuten Japan 뷰티 랭킹 160 — 오늘*","", "*TOP 10*"]
    for i, ln in enumerate(base):
        lines.append(ln)
        if kos[i]:
            lines.append(kos[i])

    # 급하락/인아웃은 요구가 없어서 생략. 필요하면 Qoo10 로직 붙이면 됨.
    return "\n".join(lines)

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[INFO] Slack 미설정 → 콘솔 출력\n", text); return
    try:
        r = requests.post(url, json={"text": text}, timeout=20)
        if r.status_code >= 300:
            print("[경고] Slack 실패", r.status_code, r.text)
    except Exception as e:
        print("[경고] Slack 예외", e)

# -------- 메인 진입점 (라쿠텐만) --------
def run_rakuten_job():
    today = dt.datetime.utcnow().date().isoformat()
    items = fetch_rakuten_top160()
    print(f"[INFO] 최종 개수: {len(items)} (<={MAX_RANK})")

    df = to_df(items, today)
    # CSV 저장 (로컬 & 드라이브 업로드는 기존 함수 사용)
    fn = f"라쿠텐재팬_뷰티_랭킹_{today}.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(os.path.join("data", fn), index=False, encoding="utf-8-sig")
    print(f"[INFO] CSV 저장: {fn}")

    # 전일 비교는 옵션 – 기존 코드에 df_prev를 구해 넘겨도 됨.
    msg = build_slack_lines(df, None)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

# 외부에서 main()에서 run_rakuten_job() 호출하도록 연결
if __name__ == "__main__":
    try:
        run_rakuten_job()
    except Exception as e:
        print("[오류]", e)
        traceback.print_exc()
        try: slack_post(f"*라쿠텐 뷰티 랭킹 실패*\n```\n{e}\n```")
        except: pass
        raise
