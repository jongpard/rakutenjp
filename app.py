# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty Daily Ranking (genre=100939)
- 수집 범위: 1~160위 (정확 상한 보장, 초과 금지)
- 렌더링: Playwright (우선), 실패 시 ScraperAPI(옵션, 환경변수) 정적 HTML 폴백
- 로딩 안정화: 네트워크 idle → #rnkRankingMain 가시화 → 스크롤 → 항목 카운트 조건대기
- 1~3위 누락 방지: 1페이지(1~80) 추가 대기/스크롤 + 2회 재시도 합집합 후 중복제거
- CSV: 라쿠텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 전일 비교: Google Drive에서 전일 파일 내려받아 TOP10 상승/하락, 급하락, 인&아웃 계산
- Slack: TOP10(괄호내용 제거), 급하락, 인&아웃 개수 요약
- 한국어 번역(옵션): SLACK_TRANSLATE_JA2KO=1 일 때 각 항목 바로 아래 1줄 번역 삽입
- 브랜드 추정: 상점명에서 '公式|ショップ|ストア|STORE|shop' 등 토큰 제거(일본어/영문 혼합)
- 환경변수:
  * SLACK_WEBHOOK_URL
  * GDRIVE_FOLDER_ID (폴더 링크/ID 모두 허용)
  * GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN
  * RAKUTEN_GENRE_ID (기본 100939)
  * SCRAPERAPI_KEY (옵션, 폴백용)
  * RAKUTEN_MAX_RANK (기본 160)
  * RAKUTEN_HEADLESS ("1" 기본) / RAKUTEN_SLOWMO_MS (기본 0)
  * SLACK_TRANSLATE_JA2KO ("1" 켜기)
"""

import os, re, io, time, math, json, pytz, traceback, random
import datetime as dt
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- 공통/시간 ----------
KST = pytz.timezone("Asia/Seoul")
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

GENRE_ID = os.getenv("RAKUTEN_GENRE_ID", "100939").strip() or "100939"
MAX_RANK  = int(os.getenv("RAKUTEN_MAX_RANK", "160"))

DAILY_URL_P1 = f"https://ranking.rakuten.co.jp/daily/{GENRE_ID}/"
DAILY_URL_P2 = f"https://ranking.rakuten.co.jp/daily/{GENRE_ID}/p=2/"

# ---------- CSV 파일명 ----------
def build_filename(d): return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"

# ---------- 상점명 → 브랜드 추정 ----------
OFFICIAL_TOKEN = re.compile(r"(公式|オフィシャル|OFFICIAL|official|ショップ|shop|Shop|SHOP|ストア|store|STORE|楽天|Rakuten|モール|mall)", re.I)
def infer_brand_from_shop(shop: str) -> str:
    s = clean_text(shop)
    s = OFFICIAL_TOKEN.sub("", s)
    s = re.sub(r"\s+", " ", s).strip(" -|•[]()")
    return s or shop

# ---------- 금액 파싱 ----------
YEN_RE = re.compile(r"(?:¥|)(\d{1,3}(?:,\d{3})+|\d+)\s*円")
def parse_price_from_block(txt: str) -> Optional[int]:
    nums = [int(m.group(1).replace(",", "")) for m in YEN_RE.finditer(txt or "")]
    nums = [n for n in nums if n > 0]
    return min(nums) if nums else None

# ---------- 번역 (큐텐 로직 이식: JA 영역만 번역, 옵션) ----------
JP_CHAR_RE = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
def contains_ja(s): return bool(JP_CHAR_RE.search(s or ""))

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "0").lower() in ("1","true","yes")
    if not flag: return ["" for _ in lines]
    # JA 세그먼트만 뽑아 배치 번역 후 재조립
    runs, pool = [], []
    ja_run = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]+")
    for line in lines:
        line = (line or "").strip()
        if not contains_ja(line):
            runs.append(None); continue
        parts, pos = [], 0
        for m in ja_run.finditer(line):
            if m.start() > pos: parts.append(("raw", line[pos:m.start()]))
            parts.append(("ja", line[m.start():m.end()]))
            pos = m.end()
        if pos < len(line): parts.append(("raw", line[pos:]))
        runs.append(parts)
        for k,t in parts:
            if k == "ja": pool.append(t)

    if not pool: return ["" for _ in lines]

    out_ja = []
    # 1차: googletrans (없으면 패스)
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(pool, src="ja", dest="ko")
        out_ja = [getattr(r,"text","") or "" for r in (res if isinstance(res,list) else [res])]
    except Exception as e:
        print("[번역 경고] googletrans 실패:", e)
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source='ja', target='ko')
            out_ja = [gt.translate(t) if t else "" for t in pool]
        except Exception as e2:
            print("[번역 경고] deep-translator 실패:", e2)
            out_ja = ["" for _ in pool]

    it = iter(out_ja)
    rebuilt = []
    for parts in runs:
        if parts is None:
            rebuilt.append("")
            continue
        buf = []
        for k,t in parts:
            buf.append(t if k=="raw" else next(it,""))
        rebuilt.append("".join(buf))
    return rebuilt

# ---------- Slack ----------
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[INFO] Slack 미설정 → 콘솔 출력\n", text)
        return
    try:
        r = requests.post(url, json={"text": text}, timeout=20)
        if r.status_code >= 300:
            print("[WARN] Slack 실패:", r.status_code, r.text)
    except Exception as e:
        print("[WARN] Slack 예외:", e)

# ---------- Google Drive ----------
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid  = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk  = os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csec and rtk):
        raise RuntimeError("Google OAuth 자격정보가 없습니다.")
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)",
                               supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media,
                               supportsAllDrives=True).execute()
        return file_id
    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id",
                                     supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
                               fields="files(id,name)", supportsAllDrives=True,
                               includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done=False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ---------- 파서: DOM에서 안전 추출 ----------
BRACKET_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")
def strip_brackets(s: str) -> str:
    return clean_text(BRACKET_PAT.sub("", s or ""))

def _js_collect():
    # 브라우저 안에서 실행되는 함수(문자열). 랭킹 영역에서 아이템 블록을 강건하게 수집
    return """
() => {
  const root = document.querySelector('#rnkRankingMain');
  const out = [];
  if (!root) return out;

  // 랭크 카드 후보: 링크는 item.rakuten.co.jp 로 제한
  const cards = root.querySelectorAll('a[href*="item.rakuten.co.jp"]');
  const seen = new Set();

  function findRank(el) {
    // 카드 근처 텍스트에서 "123位" 패턴 찾기
    let node = el;
    for (let i=0;i<6 && node;i++){
      const txt = (node.innerText||'').replace(/\\s+/g,' ').trim();
      const m = txt.match(/(\\d+)位/);
      if (m) return parseInt(m[1],10);
      node = node.parentElement;
    }
    return null;
  }
  function findShop(el) {
    // 상점명: 카드 근처에서 "ショップ"/"shop" 영역(작은 회색 텍스트) 탐색
    let base = el.closest('div') || el.parentElement;
    let best = '';
    if (!base) return best;
    const smalls = base.querySelectorAll('div,span,p,small');
    for (const s of smalls) {
      const t = (s.textContent||'').replace(/\\s+/g,' ').trim();
      if (!t) continue;
      if (/ショップ|shop|SHOP|ストア|store/i.test(t) || t.length<=20) {
        // 후보
        if (!best || t.length < best.length) best = t;
      }
    }
    return best;
  }

  for (const a of cards) {
    let href = a.getAttribute('href') || '';
    if (!href) continue;
    if (href.startsWith('//')) href = 'https:' + href;
    else if (href.startsWith('/')) href = 'https://ranking.rakuten.co.jp' + href;
    // 실제 상품 도메인으로 보정
    if (!/https?:\\/\\/.+/.test(href)) continue;

    const name = (a.textContent||'').replace(/\\s+/g,' ').trim();
    if (!name) continue;

    const r = findRank(a);
    if (!r) continue;

    const key = r + '|' + href;
    if (seen.has(key)) continue;
    seen.add(key);

    // 가격 텍스트 근사
    const blk = (a.closest('li')||a.closest('div')||document.body).innerText.replace(/\\s+/g,' ').trim();
    const shop = findShop(a);

    out.push({rank:r, name, href, block: blk, shop});
  }
  return out;
}
"""

def render_and_collect(url: str, expect_count: int, wait_more: bool=False) -> List[Dict]:
    from playwright.sync_api import sync_playwright
    headless = os.getenv("RAKUTEN_HEADLESS", "1") not in ("0","false","False")
    slowmo = int(os.getenv("RAKUTEN_SLOWMO_MS","0") or "0")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
            slow_mo=slowmo
        )
        ctx = browser.new_context(
            viewport={"width": 1366, "height": 950},
            locale="ja-JP", timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7"},
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_load_state("networkidle", timeout=25_000)
        except: pass

        # 랭킹 컨테이너 뜰 때까지
        page.wait_for_selector("#rnkRankingMain", timeout=45_000)

        # 스크롤 다운으로 lazy load 자극
        def autoscroll(full=False):
            total = 0
            step = 800
            limit = 7000 if not full else 16000
            while total < limit:
                page.evaluate("window.scrollBy(0, %d)" % step)
                total += step
                time.sleep(0.25)

        autoscroll(full=True if wait_more else False)
        try: page.wait_for_load_state("networkidle", timeout=10_000)
        except: pass

        # 항목 최소 보장 조건대기
        try:
            page.wait_for_function(
                f"() => (document.querySelectorAll('#rnkRankingMain a[href*=\"item.rakuten.co.jp\"]').length >= {max(10, expect_count//2)})",
                timeout=25_000
            )
        except: pass

        data = page.evaluate(_js_collect())
        # 디버그 HTML 저장(옵션)
        try:
            os.makedirs("data/debug", exist_ok=True)
            page_content = page.content()
            tag = "p1" if "p=2" not in url else "p2"
            with open(f"data/debug/rakuten_{tag}_{int(time.time())}.html","w",encoding="utf-8") as f:
                f.write(page_content)
        except: pass

        ctx.close(); browser.close()
        return data

def fetch_top160() -> List[Dict]:
    # 1페이지 2회(추가대기 포함) + 2페이지 1회 → 합집합, 랭크 키 기준으로 최신 우선
    all_rows: Dict[int, Dict] = {}

    # 1~80 (빠짐 방지: 보통 + 추가대기 버전)
    p1a = render_and_collect(DAILY_URL_P1, expect_count=60, wait_more=False)
    p1b = render_and_collect(DAILY_URL_P1, expect_count=80, wait_more=True)

    # 81~160
    p2  = render_and_collect(DAILY_URL_P2, expect_count=80, wait_more=True)

    for arr in (p1a, p1b, p2):
        for r in arr:
            rk = int(r.get("rank") or 0)
            if rk<1 or rk>MAX_RANK: continue
            all_rows[rk] = r  # 뒤에 온 데이터가 덮어씀(추가대기본 우선)

    rows = [all_rows[k] for k in sorted(all_rows.keys())]
    return rows[:MAX_RANK]

# ---------- DataFrame 변환 ----------
def to_dataframe(items: List[Dict], date_str: str) -> pd.DataFrame:
    recs = []
    for it in items:
        price = parse_price_from_block(it.get("block",""))
        name  = clean_text(it.get("name",""))
        url   = it.get("href","")
        shop  = clean_text(it.get("shop",""))
        brand = infer_brand_from_shop(shop)

        recs.append({
            "date": date_str,
            "rank": int(it.get("rank")),
            "product_name": name,
            "price": price,
            "url": url,
            "shop": shop,
            "brand": brand,
        })
    df = pd.DataFrame(recs)
    # 정렬/형 보정
    if not df.empty:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        df = df.drop_duplicates(subset=["rank"]).sort_values("rank").reset_index(drop=True)
    return df

# ---------- Slack 섹션 빌더 (큐텐 포맷 기반) ----------
def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "falling": [], "inout_count": 0}

    def _plain(row):
        nm = strip_brackets(clean_text(row.get("product_name","")))
        br = clean_text(row.get("brand",""))
        if br and not nm.lower().startswith(br.lower()):
            nm = f"{br} {nm}"
        return nm

    def _link(row):
        return f"<{row['url']}|{slack_escape(_plain(row))}>"

    def _interleave(lines, jp_texts):
        kos = translate_ja_to_ko_batch(jp_texts)
        out = []
        for i, ln in enumerate(lines):
            out.append(ln)
            if kos and i < len(kos) and kos[i]:
                out.append(kos[i])
        return out

    # TOP10
    jp_rows, lines = [], []
    t10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    prev_index = None
    if df_prev is not None and not df_prev.empty:
        prev_index = df_prev.set_index("url") if "url" in df_prev.columns else None

    for _, r in t10.iterrows():
        jp_rows.append(_plain(r))
        marker = ""
        if prev_index is not None and r["url"] in prev_index.index and pd.notnull(prev_index.loc[r["url"], "rank"]):
            pr, cr = int(prev_index.loc[r["url"], "rank"]), int(r["rank"])
            d = pr - cr
            marker = f"(↑{d}) " if d>0 else (f"(↓{abs(d)}) " if d<0 else "")
        else:
            marker = "(New) "
        price_str = f"￥{int(r['price']):,}" if pd.notnull(r.get("price")) else "￥0"
        lines.append(f"{int(r['rank'])}. {marker}{_link(r)} — {price_str}")
    S["top10"] = _interleave(lines, jp_rows)

    if df_prev is None or df_prev.empty:
        return S

    # 급하락 (Top160 기준, OUT 포함)
    t160 = df_today[(df_today["rank"].notna()) & (df_today["rank"] <= MAX_RANK)].copy()
    p160 = df_prev[(df_prev["rank"].notna()) & (df_prev["rank"] <= MAX_RANK)].copy()

    cur = t160.set_index("url"); prev = p160.set_index("url")
    common = list(set(cur.index) & set(prev.index))
    outs   = list(set(prev.index) - set(cur.index))

    movers = []
    for k in common:
        pr, cr = int(prev.loc[k,"rank"]), int(cur.loc[k,"rank"])
        drop = cr - pr
        if drop > 0:
            row = cur.loc[k]
            movers.append((drop, cr, pr, f"- {_link(row)} {pr}위 → {cr}위 (↓{drop})", _plain(row)))
    movers.sort(key=lambda x:(-x[0], x[1], x[2], x[4]))
    chosen, jp = [], []
    for _,_,_,txt,jpn in movers[:5]:
        chosen.append(txt); jp.append(jpn)

    if len(chosen) < 5:
        outs_sorted = sorted(outs, key=lambda k:int(prev.loc[k,"rank"]))
        for k in outs_sorted:
            if len(chosen) >= 5: break
            row = prev.loc[k]
            chosen.append(f"- <{k}|{slack_escape(_plain(row))}> {int(row['rank'])}위 → OUT")
            jp.append(_plain(row))

    S["falling"] = _interleave(chosen, jp)

    # 인&아웃 개수
    S["inout_count"] = len(set(cur.index) ^ set(prev.index)) // 2
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines = []
    lines.append(f"*Rakuten Japan 뷰티 랭킹 {MAX_RANK} — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*")
    lines.extend(S.get("top10") or ["- 데이터 없음"])
    lines.append("")
    lines.append("*📉 급하락*")
    lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.append("")
    lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count',0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- 실행 ----------
def run_rakuten_job():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    items = []
    err = None
    for attempt in range(1, 3):  # 2회 시도
        try:
            print(f"[INFO] 렌더 시도 {attempt}/2")
            items = fetch_top160()
            if len(items) >= 120:  # 안정선
                break
        except Exception as e:
            err = e
            print("[WARN] 렌더 실패:", e)
            time.sleep(3)

    if not items:
        raise RuntimeError(f"수집 실패 (에러: {err})")

    # → DF
    date_str = today_kst_str()
    df_today = to_dataframe(items, date_str)
    # 상한 보장 및 결측 제거
    df_today = df_today.dropna(subset=["rank"]).sort_values("rank").head(MAX_RANK).reset_index(drop=True)

    print(f"[INFO] 최종 건수: {len(df_today)} (<= {MAX_RANK})")

    # CSV 저장
    os.makedirs("data", exist_ok=True)
    file_today = build_filename(date_str)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print(f"[INFO] CSV 저장: {file_today}")

    # Drive 업로드 + 전일 다운로드
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("[INFO] 드라이브 업로드 OK")
            yday = yesterday_kst_str()
            file_yday = build_filename(yday)
            df_prev = drive_download_csv(svc, folder, file_yday)
            print("[INFO] 전일 CSV", "없음" if df_prev is None else "확인")
        except Exception as e:
            print("[WARN] Drive 처리 경고:", e)

    # Slack 메시지
    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

def main():
    try:
        run_rakuten_job()
    except Exception as e:
        print("[오류]", e)
        traceback.print_exc()
        try:
            slack_post(f"*라쿠텐 재팬 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise

if __name__ == "__main__":
    main()
