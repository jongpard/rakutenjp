# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty(100939) Daily Rank 1~160
- ScraperAPI(JP)로 우회, render=False → 0개면 True 재시도(크레딧 절약)
- 파싱: div.rnkRanking_after4box 기준 (rank/name/url/price/shop/brand)
- CSV 저장: 라쿠텐재팬_뷰티_랭킹_YYYY-MM-DD.csv
- (옵션) Google Drive 업로드 + 전일 비교로 Slack 메시지 전송
- 변동이 없으면 "-" 로 표기

필수 env:
  SCRAPERAPI_KEY, SLACK_WEBHOOK_URL,
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, GDRIVE_FOLDER_ID
옵션 env:
  RAKUTEN_MAX_RANK(기본 160), RAKUTEN_FORCE_RENDER(기본 0), RAKUTEN_SAVE_DEBUG(기본 1)
"""

import os, re, io, time, traceback, datetime as dt
from typing import List, Dict, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ===== 기본 =====
KST = dt.timezone(dt.timedelta(hours=9))
def now_kst(): return dt.datetime.now(KST)
def today(): return now_kst().strftime("%Y-%m-%d")
def yesterday(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s: str) -> str: return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

DATA_DIR = "data"; DBG_DIR = "data/debug"
os.makedirs(DATA_DIR, exist_ok=True); os.makedirs(DBG_DIR, exist_ok=True)

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))
FORCE_RENDER = os.getenv("RAKUTEN_FORCE_RENDER", "0") in ("1","true","True")
SAVE_DEBUG   = os.getenv("RAKUTEN_SAVE_DEBUG", "1") in ("1","true","True")

# 대상 페이지(1~80, 81~160)
RANK_URLS = [
    "https://ranking.rakuten.co.jp/daily/100939/",
    "https://ranking.rakuten.co.jp/daily/100939/p=2/",
]

# ===== ScraperAPI =====
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9"
}

def scraperapi_get(url: str, render: bool) -> str:
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPER_KEY,
        "url": url,
        "country_code": "jp",
        "retry_404": "true",
        "keep_headers": "true",
        "render": "true" if render else "false",
    }
    r = requests.get(SCRAPER_ENDPOINT, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text

# ===== 파싱 =====
YEN_RE = re.compile(r"([0-9,]+)\s*円")

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

def parse_rank_page(html: str, add_offset: int) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.rnkRanking_after4box")
    rows: List[Dict] = []
    for c in cards:
        r_el = c.select_one(".rnkRanking_dispRank")
        if not r_el: 
            continue
        rk_txt = clean(r_el.get_text())
        rk_m = re.search(r"\d+", rk_txt)
        if not rk_m: 
            continue
        rank = int(rk_m.group(0))

        a = c.select_one(".rnkRanking_itemName a")
        name = clean(a.get_text()) if a else ""
        href = a.get("href") if a else ""
        if href: href = re.sub(r"[?#].*$","",href.strip())

        pr_el = c.select_one(".rnkRanking_price")
        pr_txt = clean(pr_el.get_text()) if pr_el else ""
        m = YEN_RE.search(pr_txt or "")
        price = int(m.group(1).replace(",", "")) if m else None

        sh_a = c.select_one(".rnkRanking_shop a")
        shop = clean(sh_a.get_text()) if sh_a else ""
        brand = brand_from_shop(shop)

        rows.append({"rank": rank + add_offset*0, "product_name": name,
                     "price": price, "url": href, "shop": shop, "brand": brand})
    return rows

def fetch_all() -> List[Dict]:
    allrows: List[Dict] = []
    for url in RANK_URLS:
        add = 80 if "p=2" in url else 0
        # 1차: render=False(절약) or 강제 설정
        render_first = True if FORCE_RENDER else False
        html = scraperapi_get(url, render=render_first)
        if SAVE_DEBUG:
            open(f"{DBG_DIR}/rakuten_{'p2' if add else 'p1'}_raw_{'r1' if render_first else 'r0'}.html","w",encoding="utf-8").write(html)
        rows = parse_rank_page(html, add)
        if len(rows) == 0 and not render_first:
            # 2차: 해당 페이지만 렌더 ON 재시도
            html = scraperapi_get(url, render=True)
            if SAVE_DEBUG:
                open(f"{DBG_DIR}/rakuten_{'p2' if add else 'p1'}_raw_r1.html","w",encoding="utf-8").write(html)
            rows = parse_rank_page(html, add)
        allrows.extend(rows)
        time.sleep(0.6)
    allrows = sorted(allrows, key=lambda r: r["rank"])[:MAX_RANK]
    return allrows

# ===== Slack =====
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[Slack 미설정] 메시지 생략")
        return
    try:
        r = requests.post(url, json={"text": text}, timeout=25)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:300])
    except Exception as e:
        print("[Slack 예외]", e)

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, list]:
    S = {"top10": [], "falling": [], "inout_count": 0}
    if "rank" not in df_today.columns or len(df_today) == 0:
        return S

    def _name(r):
        nm = clean(r.get("product_name",""))
        br = clean(r.get("brand",""))
        return f"{br} {nm}" if br and not nm.lower().startswith(br.lower()) else nm

    def _link(r):
        return f"<{r['url']}|{slack_escape(_name(r))}>" if r.get("url") else slack_escape(_name(r))

    prev_idx = None
    if df_prev is not None and len(df_prev) and "rank" in df_prev.columns:
        prev_idx = df_prev.copy()
        prev_idx["__k__"] = prev_idx["product_name"].astype(str).str.strip()
        prev_idx.set_index("__k__", inplace=True)

    # TOP10
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    lines = []
    for _, r in top10.iterrows():
        mark = ""
        if prev_idx is not None:
            k = str(r.get("product_name")).strip()
            if k in prev_idx.index and pd.notnull(prev_idx.loc[k, "rank"]):
                pr = int(prev_idx.loc[k, "rank"]); cr = int(r["rank"])
                diff = pr - cr
                if diff > 0: mark = f"(↑{diff}) "
                elif diff < 0: mark = f"(↓{abs(diff)}) "
                else: mark = "(-) "
            else:
                mark = "(New) "
        price_txt = f"￥{int(r['price']):,}" if pd.notnull(r.get("price")) else "￥0"
        lines.append(f"{int(r['rank'])}. {mark}{_link(r)} — {price_txt}")
    S["top10"] = lines

    if prev_idx is None:
        return S

    cur_idx = df_today.copy()
    cur_idx["__k__"] = cur_idx["product_name"].astype(str).str.strip()
    cur_idx.set_index("__k__", inplace=True)

    tN = cur_idx[(cur_idx["rank"].notna()) & (cur_idx["rank"] <= MAX_RANK)]
    pN = prev_idx[(prev_idx["rank"].notna()) & (prev_idx["rank"] <= MAX_RANK)]

    common = set(tN.index) & set(pN.index)
    out_only = set(pN.index) - set(tN.index)

    movers = []
    for k in common:
        pr, cr = int(pN.loc[k, "rank"]), int(tN.loc[k, "rank"])
        drop = cr - pr
        if drop > 0:
            row = tN.loc[k]
            movers.append((drop, cr, pr, f"- {_link(row)} {pr}위 → {cr}위 (↓{drop})"))
    movers.sort(key=lambda x: (-x[0], x[1], x[2]))
    S["falling"] = [m[3] for m in movers[:5]]

    if len(S["falling"]) < 5:
        outs = sorted(list(out_only), key=lambda k: int(pN.loc[k, "rank"]))
        for k in outs:
            if len(S["falling"]) >= 5: break
            row = pN.loc[k]
            S["falling"].append(f"- {slack_escape(str(k))} {int(row['rank'])}위 → OUT")

    today_keys, prev_keys = set(tN.index), set(pN.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2
    return S

def build_slack_message(date_str: str, S: Dict[str, list]) -> str:
    lines = []
    lines.append(f"*Rakuten Japan 뷰티 랭킹 {MAX_RANK} — {date_str}*")
    lines.append("")
    if S["top10"]:
        lines.append("*TOP 10*"); lines.extend(S["top10"])
        lines.append(""); lines.append("*📉 급하락*"); lines.extend(S.get("falling") or ["- 해당 없음"])
        lines.append(""); lines.append("*↔ 랭크 인&아웃*")
        lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    else:
        lines.append("_수집된 랭킹이 없습니다. data/debug HTML을 확인하세요._")
    return "\n".join(lines)

# ===== Google Drive =====
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid, csec, rtk = (os.getenv("GOOGLE_CLIENT_ID"), os.getenv("GOOGLE_CLIENT_SECRET"), os.getenv("GOOGLE_REFRESH_TOKEN"))
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(q=q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8-sig"); buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute(); return file_id
    meta = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload
    res = service.files().list(q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
                               fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = res.get("files", [])
    if not files: return None
    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done = False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ===== 메인 =====
def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작(ScraperAPI, 절약모드)")
    rows = fetch_all()
    print("[INFO] 수집:", len(rows))

    date_str = today()
    df_today = pd.DataFrame(rows)
    df_today.insert(0, "date", date_str)

    # CSV 저장
    os.makedirs(DATA_DIR, exist_ok=True)
    file_today = build_filename(date_str)
    df_today[["rank","product_name","price","url","shop","brand"]].to_csv(
        os.path.join(DATA_DIR, file_today), index=False, encoding="utf-8-sig"
    )
    print("[INFO] 로컬 CSV 저장:", file_today)

    # Drive 업로드 + 전일 로드
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID", ""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            y_name = build_filename(yesterday())
            df_prev = drive_download_csv(svc, folder, y_name)
            print("[INFO] 드라이브 업로드 OK, 전일:", "있음" if df_prev is not None else "없음")
        except Exception as e:
            print("[Drive 오류]", e); traceback.print_exc()
    else:
        print("[INFO] GDRIVE_FOLDER_ID 미설정 → 업로드 생략")

    # Slack
    S = build_sections(df_today, df_prev)
    slack_post(build_slack_message(date_str, S))
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류]", e); traceback.print_exc()
        try: slack_post(f"*라쿠텐 랭킹 실패*\n```\n{e}\n```")
        except: pass
        raise
