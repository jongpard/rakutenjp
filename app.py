# -*- coding: utf-8 -*-
import os, re, io, time, traceback, datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict

import requests
import pandas as pd
import pytz

# ====== 기본 설정 ======
KST = pytz.timezone("Asia/Seoul")
MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))

RANK_URLS = [
    "https://ranking.rakuten.co.jp/daily/100939/",       # 1~80
    "https://ranking.rakuten.co.jp/daily/100939/p=2/",    # 81~160
]

SCRAPER_ENDPOINT = "https://api.scraperapi.com/"
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()

# ====== 공통 유틸 ======
def now_kst(): return dt.datetime.now(KST)
def today(): return now_kst().strftime("%Y-%m-%d")
def yesterday(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def slack_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# ====== Slack ======
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[Slack 미설정] 메시지:\n", text)
        return
    try:
        r = requests.post(url, json={"text": text}, timeout=25)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:300])
    except Exception as e:
        print("[Slack 예외]", e)

# ====== Google Drive ======
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    cid, csec, rtk = (os.getenv("GOOGLE_CLIENT_ID"), os.getenv("GOOGLE_CLIENT_SECRET"), os.getenv("GOOGLE_REFRESH_TOKEN"))
    if not (cid and csec and rtk):
        raise RuntimeError("Google Drive OAuth 시크릿 누락")
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

# ====== ScraperAPI 호출 + 파싱 ======
from bs4 import BeautifulSoup
YEN_RE = re.compile(r"([0-9,]+)\s*円")

def scraperapi_get(url: str, render: bool) -> str:
    if not SCRAPER_KEY:
        raise RuntimeError("SCRAPERAPI_KEY 미설정")
    params = {
        "api_key": SCRAPER_KEY,
        "url": url,
        "country_code": "jp",
        "retry_404": "true",
        "render": "true" if render else "false",
    }
    r = requests.get(SCRAPER_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    return r.text

def parse_rank_page(html: str, add_offset: int) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("li.rnkRanking_item")
    out = []
    for i, li in enumerate(cards, start=1):
        # rank
        rk_el = li.select_one(".rnkRanking_rank")
        rk_txt = clean(rk_el.get_text()) if rk_el else ""
        try: rank = int(re.sub(r"\D", "", rk_txt)) if rk_txt else i + add_offset
        except: rank = i + add_offset

        # name/url
        a = li.select_one(".rnkRanking_itemName a")
        name = clean(a.get_text()) if a else ""
        href = a.get("href") if a else ""
        if href:
            href = re.sub(r"[?#].*$", "", href.strip())

        # shop
        shop = ""
        sh = li.select_one(".rnkRanking_itemShop a")
        if sh: shop = clean(sh.get_text())

        # price
        raw_price = clean(li.select_one(".rnkRanking_itemPrice").get_text()) if li.select_one(".rnkRanking_itemPrice") else ""
        m = YEN_RE.search(raw_price or "")
        price = int(m.group(1).replace(",", "")) if m else None

        out.append({"rank": rank, "brand": shop, "product_name": name, "price": price, "url": href})
    return out

def fetch_all() -> List[Dict]:
    allrows = []
    for url in RANK_URLS:
        add = 80 if "p=2" in url else 0
        # 1차: render=false (저렴)
        html = scraperapi_get(url, render=False)
        rows = parse_rank_page(html, add)
        if len(rows) == 0:
            # 2차: render=true (해당 페이지만 재시도)
            html = scraperapi_get(url, render=True)
            rows = parse_rank_page(html, add)
        allrows.extend(rows)
        time.sleep(1.0)
    # 상한/정렬
    allrows = sorted(allrows, key=lambda r: r["rank"])[:MAX_RANK]
    return allrows

# ====== Slack 메시지 구성 (변동 0은 '(-)' 고정) ======
def fmt_jpy(v): 
    try: return f"￥{int(v):,}"
    except: return "￥0"

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "falling": [], "inout_count": 0}

    def _name(r):
        nm = clean(r.get("product_name", ""))
        br = clean(r.get("brand", ""))
        return f"{br} {nm}" if br and not nm.lower().startswith(br.lower()) else nm

    def _link(r):
        return f"<{r['url']}|{slack_escape(_name(r))}>" if r.get("url") else slack_escape(_name(r))

    prev_idx = None
    if df_prev is not None and len(df_prev):
        prev_idx = df_prev.copy()
        prev_idx["__k__"] = prev_idx["url"].astype(str).str.strip()
        prev_idx.set_index("__k__", inplace=True)

    # TOP10
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    lines = []
    for _, r in top10.iterrows():
        mark = ""
        if prev_idx is not None:
            k = str(r.get("url")).strip()
            if k in prev_idx.index and pd.notnull(prev_idx.loc[k, "rank"]):
                pr = int(prev_idx.loc[k, "rank"]); cr = int(r["rank"])
                diff = pr - cr
                if diff > 0: mark = f"(↑{diff}) "
                elif diff < 0: mark = f"(↓{abs(diff)}) "
                else: mark = "(-) "
            else:
                mark = "(New) "
        price_txt = fmt_jpy(r.get("price")) if pd.notnull(r.get("price")) else "￥0"
        lines.append(f"{int(r['rank'])}. {mark}{_link(r)} — {price_txt}")
    S["top10"] = lines

    if prev_idx is None:
        return S

    cur_idx = df_today.copy()
    cur_idx["__k__"] = cur_idx["url"].astype(str).str.strip()
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

    falling = [m[3] for m in movers[:5]]
    if len(falling) < 5:
        outs = sorted(list(out_only), key=lambda k: int(pN.loc[k, "rank"]))
        for k in outs:
            if len(falling) >= 5: break
            row = pN.loc[k]
            falling.append(f"- {_link(row)} {int(row['rank'])}위 → OUT")
    S["falling"] = falling

    today_keys, prev_keys = set(tN.index), set(pN.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2
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
    lines.append("*↔ 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ====== 메인 ======
def main():
    print("[INFO] 라쿠텐 랭킹 수집 시작(ScraperAPI, 절약모드)")
    rows = fetch_all()
    print("[INFO] 수집:", len(rows))

    date_str = today()
    df_today = pd.DataFrame(rows)
    df_today.insert(0, "date", date_str)

    os.makedirs("data", exist_ok=True)
    file_today = build_filename(date_str)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
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
            print("[Drive 오류]", e)
            traceback.print_exc()
    else:
        print("[INFO] GDRIVE_FOLDER_ID 미설정 → 업로드 생략")

    S = build_sections(df_today, df_prev)
    slack_post(build_slack_message(date_str, S))
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류]", e)
        traceback.print_exc()
        try:
            slack_post(f"*라쿠텐 랭킹 실패*\n```\n{e}\n```")
        except: pass
        raise
