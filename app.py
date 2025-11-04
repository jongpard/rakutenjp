# -*- coding: utf-8 -*-
"""
Rakuten JP Beauty(100939) Daily Top N (전문)
- ScraperAPI(JP, render=true)로 1~160 수집 (p=1,2; 필요시 p=3,4 보정)
- TOP3 포함 통합 파서
- rank_int/price_int 정규화 + 안전 비교
- CSV + (옵션) Google Drive 업로드
- Slack: TOP10(ja+ko), 📉급하락, 🔄인&아웃 / 변동 없으면 "(-)"
"""

import os, re, io, time, traceback, datetime as dt
from typing import List, Dict, Optional

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

# ===== 공통 =====
KST = dt.timezone(dt.timedelta(hours=9))
def now_kst(): return dt.datetime.now(KST)
def today(): return now_kst().strftime("%Y-%m-%d")
def yesterday(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"
def clean(s: str) -> str: return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s: str) -> str: return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

DATA_DIR, DBG_DIR = "data", "data/debug"
os.makedirs(DATA_DIR, exist_ok=True); os.makedirs(DBG_DIR, exist_ok=True)

MAX_RANK     = int(os.getenv("RAKUTEN_MAX_RANK", "160"))
SAVE_DEBUG   = os.getenv("RAKUTEN_SAVE_DEBUG", "1") in ("1","true","True")
DO_TRANSLATE = os.getenv("SLACK_TRANSLATE_JA2KO", "1") in ("1","true","True")

BASE = "https://ranking.rakuten.co.jp/daily/100939/"
BASE_PAGES   = [BASE, BASE + "p=2/"]          # 1~80, 81~160
BACKUP_PAGES = [BASE + "p=3/", BASE + "p=4/"] # 혹시 누락 시 보정

# ===== ScraperAPI =====
SCRAPER_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
SCRAPER_ENDPOINT = "https://api.scraperapi.com/"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en-US;q=0.9"
}

def scraperapi_get(url: str, render: bool=True) -> str:
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
YEN_RE      = re.compile(r"([0-9,]+)\s*円")
RANK_TXT_RE = re.compile(r"(\d+)\s*位")
BRAND_STOPWORDS = [
    "楽天市場店","公式","オフィシャル","ショップ","ストア","専門店","直営",
    "店","本店","支店","楽天市場","楽天","mall","MALL","shop","SHOP","store","STORE"
]

def brand_from_shop(shop: str) -> str:
    b = clean(shop)
    for w in BRAND_STOPWORDS: b = re.sub(w, "", b, flags=re.IGNORECASE)
    b = re.sub(r"[【】\[\]（）()]", "", b)
    return b.strip(" -_·|·")

def find_rank_in_block(block: BeautifulSoup) -> Optional[int]:
    if not block: return None
    # 우선 순위: 특정 클래스
    el = block.select_one(".rnkRanking_dispRank, .rank, .rnkRanking_rank")
    if el:
        m = RANK_TXT_RE.search(el.get_text(" ", strip=True) or "")
        if m: return int(m.group(1))
    # 블록 전체 텍스트
    txt = block.get_text(" ", strip=True) if block else ""
    m2 = RANK_TXT_RE.search(txt or "")
    if m2: return int(m2.group(1))
    # 이미지 alt
    img = block.select_one("img[alt*='位']")
    if img:
        alt = img.get("alt") or ""
        m3 = RANK_TXT_RE.search(alt)
        if m3: return int(m3.group(1))
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
    items: List[Dict] = []
    seen = set()

    for a in soup.select("div.rnkRanking_itemName a"):
        block = nearest_item_block(a)
        if not block: continue

        rank = find_rank_in_block(block)
        if not rank: 
            continue
        if rank in seen: 
            continue
        seen.add(rank)

        name = clean(a.get_text())
        href = (a.get("href") or "").strip()
        href = re.sub(r"[?#].*$", "", href)

        pr_el = block.select_one(".rnkRanking_price")
        pr_txt = clean(pr_el.get_text()) if pr_el else ""
        m_y = YEN_RE.search(pr_txt)
        price = int(m_y.group(1).replace(",", "")) if m_y else np.nan

        sh_a = block.select_one(".rnkRanking_shop a")
        shop = clean(sh_a.get_text()) if sh_a else ""
        brand = brand_from_shop(shop)

        items.append({
            "rank": rank, "product_name": name, "price": price,
            "url": href, "shop": shop, "brand": brand
        })

    items.sort(key=lambda r: r["rank"])
    return items

def fetch_all() -> pd.DataFrame:
    rows: List[Dict] = []
    for url in BASE_PAGES:
        html = scraperapi_get(url, render=True)
        if SAVE_DEBUG:
            tag = "p2" if "p=2" in url else "p1"
            open(f"{DBG_DIR}/rakuten_{tag}.html", "w", encoding="utf-8").write(html)
        rows.extend(parse_page(html))
        time.sleep(0.6)

    # 보정(광고 섞여 누락되는 날 보완)
    if len({r["rank"] for r in rows}) < 120:
        for url in BACKUP_PAGES:
            html = scraperapi_get(url, render=True)
            if SAVE_DEBUG:
                tag = "p3" if "p=3" in url else "p4"
                open(f"{DBG_DIR}/rakuten_{tag}.html", "w", encoding="utf-8").write(html)
            rows.extend(parse_page(html)); time.sleep(0.6)

    df = pd.DataFrame(rows)
    return df

# ===== 정규화 유틸 =====
def extract_int_first(s):
    if pd.isna(s): return np.nan
    m = re.search(r'\d+', str(s))
    return int(m.group()) if m else np.nan

def parse_price_val(s):
    if pd.isna(s): return np.nan
    ds = re.findall(r'\d+', str(s))
    return int(''.join(ds)) if ds else np.nan

def normalize_df(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df.insert(0, "date", date_str)
    df["rank_int"]  = df["rank"].apply(extract_int_first)
    df["price_int"] = df["price"].apply(parse_price_val)
    # 중복 제거 (url이 가장 강한 유니크, 보조로 rank_int)
    df = df.drop_duplicates(subset=["date", "rank_int", "url"], keep="first")
    # 유효 랭크만, 상한 적용
    df = df[df["rank_int"].notna()].sort_values("rank_int").head(MAX_RANK)
    return df

def get_scalar_int(val):
    """Series/array/scalar 무엇이 오든 안전하게 int or raise."""
    if isinstance(val, pd.Series) or isinstance(val, np.ndarray):
        val = val.iloc[0] if isinstance(val, pd.Series) else val[0]
    if pd.isna(val): 
        raise ValueError("NaN rank encountered")
    return int(val)

# ===== 번역 (폴백 안전) =====
def translate_ja2ko_batch(texts: List[str]) -> List[str]:
    if not DO_TRANSLATE or not texts: 
        return ["" for _ in texts]
    # 1) googletrans
    try:
        from googletrans import Translator
        tr = Translator(service_urls=['translate.googleapis.com'])
        res = tr.translate(texts, src="ja", dest="ko")
        return [getattr(r, "text", "") or "" for r in (res if isinstance(res, list) else [res])]
    except Exception as e:
        print("[번역 경고] googletrans 실패:", e)
    # 2) deep-translator
    try:
        from deep_translator import GoogleTranslator
        gt = GoogleTranslator(source="ja", target="ko")
        return [gt.translate(t) if t else "" for t in texts]
    except Exception as e2:
        print("[번역 경고] deep-translator 실패:", e2)
        return ["" for _ in texts]

# ===== Slack =====
def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[Slack 미설정] 메시지 생략"); return
    try:
        r = requests.post(url, json={"text": text}, timeout=25)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text[:300])
    except Exception as e:
        print("[Slack 예외]", e)

def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, list]:
    S = {"top10": [], "falling": [], "inout_count": 0}
    if df_today.empty: 
        return S

    name_today = "product_name" if "product_name" in df_today.columns else "name"
    name_prev  = None
    if df_prev is not None and not df_prev.empty:
        if "product_name" in df_prev.columns: name_prev = "product_name"
        elif "name" in df_prev.columns:       name_prev = "name"

    # TOP10
    t10 = df_today.dropna(subset=["rank_int"]).sort_values("rank_int").head(10).copy()
    ja = t10[name_today].astype(str).tolist()
    ko = translate_ja2ko_batch(ja)
    lines = []
    prev_idx = None
    if name_prev:
        prev_idx = df_prev.copy()
        prev_idx["__k__"] = prev_idx[name_prev].astype(str).str.strip()
        prev_idx.set_index("__k__", inplace=True)

    for i, (_, r) in enumerate(t10.iterrows()):
        mark = ""
        if prev_idx is not None:
            k = str(r.get(name_today)).strip()
            if k in prev_idx.index and pd.notnull(prev_idx.loc[k, "rank"]).any():
                try:
                    pr = get_scalar_int(prev_idx.loc[k, "rank"])
                    cr = int(r["rank_int"])
                    diff = pr - cr
                    mark = f"(↑{diff}) " if diff>0 else (f"(↓{abs(diff)}) " if diff<0 else "(-) ")
                except Exception:
                    mark = "(New) "
            else:
                mark = "(New) "
        ptxt = f"￥{int(r['price_int']):,}" if pd.notnull(r.get("price_int")) else "￥0"
        link = f"<{r['url']}|{slack_escape(r[name_today])}>"
        lines.append(f"{int(r['rank_int'])}. {mark}{link} — {ptxt}")
        if ko[i]: lines.append(f"    ▶ {slack_escape(ko[i])}")
    S["top10"] = lines

    if prev_idx is None: 
        return S

    cur_idx = df_today.copy()
    cur_idx["__k__"] = cur_idx[name_today].astype(str).str.strip()
    cur_idx.set_index("__k__", inplace=True)

    tN = cur_idx[(cur_idx["rank_int"].notna()) & (cur_idx["rank_int"] <= MAX_RANK)]
    pN = prev_idx[(prev_idx["rank"].notna())]  # prev는 rank 문자열일 수 있음

    # 공통 키 / OUT 키
    common = set(tN.index) & set(pN.index)
    out_only = set(pN.index) - set(tN.index)

    movers = []
    for k in common:
        try:
            pr = get_scalar_int(pN.loc[k, "rank"])
            cr = get_scalar_int(tN.loc[k, "rank_int"])
            drop = cr - pr
            if drop > 0:
                row = tN.loc[k]
                name_k = k
                movers.append((drop, cr, pr, f"- {slack_escape(name_k)} {pr}위 → {cr}위 (↓{drop})"))
        except Exception:
            continue
    movers.sort(key=lambda x: (-x[0], x[1], x[2]))
    chosen = [m[3] for m in movers[:5]]

    if len(chosen) < 5:
        outs = sorted(list(out_only), key=lambda k: extract_int_first(pN.loc[k, "rank"]))
        for k in outs:
            if len(chosen) >= 5: break
            try:
                rk = get_scalar_int(pN.loc[k, "rank"])
                chosen.append(f"- {slack_escape(str(k))} {rk}위 → OUT")
            except Exception:
                pass

    S["falling"] = chosen

    today_keys = set(tN.index); prev_keys = set(pN.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2
    return S

def build_slack_message(date_str: str, S: Dict[str, list]) -> str:
    lines = []
    lines.append(f"*Rakuten Japan 뷰티 랭킹 {MAX_RANK} — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*"); lines.extend(S.get("top10") or ["- 데이터 없음"])
    lines.append(""); lines.append("*📉 급하락*"); lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.append(""); lines.append("*🔄 랭크 인&아웃*"); lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ===== Google Drive =====
def normalize_folder_id(raw: str) -> str:
    if not raw: return ""
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", raw) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", raw)
    return (m.group(1) if m else raw.strip())

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
    fid = files[0]["id"]; req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO(); dl = MediaIoBaseDownload(fh, req); done = False
    while not done: _, done = dl.next_chunk()
    fh.seek(0); return pd.read_csv(fh)

# ===== 메인 =====
def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작(ScraperAPI, render=true)")
    raw_df = fetch_all()
    print(f"[INFO] 크롤 수집: {len(raw_df)} rows")

    date_str = today()
    df_today = normalize_df(raw_df, date_str)

    # CSV 저장
    file_today = build_filename(date_str)
    df_out = df_today[["date","rank_int","product_name","price_int","url","shop","brand"]].rename(columns={
        "rank_int":"rank","price_int":"price"
    })
    df_out.to_csv(os.path.join(DATA_DIR, file_today), index=False, encoding="utf-8-sig")
    print("[INFO] 로컬 CSV 저장:", file_today)

    # Drive 업로드 + 전일 로드
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_out)
            y_name = build_filename(yesterday())
            df_prev = drive_download_csv(svc, folder, y_name)
            print("[INFO] 드라이브 업로드 OK, 전일:", "있음" if (df_prev is not None and not df_prev.empty) else "없음")
        except Exception as e:
            print("[Drive 오류]", e); traceback.print_exc()

    # Slack
    S = build_sections(df_out, df_prev if (df_prev is not None and not df_prev.empty) else None)
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
