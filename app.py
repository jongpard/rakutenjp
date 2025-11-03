# -*- coding: utf-8 -*-
"""
Rakuten Japan Beauty / Cosmetics / Fragrance (デイリー)
- 대상:
    1) https://ranking.rakuten.co.jp/daily/100939/         (1~80위)
    2) https://ranking.rakuten.co.jp/daily/100939/p=2/     (81~160위)
- CSV: 라쿠텐재팬_뷰티_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: item.rakuten.co.jp 상품 URL (쿼리/프래그먼트 제거)
- 슬랙 포맷: 큐텐 버전과 동일하되,
    → 전일 대비 순위변동이 0이면 "( - )" 가 아니라 너가 말한 그대로 "(-)" 로 표기
- 수집 상한: 160위
"""

import os, re, io, math, pytz, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- Config ----------
KST = pytz.timezone("Asia/Seoul")

RAKUTEN_URLS = [
    "https://ranking.rakuten.co.jp/daily/100939/",        # 1~80
    "https://ranking.rakuten.co.jp/daily/100939/p=2/",     # 81~160
]

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))  # ← 기본 160위까지 수집

# ---------- time/utils ----------
def now_kst(): return dt.datetime.now(KST)
def today_kst_str(): return now_kst().strftime("%Y-%m-%d")
def yesterday_kst_str(): return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
def build_filename(d): return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"
def clean_text(s): return re.sub(r"\s+", " ", (s or "")).strip()
def slack_escape(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ---------- '公式' 제거 / 괄호 제거 ----------
OFFICIAL_PAT = re.compile(r"^\s*(公式|公式ショップ|公式ストア|楽天市場店)\s*", re.I)
BRACKETS_PAT = re.compile(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))")

JP_CHAR_RE = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
def contains_japanese(s: str) -> bool:
    return bool(JP_CHAR_RE.search(s or ""))

def remove_official_token(s: str) -> str:
    if not s: return ""
    s = clean_text(s)
    s = OFFICIAL_PAT.sub("", s)
    return s

def strip_brackets_for_slack(s: str) -> str:
    if not s: return ""
    return clean_text(BRACKETS_PAT.sub("", s))

# ---------- price ----------
YEN_AMOUNT_RE = re.compile(r"(\d[\d,]*)\s*円")

def parse_price_from_text(txt: str) -> Optional[int]:
    if not txt: return None
    m = YEN_AMOUNT_RE.search(txt)
    if not m: return None
    return int(m.group(1).replace(",", ""))

# ---------- model ----------
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    title: str
    price: Optional[int]
    orig_price: Optional[int]
    discount_percent: Optional[int]
    url: str
    product_code: str = ""   # 라쿠텐은 사실상 URL로 비교

# ---------- Rakuten HTML 파서 ----------
ITEM_LINK_RE = re.compile(r"https?://item\.rakuten\.co\.jp/", re.I)
SHOP_LINK_RE = re.compile(r"https?://www\.rakuten\.co\.jp/", re.I)

def normalize_rakuten_url(href: str) -> str:
    if not href: return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    # 쿼리/프래그먼트 날려서 비교 키로 쓰기
    href = re.sub(r"[?#].*$", "", href)
    return href

def parse_rakuten_html(html: str, start_rank: int) -> List[Product]:
    """
    한 페이지(1~80 또는 81~160)를 파싱해서 Product 리스트로 반환
    - 기준: 문서 안에서 item.rakuten.co.jp 링크가 등장하는 순서를 그대로 믿는다
    - 각 상품 링크 뒤에 나오는 www.rakuten.co.jp 링크를 브랜드/스토어로 친다
    - 그 뒤에 나오는 '...円' 텍스트를 가격으로 본다
    """
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=ITEM_LINK_RE)
    items: List[Product] = []
    seen = set()
    cur_rank = start_rank

    for a in anchors:
        href = normalize_rakuten_url(a.get("href", ""))
        if not href:
            continue
        if href in seen:
            continue
        seen.add(href)

        name = remove_official_token(a.get_text(" ", strip=True))

        # 브랜드/샵: 같은 블록에서 다음에 나오는 www.rakuten.co.jp 링크
        brand = ""
        shop_a = a.find_next("a", href=SHOP_LINK_RE)
        if shop_a:
            brand = remove_official_token(shop_a.get_text(" ", strip=True))

        # 가격: 다음에 나오는 '円' 포함 텍스트
        price = None
        price_node = a.find_next(string=re.compile(r"円"))
        if price_node:
            price = parse_price_from_text(price_node)

        items.append(Product(
            rank=cur_rank,
            brand=brand,
            title=name,
            price=price,
            orig_price=None,
            discount_percent=None,
            url=href,
            product_code="",  # 라쿠텐은 URL키
        ))
        cur_rank += 1
        if cur_rank > start_rank + 80 - 1:
            break

    return items

# ---------- fetchers ----------
def fetch_by_http_rakuten() -> List[Product]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
    }
    all_items: List[Product] = []
    start_rank = 1
    for url in RAKUTEN_URLS:
        try:
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            part = parse_rakuten_html(r.text, start_rank)
            print(f"[HTTP] {url} → {len(part)}개")
            all_items.extend(part)
            start_rank += 80
        except Exception as e:
            print("[HTTP 라쿠텐 오류]", url, e)
    return all_items[:MAX_RANK]

def fetch_by_playwright_rakuten() -> List[Product]:
    """
    HTTP 파싱이 실패했을 때만 폴백
    """
    from playwright.sync_api import sync_playwright
    all_items: List[Product] = []
    start_rank = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width":1366,"height":900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"),
            extra_http_headers={"Accept-Language":"ja,en-US;q=0.9,en;q=0.8,ko;q=0.7"},
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = context.new_page()

        for url in RAKUTEN_URLS:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                page.wait_for_load_state("networkidle", timeout=25_000)
            except:
                pass

            data = page.evaluate("""
                () => {
                  const as = Array.from(document.querySelectorAll("a[href*='item.rakuten.co.jp/']"));
                  const rows = [];
                  const seen = new Set();
                  for (const a of as) {
                    let href = a.getAttribute('href') || '';
                    if (!href) continue;
                    if (href.startsWith('//')) href = 'https:' + href;
                    href = href.replace(/[?#].*$/, '');
                    if (seen.has(href)) continue;
                    seen.add(href);
                    const name = (a.textContent || '').replace(/\\s+/g,' ').trim();

                    // 브랜드 후보: 다음에 나오는 www.rakuten.co.jp 링크
                    let brand = '';
                    const shop = a.closest('li,div,section,article') || a.parentElement;
                    let nextA = a.nextElementSibling;
                    let found = null;
                    while (nextA) {
                      const h = (nextA.getAttribute && nextA.getAttribute('href')) || '';
                      if (h && h.includes('www.rakuten.co.jp')) { found = nextA; break; }
                      nextA = nextA.nextElementSibling;
                    }
                    if (found) {
                      brand = (found.textContent || '').replace(/\\s+/g,' ').trim();
                    }

                    // 가격: a 이후의 텍스트에서 '円' 찾기
                    let priceText = '';
                    let node = a.nextSibling;
                    while (node) {
                      if (node.nodeType === Node.TEXT_NODE) {
                        const t = node.textContent.trim();
                        if (t.includes('円')) { priceText = t; break; }
                      }
                      node = node.nextSibling;
                    }

                    rows.push({href, name, brand, priceText});
                  }
                  return rows;
                }
            """)
            for row in data:
                href = row.get("href","")
                name = remove_official_token(row.get("name",""))
                brand = remove_official_token(row.get("brand",""))
                price = parse_price_from_text(row.get("priceText","") or "")
                all_items.append(Product(
                    rank=start_rank,
                    brand=brand,
                    title=name,
                    price=price,
                    orig_price=None,
                    discount_percent=None,
                    url=href,
                    product_code="",
                ))
                start_rank += 1

        context.close(); browser.close()

    return all_items[:MAX_RANK]

def fetch_products() -> List[Product]:
    items = fetch_by_http_rakuten()
    if len(items) >= 50:   # 1페이지만이라도 잘 나오면 그걸로 끝
        return items
    print("[Playwright 폴백 진입 - Rakuten]")
    return fetch_by_playwright_rakuten()

# ---------- Drive ----------
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
        raise RuntimeError("OAuth 자격정보가 없습니다.")
    creds = Credentials(None, refresh_token=rtk, token_uri="https://oauth2.googleapis.com/token",
                        client_id=cid, client_secret=csec)
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        about = svc.about().get(fields="user(displayName,emailAddress)").execute()
        u = about.get("user", {})
        print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
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
        service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
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

# ---------- Slack / translate ----------
def fmt_currency_jpy(v) -> str:
    try: return f"￥{int(round(float(v))):,}"
    except: return "￥0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text); return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "0").lower() in ("1", "true", "yes")
    texts = [(l or "").strip() for l in lines]
    if not flag or not texts:
        print("[Translate] OFF")
        return ["" for _ in texts]

    seg_lists: List[Optional[List[Tuple[str, str]]]] = []
    ja_pool: List[str] = []
    ja_run = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]+")

    for line in texts:
        if not contains_japanese(line):
            seg_lists.append(None)
            continue
        parts: List[Tuple[str, str]] = []
        last = 0
        for m in ja_run.finditer(line):
            if m.start() > last:
                parts.append(("raw", line[last:m.start()]))
            parts.append(("ja", line[m.start():m.end()]))
            last = m.end()
        if last < len(line):
            parts.append(("raw", line[last:]))
        seg_lists.append(parts)
        for kind, txt in parts:
            if kind == "ja":
                ja_pool.append(txt)

    if not ja_pool:
        return ["" for _ in texts]

    def _translate_batch(src_list: List[str]) -> List[str]:
        try:
            from googletrans import Translator
            tr = Translator(service_urls=['translate.googleapis.com'])
            res = tr.translate(src_list, src="ja", dest="ko")
            return [getattr(r, "text", "") or "" for r in (res if isinstance(res, list) else [res])]
        except Exception as e1:
            print("[Translate] googletrans 실패:", e1)
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source='ja', target='ko')
            return [gt.translate(t) if t else "" for t in src_list]
        except Exception as e2:
            print("[Translate] deep-translator 실패:", e2)
            return ["" for _ in src_list]

    ja_translated = _translate_batch(ja_pool)

    out: List[str] = []
    it = iter(ja_translated)
    for parts in seg_lists:
        if parts is None:
            out.append("")
            continue
        buf = []
        for kind, txt in parts:
            val = txt if kind == "raw" else next(it, "")
            if val is None:
                val = ""
            buf.append(str(val))
        out.append("".join(buf))

    print(f"[Translate] done (JA-only): {sum(1 for x in out if x)} lines")
    return out

# ---------- DF 변환 ----------
def to_dataframe(products: List[Product], date_str: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": date_str,
        "rank": p.rank,
        "brand": p.brand,
        "product_name": p.title,
        "price": p.price,
        "orig_price": p.orig_price,
        "discount_percent": p.discount_percent,
        "url": p.url,
        "product_code": p.product_code,
    } for p in products])

# ---------- Slack 섹션 ----------
def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "falling": [], "inout_count": 0}

    def _plain_name(row):
        nm = strip_brackets_for_slack(clean_text(row.get("product_name", "")))
        br = clean_text(row.get("brand", ""))
        if br and not nm.lower().startswith(br.lower()):
            nm = f"{br} {nm}"
        return nm

    def _link(row):
        return f"<{row['url']}|{slack_escape(_plain_name(row))}>"

    def _interleave(lines, jp_texts):
        kos = translate_ja_to_ko_batch(jp_texts)
        out = []
        for i, ln in enumerate(lines):
            out.append(ln)
            if kos and i < len(kos) and kos[i]:
                out.append(kos[i])
        return out

    # ----- 전일 인덱스 -----
    prev_index = None
    if df_prev is not None and len(df_prev):
        prev_index = df_prev.copy()
        prev_index["__key__"] = prev_index.apply(
            lambda x: (str(x.get("url")).strip()),
            axis=1
        )
        prev_index.set_index("__key__", inplace=True)

    # ----- TOP 10 -----
    jp_rows, lines = [], []
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        jp_rows.append(_plain_name(r))
        marker = ""
        if prev_index is not None:
            key = str(r.get("url")).strip()
            if key in prev_index.index and pd.notnull(prev_index.loc[key, "rank"]):
                pr, cr = int(prev_index.loc[key, "rank"]), int(r["rank"])
                d = pr - cr
                if d > 0:
                    marker = f"(↑{d}) "
                elif d < 0:
                    marker = f"(↓{abs(d)}) "
                else:
                    # 💡 여기서 네가 요청한 '순위변동 없으면 - 로 표기'
                    marker = "(-) "
            else:
                marker = "(New) "
        # 가격/할인
        tail = ""
        if pd.notnull(r.get("price")):
            tail = f"{fmt_currency_jpy(r.get('price'))}"
        else:
            tail = "￥0"
        lines.append(f"{int(r['rank'])}. {marker}{_link(r)} — {tail}")
    S["top10"] = _interleave(lines, jp_rows)

    if prev_index is None:
        return S

    # ----- 급하락 (Top160 기준, OUT 포함) -----
    cur_index = df_today.copy()
    cur_index["__key__"] = cur_index.apply(lambda x: str(x.get("url")).strip(), axis=1)
    cur_index.set_index("__key__", inplace=True)

    tN = cur_index[(cur_index["rank"].notna()) & (cur_index["rank"] <= MAX_RANK)]
    pN = prev_index[(prev_index["rank"].notna()) & (prev_index["rank"] <= MAX_RANK)]

    common = set(tN.index) & set(pN.index)
    out_keys = set(pN.index) - set(tN.index)

    movers = []
    for k in common:
        pr, cr = int(pN.loc[k, "rank"]), int(tN.loc[k, "rank"])
        drop = cr - pr
        if drop > 0:
            row = tN.loc[k]
            movers.append((drop, cr, pr, f"- {_link(row)} {pr}위 → {cr}위 (↓{drop})", _plain_name(row)))

    movers.sort(key=lambda x: (-x[0], x[1], x[2], x[4]))

    chosen_lines, chosen_jp = [], []
    for _, _, _, txt, jpn in movers:
        if len(chosen_lines) >= 5:
            break
        chosen_lines.append(txt)
        chosen_jp.append(jpn)

    # OUT 보충
    if len(chosen_lines) < 5:
        outs_sorted = sorted(list(out_keys), key=lambda k: int(pN.loc[k, "rank"]))
        for k in outs_sorted:
            if len(chosen_lines) >= 5:
                break
            row = pN.loc[k]
            txt = f"- {_link(row)} {int(row['rank'])}위 → OUT"
            chosen_lines.append(txt)
            chosen_jp.append(_plain_name(row))

    S["falling"] = _interleave(chosen_lines, chosen_jp)

    # ----- 인&아웃 -----
    today_keys = set(tN.index)
    prev_keys  = set(pN.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2

    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    lines: List[str] = []
    lines.append(f"*Rakuten Japan 뷰티 랭킹 {MAX_RANK} — {date_str}*")
    lines.append("")
    lines.append("*TOP 10*")
    lines.extend(S.get("top10") or ["- 데이터 없음"])
    lines.append("")
    lines.append("*📉 급하락*")
    lines.extend(S.get("falling") or ["- 해당 없음"])
    lines.append("")
    lines.append("*🔄 랭크 인&아웃*")
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- main ----------
def main():
    date_str = today_kst_str()
    ymd_yesterday = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_yesterday = build_filename(ymd_yesterday)

    print("라쿠텐 랭킹 수집 시작:", RAKUTEN_URLS[0])
    items = fetch_products()
    print("수집 완료:", len(items))
    if len(items) < 20:
        raise RuntimeError("라쿠텐 제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(items, date_str)
    os.makedirs("data", exist_ok=True)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("로컬 저장:", file_today)

    # Google Drive
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID",""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("Google Drive 업로드 완료:", file_today)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            print("전일 CSV", "미발견" if df_prev is None else "성공")
        except Exception as e:
            print("Google Drive 처리 오류:", e)
            traceback.print_exc()
    else:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일 비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e); traceback.print_exc()
        try:
            slack_post(f"*라쿠텐 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except: pass
        raise
