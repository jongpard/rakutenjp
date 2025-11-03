# -*- coding: utf-8 -*-
"""
Rakuten Japan Beauty / Cosmetics / Fragrance (デイリー) 스크래퍼
대상:
  1) https://ranking.rakuten.co.jp/daily/100939/        (1~80위)
  2) https://ranking.rakuten.co.jp/daily/100939/p=2/    (81~160위)

특징:
- GitHub Actions에서 requests로 접근하면 403 뜨므로 → Playwright만 강제
- DOM 구조는 li.rnkRanking_item 기준으로 파싱
- 슬랙 포맷은 네가 쓰던 큐텐 포맷 그대로
  → 단, 순위 변동 0일 때 "(-)" 강제
- CSV 이름: 라쿠텐재팬_뷰티_랭킹_YYYY-MM-DD.csv
- 전일 CSV 있으면 랭크 인&아웃 계산해서 슬랙에 같이 보냄
"""

import os, re, io, traceback
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional

import pandas as pd
import pytz
import requests  # 슬랙만 여기서 씀

# ---------- 기본 설정 ----------
KST = pytz.timezone("Asia/Seoul")

RAKUTEN_URLS = [
    "https://ranking.rakuten.co.jp/daily/100939/",       # 1~80
    "https://ranking.rakuten.co.jp/daily/100939/p=2/",    # 81~160
]

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))  # 기본 160

# ---------- 공통 유틸 ----------
def now_kst():
    return dt.datetime.now(KST)

def today_kst_str():
    return now_kst().strftime("%Y-%m-%d")

def yesterday_kst_str():
    return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

def build_filename(d: str) -> str:
    return f"라쿠텐재팬_뷰티_랭킹_{d}.csv"

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def slack_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

YEN_AMOUNT_RE = re.compile(r"(\d[\d,]*)\s*円")

def parse_price_from_text(txt: str) -> Optional[int]:
    if not txt:
        return None
    m = YEN_AMOUNT_RE.search(txt)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))

# ---------- 모델 ----------
@dataclass
class Product:
    rank: int
    brand: str
    title: str
    price: Optional[int]
    orig_price: Optional[int]
    discount_percent: Optional[int]
    url: str
    product_code: str = ""  # 라쿠텐은 사실상 URL이 키

# ---------- Playwright 수집 ----------
def normalize_rakuten_url(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    # 쿼리, 프래그먼트 제거
    href = re.sub(r"[?#].*$", "", href)
    return href

def fetch_by_playwright_rakuten() -> List[Product]:
    """
    라쿠텐은 GitHub Actions에서 requests로 403이 떠서
    무조건 Playwright로 DOM을 읽어온다.
    """
    from playwright.sync_api import sync_playwright

    all_items: List[Product] = []
    start_rank = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
            },
        )
        # webdriver 값 감추기
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        page = context.new_page()

        for url in RAKUTEN_URLS:
            print("[Playwright] 페이지 진입:", url)
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            try:
                page.wait_for_load_state("networkidle", timeout=25_000)
            except Exception:
                pass

            # 여기서 실제 DOM 파싱
            elements = page.query_selector_all("li.rnkRanking_item")
            print(f"[Playwright] 발견된 상품 블록 수: {len(elements)}")

            for el in elements:
                try:
                    # 순위
                    rank_el = el.query_selector(".rnkRanking_rank")
                    # 페이지에 순위가 박혀있으면 그걸 쓰고, 없으면 start_rank
                    if rank_el:
                        try:
                            rank_val = int(clean_text(rank_el.inner_text()))
                        except Exception:
                            rank_val = start_rank
                    else:
                        rank_val = start_rank

                    # 제품명
                    name_a = el.query_selector(".rnkRanking_itemName a")
                    title = clean_text(name_a.inner_text()) if name_a else ""

                    # URL
                    href = name_a.get_attribute("href") if name_a else ""
                    href = normalize_rakuten_url(href)

                    # 브랜드/샵
                    shop_a = el.query_selector(".rnkRanking_itemShop a")
                    brand = clean_text(shop_a.inner_text()) if shop_a else ""

                    # 가격
                    price_span = el.query_selector(".rnkRanking_itemPrice span.important")
                    price_txt = clean_text(price_span.inner_text()) if price_span else ""
                    price = parse_price_from_text(price_txt)

                    all_items.append(Product(
                        rank=rank_val,
                        brand=brand,
                        title=title,
                        price=price,
                        orig_price=None,
                        discount_percent=None,
                        url=href,
                        product_code="",  # URL이 키
                    ))
                    start_rank += 1
                    if start_rank > MAX_RANK:
                        break
                except Exception as e:
                    print("[Playwright][item skip]", e)
            if start_rank > MAX_RANK:
                break

        context.close()
        browser.close()

    return all_items[:MAX_RANK]

def fetch_products() -> List[Product]:
    # 💡 HTTP는 아예 시도하지 않고 바로 Playwright만 씀
    print("[Rakuten] Playwright 강제 수집 모드")
    items = fetch_by_playwright_rakuten()
    return items

# ---------- Google Drive ----------
def normalize_folder_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{10,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    cid = os.getenv("GOOGLE_CLIENT_ID")
    csec = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk = os.getenv("GOOGLE_REFRESH_TOKEN")

    if not (cid and csec and rtk):
        raise RuntimeError("Google Drive OAuth 시크릿이 없습니다.")

    creds = Credentials(
        None,
        refresh_token=rtk,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
    )
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return svc

def drive_upload_csv(service, folder_id: str, name: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload

    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)

    if file_id:
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        return file_id

    meta = {
        "name": name,
        "parents": [folder_id],
        "mimeType": "text/csv",
    }
    created = service.files().create(
        body=meta,
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, name: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload

    res = service.files().list(
        q=f"name = '{name}' and '{folder_id}' in parents and trashed = false",
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if not files:
        return None
    fid = files[0]["id"]

    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO()
    dl = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

# ---------- 번역 (일본어 → 한국어 1줄) ----------
JP_CHAR_RE = re.compile(r"[\u3040-\u30FF\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")

def contains_japanese(s: str) -> bool:
    return bool(JP_CHAR_RE.search(s or ""))

def translate_ja_to_ko_batch(lines: List[str]) -> List[str]:
    flag = os.getenv("SLACK_TRANSLATE_JA2KO", "0").lower() in ("1", "true", "yes")
    texts = [(l or "").strip() for l in lines]
    if not flag:
        return ["" for _ in texts]

    # 일본어만 뽑기
    ja_only = [t for t in texts if contains_japanese(t)]
    if not ja_only:
        return ["" for _ in texts]

    def _try_translate(srcs: List[str]) -> List[str]:
        # 1차: googletrans
        try:
            from googletrans import Translator
            tr = Translator(service_urls=["translate.googleapis.com"])
            res = tr.translate(srcs, src="ja", dest="ko")
            if not isinstance(res, list):
                res = [res]
            return [r.text for r in res]
        except Exception as e:
            print("[Translate] googletrans 실패:", e)
        # 2차: deep-translator
        try:
            from deep_translator import GoogleTranslator as DT
            gt = DT(source="ja", target="ko")
            return [gt.translate(t) if t else "" for t in srcs]
        except Exception as e:
            print("[Translate] deep-translator 실패:", e)
        return ["" for _ in srcs]

    ja_trans = _try_translate(ja_only)
    ja_iter = iter(ja_trans)

    out = []
    for t in texts:
        if contains_japanese(t):
            out.append(next(ja_iter, ""))
        else:
            out.append("")
    return out

# ---------- DF ----------
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

# ---------- Slack ----------
def fmt_currency_jpy(v) -> str:
    try:
        return f"￥{int(v):,}"
    except Exception:
        return "￥0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[Slack 미설정] 메시지:\n", text)
        return
    r = requests.post(url, json={"text": text}, timeout=20)
    if r.status_code >= 300:
        print("[Slack 실패]", r.status_code, r.text)

def strip_brackets_for_slack(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"(\[.*?\]|【.*?】|（.*?）|\(.*?\))", "", s).strip()

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

    # 전일 인덱스
    prev_index = None
    if df_prev is not None and len(df_prev):
        prev_index = df_prev.copy()
        prev_index["__key__"] = prev_index["url"].astype(str).str.strip()
        prev_index.set_index("__key__", inplace=True)

    # TOP10
    jp_rows, lines = [], []
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    for _, r in top10.iterrows():
        jp_rows.append(_plain_name(r))
        marker = ""
        if prev_index is not None:
            key = str(r.get("url")).strip()
            if key in prev_index.index and pd.notnull(prev_index.loc[key, "rank"]):
                pr = int(prev_index.loc[key, "rank"])
                cr = int(r["rank"])
                diff = pr - cr
                if diff > 0:
                    marker = f"(↑{diff}) "
                elif diff < 0:
                    marker = f"(↓{abs(diff)}) "
                else:
                    marker = "(-) "
            else:
                marker = "(New) "
        price_txt = fmt_currency_jpy(r.get("price")) if pd.notnull(r.get("price")) else "￥0"
        lines.append(f"{int(r['rank'])}. {marker}{_link(r)} — {price_txt}")

    S["top10"] = _merge_translation(lines, jp_rows)

    if prev_index is None:
        return S

    # 현재 인덱스
    cur_index = df_today.copy()
    cur_index["__key__"] = cur_index["url"].astype(str).str.strip()
    cur_index.set_index("__key__", inplace=True)

    tN = cur_index[(cur_index["rank"].notna()) & (cur_index["rank"] <= MAX_RANK)]
    pN = prev_index[(prev_index["rank"].notna()) & (prev_index["rank"] <= MAX_RANK)]

    # 급하락
    common = set(tN.index) & set(pN.index)
    out_keys = set(pN.index) - set(tN.index)
    movers = []
    for k in common:
        pr = int(pN.loc[k, "rank"])
        cr = int(tN.loc[k, "rank"])
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

    S["falling"] = _merge_translation(chosen_lines, chosen_jp)

    # 인&아웃 개수
    today_keys = set(tN.index)
    prev_keys = set(pN.index)
    S["inout_count"] = len(today_keys ^ prev_keys) // 2

    return S

def _merge_translation(lines: List[str], jp_texts: List[str]) -> List[str]:
    kos = translate_ja_to_ko_batch(jp_texts)
    out = []
    for i, ln in enumerate(lines):
        out.append(ln)
        if kos and i < len(kos) and kos[i]:
            out.append(kos[i])
    return out

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
    lines.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# ---------- main ----------
def main():
    date_str = today_kst_str()
    yesterday_str = yesterday_kst_str()

    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    products = fetch_products()
    print("[INFO] 수집 개수:", len(products))

    if len(products) < 20:
        raise RuntimeError("라쿠텐 제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_dataframe(products, date_str)

    os.makedirs("data", exist_ok=True)
    file_today = build_filename(date_str)
    df_today.to_csv(os.path.join("data", file_today), index=False, encoding="utf-8-sig")
    print("[INFO] 로컬 CSV 저장:", file_today)

    # 전일 데이터
    df_prev = None
    folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID", ""))
    if folder:
        try:
            svc = build_drive_service()
            drive_upload_csv(svc, folder, file_today, df_today)
            print("[INFO] Google Drive 업로드 완료:", file_today)

            file_yesterday = build_filename(yesterday_str)
            df_prev = drive_download_csv(svc, folder, file_yesterday)
            if df_prev is None:
                print("[INFO] 전일 CSV 미발견:", file_yesterday)
            else:
                print("[INFO] 전일 CSV 로드 성공:", file_yesterday)
        except Exception as e:
            print("[Drive 오류]", e)
            traceback.print_exc()
    else:
        print("[INFO] GDRIVE_FOLDER_ID 미설정 → 드라이브 업로드/전일비교 생략")

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("[INFO] Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e)
        traceback.print_exc()
        try:
            slack_post(f"*라쿠텐 뷰티 랭킹 자동화 실패*\n```\n{e}\n```")
        except Exception:
            pass
        raise
