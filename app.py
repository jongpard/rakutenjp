# app.py — 라쿠텐 재팬 '뷰티/코스메/향수(100939)' 데일리 Top160 (1~80, 81~160)
# 요구사항:
#  - ScraperAPI(country=jp, render=true)로 렌더링된 DOM 수집
#  - 전일 비교 = 제품명 기준(정확 일치)
#  - 변동없음은 '-' 로 표기
#  - "↔ 랭크 인&아웃" 섹션 문구 고정
#  - 슬랙 포맷은 기존 큐텐 포맷 유지

import os, io, re, sys, time, json, math, shutil, datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup

# -----------------------
# 공통 설정
# -----------------------
KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
YMD = TODAY.strftime("%Y-%m-%d")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DBG_DIR  = os.path.join(DATA_DIR, "debug")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

# 입력 URL(1~80, 81~160)
PAGE_URLS = [
    "https://ranking.rakuten.co.jp/daily/100939/",
    "https://ranking.rakuten.co.jp/daily/100939/p=2/",
]

MAX_RANK = int(os.getenv("RAKUTEN_MAX_RANK", "160"))

# Slack & Drive
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "")
TRANSLATE_JA2KO     = os.getenv("SLACK_TRANSLATE_JA2KO", "1") == "1"

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN")
GDRIVE_FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID")

# ScraperAPI
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "").strip()
if not SCRAPERAPI_KEY:
    print("[경고] SCRAPERAPI_KEY 가 설정되지 않았습니다. (render=false 체인으로만 시도)")

SESS = requests.Session()
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
}

def scraperapi_get(url: str, render: bool = True, save_prefix: str = "") -> str:
    """ScraperAPI로 (country=jp) HTML을 가져온다. render=True가 핵심."""
    try:
        if SCRAPERAPI_KEY:
            params = {
                "api_key": SCRAPERAPI_KEY,
                "url": url,
                "country_code": "jp",
                "render": "true" if render else "false",
                "keep_headers": "true",
                "retry_404": "true",
            }
            r = SESS.get("https://api.scraperapi.com/", params=params, headers=DEFAULT_HEADERS, timeout=60)
        else:
            # 키 없으면 best-effort
            r = SESS.get(url, headers=DEFAULT_HEADERS, timeout=60, allow_redirects=True)
        r.raise_for_status()
        html = r.text
        if save_prefix:
            with open(os.path.join(DBG_DIR, f"{save_prefix}.html"), "w", encoding="utf-8") as f:
                f.write(html)
        return html
    except Exception as e:
        print(f"[HTTP 에러] {url} -> {e}")
        return ""

# -----------------------
# 파서(복수 셀렉터 + 백업 정규식)
# -----------------------

SEL_SETS = [
    # 1) 흔한 구조: 카드 루트
    {"card": "div.rnkRanking_item"},
    # 2) 다른 테마: li 단위
    {"card": "li[class*='rnkRanking']"},
    # 3) 백업: data-rnk-*
    {"card": "div[id^='rnkRanking']"},  # 굉장히 느슨한 백업
]

def text(el):
    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""

def pick_one(el, selectors):
    for sel in selectors:
        f = el.select_one(sel)
        if f: return f
    return None

def parse_cards_with_css(soup: BeautifulSoup):
    items = []
    for S in SEL_SETS:
        cards = soup.select(S["card"])
        if not cards:
            continue
        for c in cards:
            # 랭크
            r_el = pick_one(c, [
                ".rnkRanking_rank", ".rnk_rank", ".rank", "[class*='rank']"
            ])
            # 상품명
            name_el = pick_one(c, [
                ".rnkRanking_itemName a", ".itemName a", "a.rnkRanking_itemName", "a"
            ])
            # 가격(있으면)
            price_el = pick_one(c, [
                ".rnkRanking_price", ".price", "[class*='price']"
            ])
            rank = text(r_el)
            name = text(name_el)
            price = text(price_el) if price_el else ""

            # 최소 필터
            if not rank or not name:
                continue

            # rank 텍스트에서 숫자만
            m = re.search(r"\d+", rank)
            if not m:
                continue
            rank_num = int(m.group(0))

            link = ""
            if name_el and name_el.has_attr("href"):
                link = name_el["href"]
                # 상대경로 보정
                if link.startswith("/"):
                    link = "https://ranking.rakuten.co.jp" + link

            items.append({
                "rank": rank_num,
                "name": name,
                "price": price,
                "url": link
            })
        if items:
            break
    return items

# 백업: 정규식으로 rank & 이름을 매칭
RE_RANK = re.compile(r'class="[^"]*rnkRanking_rank[^"]*"[^>]*>\s*([0-9]+)\s*<', re.I)
RE_NAME = re.compile(r'class="[^"]*rnkRanking_itemName[^"]*".*?<a[^>]*>(.*?)</a>', re.I|re.S)

def parse_cards_backup_regex(html: str):
    # 아주 보수적 백업 (순서대로 대응)
    ranks = [int(x) for x in RE_RANK.findall(html)]
    names = [re.sub(r"\s+", " ", re.sub("<.*?>", "", n)).strip() for n in RE_NAME.findall(html)]
    items = []
    for i, r in enumerate(ranks):
        nm = names[i] if i < len(names) else ""
        if nm:
            items.append({"rank": r, "name": nm, "price": "", "url": ""})
    return items

def fetch_one(url: str, prefix: str):
    print(f"[Playwright 대체] GET(렌더): {url}")
    html = scraperapi_get(url, render=True, save_prefix=prefix+"_render")
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    items = parse_cards_with_css(soup)
    if not items:
        # 백업: 정규식
        items = parse_cards_backup_regex(html)
    print(f"[디버그] {url} -> 파싱 {len(items)}건")
    return items

# -----------------------
# 전일 CSV 대비 비교(제품명 기준)
# -----------------------
def load_prev_csv(csv_path_today: str) -> pd.DataFrame:
    # 같은 디렉토리에서 '어제 날짜' 파일 탐색
    d = os.path.dirname(csv_path_today)
    yesterday = (TODAY - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    target = os.path.join(d, f"라쿠텐재팬_뷰티_랭킹_{yesterday}.csv")
    if os.path.exists(target):
        try:
            return pd.read_csv(target)
        except:
            pass
    return pd.DataFrame()

def build_sections(df_today: pd.DataFrame, df_prev: pd.DataFrame):
    # 랭크 변동: 오늘.rank - 어제.rank (제품명 매칭)
    m = df_today[["rank","name","url","price"]].copy()
    m["prev_rank"] = None
    if not df_prev.empty:
        prev_map = {n: r for r, n in zip(df_prev["rank"], df_prev["name"])}
        m["prev_rank"] = m["name"].map(prev_map)

    def rank_delta(row):
        pr = row["prev_rank"]
        if pd.isna(pr): return None
        try:
            return int(pr) - int(row["rank"])
        except: return None

    m["delta"] = m.apply(rank_delta, axis=1)

    # 변동 텍스트: ↑/↓/-
    def arrow(d):
        if d is None: return "-"  # 전일 없음도 '-' 처리(요청사항: 변동없음 표기)
        if d > 0: return f"↑{abs(d)}"
        if d < 0: return f"↓{abs(d)}"
        return "-"

    m["delta_txt"] = m["delta"].apply(arrow)

    # Top10 텍스트 (raw 제품명 그대로)
    top10 = (m.sort_values("rank").head(10))[["rank","delta_txt","name"]].values.tolist()
    # IN & OUT: 집합 차이
    inout = 0
    if not df_prev.empty:
        tset = set(m["name"])
        pset = set(df_prev["name"])
        ins  = tset - pset
        outs = pset - tset
        # 네가 정의한 텍스트 규칙: "인/아웃 개수는 동일" → 보고는 x개
        inout = max(len(ins), len(outs))

    return m, top10, inout

# -----------------------
# Slack 전송
# -----------------------
def slack_post(lines):
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 미설정 — 메시지 미전송")
        return
    payload = {"text": "\n".join(lines)}
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        r.raise_for_status()
        print("[INFO] 슬랙 전송 OK")
    except Exception as e:
        print("[경고] 슬랙 전송 실패:", e)

# -----------------------
# Google Drive 업로드(선택)
# -----------------------
def upload_gdrive(local_path: str):
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        print("[INFO] 드라이브 업로드 건너뜀(시크릿 없음)")
        return
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
        )
        service = build("drive", "v3", credentials=creds)
        file_metadata = {"name": os.path.basename(local_path), "parents": [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(local_path, resumable=True)
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print("[INFO] 드라이브 업로드 OK:", os.path.basename(local_path))
    except Exception as e:
        print("[경고] 드라이브 업로드 실패:", e)

# -----------------------
# 메인
# -----------------------
def main():
    print("[INFO] 라쿠텐 뷰티 랭킹 수집 시작")
    all_rows = []
    for i, url in enumerate(PAGE_URLS, start=1):
        rows = fetch_one(url, prefix=f"rakuten_p{i}")
        all_rows.extend(rows)

    # 정리/필터
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["name"]).reset_index(drop=True)
    df = df[(df["rank"] >= 1) & (df["rank"] <= MAX_RANK)]
    df = df.sort_values("rank").reset_index(drop=True)

    print(f"[INFO] 수집 개수: {len(df)}")

    # CSV 저장
    csv_path = os.path.join(DATA_DIR, f"라쿠텐재팬_뷰티_랭킹_{YMD}.csv")
    df_out = df[["rank","name","price","url"]].copy()
    df_out.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print("[INFO] 로컬 CSV 저장:", os.path.basename(csv_path))

    # 전일 비교
    df_prev = load_prev_csv(csv_path)
    m, top10, inout_cnt = build_sections(df_out, df_prev)

    # 슬랙 메시지
    title = f"📊 일간 리포트 · 라쿠텐JP 뷰티 Top160 ({YMD})"
    lines = [f"*{title}*"]
    # Top10
    lines.append("\n*🏆 Top10 (일간, raw 제품명)*")
    for r, dtxt, name in top10:
        lines.append(f"{r:>3}위 | {dtxt} | {name}")

    # 인&아웃
    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{inout_cnt}개의 제품이 인&아웃 되었습니다.")

    slack_post(lines)

    # 드라이브 업로드(옵션)
    upload_gdrive(csv_path)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e)
        raise
