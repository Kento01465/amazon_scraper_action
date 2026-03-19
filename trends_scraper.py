"""
trends_scraper.py
─────────────────────────────────────────────────────────
Google Trends の内部 JSON API を ScraperAPI 経由で叩いて
24時間のトレンドスコア（平均値）を取得し Sheets に追記する

JSレンダリング不要 → 安定して取得できる
手順:
  1. /trends/api/explore でトークン取得
  2. /trends/api/widgetdata/multiline でスコア取得
"""

import json
import os
import re
import time
import random
import logging
import requests
import urllib3
import urllib.parse
from datetime import datetime, timezone, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================
# CONFIG
# ==============================

SPREADSHEET_ID  = "1DMxbjF2RfxA7S-Q2sPMnO2A5c7t7wXRdxS2flclPXPw"
TRENDS_SHEET    = "trends"
JAN_SHEET       = "jan_list"
SCRAPER_API_KEY = os.environ["SCRAPER_API_KEY"]

EXTRA_KEYWORDS = [
    "カビ取り剤",
    "洗濯槽クリーナー",
    "風呂釜洗浄",
    "浴槽掃除",
    "配管掃除"
    "クリーンプラネット",
    "大掃除",
    "カビ掃除",
    "洗濯槽 掃除",
    "風呂 掃除",
    "梅雨 カビ対策",
]

RETRY = 3
JST   = timezone(timedelta(hours=9))

# ==============================
# LOGGING
# ==============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# GOOGLE SHEETS AUTH
# ==============================

def connect_sheets():
    creds_json  = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scope       = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client      = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    jan_sheet   = spreadsheet.worksheet(JAN_SHEET)

    try:
        trends_sheet = spreadsheet.worksheet(TRENDS_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        trends_sheet = spreadsheet.add_worksheet(title=TRENDS_SHEET, rows=10000, cols=5)
        trends_sheet.append_row(["timestamp", "keyword", "trend_score"])
        logging.info(f"シート '{TRENDS_SHEET}' を新規作成しました")

    return jan_sheet, trends_sheet

# ==============================
# キーワード収集
# ==============================

def collect_keywords(jan_sheet) -> list:
    rows  = jan_sheet.get_all_values()
    names = []
    for r in rows[1:]:
        if len(r) >= 4 and r[3].strip():
            short = " ".join(r[3].strip().split()[:2])
            if short and short not in names:
                names.append(short)
    keywords = list(dict.fromkeys(names + EXTRA_KEYWORDS))
    logging.info(f"収集キーワード ({len(keywords)}件): {keywords}")
    return keywords

# ==============================
# FETCH: ScraperAPI（レンダリングなし）
# ==============================

def fetch_scraperapi(url, label=""):
    """ScraperAPI 経由で URL を取得（JS レンダリングなし）"""
    session = requests.Session()
    session.verify = False

    for attempt in range(1, RETRY + 1):
        try:
            scraper_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={urllib.parse.quote(url, safe='')}"
                f"&country_code=jp"
                f"&cache=false"
            )
            r = session.get(scraper_url, timeout=60, verify=False)
            if r.status_code == 200 and len(r.text) > 100:
                return r.text
            logging.warning(f"[{label}] HTTP {r.status_code} len={len(r.text)} ({attempt}/{RETRY})")
        except Exception as e:
            logging.warning(f"[{label}] error ({attempt}/{RETRY}): {e}")
        time.sleep(random.uniform(5, 10))

    return None

# ==============================
# Step1: /trends/api/explore でウィジェットトークン取得
# ==============================

def get_widget_token(keyword: str) -> tuple[str | None, str | None]:
    """
    explore エンドポイントからTIMESERIESウィジェットの token と request を取得
    Returns: (token, request_json) or (None, None)
    """
    req_payload = json.dumps([{
        "keyword": keyword,
        "geo": "JP",
        "time": "now 1-d"
    }])
    url = (
        f"https://trends.google.co.jp/trends/api/explore"
        f"?hl=ja&tz=-540"
        f"&req={urllib.parse.quote(req_payload)}"
        f"&type=TIMESERIES&property="
    )

    raw = fetch_scraperapi(url, label=f"{keyword}[explore]")
    if not raw:
        return None, None

    # レスポンスは ")]}'\n{...}" 形式 → 先頭の )]}'\ を除去
    try:
        clean = re.sub(r"^\)\]\}'\n", "", raw.strip())
        data  = json.loads(clean)

        for widget in data.get("widgets", []):
            if widget.get("id") == "TIMESERIES":
                token   = widget.get("token")
                req_str = json.dumps(widget.get("request", {}))
                logging.info(f"[{keyword}] token取得成功: {token[:20]}...")
                return token, req_str

        logging.warning(f"[{keyword}] TIMESERIESウィジェットが見つかりません")
        logging.warning(f"[{keyword}] レスポンス先頭: {raw[:300]}")
        return None, None

    except Exception as e:
        logging.warning(f"[{keyword}] explore JSON パース失敗: {e}")
        logging.warning(f"[{keyword}] レスポンス先頭: {raw[:300]}")
        return None, None

# ==============================
# Step2: /trends/api/widgetdata/multiline でスコア取得
# ==============================

def get_scores_from_widget(keyword: str, token: str, req_str: str) -> int | None:
    """
    multiline エンドポイントから時系列スコアを取得して平均値を返す
    """
    url = (
        f"https://trends.google.co.jp/trends/api/widgetdata/multiline"
        f"?hl=ja&tz=-540"
        f"&req={urllib.parse.quote(req_str)}"
        f"&token={urllib.parse.quote(token)}"
        f"&property="
    )

    raw = fetch_scraperapi(url, label=f"{keyword}[multiline]")
    if not raw:
        return None

    try:
        clean = re.sub(r"^\)\]\}'\n", "", raw.strip())
        data  = json.loads(clean)

        # timelineData から value を抽出
        timeline = data.get("default", {}).get("timelineData", [])
        values   = []
        for point in timeline:
            v = point.get("value", [])
            if v:
                values.append(int(v[0]))

        if not values:
            logging.warning(f"[{keyword}] timelineData が空")
            return None

        avg = round(sum(values) / len(values))
        logging.info(f"[{keyword}] スコア取得成功: avg={avg} (n={len(values)}, max={max(values)})")
        return avg

    except Exception as e:
        logging.warning(f"[{keyword}] multiline JSON パース失敗: {e}")
        logging.warning(f"[{keyword}] レスポンス先頭: {raw[:300]}")
        return None

# ==============================
# 1キーワードのスコアを取得（Step1 + Step2）
# ==============================

def fetch_trend_score(keyword: str) -> int | None:
    # Step1: トークン取得
    token, req_str = get_widget_token(keyword)
    if not token:
        return None

    time.sleep(random.uniform(2, 4))

    # Step2: スコア取得
    return get_scores_from_widget(keyword, token, req_str)

# ==============================
# 全キーワード取得
# ==============================

def fetch_trends(keywords: list) -> dict:
    scores = {}
    for kw in keywords:
        logging.info(f"===== 取得中: {kw} =====")
        scores[kw] = fetch_trend_score(kw)
        logging.info(f"[{kw}] → {scores[kw]}")
        time.sleep(random.uniform(8, 15))
    return scores

# ==============================
# Sheets に追記
# ==============================

def save_trends(trends_sheet, scores: dict):
    today = datetime.now(JST).strftime("%Y/%m/%d")
    rows  = [
        [today, keyword, score]
        for keyword, score in scores.items()
        if score is not None
    ]
    if rows:
        trends_sheet.append_rows(rows)
        logging.info(f"{len(rows)}件 を '{TRENDS_SHEET}' シートに書き込みました")
    else:
        logging.warning("書き込むデータがありませんでした")

# ==============================
# MAIN
# ==============================

def run():
    logging.info(f"=== Google Trends 取得開始: {datetime.now(JST)} ===")
    jan_sheet, trends_sheet = connect_sheets()
    keywords = collect_keywords(jan_sheet)
    scores   = fetch_trends(keywords)
    save_trends(trends_sheet, scores)
    logging.info(f"=== Google Trends 取得完了: {datetime.now(JST)} ===")

if __name__ == "__main__":
    run()
