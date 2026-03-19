"""
trends_scraper.py
─────────────────────────────────────────────────────────
Google Trends の「インタレストの平均値（24時間）」を
ScraperAPI の JS レンダリング経由で画面から直接取得し
Sheets の "trends" シートに追記する
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
from bs4 import BeautifulSoup
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
# FETCH: ScraperAPI + JS レンダリング
# ==============================

def fetch_scraperapi(url, label=""):
    session = requests.Session()
    session.verify = False

    for attempt in range(1, RETRY + 1):
        try:
            scraper_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={urllib.parse.quote(url, safe='')}"  # URLをまるごとエンコード
                f"&country_code=jp"
                f"&render=true"
                f"&wait=5000"    # JS描画完了まで5秒待つ
                f"&cache=false"
            )
            r = session.get(scraper_url, timeout=90, verify=False)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
            logging.warning(f"[{label}] HTTP {r.status_code} len={len(r.text)} ({attempt}/{RETRY})")
        except Exception as e:
            logging.warning(f"[{label}] error ({attempt}/{RETRY}): {e}")
        time.sleep(random.uniform(5, 10))

    return None

# ==============================
# Google Trends ページから平均値を抽出
#
# 取得するページ:
# https://trends.google.co.jp/trends/explore?q=カビ掃除&geo=JP&hl=ja&date=now+1-d
#
# 右側に表示される「インタレストの平均値」を抜く
# スクリーンショットで見えていた 2, 1, 0, 0, 0, 0 の数値
# ==============================

def parse_trend_score(html: str, keyword: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")

    # ── 方法1: summary-value クラス（平均値表示エリア）──────────
    # 右側の「インタレストの平均値」の数字
    for el in soup.select(".summary-value, .summary-value-group, [class*='summary']"):
        text = el.get_text(strip=True)
        if re.match(r"^\d+$", text):
            score = int(text)
            logging.info(f"[{keyword}] summary-value から取得: {score}")
            return score

    # ── 方法2: script タグ内の JSON データ ──────────────────────
    # Google Trends はデータを <script> 内に埋め込む場合がある
    for script in soup.find_all("script"):
        text = script.string or ""
        # "averages":[数値] パターン
        m = re.search(r'"averages"\s*:\s*\[(\d+)', text)
        if m:
            score = int(m.group(1))
            logging.info(f"[{keyword}] script[averages] から取得: {score}")
            return score
        # "value":[数値] パターン
        m = re.search(r'"value"\s*:\s*\[(\d+)\]', text)
        if m:
            score = int(m.group(1))
            logging.info(f"[{keyword}] script[value] から取得: {score}")
            return score

    # ── 方法3: widgets データから直接 ────────────────────────────
    for script in soup.find_all("script"):
        text = script.string or ""
        if "TIMESERIES" in text or "interestOverTime" in text:
            numbers = re.findall(r'"value"\s*:\s*(\d+)', text)
            if numbers:
                vals  = [int(n) for n in numbers]
                avg   = round(sum(vals) / len(vals))
                logging.info(f"[{keyword}] TIMESERIES から取得: avg={avg} (n={len(vals)})")
                return avg

    # ── デバッグ: 取れなかった場合にHTMLの一部をログ出力 ─────────
    logging.warning(f"[{keyword}] スコア抽出失敗。HTML先頭500文字: {html[:500]}")
    return None

# ==============================
# 1キーワードのスコアを取得
# ==============================

def fetch_trend_score(keyword: str) -> int | None:
    # URLはエンコードせずそのまま渡す（fetch_scraperapi内でまとめてエンコード）
    url = (
        f"https://trends.google.co.jp/trends/explore"
        f"?q={keyword}&geo=JP&hl=ja&date=now+1-d"
    )
    logging.info(f"[{keyword}] URL: {url}")

    html = fetch_scraperapi(url, label=keyword)
    if not html:
        logging.warning(f"[{keyword}] HTML取得失敗")
        return None

    return parse_trend_score(html, keyword)

# ==============================
# 全キーワード取得
# ==============================

def fetch_trends(keywords: list) -> dict:
    scores = {}
    for kw in keywords:
        logging.info(f"===== 取得中: {kw} =====")
        scores[kw] = fetch_trend_score(kw)
        logging.info(f"[{kw}] → {scores[kw]}")
        time.sleep(random.uniform(8, 15))  # scraper_pro.py と同じ間隔
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
