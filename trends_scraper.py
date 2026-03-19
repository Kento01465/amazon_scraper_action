"""
trends_scraper.py
─────────────────────────────────────────────────────────
Google Trends の検索スコアを ScraperAPI 経由で取得して
Sheets の "trends" シートに追記する

pytrends の 429 問題を回避するため ScraperAPI を使用
scraper_pro.py と同じ認証・fetch パターン
"""

import json
import os
import re
import time
import random
import logging
import requests
import urllib3
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================
# CONFIG
# ==============================

SPREADSHEET_ID = "1DMxbjF2RfxA7S-Q2sPMnO2A5c7t7wXRdxS2flclPXPw"
TRENDS_SHEET   = "trends"
JAN_SHEET      = "jan_list"
SCRAPER_API_KEY = os.environ["SCRAPER_API_KEY"]

# 取得するキーワード（ボリュームのある上位カテゴリ推奨）
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
# GOOGLE SHEETS AUTH（scraper_pro.py と同じ）
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
# キーワード収集（jan_list + EXTRA_KEYWORDS）
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
# FETCH: ScraperAPI（scraper_pro.py と同じパターン）
# ==============================

def fetch_scraperapi(url, label=""):
    session = requests.Session()
    session.verify = False

    for attempt in range(1, RETRY + 1):
        try:
            scraper_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={url}"
                f"&country_code=jp"
                f"&render=true"      # JS レンダリングON（Trends は JS 必須）
                f"&cache=false"
            )
            r = session.get(scraper_url, timeout=60, verify=False)
            if r.status_code == 200:
                return r.text
            logging.warning(f"[{label}] ScraperAPI HTTP {r.status_code} ({attempt}/{RETRY})")
        except Exception as e:
            logging.warning(f"[{label}] ScraperAPI error ({attempt}/{RETRY}): {e}")
        time.sleep(random.uniform(4, 8))

    return None

# ==============================
# Google Trends CSV エンドポイントで取得
# ScraperAPI 経由で CSV を直接ダウンロードする
# ==============================

def fetch_trend_score_csv(keyword: str) -> int | None:
    """
    Google Trends の CSV エクスポートエンドポイントを使用
    過去7日間の平均スコアを返す
    """
    import urllib.parse

    encoded  = urllib.parse.quote(keyword)
    # CSV エンドポイント（ブラウザからダウンロードできるものと同じ）
    csv_url  = (
        f"https://trends.google.co.jp/trends/api/widgetdata/multiline/csv"
        f"?req=%7B%22time%22%3A%22now+7-d%22%2C%22resolution%22%3A%22HOUR%22"
        f"%2C%22locale%22%3A%22ja%22%2C%22comparisonItem%22%3A%5B%7B%22geo%22"
        f"%3A%7B%22country%22%3A%22JP%22%7D%2C%22complexKeywordsRestriction%22"
        f"%3A%7B%22keyword%22%3A%5B%7B%22type%22%3A%22BROAD%22%2C%22value%22"
        f"%3A%22{encoded}%22%7D%5D%7D%7D%5D%2C%22requestOptions%22%3A%7B%7D%7D"
        f"&token=APP6_UEAAAAAaBC&tz=-540"
    )

    html = fetch_scraperapi(csv_url, label=keyword)
    if not html:
        return None

    # CSV をパースして平均スコアを計算
    lines  = [l for l in html.strip().split("\n") if l and not l.startswith("#") and not l.startswith("日")]
    scores = []
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 2:
            try:
                scores.append(int(parts[1].strip()))
            except ValueError:
                continue

    if not scores:
        logging.warning(f"[{keyword}] スコアデータなし")
        return None

    avg = round(sum(scores) / len(scores))
    logging.info(f"[{keyword}] 平均スコア: {avg} (データ点数: {len(scores)})")
    return avg

# ==============================
# フォールバック: Interest Over Time API
# ==============================

def fetch_trend_score_api(keyword: str) -> int | None:
    """
    Google Trends の内部 API エンドポイントを ScraperAPI 経由で叩く
    CSV が取れない場合のフォールバック
    """
    import urllib.parse

    encoded = urllib.parse.quote(json.dumps([{"keyword": keyword, "geo": "JP", "time": "now 7-d"}]))
    api_url = (
        f"https://trends.google.co.jp/trends/api/explore"
        f"?hl=ja&tz=-540&req={encoded}&type=TIMESERIES&property="
    )

    html = fetch_scraperapi(api_url, label=f"{keyword}[api]")
    if not html:
        return None

    # レスポンスから数値を抽出
    numbers = re.findall(r'"value":\[(\d+)\]', html)
    if not numbers:
        logging.warning(f"[{keyword}] API レスポンスからスコア抽出失敗")
        return None

    scores = [int(n) for n in numbers]
    avg    = round(sum(scores) / len(scores))
    logging.info(f"[{keyword}] API 平均スコア: {avg}")
    return avg

# ==============================
# トレンドスコア取得（メイン）
# ==============================

def fetch_trends(keywords: list) -> dict:
    scores = {}
    for kw in keywords:
        logging.info(f"取得中: {kw}")

        # まず CSV エンドポイントを試す
        score = fetch_trend_score_csv(kw)

        # 取れなければ API エンドポイントで再試行
        if score is None:
            logging.info(f"[{kw}] CSV失敗 → API フォールバック")
            score = fetch_trend_score_api(kw)

        scores[kw] = score
        logging.info(f"[{kw}] 最終スコア: {score}")
        time.sleep(random.uniform(5, 10))  # レート制限対策

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
