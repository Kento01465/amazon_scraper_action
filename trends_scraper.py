"""
trends_scraper.py
─────────────────────────────────────────────────────────
Google Trends の検索スコアを取得して Sheets の "trends" シートに追記する
既存の scraper_pro.py と同じ認証・Sheets 接続パターンを使用

依存: pip install pytrends gspread oauth2client
（requirements.txt に追記してください）
"""

import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from pytrends.request import TrendReq
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# CONFIG
# ==============================

SPREADSHEET_ID   = "1DMxbjF2RfxA7S-Q2sPMnO2A5c7t7wXRdxS2flclPXPw"  # scraper_pro.py と同じ
TRENDS_SHEET     = "trends"       # 書き込み先シート名
JAN_SHEET        = "jan_list"     # 商品一覧シート（キーワード取得元）

# jan_list から自動取得するほか、固定キーワードを追加したい場合はここに書く
EXTRA_KEYWORDS = [
    "カビ取り剤",
    "洗濯槽クリーナー",
    "風呂釜洗浄",
    "浴槽 配管 掃除",
    "クリーンプラネット",
]

JST = timezone(timedelta(hours=9))

# ==============================
# LOGGING（scraper_pro.py と同形式）
# ==============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# GOOGLE SHEETS AUTH
# scraper_pro.py と完全に同じ認証パターン
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

    jan_sheet = spreadsheet.worksheet(JAN_SHEET)

    # trends シートがなければ自動作成
    try:
        trends_sheet = spreadsheet.worksheet(TRENDS_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        trends_sheet = spreadsheet.add_worksheet(title=TRENDS_SHEET, rows=10000, cols=5)
        trends_sheet.append_row(["timestamp", "keyword", "trend_score"])
        logging.info(f"シート '{TRENDS_SHEET}' を新規作成しました")

    return jan_sheet, trends_sheet

# ==============================
# キーワード収集
# jan_list の product_name 列（4列目）+ EXTRA_KEYWORDS
# ==============================

def collect_keywords(jan_sheet) -> list:
    rows = jan_sheet.get_all_values()
    # jan_list の列構成: jan | asin | ... | product_name（4列目がある場合）
    # 商品名が取れる場合は使う。なければ EXTRA_KEYWORDS のみ
    names = []
    for r in rows[1:]:
        if len(r) >= 4 and r[3].strip():
            # 長い商品名はそのまま使うとノイズになるので先頭の短縮名を取る
            name = r[3].strip()
            # 「クリーンプラネット 風呂釜のカビ丸洗浄 ...」→ 先頭2単語に絞る
            short = " ".join(name.split()[:2])
            if short and short not in names:
                names.append(short)

    keywords = list(dict.fromkeys(names + EXTRA_KEYWORDS))  # 重複排除・順序保持
    logging.info(f"収集キーワード ({len(keywords)}件): {keywords}")
    return keywords

# ==============================
# pytrends でスコア取得
# ==============================

def fetch_trends(keywords: list) -> dict:
    """
    キーワードリストの直近7日間トレンドスコア（最新日）を返す
    Returns: { "キーワード": score(int) or None }
    """
    pytrends = TrendReq(hl="ja-JP", tz=540)  # 日本語・JST
    scores   = {}
    BATCH    = 5  # pytrends は一度に最大5件まで比較可能

    for i in range(0, len(keywords), BATCH):
        batch = keywords[i:i + BATCH]
        logging.info(f"Trends取得: {batch}")

        try:
            pytrends.build_payload(
                batch,
                cat=0,
                timeframe="now 7-d",
                geo="JP",
                gprop="",
            )
            df = pytrends.interest_over_time()

            if df.empty:
                logging.warning(f"データなし: {batch}")
                for kw in batch:
                    scores[kw] = None
            else:
                latest = df.iloc[-1]
                for kw in batch:
                    scores[kw] = int(latest[kw]) if kw in latest else None
                    logging.info(f"  {kw}: {scores[kw]}")

        except Exception as e:
            logging.warning(f"pytrends error ({batch}): {e}")
            for kw in batch:
                scores[kw] = None

        time.sleep(3)  # レート制限対策（scraper_pro.py の sleep と同様）

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
