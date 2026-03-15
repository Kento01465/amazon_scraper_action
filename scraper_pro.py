import requests
import urllib3
from bs4 import BeautifulSoup
import gspread
import json
import os
import re
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from oauth2client.service_account import ServiceAccountCredentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================
# CONFIG
# ==============================

SPREADSHEET_ID = "1DMxbjF2RfxA7S-Q2sPMnO2A5c7t7wXRdxS2flclPXPw"
SCRAPER_API_KEY = os.environ["SCRAPER_API_KEY"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

RETRY = 3
JST = timezone(timedelta(hours=9))

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

creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
client = gspread.authorize(credentials)
spreadsheet = client.open_by_key(SPREADSHEET_ID)
jan_sheet = spreadsheet.worksheet("jan_list")
data_sheet = spreadsheet.worksheet("data")

# ==============================
# GET PRODUCTS（2行目から読み取り）
# ==============================

def get_products():
    rows = jan_sheet.get_all_values()
    products = []
    for r in rows[1:]:
        if len(r) >= 2:
            jan = r[0].strip()
            asin = r[1].strip()
            if jan and asin:
                products.append((jan, asin))
    return products

# ==============================
# CAPTCHA 判定
# ==============================

def is_captcha_page(html):
    text = html.lower()
    return any(kw in text for kw in [
        "api-services-support.amazon.com",
        "robot check",
        "captcha",
        "enter the characters you see below"
    ])

# ==============================
# FETCH: 直接アクセス（ランキング・レビュー・月間販売数用）
# ==============================

def fetch_direct(url, asin=""):
    session = requests.Session()
    session.verify = False

    for attempt in range(1, RETRY + 1):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "ja-JP,ja;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.google.com/",
            }
            r = session.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                if is_captcha_page(r.text):
                    logging.warning(f"[{asin}] 直接アクセス CAPTCHA {attempt}/{RETRY}")
                    time.sleep(random.uniform(10, 20))
                    continue
                return r.text
            logging.warning(f"[{asin}] 直接アクセス HTTP {r.status_code}")
        except Exception as e:
            logging.warning(f"[{asin}] 直接アクセス error ({attempt}/{RETRY}): {e}")
        time.sleep(random.uniform(4, 8))
    return None

# ==============================
# FETCH: ScraperAPI（価格取得用）
# ==============================

def fetch_scraperapi(url, asin=""):
    session = requests.Session()
    session.verify = False

    for attempt in range(1, RETRY + 1):
        try:
            scraper_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={url}"
                f"&country_code=jp"
                f"&cache=false"
            )
            r = session.get(scraper_url, timeout=60)
            if r.status_code == 200:
                if is_captcha_page(r.text):
                    logging.warning(f"[{asin}] ScraperAPI CAPTCHA {attempt}/{RETRY}")
                    time.sleep(random.uniform(10, 20))
                    continue
                return r.text
            logging.warning(f"[{asin}] ScraperAPI HTTP {r.status_code}")
        except Exception as e:
            logging.warning(f"[{asin}] ScraperAPI error ({attempt}/{RETRY}): {e}")
        time.sleep(random.uniform(4, 8))
    return None

# ==============================
# AMAZON URL
# ==============================

def build_product_url(asin):
    return f"https://www.amazon.co.jp/dp/{asin}"

# ==============================
# PARSE
# ==============================

def extract_price(soup):
    # 方法1: priceToPay内の a-price-whole を直接取得（最優先）
    for sel in [".priceToPay", ".apexPriceToPay", ".apex-pricetopay-value"]:
        el = soup.select_one(sel)
        if el:
            whole = el.select_one(".a-price-whole")
            if whole:
                price = re.sub(r"[^\d]", "", whole.text.strip())
                if price and len(price) >= 2:
                    return price

    # 方法2: 各種セレクタ（フォールバック）
    selectors = [
        "#priceblock_ourprice",
        "#priceblock_dealprice",
        "#apex_offerDisplay_desktop .a-price .a-offscreen",
        "#corePrice_desktop .a-price .a-offscreen",
        "#corePrice_feature_div .a-price .a-offscreen",
        "#buyNewSection .a-price .a-offscreen",
        "#newBuyBoxPrice",
    ]
    for sel in selectors:
        for el in soup.select(sel):
            text = el.text.strip()
            if any(c in text for c in ["USD", "EUR", "GBP"]):
                continue
            price = re.sub(r"[^\d]", "", text)
            if price and len(price) >= 2:
                return price

    # 方法3: a-price-whole を全体から探す（最終手段）
    for el in soup.select(".a-price-whole"):
        text = el.text.strip()
        price = re.sub(r"[^\d]", "", text)
        if price and len(price) >= 2:
            return price

    return ""

def extract_ranking_text(soup):
    # 方法1: detailBullets
    for li in soup.select("#detailBullets_feature_div li"):
        if "ランキング" in li.text:
            return li.text

    # 方法2: productDetails テーブル（th/tdペア）
    for table_id in ["productDetails_db_sections", "productDetails_detailBullets_sections"]:
        for tr in soup.select(f"#{table_id} tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td and "売れ筋ランキング" in th.text:
                return td.get_text(" ", strip=True)

    # 方法3: 全テーブルから検索
    for tr in soup.select("tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if th and td and "売れ筋ランキング" in th.text:
            return td.get_text(" ", strip=True)

    return None

def parse_ranking(text):
    text = re.sub(r"\(.*?\)", "", text)
    pattern = re.findall(r"([^\-\n]+?)\s*-\s*([\d,]+)位", text)

    main_category = main_rank = ""
    subs = []
    clean = []

    for name, rank in pattern:
        name = re.sub(r".*ランキング[:：]\s*", "", name).strip()
        rank = rank.replace(",", "")
        if name and rank:
            clean.append((name, rank))

    if len(clean) >= 1:
        main_category, main_rank = clean[0]
    if len(clean) >= 2:
        subs = clean[1:]

    return main_category, main_rank, subs

def extract_from_direct(html, asin=""):
    """直接アクセスHTMLからランキング・レビュー・月間販売数を取得"""
    soup = BeautifulSoup(html, "html.parser")

    title = soup.select_one("#productTitle")

    review_count = ""
    for sel in ["#acrCustomerReviewText", "#acrPopover"]:
        el = soup.select_one(sel)
        if el:
            review_count = re.sub(r"[^\d]", "", el.text)
            if review_count:
                break

    main_category = main_rank = ""
    subs = []
    ranking_text = extract_ranking_text(soup)
    if ranking_text:
        main_category, main_rank, subs = parse_ranking(
            ranking_text if isinstance(ranking_text, str) else ranking_text.text
        )

    monthly_sales = ""
    sales_el = soup.select_one("#social-proofing-faceout-title-tk_bought")
    if not sales_el:
        sales_el = soup.find(string=re.compile(r"過去1か月で.+購入されました"))
    if sales_el:
        text_val = sales_el if isinstance(sales_el, str) else sales_el.text
        m = re.search(r"([\d,]+)点以上購入", text_val)
        if m:
            monthly_sales = m.group(1).replace(",", "")

    return {
        "title": title.text.strip() if title else "",
        "review_count": review_count,
        "main_category": main_category,
        "main_rank": main_rank,
        "subs": subs,
        "monthly_sales": monthly_sales,
    }

def extract_price_from_scraperapi(html):
    """ScraperAPIのHTMLから価格のみ取得"""
    soup = BeautifulSoup(html, "html.parser")
    return extract_price(soup)

# ==============================
# SAVE TO SHEETS
# ==============================

def save_rows(jan, asin, data):
    now_jst = datetime.now(JST).strftime("%Y/%m/%d")
    base = [
        now_jst,
        jan,
        asin,
        data["title"],
        data["monthly_sales"],
        data["price"],
        data["main_category"],
        data["main_rank"],
    ]
    subs = data["subs"]
    if not subs:
        data_sheet.append_row(base + ["", "", data["review_count"]])
    else:
        for sub_category, sub_rank in subs:
            data_sheet.append_row(base + [sub_category, sub_rank, data["review_count"]])

# ==============================
# MAIN PIPELINE
# ==============================

def run():
    products = get_products()
    logging.info(f"Product count: {len(products)}")

    for jan, asin in products:
        logging.info(f"Processing JAN {jan} / ASIN {asin}")
        url = build_product_url(asin)

        # 直接アクセス: ランキング・レビュー・月間販売数
        html_direct = fetch_direct(url, asin)
        if not html_direct:
            logging.warning(f"[{asin}] 直接アクセス失敗")
            continue
        data = extract_from_direct(html_direct, asin)

        # ScraperAPI: 価格・月間販売数
        html_scraper = fetch_scraperapi(url, asin)
        if html_scraper:
            soup_scraper = BeautifulSoup(html_scraper, "html.parser")
            price = extract_price(soup_scraper)
            if price:
                data["price"] = price

            # 月間販売数（ScraperAPIのHTMLを優先、取れなければ直接アクセス結果を維持）
            sales_el = soup_scraper.select_one("#social-proofing-faceout-title-tk_bought")
            if not sales_el:
                sales_el = soup_scraper.find(string=re.compile(r"過去1か月で.+購入されました"))
            if sales_el:
                text_val = sales_el if isinstance(sales_el, str) else sales_el.text
                m = re.search(r"([\d,]+)点以上購入", text_val)
                if m:
                    data["monthly_sales"] = m.group(1).replace(",", "")
        else:
            logging.warning(f"[{asin}] ScraperAPI失敗 → 価格・月間販売数なし")
            data["price"] = ""

        # 価格が取れなかった場合のデバッグ
        if not data["price"]:
            soup_tmp = BeautifulSoup(html_scraper or html_direct, "html.parser")
            for keyword in ["1,080", "980", "円", "￥"]:
                idx = (html_scraper or html_direct).find(keyword)
                if idx >= 0:
                    snippet = (html_scraper or html_direct)[max(0,idx-200):idx+100]
                    logging.info(f"[{asin}] price_snippet({keyword}): {repr(snippet[:300])}")
                    break

        logging.info(f"[{asin}] price={data['price']} rank={data['main_category']}/{data['main_rank']} subs={data['subs']} review={data['review_count']} sales={data['monthly_sales']}")

        save_rows(jan, asin, data)
        logging.info(f"[{asin}] Saved {max(1, len(data['subs']))} row(s)")

        time.sleep(random.uniform(8, 15))

# ==============================
# ENTRY
# ==============================

if __name__ == "__main__":
    run()
