"""HOME'S（LIFULL HOME'S）の賃貸一覧ページをスクレイピングし、bronze テーブルに追記する。

- 対象: 一覧ページ（例: https://www.homes.co.jp/chintai/osaka/list/?page=N）。詳細ページには行かない
- 1 行 = 一覧に出ている 1 部屋。建物の情報（名前・所在地・交通・築年数）は部屋の行に複製して持つ
- bronze なので値はページ上の文字列のまま保存する（「6.4万円」→ 数値 への変換は silver でやる）
- サイトに負荷をかけないよう、1 ページ取るごとに --delay 秒待つ。robots.txt で禁止されているパスは取りに行かない

ジョブからは spark_python_task として起動される（引数は resources/homes_rental_ingest.job.yml を参照）。
parse_list_page() は Spark に依存しないので、ローカルでも保存した HTML で動作確認できる。
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import re
import sys
import time
import urllib.robotparser
import uuid
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.homes.co.jp"
# 普通のブラウザと同じ UA に、取り込みジョブの名前を付け足して身元を明示する
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36 homes-rental-ingest/1.0"
)
JST = dt.timezone(dt.timedelta(hours=9))

# bronze テーブルの列。すべてページ上の文字列のまま（access / features は文字列の配列）
COLUMNS = [
    "room_key",  # 部屋の ID（data-kykey）。詳細ページ URL の末尾と同じ
    "building_key",  # 建物の ID（data-tykey）
    "property_code",  # 物件コード（例: BRent_31029140180251）
    "building_type",  # 賃貸アパート / 賃貸マンション など
    "building_name",
    "address",  # 所在地
    "access",  # 交通（「○○線 ○○駅 徒歩3分」の配列）
    "building_age_floors",  # 築年数/階数（例: 新築 / 3階建）
    "listed_rooms_text",  # 建物の「掲載物件 14件」の件数部分
    "floor",  # 階（例: 3階）
    "room_number",
    "rent",  # 賃料（例: 6.4万円）
    "admin_fee",  # 管理費等（例: 9,000円）
    "deposit_key_money",  # 敷金/礼金/保証/敷引・償却（例: 無/無/-/-）
    "layout",  # 間取り（例: 1LDK）
    "area",  # 専有面積（例: 27.32m²）
    "features",  # こだわり条件（オートロック など）の配列
    "agent_name",  # 掲載している不動産会社
    "agent_comment",
    "detail_url",
    "source_url",  # 取得した一覧ページの URL
    "page",
]
ARRAY_COLUMNS = {"access", "features"}


def _text(el) -> str | None:
    if el is None:
        return None
    t = el.get_text(" ", strip=True)
    return t or None


def _spec(table, label: str):
    """建物のスペック表から <th>label…</th> の隣の <td> を返す。"""
    if table is None:
        return None
    for th in table.find_all("th"):
        if th.get_text(strip=True).startswith(label):
            return th.find_next_sibling("td")
    return None


def _has_class(el, name: str) -> bool:
    return el is not None and name in (el.get("class") or [])


def parse_list_page(html: str, source_url: str, page: int) -> list[dict]:
    """一覧ページの HTML から、部屋ごとの dict のリストを作る。"""
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    # 建物ごとのブロック（建物の種別で mod-mergeBuilding--rent--photo rApart / rMansion などに分かれる）
    for block in soup.select("div[class*='mod-mergeBuilding--rent']"):
        spec_table = block.select_one(".bukkenSpec table")
        access_td = _spec(spec_table, "交通")
        unit_body = block.select_one(".prg-unitListBody")
        building = {
            "building_type": _text(block.select_one(".bType")),
            "building_name": _text(block.select_one(".bukkenName")),
            "address": _text(_spec(spec_table, "所在地")),
            "access": [s.get_text(strip=True) for s in access_td.select(".prg-stationText")] if access_td else [],
            "building_age_floors": _text(_spec(spec_table, "築年数")),
            "listed_rooms_text": _text(block.select_one(".unitListHeading .num")),
            "building_key": unit_body.get("data-tykey") if unit_body else None,
        }

        # 部屋は 3 行で 1 組: 本体（prg-roomInfo）→ こだわり条件 → 不動産会社。後ろ 2 行は無いこともある
        for info in block.select("tr.prg-roomInfo"):
            features_tr = info.find_next_sibling("tr")
            if not _has_class(features_tr, "prg-relatedKeywordsRow"):
                features_tr = None
            member_tr = (features_tr or info).find_next_sibling("tr")
            if not _has_class(member_tr, "memberDataRow"):
                member_tr = None

            rent = admin_fee = deposit = None
            price_td = info.select_one("td.price")
            if price_td is not None:
                label = price_td.select_one(".priceLabel")
                rent = label.get_text("", strip=True) or None if label else None
                first = price_td.find("span")  # 「賃料/管理費等」の部分
                if first is not None:
                    parts = first.get_text("", strip=True).split("/", 1)
                    admin_fee = parts[1].strip() or None if len(parts) > 1 else None
                br = price_td.find("br")  # <br> の後ろが 敷金/礼金/保証/敷引・償却
                if br is not None and br.next_sibling:
                    deposit = str(br.next_sibling).strip() or None

            layout = area = None
            layout_td = info.select_one("td.layout")
            if layout_td is not None:
                parts = list(layout_td.stripped_strings)
                layout = parts[0] if parts else None
                area = parts[1] if len(parts) > 1 else None

            checkbox = info.select_one("input.prg-bCheck")
            detail_url = info.get("data-href")
            rows.append(
                {
                    **building,
                    "room_key": info.get("data-kykey"),
                    "property_code": checkbox.get("value") if checkbox else None,
                    "floor": _text(info.select_one(".roomKaisuu")),
                    "room_number": _text(info.select_one(".roomNumber")),
                    "rent": rent,
                    "admin_fee": admin_fee,
                    "deposit_key_money": deposit,
                    "layout": layout,
                    "area": area,
                    "features": [s.get_text(strip=True) for s in features_tr.select(".relatedKeyword")]
                    if features_tr
                    else [],
                    "agent_name": _text(member_tr.select_one(".memberName")) if member_tr else None,
                    "agent_comment": _text(member_tr.select_one(".textFeatureComment")) if member_tr else None,
                    "detail_url": urljoin(BASE_URL, detail_url) if detail_url else None,
                    "source_url": source_url,
                    "page": page,
                }
            )
    return rows


def parse_total_count(html: str) -> int | None:
    """ページ上部の「201,152件」の総件数。取れなければ None。"""
    m = re.search(r'class="totalNum">([\d,]+)<', html)
    return int(m.group(1).replace(",", "")) if m else None


class Fetcher:
    """robots.txt を守り、リクエストの間隔をあけ、一時的な失敗は少し待って再試行する。"""

    def __init__(self, delay: float, max_retries: int = 3):
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja"})
        self.robots = urllib.robotparser.RobotFileParser(urljoin(BASE_URL, "/robots.txt"))
        self.robots.read()
        self._last = 0.0

    def get(self, url: str) -> str:
        if not self.robots.can_fetch(USER_AGENT, url):
            raise RuntimeError(f"robots.txt で禁止されている URL です: {url}")
        for attempt in range(1, self.max_retries + 1):
            wait = self.delay - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait + random.uniform(0, 0.5))
            self._last = time.monotonic()
            try:
                res = self.session.get(url, timeout=30)
            except requests.RequestException as e:
                print(f"  取得失敗（{attempt}/{self.max_retries}）: {e}")
            else:
                if res.status_code == 200:
                    return res.text
                print(f"  HTTP {res.status_code}（{attempt}/{self.max_retries}）: {url}")
                # 404 などは再試行しても変わらないので即終了。429 / 5xx だけ待って再試行する
                if res.status_code not in (429, 500, 502, 503, 504):
                    res.raise_for_status()
            time.sleep(self.delay * 5 * attempt)
        raise RuntimeError(f"{self.max_retries} 回試しても取得できませんでした: {url}")


def page_url(list_url: str, page: int) -> str:
    if page == 1:
        return list_url
    return f"{list_url}{'&' if '?' in list_url else '?'}page={page}"


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--table", default="homes_rental_listings")
    p.add_argument("--list-url", default="https://www.homes.co.jp/chintai/osaka/list/")
    p.add_argument("--max-pages", type=int, default=100, help="取得する最大ページ数。0 なら最後のページまで")
    p.add_argument("--delay", type=float, default=2.0, help="リクエストの間隔（秒）")
    p.add_argument("--flush-pages", type=int, default=50, help="何ページごとにテーブルへ書き込むか")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = SparkSession.builder.getOrCreate()
    target = f"{args.catalog}.{args.schema}.{args.table}"
    run_id = str(uuid.uuid4())
    ingested_at = dt.datetime.now(JST)

    schema = T.StructType(
        [
            T.StructField(
                c,
                T.IntegerType()
                if c == "page"
                else T.ArrayType(T.StringType())
                if c in ARRAY_COLUMNS
                else T.StringType(),
            )
            for c in COLUMNS
        ]
    )

    def flush(buf: list[dict]) -> int:
        if not buf:
            return 0
        df = (
            spark.createDataFrame([tuple(r.get(c) for c in COLUMNS) for r in buf], schema)
            .withColumn("ingest_run_id", F.lit(run_id))
            .withColumn("ingested_at", F.lit(ingested_at.astimezone(dt.timezone.utc).replace(tzinfo=None)).cast("timestamp"))
            .withColumn("ingest_date", F.lit(ingested_at.date().isoformat()).cast("date"))
        )
        # 実行ごとに追記する（その日の掲載状況のスナップショットとして残す）。重複の除去は silver でやる
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(target)
        return len(buf)

    fetcher = Fetcher(delay=args.delay)
    page = 1
    html = fetcher.get(page_url(args.list_url, page))
    print(f"総件数: {parse_total_count(html)} / 取り込み先: {target} / run_id: {run_id}")

    buf: list[dict] = []
    written = 0
    while True:
        rows = parse_list_page(html, page_url(args.list_url, page), page)
        print(f"page {page}: {len(rows)} 部屋")
        if not rows:
            if page == 1:
                raise RuntimeError("1 ページ目から 1 件も取れませんでした。ページの構造が変わった可能性があります")
            break  # 最後のページを過ぎた
        buf.extend(rows)
        if page % args.flush_pages == 0:
            written += flush(buf)
            buf = []
        if args.max_pages and page >= args.max_pages:
            break
        page += 1
        try:
            html = fetcher.get(page_url(args.list_url, page))
        except requests.HTTPError as e:
            print(f"page {page} で終了: {e}")
            break
    written += flush(buf)
    print(f"完了: {page} ページ / {written} 行を {target} に追記しました")


if __name__ == "__main__":
    main(sys.argv[1:])
