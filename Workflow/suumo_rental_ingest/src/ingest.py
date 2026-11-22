"""SUUMO の賃貸一覧（大阪市・新着順）をスクレイピングし、bronze テーブルに新しい部屋だけを追記する。

利用条件（SUUMO 利用規約 2025-10-01 改定 / robots.txt を確認したうえでの運用ルール）:
  - 個人の私的利用の範囲で使う（第2条）。データや加工結果を公開・共有しない。商業目的で使わない（第3条第1項第7号）
  - サイトの運営を妨げない（第3条第1項第6号）: robots.txt の bingbot 向け Crawl-delay（30 秒）に合わせて 30 秒間隔で取る
  - 202 / 403 / 429 が返ったら（ボット対策・拒否・回数制限）、再試行も回避もせずに止める
  - robots.txt で禁止されたパスは取らない。詳細ページには行かない

取り込み方:
  - 全件は読まない（約 2,500 ページあり、30 秒間隔だと約 21 時間かかるため）
  - 初回（テーブルが空）: 新着順の先頭から --initial-pages ページだけ読む
  - 2 回目以降: 新着順の先頭から読み、新しい部屋が無いページが --stop-after-known-pages 回続いたら止める
  - すでに取り込んだ部屋（room_code = SUUMO の bc）は書かない。テーブルは 1 部屋 1 行（初めて見つけたときの内容）
  - bronze なので値はページ上の文字列のまま（「6.5万円」→ 数値 は silver でやる）

ジョブからは spark_python_task として起動される。parse_list_page() / crawl() は Spark に依存しないので、ローカルで確認できる。
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
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://suumo.jp"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36 suumo-rental-ingest/1.0"
)
JST = dt.timezone(dt.timedelta(hours=9))

# bronze テーブルの列。すべてページ上の文字列のまま（access / tags は文字列の配列）
COLUMNS = [
    "room_code",  # 部屋のコード（bc）。詳細ページ URL の ?bc= と同じ
    "building_code",  # 建物のコード（詳細ページ URL の jnc_xxxxx）
    "building_type",  # 賃貸マンション / 賃貸アパート など
    "building_name",
    "address",  # 所在地
    "access",  # 交通（「ＪＲ東西線/海老江駅 歩13分」の配列）
    "building_age",  # 築年数（例: 築23年 / 新築）
    "building_floors",  # 階建（例: 4階建）
    "floor",  # 階（例: 1階）
    "rent",  # 賃料（例: 6.5万円）
    "admin_fee",  # 管理費（例: 6000円）
    "deposit",  # 敷金（例: - / 6.5万円）
    "key_money",  # 礼金
    "layout",  # 間取り（例: 1K）
    "area",  # 専有面積（例: 26.48m2）
    "tags",  # 一覧のタグ（パノラマ / 動画 など）の配列
    "detail_url",
    "source_url",  # 取得した一覧ページの URL
    "page",
]
ARRAY_COLUMNS = {"access", "tags"}


def _text(el) -> str | None:
    if el is None:
        return None
    t = el.get_text("", strip=True)
    return t or None


def parse_list_page(html: str, source_url: str, page: int) -> list[dict]:
    """一覧ページの HTML から、部屋ごとの dict のリストを作る。"""
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    for item in soup.select(".cassetteitem"):
        col3 = item.select(".cassetteitem_detail-col3 > div")
        building = {
            "building_type": _text(item.select_one(".cassetteitem_content-label")),
            "building_name": _text(item.select_one(".cassetteitem_content-title")),
            "address": _text(item.select_one(".cassetteitem_detail-col1")),
            "access": [t for t in (_text(d) for d in item.select(".cassetteitem_detail-col2 .cassetteitem_detail-text")) if t],
            "building_age": _text(col3[0]) if len(col3) > 0 else None,
            "building_floors": _text(col3[1]) if len(col3) > 1 else None,
        }
        for tr in item.select("tr.js-cassette_link"):
            tds = tr.find_all("td", recursive=False)
            link = tr.select_one("a.js-cassette_link_href")
            href = link.get("href") if link else None
            m = re.search(r"/(jnc_\d+)/", href or "")
            code_input = tr.select_one("input.js-clipkey") or tr.select_one("input[name=bc]")
            room_code = code_input.get("value") if code_input else None
            if not room_code and href:
                room_code = (parse_qs(urlparse(href).query).get("bc") or [None])[0]
            rows.append(
                {
                    **building,
                    "room_code": room_code,
                    "building_code": m.group(1) if m else None,
                    "floor": _text(tds[2]) if len(tds) > 2 else None,
                    "rent": _text(tr.select_one(".cassetteitem_price--rent")),
                    "admin_fee": _text(tr.select_one(".cassetteitem_price--administration")),
                    "deposit": _text(tr.select_one(".cassetteitem_price--deposit")),
                    "key_money": _text(tr.select_one(".cassetteitem_price--gratuity")),
                    "layout": _text(tr.select_one(".cassetteitem_madori")),
                    "area": _text(tr.select_one(".cassetteitem_menseki")),
                    "tags": [t for t in (_text(x) for x in tr.select(".cassetteitem-taglist li")) if t],
                    "detail_url": urljoin(BASE_URL, href) if href else None,
                    "source_url": source_url,
                    "page": page,
                }
            )
    return rows


def parse_total_count(html: str) -> str | None:
    m = re.search(r'paginate_set-hit">\s*([\d,]+)', html)
    return m.group(1) if m else None


class Blocked(Exception):
    """サイトに自動アクセスを止められた（ボット対策の確認画面・アクセス拒否・回数制限）。"""


# どれもサイト側の「自動で取らないで」という意思表示なので、再試行も回避もせずに止める
BLOCKED_STATUSES = (202, 403, 429)


class Fetcher:
    """robots.txt を守り、リクエストの間隔をあけ、一時的な失敗（5xx・通信エラー）は少し待って再試行する。"""

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
                time.sleep(wait + random.uniform(0, 2))
            self._last = time.monotonic()
            try:
                res = self.session.get(url, timeout=60)
            except requests.RequestException as e:
                print(f"  取得失敗（{attempt}/{self.max_retries}）: {e}")
            else:
                if res.status_code == 200:
                    return res.text
                print(f"  HTTP {res.status_code}（{attempt}/{self.max_retries}）")
                if res.status_code in BLOCKED_STATUSES:
                    raise Blocked(f"HTTP {res.status_code}: サイトに自動アクセスを止められました")
                if res.status_code not in (500, 502, 503, 504):
                    res.raise_for_status()
            time.sleep(self.delay * 2 * attempt)
        raise RuntimeError(f"{self.max_retries} 回試しても取得できませんでした")


def page_url(list_url: str, page: int) -> str:
    if page == 1:
        return list_url
    return f"{list_url}{'&' if '?' in list_url else '?'}page={page}"


def crawl(fetch_page, known: set[str], max_pages: int, stop_after_known_pages: int, flush_pages: int, on_flush) -> dict:
    """新着順の 1 ページ目から読み、まだ取り込んでいない部屋だけを集める。

    fetch_page(page) -> rows（取得できない・最後のページを過ぎたら空）。Blocked が飛んだらそこで止める。
    on_flush(rows) を flush_pages ページごとと最後に呼ぶ。
    stop_after_known_pages が 0 なら「追いついたら止める」はせず、max_pages まで読む（初回用）。
    """
    buf: list[dict] = []
    pages = new_total = streak = 0
    caught_up = reached_end = blocked = False
    page = 1
    while True:
        try:
            rows = fetch_page(page)
        except Blocked as e:
            print(f"page {page} で止められたため終了します: {e}")
            blocked = True
            break
        if not rows:
            if page == 1:
                raise RuntimeError("1 ページ目から 1 件も取れませんでした。ページの構造が変わった可能性があります")
            reached_end = True
            break
        pages += 1
        new_rows = []
        for r in rows:
            key = r.get("room_code")
            if key and key not in known:
                known.add(key)
                new_rows.append(r)
        buf.extend(new_rows)
        new_total += len(new_rows)
        print(f"page {page}: {len(rows)} 部屋（うち新規 {len(new_rows)}）")
        streak = 0 if new_rows else streak + 1
        if pages % flush_pages == 0:
            on_flush(buf)
            buf = []
        if stop_after_known_pages and streak >= stop_after_known_pages:
            caught_up = True
            break
        if max_pages and pages >= max_pages:
            break
        page += 1
    on_flush(buf)
    return {"pages": pages, "new_rows": new_total, "caught_up": caught_up, "reached_end": reached_end, "blocked": blocked}


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--table", default="suumo_rental_listings")
    p.add_argument("--list-url", required=True, help="新着順（po1=09）の一覧 URL")
    p.add_argument("--initial-pages", type=int, default=20, help="初回（テーブルが空）に読むページ数")
    p.add_argument("--max-pages", type=int, default=400, help="2 回目以降に 1 回で読む最大ページ数")
    p.add_argument("--stop-after-known-pages", type=int, default=3)
    p.add_argument("--delay", type=float, default=30.0, help="リクエストの間隔（秒）。robots.txt の Crawl-delay に合わせる")
    p.add_argument("--flush-pages", type=int, default=20)
    args = p.parse_args(argv)

    if "po1=09" not in args.list_url:
        raise ValueError("list_url は新着順（po1=09）にしてください。増分の判定が新着順を前提にしているため")
    if args.delay < 30:
        raise ValueError("delay は 30 秒以上にしてください（robots.txt の Crawl-delay に合わせる運用ルール）")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = SparkSession.builder.getOrCreate()
    target = f"{args.catalog}.{args.schema}.{args.table}"
    run_id = str(uuid.uuid4())

    known: set[str] = set()
    if spark.catalog.tableExists(target):
        known = {r[0] for r in spark.table(target).select("room_code").where("room_code IS NOT NULL").distinct().collect()}
    first_run = not known
    mode = "initial" if first_run else "incremental"

    schema = T.StructType(
        [
            T.StructField(
                c, T.IntegerType() if c == "page" else T.ArrayType(T.StringType()) if c in ARRAY_COLUMNS else T.StringType()
            )
            for c in COLUMNS
        ]
    )
    written = 0

    def on_flush(rows: list[dict]) -> None:
        nonlocal written
        if not rows:
            return
        now = dt.datetime.now(JST)
        (
            spark.createDataFrame([tuple(r.get(c) for c in COLUMNS) for r in rows], schema)
            .withColumn("ingest_mode", F.lit(mode))
            .withColumn("ingest_run_id", F.lit(run_id))
            .withColumn("ingested_at", F.lit(now.astimezone(dt.timezone.utc).replace(tzinfo=None)).cast("timestamp"))
            .withColumn("ingest_date", F.lit(now.date().isoformat()).cast("date"))
            .write.mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )
        written += len(rows)

    fetcher = Fetcher(delay=args.delay)

    def fetch_page(page: int) -> list[dict]:
        url = page_url(args.list_url, page)
        try:
            html = fetcher.get(url)
        except requests.HTTPError as e:
            print(f"page {page} を取得できませんでした（最後のページを過ぎたとみなす）: {e}")
            return []
        if page == 1:
            print(f"サイト上の掲載物件総数: {parse_total_count(html)}")
        return parse_list_page(html, url, page)

    print(f"モード: {mode} / 取り込み済みの部屋: {len(known)} / 取り込み先: {target} / run_id: {run_id}")
    result = crawl(
        fetch_page,
        known=known,
        max_pages=args.initial_pages if first_run else args.max_pages,
        stop_after_known_pages=0 if first_run else args.stop_after_known_pages,
        flush_pages=args.flush_pages,
        on_flush=on_flush,
    )
    print(f"完了: {result['pages']} ページを読み、新しい部屋 {written} 件を {target} に追記しました")
    if not first_run and not (result["caught_up"] or result["reached_end"] or result["blocked"]):
        print(f"注意: 取り込み済みの部屋に追いつく前に上限（{args.max_pages} ページ）に達しました。"
              "この間の新着は取りこぼしている可能性があります。max_pages か実行頻度を見直してください")
    if result["blocked"]:
        raise RuntimeError("サイトのボット対策に止められました。再試行や回避はしません。取り込み方を見直してください")


if __name__ == "__main__":
    main(sys.argv[1:])
