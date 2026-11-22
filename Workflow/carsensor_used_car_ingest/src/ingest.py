"""カーセンサーの中古車一覧（新着順）をスクレイピングし、bronze テーブルに取り込む。掲載終了は sitemap で判定する。

利用条件（カーセンサー利用規約 car-t-1008 / robots.txt を 2026-11-22 に確認したうえでの運用ルール）:
  - 個人の私的利用の範囲で使う。データや加工結果を公開・共有・販売しない（第11条: 無断の複製・転用の禁止）
  - サイトの運営を妨げない（第8条第1項第3号・第7号）: リクエストは 10 秒以上あけ、1 本ずつ順番に取る
  - 202 / 403 / 429 が返ったら（ボット対策・拒否・回数制限）、再試行も回避もせずに止める
  - robots.txt で禁止されたパス（search.php など）は取らない。詳細ページには行かない。画像は取らない
  - ログインしない（リクルートID規約の対象にならないようにする）

取り込み方:
  - 初回（全件読み込み）: 新着順の一覧を先頭から最後のページまで読む（約 2,100 ページ、10 秒間隔で約 6 時間）。
    --initial-pages を 1 以上にすると、そのページ数で止めて完了扱いにする（動作確認用）
    途中で止まっても、書き込むたびに保存した再開位置から次の実行で続きを読む
  - 全件読み込みが終わった後（増分）: 新着順の先頭から読み、新しい車が無いページが --stop-after-known-pages 回続いたら止める
  - すでに取り込んだ車（car_id = カーセンサーの物件番号 BKKN）は追記しない。テーブルは 1 台 1 行（初めて見つけたときの内容）
  - bronze なので値はページ上の文字列のまま（「297万円」→ 数値 は silver でやる）

掲載終了フラグ（is_delisted）:
  - 一覧を全件読み直すと毎日 6 時間かかるので、代わりにカーセンサーが検索エンジン向けに公開している
    在庫詳細の sitemap（usedcar-detail-index.xml、全国の掲載中の車 約 56 万件、毎日更新）を毎回読む（約 57 リクエスト）
  - テーブルの車が sitemap に無ければ掲載終了（is_delisted = true、delisted_date = 判定した日）
    sitemap に戻ってきたら掲載中に戻す。sitemap にあった日は last_seen_date を更新する
  - sitemap の更新時刻より後に取り込んだ車は、まだ sitemap に載っていないだけなので判定しない
  - sitemap が 1 つでも取れない、または 1 日で掲載中の 20% を超える車が消えた場合は、sitemap 側の異常とみなしてフラグを更新しない

ジョブからは spark_python_task として起動される。parse_list_page() / parse_sitemap() / crawl() は Spark に依存しないので、ローカルで確認できる。
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
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.carsensor.net"
SITEMAP_INDEX_URL = f"{BASE_URL}/usedcar-detail-index.xml"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36 carsensor-used-car-ingest/1.0"
)
JST = dt.timezone(dt.timedelta(hours=9))
MIN_DELAY = 10.0

# bronze テーブルの列（掲載終了フラグの列を除く）。すべてページ上の文字列のまま（labels は文字列の配列）
COLUMNS = [
    "car_id",  # 物件番号（BKKN）。詳細ページ URL /usedcar/detail/<car_id>/ と同じ
    "maker",  # メーカー（例: トヨタ）
    "title",  # 車名・グレード・装備（例: ヤリスクロス 1.5 ハイブリッド Z ...）
    "sub_text",  # 販売店のひとこと
    "labels",  # ラベル（ディーラー保証 / 360°画像付 など）の配列
    "listed_label",  # 一覧の掲載日ラベル（例: 11/22）。新着のときだけ出る
    "body_type",  # ボディタイプ（例: クロカン・ＳＵＶ）
    "color",  # 色（例: 黒Ｍ）
    "total_price",  # 支払総額（例: 297万円）
    "base_price",  # 車両本体価格（例: 289.2万円）
    "model_year",  # 年式（例: 2025(R07)）
    "mileage",  # 走行距離（例: 1.8万km）
    "inspection",  # 車検（例: 2028(R10)年07月）
    "repair_history",  # 修復歴（例: なし）
    "warranty",  # 保証（例: 保証付）
    "maintenance",  # 整備（例: 法定整備付）
    "displacement",  # 排気量（例: 1500CC）
    "transmission",  # ミッション（例: フロアCVT）
    "prefecture",  # 販売店の都道府県
    "city",  # 販売店の市区町村
    "shop_code",  # 販売店のコード（/shop/<県>/<shop_code>/）
    "shop_name",
    "shop_review",  # クチコミ評価（例: クチコミ評価：5点／3件）
    "detail_url",
    "source_url",  # 取得した一覧ページの URL
    "page",
]
ARRAY_COLUMNS = {"labels"}

# 一覧の「年式」「走行距離」などの見出し → 列名
SPEC_COLUMNS = {
    "年式": "model_year",
    "走行距離": "mileage",
    "車検": "inspection",
    "修復歴": "repair_history",
    "保証": "warranty",
    "整備": "maintenance",
    "排気量": "displacement",
    "ミッション": "transmission",
}


def _text(el, sep: str = "") -> str | None:
    if el is None:
        return None
    t = re.sub(r"\s+", " ", el.get_text(sep, strip=True)).strip()
    return t or None


def parse_list_page(html: str, source_url: str, page: int) -> list[dict]:
    """一覧ページの HTML から、車ごとの dict のリストを作る。画像の URL は取らない。"""
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    for cas in soup.select("div.cassette.js_listTableCassette"):
        car_id = (cas.get("id") or "").removesuffix("_cas") or None
        info = cas.select_one(".cassetteMain__carInfoContainer")
        title_wrap = info.select_one(".cassetteMain__titleWrapper") if info else None
        maker_el = title_wrap.find_previous_sibling("p") if title_wrap else None
        link = cas.select_one("a.cassetteMain__link")
        href = link.get("href") if link else None
        body = [_text(li) for li in cas.select(".carBodyInfoList__item")]
        area = [_text(p) for p in cas.select(".cassetteSub__area p")]
        shop = cas.select_one(".cassetteSub__shop a")
        shop_href = shop.get("href") if shop else None
        m = re.search(r"/shop/[^/]+/(\w+)/", shop_href or "")
        review = cas.select_one(".cassetteSub__review a")
        row = {
            "car_id": car_id,
            "maker": _text(maker_el),
            "title": _text(link),
            "sub_text": _text(cas.select_one(".cassetteMain__subText")),
            "labels": [t for t in (_text(s) for s in cas.select(".cassetteMain__label > span")) if t],
            "listed_label": _text(cas.select_one(".labelIcon--data")),
            "body_type": body[0] if len(body) > 0 else None,
            "color": body[1] if len(body) > 1 else None,
            "total_price": _text(cas.select_one(".totalPrice__content")),
            "base_price": _text(cas.select_one(".basePrice__content")),
            "prefecture": area[0] if len(area) > 0 else None,
            "city": area[1] if len(area) > 1 else None,
            "shop_code": m.group(1) if m else None,
            "shop_name": (shop.get("title") or _text(shop)) if shop else None,
            "shop_review": (review.get("title") or _text(review)) if review else None,
            "detail_url": urljoin(BASE_URL, href) if href else (f"{BASE_URL}/usedcar/detail/{car_id}/index.html" if car_id else None),
            "source_url": source_url,
            "page": page,
        }
        for box in cas.select(".specList__detailBox"):
            col = SPEC_COLUMNS.get(_text(box.select_one(".specList__title")) or "")
            if col:
                row[col] = _text(box.select_one(".specList__data"))
        rows.append(row)
    return rows


def parse_last_page(html: str) -> int | None:
    """ページ送りに出ている最大のページ番号（「最後」のリンク先 indexNNN.html）。"""
    nums = [int(n) for n in re.findall(r"/usedcar/index(\d+)\.html", html)]
    return max(nums) if nums else None


def parse_total_count(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    return _text(soup.select_one(".resultBar__result"))


def parse_sitemap(xml: str) -> tuple[list[str], list[str | None]]:
    """sitemap（index でも urlset でも）から loc と lastmod の一覧を返す。"""
    root = ET.fromstring(xml.encode("utf-8"))
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs, lastmods = [], []
    for node in root.findall("s:sitemap", ns) + root.findall("s:url", ns):
        loc = node.findtext("s:loc", namespaces=ns)
        if loc:
            locs.append(loc.strip())
            lm = node.findtext("s:lastmod", namespaces=ns)
            lastmods.append(lm.strip() if lm else None)
    return locs, lastmods


def car_id_from_detail_url(url: str) -> str | None:
    m = re.search(r"/usedcar/detail/([A-Za-z0-9]+)/", url)
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
        # カーセンサーの robots.txt は「User-agent: *」と Disallow の間に空行があり、Python 標準のパーサーでは
        # Disallow が全部読み飛ばされる（何でも許可と判定される）。Google 等と同じく空行を無視して解釈する
        res = self.session.get(urljoin(BASE_URL, "/robots.txt"), timeout=60)
        res.raise_for_status()
        self.robots = urllib.robotparser.RobotFileParser()
        self.robots.parse([line for line in res.text.splitlines() if line.strip()])
        if not self.robots.can_fetch(USER_AGENT, urljoin(BASE_URL, "/usedcar/search.php")):
            print("robots.txt を読み込みました（/usedcar/search.php などは取らない）")
        else:
            raise RuntimeError("robots.txt の Disallow を読み取れませんでした。形式が変わった可能性があります")
        self._last = time.monotonic()

    def get(self, url: str) -> str:
        if urlparse(url).netloc != urlparse(BASE_URL).netloc:
            raise RuntimeError(f"カーセンサー以外の URL は取りません: {url}")
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
                    res.encoding = res.encoding or "utf-8"
                    return res.text
                print(f"  HTTP {res.status_code}（{attempt}/{self.max_retries}）")
                if res.status_code in BLOCKED_STATUSES:
                    raise Blocked(f"HTTP {res.status_code}: サイトに自動アクセスを止められました")
                if res.status_code not in (500, 502, 503, 504):
                    res.raise_for_status()
            time.sleep(self.delay * 2 * attempt)
        raise RuntimeError(f"{self.max_retries} 回試しても取得できませんでした: {url}")


def page_url(list_url: str, page: int) -> str:
    """1 ページ目の URL（/usedcar/index.html?...）から n ページ目（/usedcar/index{n}.html?...）を作る。"""
    if page == 1:
        return list_url
    return re.sub(r"/usedcar/index\d*\.html", f"/usedcar/index{page}.html", list_url, count=1)


def crawl(
    fetch_page,
    known: set[str],
    max_pages: int,
    stop_after_known_pages: int,
    flush_pages: int,
    on_flush,
    start_page: int = 1,
    last_page=lambda: None,
) -> dict:
    """新着順の start_page から読み、まだ取り込んでいない車だけを集める。

    fetch_page(page) -> rows（取得できない・最後のページを過ぎたら空）。Blocked が飛んだらそこで止める。
    on_flush(rows, next_page) を flush_pages ページごとと最後に呼ぶ（next_page = 次に読むページ = 再開位置）。
    stop_after_known_pages が 0 なら「追いついたら止める」はせず、max_pages（0 なら無制限）か最後のページまで読む。
    last_page() は最後のページ番号（分かれば）。そのページを読んだら reached_end で止める。
    """
    buf: list[dict] = []
    pages = new_total = streak = 0
    caught_up = reached_end = blocked = False
    page = start_page
    while True:
        try:
            rows = fetch_page(page)
        except Blocked as e:
            print(f"page {page} で止められたため終了します: {e}")
            blocked = True
            break
        if not rows:
            if page == start_page == 1:
                raise RuntimeError("1 ページ目から 1 件も取れませんでした。ページの構造が変わった可能性があります")
            reached_end = True
            break
        pages += 1
        new_rows = []
        for r in rows:
            key = r.get("car_id")
            if key and key not in known:
                known.add(key)
                new_rows.append(r)
        buf.extend(new_rows)
        new_total += len(new_rows)
        print(f"page {page}: {len(rows)} 台（うち新規 {len(new_rows)}）")
        streak = 0 if new_rows else streak + 1
        lp = last_page()
        page += 1
        if pages % flush_pages == 0:
            on_flush(buf, page)
            buf = []
        if stop_after_known_pages and streak >= stop_after_known_pages:
            caught_up = True
            break
        if lp and page > lp:
            reached_end = True
            break
        if max_pages and pages >= max_pages:
            break
    on_flush(buf, page)
    return {"pages": pages, "new_rows": new_total, "next_page": page, "caught_up": caught_up,
            "reached_end": reached_end, "blocked": blocked}


def fetch_sitemap_car_ids(fetcher: Fetcher) -> tuple[set[str], dt.datetime | None]:
    """在庫詳細の sitemap をすべて読み、掲載中の車の car_id と、sitemap の更新時刻（最も古いもの、UTC）を返す。

    1 ファイルでも取れなければ例外にする（一部だけで判定すると、取れなかった分の車がすべて掲載終了になってしまうため）。
    """
    files, lastmods = parse_sitemap(fetcher.get(SITEMAP_INDEX_URL))
    if not files:
        raise RuntimeError("sitemap の目次が空でした")
    ids: set[str] = set()
    for i, url in enumerate(files, 1):
        locs, _ = parse_sitemap(fetcher.get(url))
        ids.update(c for c in (car_id_from_detail_url(u) for u in locs) if c)
        if i % 10 == 0:
            print(f"  sitemap {i}/{len(files)}: 累計 {len(ids)} 台")
    stamps = []
    for lm in lastmods:
        if lm:
            try:
                stamps.append(dt.datetime.fromisoformat(lm.replace("Z", "+00:00")).astimezone(dt.timezone.utc))
            except ValueError:
                pass
    oldest = min(stamps).replace(tzinfo=None) if stamps else None
    print(f"sitemap: {len(files)} ファイル / 掲載中 {len(ids)} 台 / 更新時刻（UTC）{oldest}")
    return ids, oldest


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--table", default="carsensor_used_car_listings")
    p.add_argument("--state-table", default="carsensor_used_car_ingest_state")
    p.add_argument("--list-url", required=True, help="新着順（SORT=19）の一覧 URL（/usedcar/index.html?...）")
    p.add_argument("--initial-pages", type=int, default=0,
                   help="全件読み込みで読むページ数。0 なら最後のページまで（1 以上は動作確認用）")
    p.add_argument("--max-pages", type=int, default=300, help="増分の 1 回で読む最大ページ数（止まらなくなったときの安全装置）")
    p.add_argument("--stop-after-known-pages", type=int, default=3)
    p.add_argument("--delay", type=float, default=MIN_DELAY, help=f"リクエストの間隔（秒）。{MIN_DELAY:.0f} 秒以上")
    p.add_argument("--flush-pages", type=int, default=20)
    p.add_argument("--delist-check", choices=["true", "false"], default="true",
                   help="sitemap で掲載終了フラグを更新するか")
    p.add_argument("--max-delist-ratio", type=float, default=0.2,
                   help="1 回で掲載終了にしてよい割合の上限。超えたら sitemap の異常とみなして更新しない")
    args = p.parse_args(argv)

    if "SORT=19" not in args.list_url or "/usedcar/index.html" not in args.list_url:
        raise ValueError("list_url は新着順（SORT=19）の 1 ページ目（/usedcar/index.html?...）にしてください。増分の判定が新着順を前提にしているため")
    if args.delay < MIN_DELAY:
        raise ValueError(f"delay は {MIN_DELAY:.0f} 秒以上にしてください（サイトの運営を妨げないための運用ルール）")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = SparkSession.builder.getOrCreate()
    target = f"{args.catalog}.{args.schema}.{args.table}"
    state_table = f"{args.catalog}.{args.schema}.{args.state_table}"
    run_id = str(uuid.uuid4())

    def utc_now() -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    # ---- 状態（全件読み込みの進み具合と、最後の掲載終了チェック）。1 行だけのテーブル
    state_schema = T.StructType(
        [
            T.StructField("initial_load_status", T.StringType()),  # in_progress / completed
            T.StructField("next_page", T.IntegerType()),  # 全件読み込みを再開するページ
            T.StructField("initial_load_started_at", T.TimestampType()),
            T.StructField("initial_load_completed_at", T.TimestampType()),
            T.StructField("last_delist_check_at", T.TimestampType()),
            T.StructField("last_sitemap_count", T.LongType()),
            T.StructField("last_run_id", T.StringType()),
            T.StructField("last_mode", T.StringType()),
            T.StructField("updated_at", T.TimestampType()),
        ]
    )

    def write_state(st: dict) -> None:
        st = {**st, "last_run_id": run_id, "updated_at": utc_now()}
        spark.createDataFrame([tuple(st.get(f.name) for f in state_schema)], state_schema) \
            .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(state_table)

    state = spark.table(state_table).first().asDict() if spark.catalog.tableExists(state_table) else None
    if state and state.get("initial_load_status") == "completed":
        mode, start_page = "incremental", 1
        state["last_mode"] = mode
    else:
        mode = "initial"
        start_page = (state or {}).get("next_page") or 1
        state = {
            **(state or {}),
            "initial_load_status": "in_progress",
            "next_page": start_page,
            "initial_load_started_at": (state or {}).get("initial_load_started_at") or utc_now(),
            "last_mode": mode,
        }

    known: set[str] = set()
    if spark.catalog.tableExists(target):
        known = {r[0] for r in spark.table(target).select("car_id").where("car_id IS NOT NULL").distinct().collect()}

    schema = T.StructType(
        [
            T.StructField(
                c, T.IntegerType() if c == "page" else T.ArrayType(T.StringType()) if c in ARRAY_COLUMNS else T.StringType()
            )
            for c in COLUMNS
        ]
    )
    written = 0

    def on_flush(rows: list[dict], next_page: int) -> None:
        nonlocal written
        if rows:
            now = dt.datetime.now(JST)
            (
                spark.createDataFrame([tuple(r.get(c) for c in COLUMNS) for r in rows], schema)
                .withColumn("ingest_mode", F.lit(mode))
                .withColumn("ingest_run_id", F.lit(run_id))
                .withColumn("ingested_at", F.lit(now.astimezone(dt.timezone.utc).replace(tzinfo=None)).cast("timestamp"))
                .withColumn("ingest_date", F.lit(now.date().isoformat()).cast("date"))
                # 掲載終了フラグ。一覧に出ていた = 掲載中。あとで sitemap との照合で更新する
                .withColumn("is_delisted", F.lit(False))
                .withColumn("delisted_date", F.lit(None).cast("date"))
                .withColumn("last_seen_date", F.lit(now.date().isoformat()).cast("date"))
                .write.mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target)
            )
            written += len(rows)
        # 書き込みのあとで再開位置を保存する（途中で止まっても、次の実行はここから続ける）
        if mode == "initial":
            state["next_page"] = next_page
            write_state(state)

    fetcher = Fetcher(delay=args.delay)
    last_page: dict[str, int | None] = {"value": None}

    def fetch_page(page: int) -> list[dict]:
        url = page_url(args.list_url, page)
        try:
            html = fetcher.get(url)
        except requests.HTTPError as e:
            print(f"page {page} を取得できませんでした（最後のページを過ぎたとみなす）: {e}")
            return []
        # 新着が増えると最後のページ番号も増えるので、毎ページ読み直す
        lp = parse_last_page(html)
        if lp:
            last_page["value"] = max(lp, page)
        if page == start_page:
            print(f"サイト上の掲載台数: {parse_total_count(html)} / 最後のページ: {last_page['value']}")
        return parse_list_page(html, url, page)

    print(f"モード: {mode} / 開始ページ: {start_page} / 取り込み済みの車: {len(known)} / "
          f"取り込み先: {target} / run_id: {run_id}")
    if mode == "initial":
        remaining = args.initial_pages - (start_page - 1) if args.initial_pages else 0
        if args.initial_pages and remaining <= 0:
            remaining = 1
        result = crawl(fetch_page, known, max_pages=remaining, stop_after_known_pages=0,
                       flush_pages=args.flush_pages, on_flush=on_flush, start_page=start_page,
                       last_page=lambda: last_page["value"])
        limit_reached = bool(args.initial_pages) and result["next_page"] > args.initial_pages
        if (result["reached_end"] or limit_reached) and not result["blocked"]:
            state.update({"initial_load_status": "completed", "initial_load_completed_at": utc_now()})
            write_state(state)
            print("全件読み込みが完了しました。次の実行からは増分になります")
        else:
            print(f"全件読み込みの途中で止まりました。次の実行は page {result['next_page']} から続けます")
    else:
        result = crawl(fetch_page, known, max_pages=args.max_pages, stop_after_known_pages=args.stop_after_known_pages,
                       flush_pages=args.flush_pages, on_flush=on_flush)
        write_state(state)
        if not (result["caught_up"] or result["reached_end"] or result["blocked"]):
            print(f"注意: 取り込み済みの車に追いつく前に上限（{args.max_pages} ページ）に達しました。"
                  "この間の新着は取りこぼしている可能性があります。max_pages か実行頻度を見直してください")
    print(f"一覧: {result['pages']} ページを読み、新しい車 {written} 台を {target} に追記しました")
    if result["blocked"]:
        raise RuntimeError("サイトのボット対策に止められました。再試行や回避はしません。取り込み方を見直してください")

    # ---- 掲載終了フラグの更新（sitemap との照合）
    if args.delist_check != "true" or not spark.catalog.tableExists(target):
        return
    try:
        present, sitemap_at = fetch_sitemap_car_ids(fetcher)
    except Blocked as e:
        raise RuntimeError(f"sitemap の取得中にボット対策に止められました。再試行や回避はしません: {e}") from e
    except Exception as e:  # noqa: BLE001 sitemap が取れないときは一覧の取り込みだけ成功にして、フラグは次回に回す
        print(f"注意: sitemap を読めなかったため、掲載終了フラグは更新しません: {e}")
        return
    # sitemap の更新時刻より後に取り込んだ車は、まだ sitemap に載っていないだけなので判定しない
    cutoff = sitemap_at or utc_now()

    spark.createDataFrame([(c,) for c in present], "car_id string").createOrReplaceTempView("carsensor_sitemap_ids")
    spark.createDataFrame([(cutoff,)], "cutoff timestamp").createOrReplaceTempView("carsensor_sitemap_cutoff")
    counts = spark.sql(f"""
        SELECT
          count_if(NOT t.is_delisted) AS active,
          count_if(NOT t.is_delisted AND s.car_id IS NULL AND t.ingested_at < c.cutoff) AS to_delist,
          count_if(t.is_delisted AND s.car_id IS NOT NULL) AS to_relist
        FROM {target} t
        CROSS JOIN carsensor_sitemap_cutoff c
        LEFT JOIN carsensor_sitemap_ids s ON t.car_id = s.car_id
    """).first()
    print(f"掲載終了チェック: 掲載中 {counts.active} 台 / 今回掲載終了 {counts.to_delist} 台 / 掲載中に戻る {counts.to_relist} 台")
    if counts.active and counts.to_delist > counts.active * args.max_delist_ratio:
        print(f"注意: 掲載中の {args.max_delist_ratio:.0%} を超える車が sitemap から消えました。"
              "sitemap 側の異常の可能性があるため、掲載終了フラグは更新しません")
        return

    today = dt.datetime.now(JST).date().isoformat()
    spark.sql(f"""
        MERGE INTO {target} t
        USING (
          -- 同じ car_id が 2 行あっても MERGE が失敗しないよう、car_id ごとに 1 行にまとめる
          SELECT t2.car_id, bool_or(s.car_id IS NOT NULL) AS present, min(t2.ingested_at) < min(c.cutoff) AS checkable
          FROM {target} t2
          CROSS JOIN carsensor_sitemap_cutoff c
          LEFT JOIN carsensor_sitemap_ids s ON t2.car_id = s.car_id
          WHERE t2.car_id IS NOT NULL
          GROUP BY t2.car_id
        ) src
        ON t.car_id = src.car_id
        WHEN MATCHED AND src.present THEN UPDATE SET
          is_delisted = false, delisted_date = NULL, last_seen_date = DATE'{today}'
        WHEN MATCHED AND NOT src.present AND src.checkable AND NOT t.is_delisted THEN UPDATE SET
          is_delisted = true, delisted_date = DATE'{today}'
    """)
    state.update({"last_delist_check_at": utc_now(), "last_sitemap_count": len(present)})
    write_state(state)
    print("掲載終了フラグを更新しました")


if __name__ == "__main__":
    main(sys.argv[1:])
