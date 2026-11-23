"""bronze の SUUMO 賃貸データから、silver の部屋テーブル（1 行 = 1 部屋、値は数値や配列に整えたもの）を作り直す。

  入力: {catalog}.{bronze}.suumo_rental_listings（部屋を初めて見つけたときの内容）
        {catalog}.{bronze}.suumo_rental_sightings（実行ごとに一覧で見かけた部屋。掲載終了の判定に使う）
        {catalog}.{bronze}.suumo_rental_ingest_runs（実行の記録。最後まで読めた日曜の全件読み直しを探す）
  出力: {catalog}.{silver}.suumo_rental_rooms（毎回まるごと作り直す）

掲載終了の判定:
  - 日曜の全件読み直しが最後まで終わった回だけを使う（毎日の増分は先頭 100 ページしか読まないので、載っていないとは言えない）
  - 一番新しい全件読み直しで見かけなかった部屋を掲載終了（listed_flag = false）とする
  - deleted_date は、最後に見かけた日より後で、最初に最後まで終わった全件読み直しの日（日曜）
  - 全件読み直しでも読み飛ばしが少し出るため、まだ掲載中の部屋が掲載終了になることがある。
    次に一覧で見かけたら掲載中に戻る（毎回作り直すため）

ジョブからは spark_python_task として、取り込みタスクの後に起動される。
"""

from __future__ import annotations

import argparse
import sys

# 同じ部屋かどうかを判定する列（取り込み側の ROOM_KEY_COLUMNS と同じ。順番も同じにする）
ROOM_KEY_COLUMNS = ("building_name", "address", "floor", "layout", "area", "rent", "admin_fee", "deposit", "key_money")

TABLE_COMMENT = (
    "SUUMO 大阪市の賃貸物件（1 行 = 1 部屋）。bronze.suumo_rental_listings の文字列を数値・配列に整え、"
    "掲載中かどうかを付けたもの。取り込みジョブのたびに作り直す。個人の私的利用に限る（公開・共有しない）"
)

# (列名, コメント)。コメントは「論理名：説明。値の意味」
COLUMNS = [
    ("room_id", "部屋ID：部屋を見分けるキー。建物名・所在地・階・間取り・面積・賃料・管理費・敷金・礼金から作るハッシュ（SHA-256）。"
                "掲載する不動産会社が変わっても同じ値になる"),
    ("room_code", "部屋コード：最初に見つけたときの SUUMO の部屋コード（bc）。同じ部屋でも掲載する広告が変わると変わるので、部屋の識別には room_id を使う"),
    ("building_type", "建物種別：賃貸マンション / 賃貸アパート / 賃貸一戸建て / 賃貸テラス・タウンハウス / 賃貸その他"),
    ("building_name", "建物名：一覧に表示された建物名。一戸建てなどは所在地や「○○駅 2階建」のような表示のことがある"),
    ("ward", "区：大阪市の区名（「区」は付けない。例: 北、城東、東淀川）"),
    ("town", "町名：所在地から区と丁目の番号を除いたもの（例: 都島北通）"),
    ("station", "駅：一覧に載っている駅名の配列（最大 3 つ、重複なし、「駅」は付けない）。バスや車での行き方の駅も含む。バス停しか載っていないときは空の配列"),
    ("nearest_station", "最寄り駅：徒歩で行ける駅のうち、徒歩分数が一番短い駅（「駅」は付けない）。"
                        "徒歩で行ける駅が無いときは、一覧の最初に載っている駅。バス停しか載っていないときは NULL"),
    ("nearest_line", "最寄り駅の路線：nearest_station の路線名（例: 地下鉄御堂筋線、ＪＲ大阪環状線）"),
    ("walking_time", "徒歩分数：nearest_station までの徒歩分数（分）。徒歩で行ける駅が無い（バス・車のみ）ときは NULL"),
    ("building_age", "築年数：建ってからの年数（年）。新築は 0。「築N年以上」は N"),
    ("building_floors", "建物の階数：地上の階数（階）。地下の階は数えない。平屋は 1"),
    ("floor", "階：部屋がある階。地下は負の数（B1階は -1）。メゾネット（1-2階など）は下の階。"
              "一戸建て・テラスハウスで階の表示が無いときは 1。それ以外で表示が無いときは NULL"),
    ("rent", "賃料：月額の賃料（円）。例: 7.1万円 → 71000"),
    ("admin_fee", "管理費：月額の管理費・共益費（円）。無しは 0"),
    ("deposit", "敷金：敷金（円）。無しは 0。サイト上の入力ミスとみられる極端な値（約 21 億円超）は NULL"),
    ("key_money", "礼金：礼金（円）。無しは 0。サイト上の入力ミスとみられる極端な値（約 21 億円超）は NULL"),
    ("layout", "間取り：例: ワンルーム、1K、1LDK"),
    ("area", "専有面積：部屋の広さ（m²）"),
    ("first_seen_date", "掲載確認日：この部屋を初めて一覧で見つけた日（日本時間）"),
    ("last_seen_date", "最終確認日：この部屋を最後に一覧で見かけた日（日本時間）"),
    ("listed_flag", "掲載中フラグ：true = 掲載中、false = 掲載終了（最後まで終わった一番新しい日曜の全件読み直しで見つからなかった）。"
                    "全件読み直しがまだ一度も終わっていない間は、すべて true"),
    ("deleted_date", "掲載終了日：掲載終了に気付いた日曜の日付（その週のどこかで掲載が終わった）。掲載中なら NULL"),
    ("detail_url", "詳細ページURL：SUUMO の部屋の詳細ページ（最初に見つけたときの広告）"),
]


def _yen(col: str) -> str:
    """「7.1万円」「6000円」「-」を円の整数にする式。
    サイト上の入力ミスとみられる極端な値（「526029万円」など。INT に収まらない約 21 億円超）は NULL にする。"""
    return (
        f"CASE WHEN {col} IS NULL THEN NULL "
        f"WHEN {col} = '-' THEN 0 "
        f"WHEN {col} LIKE '%万円' THEN try_cast(ROUND(try_cast(regexp_extract({col}, '([0-9.]+)万円', 1) AS DECIMAL(16, 4)) * 10000) AS INT) "
        f"WHEN {col} RLIKE '^[0-9,]+円$' THEN try_cast(replace(regexp_extract({col}, '([0-9,]+)円', 1), ',', '') AS INT) "
        f"END"
    )


def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def comment_statements(table: str) -> list[str]:
    """列ごとのコメント（論理名と値の意味）を付ける文。CREATE ... AS SELECT では列コメントを書けないので、作った後に付ける。"""
    return [f"ALTER TABLE {table} ALTER COLUMN {name} COMMENT {_quote(comment)}" for name, comment in COLUMNS]


def room_id_expr(prefix: str = "") -> str:
    """取り込み側の room_id()（SHA-256 of 9 列を \\x1f でつないだ文字列、NULL は空文字）と同じ値になる式。"""
    parts = ", ".join(f"coalesce({prefix}{c}, '')" for c in ROOM_KEY_COLUMNS)
    return f"sha2(concat_ws(char(31), {parts}), 256)"


def build_sql(catalog: str, bronze: str, silver: str, has_sightings: bool, has_runs: bool) -> str:
    listings = f"{catalog}.{bronze}.suumo_rental_listings"
    sightings_union = (
        f"UNION ALL SELECT room_id, seen_date FROM {catalog}.{bronze}.suumo_rental_sightings" if has_sightings else ""
    )
    full_runs = (
        f"SELECT DISTINCT run_date FROM {catalog}.{bronze}.suumo_rental_ingest_runs "
        f"WHERE ingest_mode = 'full' AND reached_end AND NOT blocked"
        if has_runs
        else "SELECT CAST(NULL AS DATE) AS run_date WHERE false"
    )
    walk_re = "'^[^/]+/[^ ]+駅 歩[0-9]+分$'"
    return f"""
CREATE OR REPLACE TABLE {catalog}.{silver}.suumo_rental_rooms
COMMENT {_quote(TABLE_COMMENT)}
AS
WITH src AS (
  SELECT *, {room_id_expr()} AS room_id
  FROM {listings}
  WHERE building_name IS NOT NULL AND address IS NOT NULL
),
first_row AS (
  -- 同じ部屋が複数行あるときは、最初に取り込んだ行を使う
  SELECT * FROM src
  QUALIFY row_number() OVER (PARTITION BY room_id ORDER BY ingested_at, page, room_code) = 1
),
seen AS (
  SELECT room_id, min(seen_date) AS first_seen_date, max(seen_date) AS last_seen_date
  FROM (SELECT room_id, ingest_date AS seen_date FROM src {sightings_union})
  GROUP BY room_id
),
full_runs AS ({full_runs}),
latest_full AS (SELECT max(run_date) AS run_date FROM full_runs),
parsed AS (
  SELECT
    r.*,
    transform(
      access,
      (a, i) -> named_struct(
        'walk', CASE WHEN a RLIKE {walk_re} THEN CAST(regexp_extract(a, '歩([0-9]+)分$', 1) AS INT) END,
        'i', i,
        'station', nullif(regexp_extract(a, '/([^ /]+?)駅', 1), ''),
        'line', nullif(regexp_extract(a, '^([^/]+)/', 1), '')
      )
    ) AS acc
  FROM first_row r
),
nearest AS (
  SELECT
    p.*,
    coalesce(
      try_element_at(array_sort(filter(acc, x -> x.walk IS NOT NULL)), 1),
      try_element_at(acc, 1)
    ) AS near
  FROM parsed p
)
SELECT
  n.room_id,
  CAST(n.room_code AS BIGINT) AS room_code,
  n.building_type,
  n.building_name,
  nullif(regexp_extract(n.address, '大阪市(.+?)区', 1), '') AS ward,
  nullif(regexp_replace(regexp_extract(n.address, '区(.*)$', 1), '[0-9０-９]+(丁目)?$', ''), '') AS town,
  array_distinct(filter(transform(n.acc, x -> x.station), s -> s IS NOT NULL)) AS station,
  n.near.station AS nearest_station,
  n.near.line AS nearest_line,
  n.near.walk AS walking_time,
  CASE WHEN n.building_age = '新築' THEN 0
       ELSE CAST(regexp_extract(n.building_age, '築([0-9]+)年', 1) AS INT) END AS building_age,
  CASE WHEN n.building_floors = '平屋' THEN 1
       WHEN n.building_floors RLIKE '([0-9]+)階建$' AND n.building_floors NOT RLIKE '^地下[0-9]+階建$'
         THEN CAST(regexp_extract(n.building_floors, '([0-9]+)階建$', 1) AS INT) END AS building_floors,
  CASE WHEN n.floor RLIKE '^B?[0-9]+(-B?[0-9]+)?階$' THEN
         array_min(transform(split(regexp_replace(n.floor, '階$', ''), '-'),
                             t -> CASE WHEN t LIKE 'B%' THEN -CAST(substr(t, 2) AS INT) ELSE CAST(t AS INT) END))
       WHEN n.building_type IN ('賃貸一戸建て', '賃貸テラス・タウンハウス') THEN 1 END AS floor,
  {_yen('n.rent')} AS rent,
  {_yen('n.admin_fee')} AS admin_fee,
  {_yen('n.deposit')} AS deposit,
  {_yen('n.key_money')} AS key_money,
  n.layout,
  CAST(regexp_extract(n.area, '([0-9.]+)', 1) AS DECIMAL(7, 2)) AS area,
  s.first_seen_date,
  s.last_seen_date,
  (lf.run_date IS NULL OR s.last_seen_date >= lf.run_date) AS listed_flag,
  CASE WHEN lf.run_date IS NOT NULL AND s.last_seen_date < lf.run_date THEN
         (SELECT min(f.run_date) FROM full_runs f WHERE f.run_date > s.last_seen_date) END AS deleted_date,
  n.detail_url
FROM nearest n
JOIN seen s USING (room_id)
CROSS JOIN latest_full lf
"""


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze-schema", required=True)
    p.add_argument("--silver-schema", required=True)
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    bronze = f"{args.catalog}.{args.bronze_schema}"
    sql = build_sql(
        args.catalog,
        args.bronze_schema,
        args.silver_schema,
        has_sightings=spark.catalog.tableExists(f"{bronze}.suumo_rental_sightings"),
        has_runs=spark.catalog.tableExists(f"{bronze}.suumo_rental_ingest_runs"),
    )
    spark.sql(sql)
    target = f"{args.catalog}.{args.silver_schema}.suumo_rental_rooms"
    for stmt in comment_statements(target):
        spark.sql(stmt)
    row = spark.sql(f"SELECT count(*) AS n, count_if(listed_flag) AS listed FROM {target}").first()
    print(f"{target} を作り直しました: {row['n']} 部屋（うち掲載中 {row['listed']}）")


if __name__ == "__main__":
    main(sys.argv[1:])
