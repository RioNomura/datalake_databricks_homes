"""bronze のカーセンサー中古車データを型変換し、silver テーブル（1 台 1 行）を作り直す。

- 入力: bronze.carsensor_used_car_listings（一覧）、carsensor_used_car_attributes（駆動方式・エンジン種別）、
        carsensor_used_car_attr_state（絞り込みの全件読み込みが終わったか）
- 出力: silver.carsensor_used_car_listings を毎回 CREATE OR REPLACE で作り直す（6 万台程度なので全件で十分速い）

変換ルール（2026-11-23 オーナー指定）:
  - 金額は円の整数（297万円 → 2970000）。「応談」は NULL
  - 走行距離は km の整数（1.8万km → 18000）。「交換車」（メーター交換）も数値は入れる
  - 排気量は cc の整数（1500CC → 1500）。「不明」は NULL
  - 年式は西暦（整数）と和暦（R07 など）の 2 列
  - 車検は満了月の 1 日の日付（2028(R10)年07月 → 2028-07-01）。
    「車検整備付」は取り込んだ月の 2 年後（2026-11 に取り込み → 2028-11-01）。それ以外（車検整備無・新車未登録）は NULL
  - 保証・整備は true / false（保証付 / 法定整備付 が true）
  - クチコミ評価は点数と件数の 2 列
  - 駆動方式は 4WD の絞り込みに出た車が 4WD、それ以外は 2WD。エンジン種別はディーゼル・ハイブリッド・電気・その他の
    絞り込みに出た車がその値、それ以外はガソリン。どちらも絞り込みの全件読み込みが終わるまでは、出ていない車を NULL にする
"""

from __future__ import annotations

import argparse
import re
import sys

# silver の列と説明（テーブルの列コメントにも入れる）
COLUMN_COMMENTS = {
    "car_id": "物件番号（カーセンサーの BKKN）。1 台 1 行のキー",
    "maker": "メーカー（例: トヨタ）",
    "car_model": "車名（例: ヤリスクロス）",
    "car_grade": "グレード（例: 1.5 ハイブリッド Z）",
    "sub_text": "販売店のひとこと",
    "body_type": "ボディタイプ（例: クロカン・ＳＵＶ）",
    "color": "色（例: 黒Ｍ）",
    "total_price": "支払総額（円）。応談は NULL",
    "base_price": "車両本体価格（円）。応談は NULL",
    "model_year": "年式（西暦）",
    "model_year_jp": "年式（和暦。例: R07、H31,R01）",
    "mileage": "走行距離（km）",
    "inspection": "車検満了月（その月の 1 日）。車検整備付は取り込んだ月の 2 年後。車検整備無・新車未登録は NULL",
    "warranty": "保証付なら true、保証無なら false",
    "maintenance": "法定整備付なら true、法定整備無なら false",
    "displacement": "排気量（cc）。不明は NULL",
    "transmission": "ミッション（例: フロアCVT）",
    "drive_type": "駆動方式（2WD / 4WD）。絞り込みの全件読み込みが終わるまでは NULL のことがある",
    "engine_type": "エンジン種別（ガソリン / ディーゼル / ハイブリッド / 電気 / その他）。同上",
    "prefecture": "販売店の都道府県",
    "city": "販売店の市区町村",
    "shop_code": "販売店コード",
    "shop_name": "販売店名",
    "shop_review_point": "販売店のクチコミ評価（点）。クチコミが無ければ NULL",
    "shop_review_count": "販売店のクチコミ件数。クチコミが無ければ NULL",
    "detail_url": "詳細ページの URL",
    "ingest_date": "初めて取り込んだ日（JST）",
    "is_delisted": "掲載終了フラグ（sitemap から消えたら true）",
    "delisted_date": "掲載終了と判定した日（JST）",
    "last_seen_date": "掲載中と最後に確認できた日（JST）",
}


def build_sql(catalog: str, bronze: str, silver: str, sfx: str = "") -> str:
    b = f"{catalog}.{bronze}"
    return f"""
CREATE OR REPLACE TABLE {catalog}.{silver}.carsensor_used_car_listings{sfx}
COMMENT 'カーセンサーの中古車（関西・関東、修復歴なし、AT、メーカー系販売店）。bronze を型変換したもの。1 行 = 1 台。carsensor_used_car_ingest ジョブが毎日作り直す'
AS
WITH listings AS (
  -- 念のため car_id ごとに 1 行（最初に取り込んだ行）にする
  SELECT * FROM {b}.carsensor_used_car_listings{sfx}
  WHERE car_id IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY car_id ORDER BY ingested_at) = 1
),
attrs AS (
  SELECT
    car_id,
    max(CASE WHEN attribute = 'drive_type' THEN value END) AS drive_type,
    max(CASE WHEN attribute = 'engine_type' THEN value END) AS engine_type
  FROM {b}.carsensor_used_car_attributes{sfx}
  GROUP BY car_id
),
facet_done AS (
  -- 属性ごとに、その絞り込みがすべて全件読み込み済みか（済んでいれば、出てこなかった車を 2WD / ガソリンとみなせる）
  SELECT
    coalesce(bool_and(initial_load_status = 'completed') FILTER (WHERE attribute = 'drive_type'), false) AS drive_done,
    coalesce(bool_and(initial_load_status = 'completed') FILTER (WHERE attribute = 'engine_type'), false) AS engine_done
  FROM {b}.carsensor_used_car_attr_state{sfx}
)
SELECT
  l.car_id,
  l.maker,
  l.car_model,
  l.car_grade,
  l.sub_text,
  l.body_type,
  l.color,
  CAST(round(try_cast(regexp_extract(l.total_price, '^([0-9.]+)万円$', 1) AS DECIMAL(12, 2)) * 10000) AS BIGINT) AS total_price,
  CAST(round(try_cast(regexp_extract(l.base_price, '^([0-9.]+)万円$', 1) AS DECIMAL(12, 2)) * 10000) AS BIGINT) AS base_price,
  try_cast(regexp_extract(l.model_year, '^([0-9]{{4}})', 1) AS INT) AS model_year,
  nullif(regexp_extract(l.model_year, '\\\\(([^)]*)\\\\)', 1), '') AS model_year_jp,
  CAST(round(CASE
    WHEN l.mileage RLIKE '[0-9.]+万km' THEN try_cast(regexp_extract(l.mileage, '([0-9.]+)万km', 1) AS DECIMAL(12, 2)) * 10000
    WHEN l.mileage RLIKE '^[0-9.]+km$' THEN try_cast(regexp_extract(l.mileage, '^([0-9.]+)km$', 1) AS DECIMAL(12, 2))
  END) AS INT) AS mileage,
  CASE
    WHEN l.inspection RLIKE '^[0-9]{{4}}\\\\(.*\\\\)年[0-9]{{1,2}}月$' THEN make_date(
      CAST(regexp_extract(l.inspection, '^([0-9]{{4}})', 1) AS INT),
      CAST(regexp_extract(l.inspection, '年([0-9]{{1,2}})月', 1) AS INT),
      1)
    WHEN l.inspection = '車検整備付' THEN add_months(trunc(l.ingest_date, 'MM'), 24)
  END AS inspection,
  CASE l.warranty WHEN '保証付' THEN true WHEN '保証無' THEN false END AS warranty,
  CASE l.maintenance WHEN '法定整備付' THEN true WHEN '法定整備無' THEN false END AS maintenance,
  try_cast(regexp_extract(l.displacement, '^([0-9]+)CC$', 1) AS INT) AS displacement,
  l.transmission,
  coalesce(a.drive_type, CASE WHEN d.drive_done THEN '2WD' END) AS drive_type,
  coalesce(a.engine_type, CASE WHEN d.engine_done THEN 'ガソリン' END) AS engine_type,
  l.prefecture,
  l.city,
  l.shop_code,
  l.shop_name,
  try_cast(regexp_extract(l.shop_review, '：([0-9.]+)点', 1) AS DOUBLE) AS shop_review_point,
  try_cast(regexp_extract(l.shop_review, '／([0-9]+)件', 1) AS INT) AS shop_review_count,
  l.detail_url,
  l.ingest_date,
  l.is_delisted,
  l.delisted_date,
  l.last_seen_date
FROM listings l
LEFT JOIN attrs a ON l.car_id = a.car_id
CROSS JOIN facet_done d
"""


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze-schema", default="bronze")
    p.add_argument("--silver-schema", default="silver")
    p.add_argument("--table-suffix", default="", help="テーブル名の末尾（dev は _dev）")
    args = p.parse_args(argv)
    sfx = args.table_suffix
    if sfx and not re.fullmatch(r"_[a-z0-9]+", sfx):
        raise ValueError("table_suffix は _dev のように、アンダースコア + 英小文字・数字にしてください")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    b = f"{args.catalog}.{args.bronze_schema}"
    for t in ("carsensor_used_car_listings", "carsensor_used_car_attributes", "carsensor_used_car_attr_state"):
        if not spark.catalog.tableExists(f"{b}.{t}{sfx}"):
            print(f"{b}.{t}{sfx} がまだ無いので silver は作りません（初回の取り込みが進むと作られます）")
            return

    target = f"{args.catalog}.{args.silver_schema}.carsensor_used_car_listings{sfx}"
    spark.sql(build_sql(args.catalog, args.bronze_schema, args.silver_schema, sfx))
    for col, comment in COLUMN_COMMENTS.items():
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN {col} COMMENT '{comment}'")

    s = spark.sql(f"""
        SELECT count(*) AS n,
               count_if(total_price IS NULL) AS no_price,
               count_if(mileage IS NULL) AS no_mileage,
               count_if(drive_type IS NULL) AS no_drive,
               count_if(engine_type IS NULL) AS no_engine,
               count_if(is_delisted) AS delisted
        FROM {target}
    """).first()
    print(f"{target} を作り直しました: {s.n} 台（支払総額なし {s.no_price} / 走行距離なし {s.no_mileage} / "
          f"駆動方式なし {s.no_drive} / エンジン種別なし {s.no_engine} / 掲載終了 {s.delisted}）")


if __name__ == "__main__":
    main(sys.argv[1:])
