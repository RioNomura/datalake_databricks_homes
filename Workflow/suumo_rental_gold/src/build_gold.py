"""silver の SUUMO 部屋テーブルから、分析用の gold テーブル（データマート）を作り直す。

  入力: {catalog}.{silver}.suumo_rental_rooms（1 行 = 1 部屋）
  出力（毎日作り直す。メトリクスビュー・ダッシュボード・Genie から使う前提で、全列にコメントを付ける）:
    - {gold}.suumo_room_fact            1 行 = 1 部屋。相場・初期費用・お得度の分析の土台（分析 1・3・4）
    - {gold}.suumo_rent_factor_effects  条件（徒歩・築年数・階・建物種別・間取り・広さ・駅）が月額をいくら動かすか（分析 2）
    - {gold}.suumo_rent_model_metrics   毎日の賃料モデルの精度の記録（追記）

賃料モデル（「この条件ならこのくらい」という妥当な月額を出すもの）:
  - 目的変数は月額（賃料 + 管理費）の対数。説明変数は 最寄り駅・間取り・建物種別・階の帯（カテゴリ）と、
    面積の対数・徒歩分数・築年数（とその 2 乗）・建物の階数
  - 最小二乗法（numpy）。外れ値（入力ミスとみられる部屋）は学習に使わない
  - 対数で当てはめるので、効果は「○% 高い/安い」の形になる。円は「典型的な月額（学習データの中央値）」に掛けた目安

ジョブからは spark_python_task として起動される。prepare() / fit_model() / factor_effects() は Spark に依存しないので、ローカルで確認できる。
個人の私的利用に限る（SUUMO 利用規約）。データや結果を公開・共有しない。
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

import numpy as np
import pandas as pd

JST = dt.timezone(dt.timedelta(hours=9))

# 賃料モデルのカテゴリ変数と、その基準（効果は基準と比べた差で出す）
CATEGORY_BASE = {"station_key": None, "layout_group": "1K", "building_type": "賃貸マンション", "floor_band": "2〜4階"}
MIN_ROOMS_PER_STATION = 30  # これより少ない駅は「区:○○」にまとめる（推定が不安定になるため）
MIN_ROOMS_PER_LAYOUT = 50  # これより少ない間取りは「その他」にまとめる

# ---------------------------------------------------------------- テーブル定義（列名, 型, コメント）
FACT_TABLE_COMMENT = (
    "SUUMO 大阪市の賃貸物件の分析用ファクト（1 行 = 1 部屋、掲載終了の部屋も含む）。silver.suumo_rental_rooms に、"
    "月額・m² 単価・初期費用・帯（徒歩・築年数・階・間取り）・賃料モデルによる妥当な月額とお得度を足したもの。"
    "相場（区・駅・間取り別）、初期費用、お得物件の分析に使う。毎日作り直す。個人の私的利用に限る（公開・共有しない）"
)
FACT_COLUMNS = [
    ("room_id", "string", "部屋ID：部屋を見分けるキー（主キー）。silver.suumo_rental_rooms.room_id と同じ"),
    ("listed_flag", "boolean", "掲載中フラグ：true = 掲載中、false = 掲載終了。今の相場やお得物件を見るときは true に絞る"),
    ("first_seen_date", "date", "掲載確認日：この部屋を初めて一覧で見つけた日（日本時間）"),
    ("deleted_date", "date", "掲載終了日：掲載終了に気付いた日曜の日付。掲載中なら NULL"),
    ("days_on_market", "int", "掲載日数：掲載確認日から、掲載終了日（掲載中なら集計日）までの日数"),
    ("building_name", "string", "建物名：一覧に表示された建物名"),
    ("building_type", "string", "建物種別：賃貸マンション / 賃貸アパート / 賃貸一戸建て / 賃貸テラス・タウンハウス / 賃貸その他"),
    ("ward", "string", "区：大阪市の区名（「区」は付けない。例: 北、城東）"),
    ("town", "string", "町名：所在地から区と丁目の番号を除いたもの（例: 都島北通）"),
    ("nearest_station", "string", "最寄り駅：徒歩で行ける駅のうち一番近い駅（「駅」は付けない）。バス停しか無い部屋は NULL"),
    ("nearest_line", "string", "最寄り駅の路線：nearest_station の路線名（例: 地下鉄御堂筋線）"),
    ("walking_time", "int", "徒歩分数：最寄り駅までの徒歩分数（分）。徒歩で行ける駅が無いときは NULL"),
    ("walking_time_band", "string", "徒歩の帯：「5分以内」「6〜10分」「11〜15分」「16分以上」「徒歩圏に駅なし」のいずれか"),
    ("building_age", "int", "築年数：建ってからの年数（年）。新築は 0"),
    ("building_age_band", "string", "築年数の帯：「新築」「築1〜5年」「築6〜10年」「築11〜20年」「築21〜30年」「築31年以上」のいずれか"),
    ("building_floors", "int", "建物の階数：地上の階数（階）"),
    ("floor", "int", "階：部屋がある階。地下は負の数。一戸建て・テラスハウスで表示が無いときは 1"),
    ("floor_band", "string", "階の帯：「地下」「1階」「2〜4階」「5〜9階」「10〜19階」「20階以上」「不明」のいずれか"),
    ("layout", "string", "間取り：一覧の表示のまま（例: ワンルーム、1K、1LDK）"),
    ("layout_category", "string", "間取りの分類：「ワンルーム・1K」「1DK・1LDK」「2K〜2LDK」「3K以上」のいずれか（S（納戸）付きも同じ分類）"),
    ("room_count", "int", "部屋数：間取りの数字（ワンルームは 1）"),
    ("area", "decimal(7,2)", "専有面積：部屋の広さ（m²）"),
    ("rent", "int", "賃料：月額の賃料（円）"),
    ("admin_fee", "int", "管理費：月額の管理費・共益費（円）。無しは 0"),
    ("monthly_cost", "int", "月額：賃料 + 管理費（円）。毎月払う額。相場の比較はこれを使う"),
    ("rent_per_sqm", "decimal(10,1)", "賃料の m² 単価：賃料 ÷ 専有面積（円/m²）"),
    ("monthly_cost_per_sqm", "decimal(10,1)", "月額の m² 単価：月額 ÷ 専有面積（円/m²）"),
    ("deposit", "int", "敷金：敷金（円）。無しは 0。入力ミスとみられる値は NULL"),
    ("key_money", "int", "礼金：礼金（円）。無しは 0。入力ミスとみられる値は NULL"),
    ("deposit_months", "decimal(5,1)", "敷金の月数：敷金 ÷ 賃料（か月）。外れ値の部屋は NULL"),
    ("key_money_months", "decimal(5,1)", "礼金の月数：礼金 ÷ 賃料（か月）。外れ値の部屋は NULL"),
    ("no_deposit_flag", "boolean", "敷金なしフラグ：true = 敷金 0 円"),
    ("no_key_money_flag", "boolean", "礼金なしフラグ：true = 礼金 0 円"),
    ("initial_cost", "int", "初期費用の目安：敷金 + 礼金 + 1 か月分の月額（円）。仲介手数料・保証料・火災保険などは含まない。外れ値の部屋は NULL"),
    ("initial_cost_months", "decimal(5,1)", "初期費用の月数：初期費用の目安 ÷ 月額（か月）。外れ値の部屋は NULL"),
    ("predicted_monthly_cost", "int", "妥当な月額：賃料モデルが条件（最寄り駅・間取り・広さ・徒歩・築年数・階・建物種別）から出した月額（円）。外れ値の部屋は NULL"),
    ("monthly_cost_gap", "int", "月額の差：妥当な月額 − 実際の月額（円）。プラスなら条件の割に安い。外れ値の部屋は NULL"),
    ("deal_score", "decimal(6,3)", "お得度：1 − 実際の月額 ÷ 妥当な月額。0.10 なら条件の割に 10% 安い、マイナスなら割高。外れ値の部屋は NULL"),
    ("deal_rank", "int", "お得度の順位：掲載中の部屋の中でお得度が高い順（1 が一番お得）。掲載終了・外れ値の部屋は NULL"),
    ("is_outlier", "boolean", "外れ値フラグ：true = 入力ミスとみられる部屋（面積 8m² 未満か 200m² 超、m² 単価が上下 0.5% の外側、敷金・礼金が賃料の 24 か月分超など）。"
                              "賃料モデルの学習とお得度からは除く。相場の集計でも除くのがよい"),
    ("detail_url", "string", "詳細ページURL：SUUMO の部屋の詳細ページ"),
    ("snapshot_date", "date", "集計日：このテーブルを作った日（日本時間）"),
]

EFFECTS_TABLE_COMMENT = (
    "SUUMO 大阪市の賃貸で、条件の違いが月額（賃料 + 管理費）をどれだけ動かすか（1 行 = 1 つの条件の比較）。"
    "賃料モデル（他の条件をそろえたうえでの比較）から出したもの。部屋選びでどの条件を妥協すると安くなるかを見るのに使う。毎日作り直す"
)
EFFECTS_COLUMNS = [
    ("factor_group", "string", "条件の種類：「広さ」「駅徒歩」「築年数」「階」「建物種別」「間取り」「駅」のいずれか"),
    ("factor", "string", "条件：比べる条件（例: 徒歩 +1分、築10年 → 築20年、1階、賃貸アパート、梅田）"),
    ("comparison_base", "string", "比較の基準：何と比べた差か（例: 2〜4階と比べて、平均的な駅と比べて）"),
    ("effect_pct", "decimal(6,3)", "効果（割合）：月額が何割変わるか。-0.05 なら 5% 安い、0.10 なら 10% 高い"),
    ("effect_yen", "int", "効果（円の目安）：典型的な月額（base_monthly_cost）に効果（割合）を掛けた額（円）。マイナスなら安くなる"),
    ("base_monthly_cost", "int", "典型的な月額：学習に使った部屋の月額の中央値（円）。effect_yen の計算に使った値"),
    ("n_rooms", "int", "部屋数：その条件に当てはまる部屋の数（学習データ）。少ないほど効果の値は不安定"),
    ("model_date", "date", "モデル作成日：賃料モデルを当てはめた日（日本時間）"),
]

METRICS_TABLE_COMMENT = "SUUMO 大阪市の賃料モデルの精度の記録（1 行 = 1 日のモデル）。毎日追記する。お得度がどれくらい信用できるかを見るのに使う"
METRICS_COLUMNS = [
    ("model_date", "date", "モデル作成日：賃料モデルを当てはめた日（日本時間）"),
    ("n_rooms", "int", "部屋数：silver の部屋数（掲載終了も含む）"),
    ("n_train", "int", "学習部屋数：外れ値を除いて学習に使った部屋数"),
    ("n_outliers", "int", "外れ値の部屋数：入力ミスとみられて学習から除いた部屋数"),
    ("n_features", "int", "説明変数の数：ダミー変数を含む説明変数の数"),
    ("r2_holdout", "decimal(6,4)", "決定係数：学習に使わなかった 2 割の部屋で測った、月額（対数）の当てはまり（1 に近いほどよい）"),
    ("median_abs_pct_error", "decimal(6,4)", "誤差の中央値：学習に使わなかった 2 割の部屋で、予測した月額が実際から何割ずれたかの中央値（0.07 なら 7%）"),
    ("base_monthly_cost", "int", "典型的な月額：学習に使った部屋の月額の中央値（円）"),
]


# ---------------------------------------------------------------- 加工（Spark に依存しない）
def _band(value, edges: list[tuple[float, str]], missing: str) -> str:
    if pd.isna(value):
        return missing
    for upper, label in edges:
        if value <= upper:
            return label
    return edges[-1][1]


def walking_band(w) -> str:
    return _band(w, [(5, "5分以内"), (10, "6〜10分"), (15, "11〜15分"), (float("inf"), "16分以上")], "徒歩圏に駅なし")


def age_band(a) -> str:
    return _band(a, [(0, "新築"), (5, "築1〜5年"), (10, "築6〜10年"), (20, "築11〜20年"), (30, "築21〜30年"), (float("inf"), "築31年以上")], "不明")


def floor_band(f) -> str:
    if pd.isna(f):
        return "不明"
    if f < 1:
        return "地下"
    return _band(f, [(1, "1階"), (4, "2〜4階"), (9, "5〜9階"), (19, "10〜19階"), (float("inf"), "20階以上")], "不明")


def room_count(layout) -> int | None:
    if not isinstance(layout, str):
        return None
    if layout == "ワンルーム":
        return 1
    digits = "".join(ch for ch in layout if ch.isdigit())
    return int(digits) if digits else None


def layout_category(layout) -> str | None:
    n = room_count(layout)
    if n is None:
        return None
    if n >= 3:
        return "3K以上"
    if n == 2:
        return "2K〜2LDK"
    if layout == "ワンルーム" or "LDK" not in layout and "DK" not in layout:
        return "ワンルーム・1K"
    return "1DK・1LDK"


def prepare(df: pd.DataFrame, today: dt.date) -> pd.DataFrame:
    """silver の行に、月額・単価・初期費用・帯・外れ値フラグを足す。"""
    d = df.copy()
    for c in ["walking_time", "building_age", "building_floors", "floor", "rent", "admin_fee", "deposit", "key_money"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["area"] = pd.to_numeric(d["area"], errors="coerce").astype(float)
    for c in ["first_seen_date", "deleted_date"]:
        d[c] = pd.to_datetime(d[c], errors="coerce").dt.date
    d["listed_flag"] = d["listed_flag"].map(lambda v: v in (True, "true", "True", 1))

    d["monthly_cost"] = d["rent"] + d["admin_fee"].fillna(0)
    d["rent_per_sqm"] = d["rent"] / d["area"]
    d["monthly_cost_per_sqm"] = d["monthly_cost"] / d["area"]
    lo, hi = d["monthly_cost_per_sqm"].quantile([0.005, 0.995])
    d["is_outlier"] = (
        d["rent"].isna() | d["area"].isna() | (d["area"] < 8) | (d["area"] > 200)
        | (d["monthly_cost_per_sqm"] < lo) | (d["monthly_cost_per_sqm"] > hi)
        | d["deposit"].isna() | d["key_money"].isna()
        | (d["deposit"] > d["rent"] * 24) | (d["key_money"] > d["rent"] * 24)
    )
    ok = ~d["is_outlier"]
    d["deposit_months"] = (d["deposit"] / d["rent"]).where(ok)
    d["key_money_months"] = (d["key_money"] / d["rent"]).where(ok)
    d["no_deposit_flag"] = d["deposit"] == 0
    d["no_key_money_flag"] = d["key_money"] == 0
    d["initial_cost"] = (d["deposit"] + d["key_money"] + d["monthly_cost"]).where(ok)
    d["initial_cost_months"] = (d["initial_cost"] / d["monthly_cost"]).where(ok)

    d["walking_time_band"] = d["walking_time"].map(walking_band)
    d["building_age_band"] = d["building_age"].map(age_band)
    d["floor_band"] = d["floor"].map(floor_band)
    d["room_count"] = d["layout"].map(room_count)
    d["layout_category"] = d["layout"].map(layout_category)
    end = d["deleted_date"].where(d["deleted_date"].notna(), today)
    d["days_on_market"] = [(e - s).days if s else None for s, e in zip(d["first_seen_date"], end)]
    d["snapshot_date"] = today
    return d


def _model_frame(d: pd.DataFrame) -> pd.DataFrame:
    m = pd.DataFrame(index=d.index)
    st = d.loc[~d["is_outlier"], "nearest_station"].value_counts()
    m["station_key"] = np.where(d["nearest_station"].map(st).fillna(0) >= MIN_ROOMS_PER_STATION,
                                d["nearest_station"], "区:" + d["ward"].fillna("不明"))
    lay = d.loc[~d["is_outlier"], "layout"].value_counts()
    m["layout_group"] = d["layout"].where(d["layout"].map(lay).fillna(0) >= MIN_ROOMS_PER_LAYOUT, "その他")
    m["building_type"] = d["building_type"].fillna("不明")
    m["floor_band"] = d["floor_band"]
    return m


def _design(d: pd.DataFrame, m: pd.DataFrame, cats: dict[str, list[str]]) -> pd.DataFrame:
    X = pd.DataFrame(index=d.index)
    X["const"] = 1.0
    X["log_area"] = np.log(d["area"])
    X["walk"] = d["walking_time"].fillna(10).clip(upper=30)
    X["walk_missing"] = d["walking_time"].isna().astype(float)
    X["age"] = d["building_age"].fillna(d["building_age"].median()).clip(upper=60)
    X["age2"] = X["age"] ** 2 / 100
    X["log_bfloors"] = np.log1p(d["building_floors"].fillna(3))
    dummies = {f"{c}={lv}": (m[c] == lv).astype(float) for c, levels in cats.items() for lv in levels[1:]}  # 先頭が基準
    return pd.concat([X, pd.DataFrame(dummies, index=d.index)], axis=1)


def fit_model(d: pd.DataFrame, seed: int = 0) -> dict:
    """月額（対数）の最小二乗モデルを当てはめ、全部屋の妥当な月額と精度を返す。"""
    m = _model_frame(d)
    train = ~d["is_outlier"]
    cats = {}
    for c, base in CATEGORY_BASE.items():
        levels = m.loc[train, c].value_counts().index.tolist()  # 部屋数の多い順。基準が無ければ一番多いもの
        base = base if base in levels else levels[0]
        cats[c] = [base] + sorted(lv for lv in levels if lv != base)
    X = _design(d, m, cats)
    y = np.log(d["monthly_cost"].astype(float))

    # 精度は学習に使わない 2 割で測り、係数は全部（外れ値以外）で当てはめ直す
    rng = np.random.default_rng(seed)
    hold = rng.random(len(d)) < 0.2
    tr, te = train.values & ~hold, train.values & hold
    beta_cv, *_ = np.linalg.lstsq(X.values[tr], y.values[tr], rcond=None)
    res = y.values[te] - X.values[te] @ beta_cv
    r2 = 1 - res.var() / y.values[te].var()
    mdape = float(np.median(np.abs(np.exp(res) - 1)))

    beta, *_ = np.linalg.lstsq(X.values[train.values], y.values[train.values], rcond=None)
    pred = pd.Series(np.exp(X.values @ beta), index=d.index).where(train)
    return {
        "beta": pd.Series(beta, index=X.columns),
        "cats": cats,
        "model_frame": m,
        "pred": pred,
        "r2": float(r2),
        "mdape": mdape,
        "n_train": int(train.sum()),
        "n_features": int(X.shape[1]),
        "base_monthly_cost": int(round(d.loc[train, "monthly_cost"].median())),
    }


def add_scores(d: pd.DataFrame, model: dict) -> pd.DataFrame:
    d = d.copy()
    d["predicted_monthly_cost"] = model["pred"].round()
    d["monthly_cost_gap"] = d["predicted_monthly_cost"] - d["monthly_cost"]
    d["deal_score"] = 1 - d["monthly_cost"] / d["predicted_monthly_cost"]
    rankable = d["listed_flag"] & d["deal_score"].notna()
    d["deal_rank"] = d["deal_score"].where(rankable).rank(ascending=False, method="first")
    return d


def factor_effects(d: pd.DataFrame, model: dict, today: dt.date) -> pd.DataFrame:
    """係数を「条件の違いで月額が何%・何円変わるか」の表にする。"""
    b, cats, m = model["beta"], model["cats"], model["model_frame"]
    base_cost = model["base_monthly_cost"]
    train = ~d["is_outlier"]
    rows = []

    def add(group, factor, base_desc, log_effect, n):
        pct = float(np.exp(log_effect) - 1)
        rows.append((group, factor, base_desc, round(pct, 3), int(round(pct * base_cost)), base_cost, int(n), today))

    med_area = float(d.loc[train, "area"].median())
    for delta in (5, 10):
        add("広さ", f"広さ +{delta}m²", f"{med_area:.0f}m²（中央値）と比べて",
            b["log_area"] * np.log((med_area + delta) / med_area), train.sum())
    add("駅徒歩", "徒歩 +1分", "徒歩分数が 1 分短い部屋と比べて", b["walk"], d.loc[train, "walking_time"].notna().sum())
    add("駅徒歩", "徒歩 +5分", "徒歩分数が 5 分短い部屋と比べて", 5 * b["walk"], d.loc[train, "walking_time"].notna().sum())
    add("駅徒歩", "徒歩圏に駅なし（バス・車のみ）", "徒歩 10 分の部屋と比べて",
        b["walk_missing"], d.loc[train, "walking_time"].isna().sum())

    def age_curve(a):
        return b["age"] * a + b["age2"] * a * a / 100

    for a0, a1 in [(0, 10), (10, 20), (20, 30), (30, 40)]:
        label0 = "新築" if a0 == 0 else f"築{a0}年"
        n = ((d["building_age"] >= a0) & (d["building_age"] <= a1) & train).sum()
        add("築年数", f"{label0} → 築{a1}年", f"{label0}の部屋と比べて", age_curve(a1) - age_curve(a0), n)

    labels = {"floor_band": ("階", "{base}と比べて"), "building_type": ("建物種別", "{base}と比べて"),
              "layout_group": ("間取り", "{base}と比べて（広さは同じとして）")}
    for c, (group, desc) in labels.items():
        base = cats[c][0]
        for lv in cats[c][1:]:
            if lv in ("不明",):
                continue
            add(group, lv, desc.format(base=base), b[f"{c}={lv}"], ((m[c] == lv) & train).sum())

    # 駅: 部屋数で重み付けした平均的な駅を 0 として、駅ごとの高さ・安さを出す
    st_levels = cats["station_key"]
    effects = pd.Series([0.0] + [b[f"station_key={lv}"] for lv in st_levels[1:]], index=st_levels)
    counts = m.loc[train, "station_key"].value_counts().reindex(st_levels).fillna(0)
    mean_effect = float((effects * counts).sum() / counts.sum())
    for lv in st_levels:
        if lv.startswith("区:"):
            continue
        add("駅", lv, "平均的な駅と比べて（駅以外の条件は同じとして）", effects[lv] - mean_effect, counts[lv])

    return pd.DataFrame(rows, columns=[c for c, _, _ in EFFECTS_COLUMNS])


# ---------------------------------------------------------------- 書き込み（Spark）
def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def write_table(spark, pdf: pd.DataFrame, table: str, columns, table_comment: str, mode: str = "overwrite",
                primary_key: list[str] | None = None) -> None:
    from pyspark.sql import functions as F

    pdf = pdf[[c for c, _, _ in columns]].astype(object)
    pdf = pdf.where(pdf.notna(), None)
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.select([F.col(c).cast(t).alias(c) for c, t, _ in columns])
    writer = sdf.write.mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table)
    spark.sql(f"COMMENT ON TABLE {table} IS {_quote(table_comment)}")
    for c, _, comment in columns:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} COMMENT {_quote(comment)}")
    if primary_key:
        # Genie やメトリクスビューが結合のキーを判断できるよう、主キー（情報としての制約）を付ける
        for c in primary_key:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} SET NOT NULL")
        name = table.split(".")[-1] + "_pk"
        existing = spark.sql(
            f"SELECT 1 FROM {table.split('.')[0]}.information_schema.table_constraints "
            f"WHERE table_schema = '{table.split('.')[1]}' AND table_name = '{table.split('.')[2]}' "
            f"AND constraint_type = 'PRIMARY KEY'"
        ).count()
        if not existing:
            spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY ({', '.join(primary_key)})")


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--silver-schema", required=True)
    p.add_argument("--gold-schema", required=True)
    p.add_argument("--table-suffix", default="", help="書き込むテーブル名の末尾（dev は _dev）")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    gold = f"{args.catalog}.{args.gold_schema}"
    today = dt.datetime.now(JST).date()

    silver = spark.table(f"{args.catalog}.{args.silver_schema}.suumo_rental_rooms")
    df = silver.select(
        "room_id", "listed_flag", "first_seen_date", "deleted_date", "building_name", "building_type", "ward", "town",
        "nearest_station", "nearest_line", "walking_time", "building_age", "building_floors", "floor", "layout",
        "area", "rent", "admin_fee", "deposit", "key_money", "detail_url",
    ).toPandas()
    print(f"silver から {len(df)} 部屋を読みました")

    d = prepare(df, today)
    model = fit_model(d)
    d = add_scores(d, model)
    effects = factor_effects(d, model, today)
    metrics = pd.DataFrame([{
        "model_date": today, "n_rooms": len(d), "n_train": model["n_train"], "n_outliers": int(d["is_outlier"].sum()),
        "n_features": model["n_features"], "r2_holdout": round(model["r2"], 4),
        "median_abs_pct_error": round(model["mdape"], 4), "base_monthly_cost": model["base_monthly_cost"],
    }])
    print(f"賃料モデル: 学習 {model['n_train']} 部屋 / 決定係数 {model['r2']:.3f} / 誤差の中央値 {model['mdape']:.1%}")

    sfx = args.table_suffix
    write_table(spark, d, f"{gold}.suumo_room_fact{sfx}", FACT_COLUMNS, FACT_TABLE_COMMENT, primary_key=["room_id"])
    write_table(spark, effects, f"{gold}.suumo_rent_factor_effects{sfx}", EFFECTS_COLUMNS, EFFECTS_TABLE_COMMENT)
    # 精度の記録は同じ日に何度実行しても 1 行になるよう、その日の行を消してから追記する
    metrics_table = f"{gold}.suumo_rent_model_metrics{sfx}"
    if spark.catalog.tableExists(metrics_table):
        spark.sql(f"DELETE FROM {metrics_table} WHERE model_date = DATE'{today.isoformat()}'")
    write_table(spark, metrics, metrics_table, METRICS_COLUMNS, METRICS_TABLE_COMMENT, mode="append")
    print(f"{gold} の suumo_room_fact{sfx}（{len(d)} 部屋）/ suumo_rent_factor_effects{sfx}（{len(effects)} 行）/ "
          f"suumo_rent_model_metrics{sfx} を更新しました")


if __name__ == "__main__":
    main(sys.argv[1:])
