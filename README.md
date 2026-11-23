# datalake_databricks_homes

趣味用 Databricks（Free Edition）の **データ取り込みジョブ（DABs: Databricks Asset Bundles）** を管理するリポジトリ。
賃貸物件データ（SUUMO / HOME'S）を Unity Catalog の `rental_property` カタログに、中古車データ（カーセンサー）を `used_car` カタログに取り込む。

カタログ・スキーマや権限などワークスペースの中身は [databricks_datalake_infra](https://github.com/RioNomura/databricks_datalake_infra)（Terraform）で管理する。

## 構成

```
Workflow/
├── suumo_rental_ingest/              SUUMO 賃貸データの取り込みジョブ（1 フォルダ = 1 バンドル）
├── suumo_rental_gold/                SUUMO の分析用データマート（gold）を作るジョブ
├── carsensor_used_car_ingest/        カーセンサー中古車データの取り込みジョブ
└── homes_rental_ingest/              HOME'S 賃貸データの取り込みジョブ（雛形。ボット対策で止められるため保留）
    ├── databricks.yml                バンドル設定と dev / prod ターゲット
    ├── resources/*.job.yml           ジョブの定義
    └── src/                          ジョブが実行するコード
.github/workflows/bundle.yml          PR で validate と plan、main へのマージで prod にデプロイ
scripts/notion_sync_bundle.py         デプロイ結果を Notion「リソース台帳」に反映するスクリプト
```

新しいジョブを足すときは `Workflow/<ジョブ名>/databricks.yml` を作る。CI が自動で見つけて処理する。

## suumo_rental_ingest（SUUMO 大阪市の賃貸）

**利用条件**（SUUMO 利用規約 2025-10-01 改定と robots.txt を確認したうえでの運用ルール）
- 個人の私的利用の範囲で使う（規約第2条）。取得したデータや加工結果を公開・共有しない。商業目的で使わない（第3条第1項第7号）
- サイトの運営を妨げない（第3条第1項第6号）。robots.txt の Crawl-delay（30 秒）に合わせて 30 秒間隔で取る（30 秒未満はスクリプトが拒否）
- 202 / 403 / 429 が返ったら再試行も回避もせずに止める。詳細ページには行かない

**取り込み**
- 取得元: 大阪市 24 区・新着順・1 ページ 50 棟の一覧（ジョブのパラメータ `list_url`）
- 取り込み先: `rental_property.bronze.suumo_rental_listings`（1 行 = 1 部屋。値はページ上の文字列のまま）
- 同じ部屋かどうかは「建物名・所在地・階・間取り・面積・賃料・管理費・敷金・礼金」で判定する（silver でまとめるときも同じ 9 列）
  - `room_code` は使わない。同じ部屋でも、まとめて表示される代表の広告が入れ替わるとコードが変わるため
  - 一覧ページには部屋番号が無いので金額も含める。含めないと、同じ建物の同じ間取りの別の部屋まで 1 部屋にまとめてしまう。金額まで同じ部屋は 1 部屋として扱う
  - 2026-11-23 に、それまでに入った行をこの 9 列で重複削除した（各部屋で最初に取り込んだ行を残した）
- 初回（全件読み込み）: 新着順の先頭から最後のページまで読む（大阪市で約 630 ページ・約 6 時間）。20 ページごとに再開位置を `suumo_rental_ingest_state` に保存し、途中で止まっても次の実行で続きから読む。最後まで読んだら完了として記録する
- 全件読み込みの後:
  - 毎日（増分）: 新着順の先頭から `max_pages` = 100 ページまで読む（約 1 時間）。新しい部屋が無いページが 3 回続いたら、その時点で止める
  - 週 1 回（`full_crawl_weekday` = 日曜）: 最後のページまで読み直す（約 6 時間）。読んでいる間に掲載終了で一覧が詰まると読み飛ばしが出るので、それを拾う。途中で止まっても再開はせず、翌週また最初から読む
  - 手動実行ではパラメータ `mode` に `incremental` / `full` を指定すると、曜日に関係なくそのモードで読む
- 全件を読み直したいときは `suumo_rental_ingest_state` を削除する（すでにある部屋は書かない）
- 実行: 毎日 04:00 JST（prod のみ）。日曜は全件の読み直しで 10 時ごろまでかかる
- 掲載終了の判定用に、実行ごとに一覧で見かけた部屋を `suumo_rental_sightings`、実行の結果を `suumo_rental_ingest_runs` に書く

**silver**（取り込みの後に毎回作り直す）
- `rental_property.silver.suumo_rental_rooms`（1 行 = 1 部屋）。賃料・面積・築年数などを数値に、駅を配列にし、区・町名・最寄り駅・徒歩分数を切り出したもの。各列の意味はテーブルの列コメントにある
- 部屋のキーは `room_id`（上の 9 列の SHA-256）。`room_code` は最初に見つけたときのコード
- 掲載終了: 最後まで終わった一番新しい日曜の全件読み直しで見つからなかった部屋を `listed_flag = false` にし、`deleted_date` にその日曜を入れる。全件読み直しでも読み飛ばしが少し出るので、次に一覧で見かけたら掲載中に戻る
- 金額がサイト上の入力ミスとみられるほど大きい（約 21 億円超）ものは NULL。それ以外の外れ値（礼金 88862.4万円 など）はそのまま残っているので、gold で除く
- 賃料の変更は追わない。不動産会社名・設備は一覧ページに無いので取らない

## carsensor_used_car_ingest（カーセンサーの中古車）

**利用条件**（カーセンサー利用規約 car-t-1008（令和8年4月1日改定）と robots.txt を 2026-11-22 に確認したうえでの運用ルール）
- 個人の私的利用の範囲で使う。取得したデータや加工結果を公開・共有・販売しない（規約第11条: 無断の複製・転用の禁止）
- サイトの運営を妨げない（第8条第1項第3号・第7号）。robots.txt に Crawl-delay は無いが、10 秒以上の間隔で 1 本ずつ取る（10 秒未満はスクリプトが拒否）
- robots.txt で禁止されたパス（`search.php` など）は取らない。詳細ページには行かない。画像は取らない。ログインしない
- 202 / 403 / 429 が返ったら再試行も回避もせずに止める

**取り込み（bronze）**
- 取得元: 関西・関東、修復歴なし、AT、メーカー系販売店、新着順の一覧（ジョブのパラメータ `list_url`。`AR=1*2` = 関東 + 関西、`OPTCD=REP0`、`SLST=AT`、`DLR=1`、`SORT=19`。1 ページ 30 台、2026-11-23 時点で約 6.25 万台）
- `used_car.bronze.carsensor_used_car_listings`: 1 行 = 1 台、`car_id`（物件番号）で重複判定。値はページ上の文字列のまま。車名（`car_model`）とグレード（`car_grade`）は見出しのノーブレークスペースで分けて取る
- `used_car.bronze.carsensor_used_car_attributes`: 駆動方式・エンジン種別。一覧の各車の欄には無いので、絞り込み（`KUDO=4WD1`、`FUEL=2/3/4/9`）ごとの一覧に出てきた車に値を付ける。一番多い区分（2WD・ガソリン）は読まず、silver で「どれにも出なかった車」とする
- 初回（全件読み込み）: 一覧 約 2,100 ページ + 絞り込み 約 1,500 ページ（約 10 時間）。1 回の実行は `time_budget_minutes`（380 分）で区切って成功扱いで終わり、次の実行で続きから読む（初回は 2 日がかり）。再開位置は `carsensor_used_car_ingest_state` / `carsensor_used_car_attr_state` に保存
- 全件読み込みの後（増分）: 一覧・絞り込みとも新着順の先頭から読み、新しい車が無いページが 3 回続いたら止める
- 掲載終了フラグ: 毎回、カーセンサーが検索エンジン向けに公開している在庫詳細の sitemap（全国の掲載中の車、約 57 リクエスト）と照合する
  - sitemap に無い車 → `is_delisted = true`、`delisted_date` = 判定した日。sitemap に戻ったら掲載中に戻す。sitemap にあった日は `last_seen_date` を更新
  - sitemap の更新時刻より後に取り込んだ車は判定しない。sitemap が 1 つでも取れない、または 1 日で掲載中の 20% を超える車が消えたときは異常とみなして更新しない
- 価格の変更は追わない（1 台 1 行・初めて見つけたときの内容）

**型変換（silver）**
- `used_car.silver.carsensor_used_car_listings` を取り込みのあと毎回作り直す（`src/silver.py`）
- 金額は円、走行距離は km、排気量は cc の整数。年式は西暦（`model_year`）と和暦（`model_year_jp`）。保証・整備は true / false。クチコミは点数と件数
- 車検（`inspection`）は満了月の 1 日。「車検整備付」は取り込んだ月の 2 年後、車検整備無・新車未登録は NULL
- 駆動方式・エンジン種別は、絞り込みの全件読み込みが終わるまで、出ていない車を NULL にする

- 実行: 毎日 05:00 JST（prod のみ）
- dev ターゲットはテーブル名の末尾に `_dev` を付ける（本番のテーブルはサービスプリンシパルが持ち主で、dev から書き換えない）

## suumo_rental_gold（SUUMO の分析用データマート）

silver（`rental_property.silver.suumo_rental_rooms`）から gold のテーブルを毎日 12:00 JST に作り直す（日曜の全件読み直しが終わった後になるよう 12:00）。semantic スキーマのメトリクスビュー、ダッシュボード、Genie から使う前提で、全テーブル・全列にコメントを付ける。

| テーブル | 1 行 | 用途 |
|---|---|---|
| `gold.suumo_room_fact` | 1 部屋（主キー `room_id`、掲載終了も含む） | 相場（区・駅・間取り別の月額・m² 単価）、初期費用、お得物件 |
| `gold.suumo_rent_factor_effects` | 1 つの条件の比較 | 徒歩・築年数・階・建物種別・間取り・広さ・駅が月額を何% / 何円動かすか |
| `gold.suumo_rent_model_metrics` | 1 日（追記） | 賃料モデルの精度（決定係数・誤差の中央値） |

- 月額 = 賃料 + 管理費。初期費用の目安 = 敷金 + 礼金 + 1 か月分の月額（仲介手数料などは含まない）
- 賃料モデル: 月額の対数を、最寄り駅・間取り・建物種別・階の帯・面積・徒歩・築年数・建物の階数で最小二乗法により当てはめる（numpy）。外れ値（入力ミスとみられる部屋、`is_outlier`）は学習から除く
- お得度（`deal_score`）= 1 − 実際の月額 ÷ モデルの妥当な月額。掲載中の部屋に順位（`deal_rank`）を付ける

## CI/CD の流れ

1. feature ブランチで `Workflow/` を編集して PR を作る
2. GitHub Actions がバンドルごとに `validate`（dev / prod）→ `plan`（prod）を実行し、結果を PR にコメントする
3. plan を確認して main にマージすると、`deploy -t prod` が走る（Environment `production`、main からのみ）
4. デプロイ後、変更のあったリソースが Notion のリソース台帳に反映される（管理方法 = DABs）

## ターゲット

| ターゲット | デプロイする人 | 置き場所 | ジョブの実行ユーザー |
|---|---|---|---|
| `dev` | 各自がローカルから | `/Workspace/Users/<自分>/.bundle/...` | 自分。ジョブ名に `[dev ユーザー名]` が付き、スケジュールは停止 |
| `prod` | GitHub Actions だけ | `/Workspace/Users/<サービスプリンシパル>/.bundle/...` | サービスプリンシパル |

ローカルで dev にデプロイ・実行する例:

```bash
cd Workflow/homes_rental_ingest
databricks bundle deploy -t dev --profile DEFAULT
databricks bundle run homes_rental_ingest -t dev --profile DEFAULT
```

## 必要な設定（GitHub の Settings → Secrets and variables → Actions）

| 種類 | 名前 | 内容 |
|---|---|---|
| Secret | `DATABRICKS_HOST` | ワークスペース URL |
| Secret | `DATABRICKS_CLIENT_ID` | サービスプリンシパル `github-actions-terraform` の Application ID |
| Secret | `DATABRICKS_CLIENT_SECRET` | 同 OAuth シークレット |
| Secret | `NOTION_TOKEN` | Notion インテグレーションのトークン |
| Variable | `NOTION_DATABASE_ID` | Notion「リソース台帳」のデータベース ID |
