# datalake_databricks_homes

趣味用 Databricks（Free Edition）の **データ取り込みジョブ（DABs: Databricks Asset Bundles）** を管理するリポジトリ。
賃貸物件データ（SUUMO / HOME'S）を Unity Catalog の `rental_property` カタログに、中古車データ（カーセンサー）を `used_car` カタログに取り込む。

カタログ・スキーマや権限などワークスペースの中身は [databricks_datalake_infra](https://github.com/RioNomura/databricks_datalake_infra)（Terraform）で管理する。

## 構成

```
Workflow/
├── suumo_rental_ingest/              SUUMO 賃貸データの取り込みジョブ（1 フォルダ = 1 バンドル）
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
- 同じ部屋かどうかは「建物名・所在地・階・間取り・面積」で判定する。`room_code` は使わない（同じ部屋でも、まとめて表示される代表の広告が入れ替わるとコードが変わるため）。2026-11-23 以前の行には、この理由で同じ部屋が別コードで重複している分がある（silver で同じ 5 列でまとめる）
- 初回（全件読み込み）: 新着順の先頭から最後のページまで読む（大阪市で約 630 ページ・約 6 時間）。20 ページごとに再開位置を `suumo_rental_ingest_state` に保存し、途中で止まっても次の実行で続きから読む。最後まで読んだら完了として記録する
- 全件読み込みの後:
  - 毎日（増分）: 新着順の先頭から `max_pages` = 100 ページまで読む（約 1 時間）。新しい部屋が無いページが 3 回続いたら、その時点で止める
  - 週 1 回（`full_crawl_weekday` = 日曜）: 最後のページまで読み直す（約 6 時間）。読んでいる間に掲載終了で一覧が詰まると読み飛ばしが出るので、それを拾う。途中で止まっても再開はせず、翌週また最初から読む
  - 手動実行ではパラメータ `mode` に `incremental` / `full` を指定すると、曜日に関係なくそのモードで読む
- 全件を読み直したいときは `suumo_rental_ingest_state` を削除する（すでにある部屋は書かない）
- 実行: 毎日 04:00 JST（prod のみ）。日曜は全件の読み直しで 10 時ごろまでかかる
- 賃料の変更や掲載終了は追わない。不動産会社名・設備は一覧ページに無いので取らない

## carsensor_used_car_ingest（カーセンサーの中古車）

**利用条件**（カーセンサー利用規約 car-t-1008（令和8年4月1日改定）と robots.txt を 2026-11-22 に確認したうえでの運用ルール）
- 個人の私的利用の範囲で使う。取得したデータや加工結果を公開・共有・販売しない（規約第11条: 無断の複製・転用の禁止）
- サイトの運営を妨げない（第8条第1項第3号・第7号）。robots.txt に Crawl-delay は無いが、10 秒以上の間隔で 1 本ずつ取る（10 秒未満はスクリプトが拒否）
- robots.txt で禁止されたパス（`search.php` など）は取らない。詳細ページには行かない。画像は取らない。ログインしない
- 202 / 403 / 429 が返ったら再試行も回避もせずに止める

**取り込み**
- 取得元: 関西・関東、修復歴なし、AT、メーカー系販売店、新着順の一覧（ジョブのパラメータ `list_url`。`AR=1*2` = 関東 + 関西、`OPTCD=REP0`、`SLST=AT`、`DLR=1`、`SORT=19`。1 ページ 30 台、2026-11-22 時点で約 6.3 万台）
- 取り込み先: `used_car.bronze.carsensor_used_car_listings`（1 行 = 1 台、`car_id`（物件番号）で重複判定。値はページ上の文字列のまま）
- 初回（全件読み込み）: 新着順の先頭から最後のページまで読む（約 2,100 ページ・約 6.5 時間）。20 ページごとに再開位置を `carsensor_used_car_ingest_state` に保存し、途中で止まっても次の実行で続きから読む
- 全件読み込みの後（増分）: 新着順の先頭から読み、新しい車が無いページが 3 回続いたら止める（安全装置として 1 回 `max_pages` = 300 ページまで）
- 掲載終了フラグ: 毎回、カーセンサーが検索エンジン向けに公開している在庫詳細の sitemap（全国の掲載中の車、約 57 リクエスト）と照合する
  - sitemap に無い車 → `is_delisted = true`、`delisted_date` = 判定した日。sitemap に戻ったら掲載中に戻す。sitemap にあった日は `last_seen_date` を更新
  - sitemap の更新時刻より後に取り込んだ車は判定しない。sitemap が 1 つでも取れない、または 1 日で掲載中の 20% を超える車が消えたときは異常とみなして更新しない
- 実行: 毎日 05:00 JST（prod のみ）
- 価格の変更は追わない（1 台 1 行・初めて見つけたときの内容）

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
