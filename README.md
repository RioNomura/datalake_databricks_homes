# datalake_databricks_homes

趣味用 Databricks（Free Edition）の **データ取り込みジョブ（DABs: Databricks Asset Bundles）** を管理するリポジトリ。
賃貸物件データ（SUUMO / HOME'S）を取り込み、Unity Catalog の `rental_property` カタログに保存する。

カタログ・スキーマや権限などワークスペースの中身は [databricks_datalake_infra](https://github.com/RioNomura/databricks_datalake_infra)（Terraform）で管理する。

## 構成

```
Workflow/
├── suumo_rental_ingest/              SUUMO 賃貸データの取り込みジョブ（1 フォルダ = 1 バンドル）
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
- 取り込み先: `rental_property.bronze.suumo_rental_listings`（1 行 = 1 部屋、`room_code` で重複判定。値はページ上の文字列のまま）
- 初回（全件読み込み）: 新着順の先頭から最後のページまで読む（大阪市で約 630 ページ・約 6 時間）。20 ページごとに再開位置を `suumo_rental_ingest_state` に保存し、途中で止まっても次の実行で続きから読む。最後まで読んだら完了として記録する
- 全件読み込みの後（増分）: 新着順の先頭から読み、新しい部屋が無いページが 3 回続いたら止める（安全装置として 1 回 `max_pages` = 400 ページまで。上限で止まったらログに警告）
- 全件を読み直したいときは `suumo_rental_ingest_state` を削除する（すでにある部屋は書かない）
- 実行: 毎日 04:00 JST（prod のみ）
- 賃料の変更や掲載終了は追わない。不動産会社名・設備は一覧ページに無いので取らない

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
