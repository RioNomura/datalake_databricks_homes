# datalake_databricks_homes

趣味用 Databricks（Free Edition）の **データ取り込みジョブ（DABs: Databricks Asset Bundles）** を管理するリポジトリ。
HOME'S の賃貸物件データを取り込み、Unity Catalog の `rental_property` カタログに保存する。

カタログ・スキーマや権限などワークスペースの中身は [databricks_datalake_infra](https://github.com/RioNomura/databricks_datalake_infra)（Terraform）で管理する。

## 構成

```
Workflow/
└── homes_rental_ingest/              HOME'S 賃貸データの取り込みジョブ（1 フォルダ = 1 バンドル）
    ├── databricks.yml                バンドル設定と dev / prod ターゲット
    ├── resources/*.job.yml           ジョブの定義
    └── src/                          ジョブが実行するコード
.github/workflows/bundle.yml          PR で validate と plan、main へのマージで prod にデプロイ
scripts/notion_sync_bundle.py         デプロイ結果を Notion「リソース台帳」に反映するスクリプト
```

新しいジョブを足すときは `Workflow/<ジョブ名>/databricks.yml` を作る。CI が自動で見つけて処理する。

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
