# datalake_databricks_homes

趣味用の Databricks（Free Edition）データレイク。Databricks の Git フォルダと連携し、インフラは Terraform + GitHub Actions で管理する。

## 構成

```
.github/workflows/terraform.yml  PR で plan、main へのマージで apply、apply 後に Notion の台帳を更新
infra/terraform/                 Terraform（Databricks provider、state は HCP Terraform）
scripts/notion_sync.py           apply 結果を Notion「リソース台帳」に反映するスクリプト
```

## CI/CD の流れ

1. feature ブランチで `infra/terraform/` を編集して PR を作る
2. GitHub Actions が `fmt` → `validate` → `plan` を実行し、結果を PR にコメントする
3. PR の plan を確認して main にマージすると、`apply` が走る（Environment `production`、main からのみ）
4. apply 後、変更のあったリソースが Notion のリソース台帳に反映される

## 必要な設定（GitHub の Settings → Secrets and variables → Actions）

| 種類 | 名前 | 内容 |
|---|---|---|
| Secret | `DATABRICKS_HOST` | ワークスペース URL |
| Secret | `DATABRICKS_CLIENT_ID` | サービスプリンシパル `github-actions-terraform` の Application ID |
| Secret | `DATABRICKS_CLIENT_SECRET` | 同 OAuth シークレット |
| Secret | `TF_API_TOKEN` | HCP Terraform の API トークン |
| Secret | `NOTION_TOKEN` | Notion インテグレーションのトークン |
| Variable | `TF_CLOUD_ORGANIZATION` | HCP Terraform の Organization 名 |
| Variable | `NOTION_DATABASE_ID` | Notion「リソース台帳」のデータベース ID |

## Notion 同期スクリプトの確認

```bash
python3 scripts/notion_sync.py --plan plan.json --state state.json --dry-run
```

`--dry-run` を付けると Notion に書き込まず、送信する内容を表示する。
