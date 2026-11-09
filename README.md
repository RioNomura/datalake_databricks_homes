# datalake_databricks_homes

趣味用 Databricks（Free Edition）の **データ取り込みジョブ（DABs: Databricks Asset Bundles）** を管理するリポジトリ。
HOME'S の賃貸物件データを取り込み、Unity Catalog の `rental_property` カタログに保存する。

カタログ・スキーマや権限などワークスペースの中身は [databricks_datalake_infra](https://github.com/RioNomura/databricks_datalake_infra)（Terraform）で管理する。

## 構成（予定）

```
Workflow/
└── homes_rental_ingest/   HOME'S 賃貸データの取り込みジョブ（DABs）
```
