# HOME'S の賃貸物件データを扱うカタログ
# 中身が残っている間は削除できない（force_destroy はデフォルトの false のまま）
resource "databricks_catalog" "rental_property" {
  name    = "rental_property"
  comment = "HOME'S の賃貸物件データ（個人の非営利利用）。Terraform で管理"
}

# メダリオンアーキテクチャの各層（スキーマ名 => 説明）
locals {
  medallion_schemas = {
    bronze   = "取得したままの生データ。加工しない"
    silver   = "型変換や重複除去で整えたデータ"
    gold     = "分析しやすく集計したデータ"
    semantic = "指標や意味づけを定義した利用者向けの層"
  }
}

resource "databricks_schema" "medallion" {
  for_each = local.medallion_schemas

  catalog_name = databricks_catalog.rental_property.name
  name         = each.key
  comment      = each.value
}

# カタログの所有者は作成したサービスプリンシパルになるため、ユーザー本人にも権限を付ける
# 下位のスキーマやテーブルにも引き継がれる
resource "databricks_grants" "rental_property" {
  catalog = databricks_catalog.rental_property.name

  grant {
    principal  = "grio3031@gmail.com"
    privileges = ["ALL_PRIVILEGES"]
  }
}
