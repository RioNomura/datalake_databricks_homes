# 動作確認用の最小リソース
resource "databricks_directory" "managed" {
  path = "/Shared/terraform-managed"
}
