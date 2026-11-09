# Databricks notebook source
# HOME'S 賃貸物件データの取り込み（雛形）
# 今は CI/CD の動作確認用に、受け取ったパラメータを表示するだけ。
# スクレイピングと bronze テーブルへの書き込みは次の PR で実装する。

catalog = dbutils.widgets.get("catalog")  # noqa: F821  dbutils は Databricks が用意する
schema = dbutils.widgets.get("schema")  # noqa: F821

print(f"取り込み先: {catalog}.{schema}")
