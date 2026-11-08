#!/usr/bin/env python3
"""terraform apply の結果を Notion の「リソース台帳」に反映する。

terraform plan の JSON（何が変わったか）と、apply 後の state の JSON（今の値）を読み、
変更のあったリソースだけ台帳を更新する。

- 作成・更新・再作成: 「Terraformアドレス」で行を探し、あれば更新、なければ追加
- 削除: 行は消さず「状態」を「削除済み」にする
- シークレットなど sensitive な値は書き込まない

使い方:
  python3 scripts/notion_sync.py --plan plan.json --state state.json [--dry-run]

環境変数:
  NOTION_TOKEN        Notion インテグレーションのトークン（--dry-run では不要）
  NOTION_DATABASE_ID  リソース台帳のデータベース ID
  GITHUB_*            GitHub Actions が自動で設定する値（PR / run のリンクに使う）
  GITHUB_TOKEN        マージ元 PR を探すのに使う（任意）
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
JST = dt.timezone(dt.timedelta(hours=9))

# Terraform のリソース種別 → 台帳の「種類」
KIND_BY_TYPE = {
    "databricks_directory": "ディレクトリ",
    "databricks_job": "ジョブ",
    "databricks_notebook": "ノートブック",
    "databricks_service_principal": "サービスプリンシパル",
    "databricks_git_credential": "Git認証情報",
    "databricks_repo": "Gitフォルダ",
    "databricks_catalog": "Unity Catalog",
    "databricks_schema": "Unity Catalog",
    "databricks_volume": "Unity Catalog",
    "databricks_sql_table": "Unity Catalog",
    "databricks_grants": "Unity Catalog",
    "databricks_grant": "Unity Catalog",
    "github_actions_secret": "GitHub Secret",
    "github_repository_environment": "GitHub Environment",
    "github_repository": "GitHubリポジトリ",
}

ACTION_LABEL = {
    ("create",): "作成",
    ("update",): "更新",
    ("delete",): "削除",
    ("delete", "create"): "再作成",
    ("create", "delete"): "再作成",
}

NAME_KEYS = ("name", "display_name", "path", "url")
PATH_KEYS = ("path", "url", "workspace_path", "full_name")


# ---------------------------------------------------------------- Terraform JSON


def iter_module_resources(module: dict):
    yield from module.get("resources", [])
    for child in module.get("child_modules", []):
        yield from iter_module_resources(child)


def load_state_resources(state: dict) -> dict[str, dict]:
    root = (state.get("values") or {}).get("root_module") or {}
    return {r["address"]: r for r in iter_module_resources(root) if r.get("mode") == "managed"}


def changed_resources(plan: dict) -> list[tuple[str, str, str]]:
    """(address, type, ラベル) のリスト。no-op と data source は除く。"""
    out = []
    for rc in plan.get("resource_changes", []):
        if rc.get("mode") != "managed":
            continue
        label = ACTION_LABEL.get(tuple(rc["change"]["actions"]))
        if label:
            out.append((rc["address"], rc["type"], label))
    return out


def safe_value(resource: dict, key: str):
    """sensitive でない文字列・数値だけ返す。"""
    if (resource.get("sensitive_values") or {}).get(key):
        return None
    value = (resource.get("values") or {}).get(key)
    if isinstance(value, (str, int, float)) and value != "":
        return str(value)
    return None


def first_value(resource: dict, keys) -> str | None:
    for key in keys:
        value = safe_value(resource, key)
        if value:
            return value
    return None


# ---------------------------------------------------------------- GitHub context


def github_context() -> dict[str, str | None]:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY")
    sha = os.environ.get("GITHUB_SHA")
    run_id = os.environ.get("GITHUB_RUN_ID")
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else None
    commit_url = f"{server}/{repo}/commit/{sha}" if repo and sha else None
    return {
        "run_url": run_url,
        "link": find_pr_url(repo, sha) or commit_url,
    }


def find_pr_url(repo: str | None, sha: str | None) -> str | None:
    token = os.environ.get("GITHUB_TOKEN")
    if not (repo and sha and token):
        return None
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    req = urllib.request.Request(
        f"{api}/repos/{repo}/commits/{sha}/pulls",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            pulls = json.load(resp)
    except urllib.error.URLError as exc:
        print(f"warning: PR を取得できませんでした: {exc}", file=sys.stderr)
        return None
    return pulls[0]["html_url"] if pulls else None


# ---------------------------------------------------------------- Notion


def rich_text(value: str | None) -> dict:
    return {"rich_text": [{"text": {"content": value[:2000]}}] if value else []}


def build_properties(address, rtype, label, resource, ctx, today) -> dict:
    run = f"（run: {ctx['run_url']}）" if ctx["run_url"] else ""
    props = {
        "Terraformアドレス": rich_text(address),
        "管理方法": {"select": {"name": "Terraform"}},
        "変更内容": rich_text(f"GitHub Actions の apply で{label}{run}"),
        "最終更新": {"date": {"start": today}},
    }
    if ctx["link"]:
        props["PR"] = {"url": ctx["link"]}

    if label == "削除":
        props["状態"] = {"status": {"name": "削除済み"}}
        return props

    resource = resource or {}
    props["リソース名"] = {"title": [{"text": {"content": first_value(resource, NAME_KEYS) or address}}]}
    props["種類"] = {"select": {"name": KIND_BY_TYPE.get(rtype, "その他")}}
    service = "GitHub" if rtype.startswith("github_") else "Databricks"
    props["サービス"] = {"select": {"name": service}}
    props["状態"] = {"status": {"name": "稼働中"}}
    props["パス・URL"] = rich_text(first_value(resource, PATH_KEYS))
    props["ID"] = rich_text(safe_value(resource, "id"))
    return props


class Notion:
    def __init__(self, token: str, database_id: str):
        self.token = token
        self.database_id = database_id

    def _request(self, method: str, path: str, body: dict) -> dict:
        req = urllib.request.Request(
            f"{NOTION_API}{path}",
            data=json.dumps(body).encode(),
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise SystemExit(f"Notion API error {exc.code} on {method} {path}: {detail}") from exc

    def find_page(self, address: str) -> str | None:
        result = self._request(
            "POST",
            f"/databases/{self.database_id}/query",
            {"filter": {"property": "Terraformアドレス", "rich_text": {"equals": address}}, "page_size": 1},
        )
        pages = result.get("results", [])
        return pages[0]["id"] if pages else None

    def upsert(self, address: str, props: dict) -> str:
        page_id = self.find_page(address)
        if page_id:
            self._request("PATCH", f"/pages/{page_id}", {"properties": props})
            return "updated"
        if "リソース名" not in props:
            # 台帳に無いリソースの削除は記録しない
            return "skipped"
        self._request("POST", "/pages", {"parent": {"database_id": self.database_id}, "properties": props})
        return "created"


# ---------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan", required=True, help="terraform show -json tfplan の出力")
    parser.add_argument("--state", required=True, help="apply 後の terraform show -json の出力")
    parser.add_argument("--dry-run", action="store_true", help="Notion に書き込まず内容を表示する")
    args = parser.parse_args()

    with open(args.plan, encoding="utf-8") as f:
        plan = json.load(f)
    with open(args.state, encoding="utf-8") as f:
        state = json.load(f)

    changes = changed_resources(plan)
    if not changes:
        print("変更されたリソースはありません。Notion は更新しません。")
        return 0

    resources = load_state_resources(state)
    ctx = github_context()
    today = dt.datetime.now(JST).date().isoformat()

    notion = None
    if not args.dry_run:
        token = os.environ.get("NOTION_TOKEN")
        database_id = os.environ.get("NOTION_DATABASE_ID")
        if not token or not database_id:
            print("error: NOTION_TOKEN と NOTION_DATABASE_ID を設定してください", file=sys.stderr)
            return 1
        notion = Notion(token, database_id)

    for address, rtype, label in changes:
        props = build_properties(address, rtype, label, resources.get(address), ctx, today)
        if notion is None:
            print(f"[dry-run] {label}: {address}")
            print(json.dumps(props, ensure_ascii=False, indent=2))
            continue
        result = notion.upsert(address, props)
        print(f"{label}: {address} -> Notion {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
