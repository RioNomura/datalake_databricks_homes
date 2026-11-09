#!/usr/bin/env python3
"""databricks bundle deploy の結果を Notion の「リソース台帳」に反映する。

deploy 前の `bundle plan -o json`（何が変わるか）と、deploy 後の `bundle summary -o json`（ID や URL）を読み、
変更のあったリソースだけ台帳を更新する。

- 行を探すキーは「Terraformアドレス」列に `dabs:<バンドル名>:<ターゲット>:<種別>.<キー>` の形で入れる
  （Terraform 側の同期スクリプトと同じ列を、一意キーとして使い回している）
- 作成・更新・再作成: 行があれば更新、なければ追加
- 削除: 行は消さず「状態」を「削除済み」にする

使い方:
  python3 scripts/notion_sync_bundle.py --plan plan.json --summary summary.json [--dry-run]

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

# バンドルのリソース種別 → 台帳の「種類」
KIND_BY_TYPE = {
    "jobs": "ジョブ",
    "pipelines": "パイプライン",
    "volumes": "Unity Catalog",
    "schemas": "Unity Catalog",
    "dashboards": "ダッシュボード",
}

ACTION_LABEL = {
    "create": "作成",
    "update": "更新",
    "update_id": "更新",
    "resize": "更新",
    "delete": "削除",
    "recreate": "再作成",
}


# ---------------------------------------------------------------- bundle JSON


def changed_resources(plan: dict) -> list[tuple[str, str, str]]:
    """(種別, キー, ラベル) のリスト。権限などの子要素（resources.jobs.x.permissions）と変更なしは除く。"""
    out = []
    for node, entry in (plan.get("plan") or {}).items():
        parts = node.split(".")
        if len(parts) != 3 or parts[0] != "resources":
            continue
        label = ACTION_LABEL.get(entry.get("action", ""))
        if label:
            out.append((parts[1], parts[2], label))
    return out


# ---------------------------------------------------------------- GitHub context


def github_context() -> dict[str, str | None]:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY")
    sha = os.environ.get("GITHUB_SHA")
    run_id = os.environ.get("GITHUB_RUN_ID")
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else None
    commit_url = f"{server}/{repo}/commit/{sha}" if repo and sha else None
    return {"run_url": run_url, "link": find_pr_url(repo, sha) or commit_url}


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


def build_properties(key, rtype, label, resource, bundle, target, ctx, today) -> dict:
    run = f"（run: {ctx['run_url']}）" if ctx["run_url"] else ""
    props = {
        "Terraformアドレス": rich_text(key),
        "管理方法": {"select": {"name": "DABs"}},
        "変更内容": rich_text(f"GitHub Actions の bundle deploy（{bundle} / {target}）で{label}{run}"),
        "最終更新": {"date": {"start": today}},
    }
    if ctx["link"]:
        props["PR"] = {"url": ctx["link"]}

    if label == "削除":
        props["状態"] = {"status": {"name": "削除済み"}}
        return props

    resource = resource or {}
    name = resource.get("name") or resource.get("display_name") or key
    props["リソース名"] = {"title": [{"text": {"content": str(name)}}]}
    props["種類"] = {"select": {"name": KIND_BY_TYPE.get(rtype, "その他")}}
    props["サービス"] = {"select": {"name": "Databricks"}}
    props["状態"] = {"status": {"name": "稼働中"}}
    props["パス・URL"] = rich_text(resource.get("url"))
    props["ID"] = rich_text(str(resource["id"]) if resource.get("id") else None)
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

    def find_page(self, key: str) -> str | None:
        result = self._request(
            "POST",
            f"/databases/{self.database_id}/query",
            {"filter": {"property": "Terraformアドレス", "rich_text": {"equals": key}}, "page_size": 1},
        )
        pages = result.get("results", [])
        return pages[0]["id"] if pages else None

    def upsert(self, key: str, props: dict) -> str:
        page_id = self.find_page(key)
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
    parser.add_argument("--plan", required=True, help="deploy 前の databricks bundle plan -o json の出力")
    parser.add_argument("--summary", required=True, help="deploy 後の databricks bundle summary -o json の出力")
    parser.add_argument("--dry-run", action="store_true", help="Notion に書き込まず内容を表示する")
    args = parser.parse_args()

    with open(args.plan, encoding="utf-8") as f:
        plan = json.load(f)
    with open(args.summary, encoding="utf-8") as f:
        summary = json.load(f)

    changes = changed_resources(plan)
    if not changes:
        print("変更されたリソースはありません。Notion は更新しません。")
        return 0

    bundle = summary.get("bundle", {}).get("name", "unknown")
    target = summary.get("bundle", {}).get("target", "unknown")
    resources = summary.get("resources", {})
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

    for rtype, rkey, label in changes:
        key = f"dabs:{bundle}:{target}:{rtype}.{rkey}"
        resource = resources.get(rtype, {}).get(rkey)
        props = build_properties(key, rtype, label, resource, bundle, target, ctx, today)
        if notion is None:
            print(f"[dry-run] {label}: {key}")
            print(json.dumps(props, ensure_ascii=False, indent=2))
            continue
        result = notion.upsert(key, props)
        print(f"{label}: {key} -> Notion {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
