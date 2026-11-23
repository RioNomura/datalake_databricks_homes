"""plan / deploy の結果を、tfcmt（infra リポジトリで使用）と同じ形で PR にコメントする。

tfcmt は Terraform の出力しか読めないため、DABs 用に同じ見た目を作る。

  python3 scripts/pr_comment.py plan  --target <バンドル> --plan-json plan.json --log plan.txt --outcome success
  python3 scripts/pr_comment.py apply --target <バンドル> --plan-json plan.json --log deploy.txt --outcome success

- plan : PR に「Plan Result (<バンドル>)」。同じ PR・同じバンドルの前回のコメントを上書きする（tfcmt の -patch と同じ）
- apply: main へのマージ後、マージ元の PR に「Apply Result (<バンドル>)」を新しく付ける
- --outcome が success 以外なら「Plan Failed / Apply Failed」とし、ログを詳細に入れる

公開リポジトリのため、コメントにはテキストのログと plan.json のリソース名だけを使い、
plan.json の中身（リモートの設定値など）は載せない。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API = os.environ.get("GITHUB_API_URL", "https://api.github.com")
MAX_DETAILS = 60000  # GitHub のコメントは 65536 文字まで

# 一覧の見出しと並び順（tfcmt と同じ）
SECTIONS = ["Import", "Create", "Update", "Replace", "Destroy"]


# ---------------------------------------------------------------- このリポジトリ固有: bundle plan JSON の読み方

# databricks bundle plan の action → 見出し。skip（変更なし）は載せない
SECTION_BY_ACTION = {
    "create": "Create",
    "update": "Update",
    "update_id": "Update",
    "resize": "Update",
    "recreate": "Replace",
    "delete": "Destroy",
}


def read_plan(path: Path) -> tuple[dict[str, list[str]], str]:
    """(見出し → リソース一覧, 要約の 1 行) を返す。"""
    sections: dict[str, list[str]] = {s: [] for s in SECTIONS}
    if not path.exists():
        return sections, ""
    plan = json.loads(path.read_text(encoding="utf-8"))
    unchanged = 0
    for node, entry in sorted((plan.get("plan") or {}).items()):
        section = SECTION_BY_ACTION.get(entry.get("action", ""))
        if section:
            sections[section].append(node.removeprefix("resources."))
        else:
            unchanged += 1
    add = len(sections["Create"]) + len(sections["Replace"])
    change = len(sections["Update"])
    delete = len(sections["Destroy"]) + len(sections["Replace"])
    if add + change + delete == 0:
        return sections, "No changes. デプロイ済みの内容と一致しています。"
    return sections, f"Plan: {add} to add, {change} to change, {delete} to delete, {unchanged} unchanged"


# ---------------------------------------------------------------- 共通: コメントの組み立て


def code_block(text: str) -> str:
    if len(text) > MAX_DETAILS:
        text = text[:20000] + "\n\n# ...\n# ... コメントの文字数上限のため省略しました。全文は CI link のログを見てください\n# ...\n\n" + text[-20000:]
    if "```" in text:
        return f"<pre><code>{text}</code></pre>"
    return f"\n```\n{text}\n```\n"


def run_url() -> str | None:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo, run_id = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_RUN_ID")
    return f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else None


def marker(command: str, target: str) -> str:
    return f"<!-- pr-comment: {json.dumps({'command': command, 'target': target})} -->"


def render(command: str, target: str, ok: bool, sections: dict[str, list[str]], summary: str, log: str) -> str:
    kind = "Plan" if command == "plan" else "Apply"
    lines = [f"## {kind} {'Result' if ok else 'Failed'} ({target})", ""]
    if url := run_url():
        lines += [f"[CI link]({url})", ""]
    if not ok:
        lines += [f"{'plan' if command == 'plan' else 'デプロイ'} が失敗しました。詳細を確認してください。", ""]
    if summary:
        lines += [f"<pre><code>{summary}</code></pre>", ""]
    for name in SECTIONS:
        if sections.get(name):
            lines.append(f"* {name}")
            lines += [f"  * {r}" for r in sections[name]]
    lines.append("")
    title = "Change Result (Click me)" if command == "plan" and ok else "Details (Click me)"
    lines += [f"<details><summary>{title}</summary>", code_block(log.strip() or "（ログがありません）"), "</details>", ""]
    lines.append(marker(command, target))
    return "\n".join(lines)


# ---------------------------------------------------------------- 共通: GitHub API


def github(method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        f"{API}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def pr_number(command: str) -> int | None:
    repo = os.environ["GITHUB_REPOSITORY"]
    if command == "plan":
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        return (event.get("pull_request") or {}).get("number")
    # main への push: マージコミットから元の PR を探す（直後は見つからないことがあるので数回試す）
    sha = os.environ["GITHUB_SHA"]
    for _ in range(3):
        pulls = github("GET", f"/repos/{repo}/commits/{sha}/pulls")
        if pulls:
            return pulls[0]["number"]
        time.sleep(5)
    return None


def post(command: str, target: str, number: int, body: str) -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    if command == "plan":
        # 同じ PR・同じ対象の前回のコメントを探して上書きする
        tag = marker(command, target)
        page, found = 1, None
        while True:
            comments = github("GET", f"/repos/{repo}/issues/{number}/comments?per_page=100&page={page}")
            for c in comments:
                if tag in (c.get("body") or ""):
                    found = c["id"]
            if len(comments) < 100:
                break
            page += 1
        if found:
            github("PATCH", f"/repos/{repo}/issues/comments/{found}", {"body": body})
            print(f"PR #{number} のコメントを更新しました")
            return
    github("POST", f"/repos/{repo}/issues/{number}/comments", {"body": body})
    print(f"PR #{number} にコメントしました")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["plan", "apply"])
    parser.add_argument("--target", required=True, help="タイトルの (…) に入れる名前")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--log", action="append", default=[], help="詳細に載せるログ（複数指定可・あるものだけ使う）")
    parser.add_argument("--outcome", default="success", help="ジョブの結果（job.status）。success 以外は Failed")
    parser.add_argument("--skip-no-changes", action="store_true", help="成功して変更が無いときはコメントしない")
    parser.add_argument("--dry-run", action="store_true", help="投稿せずに本文を表示する")
    args = parser.parse_args()

    sections, summary = read_plan(Path(args.plan_json))
    log = "\n".join(Path(p).read_text(encoding="utf-8") for p in args.log if Path(p).exists())
    ok = args.outcome == "success"
    if args.skip_no_changes and ok and not any(sections.values()):
        print(f"{args.target} は変更が無いため、コメントしません")
        return 0
    body = render(args.command, args.target, ok, sections, summary if ok or args.command == "apply" else "", log)

    if args.dry_run:
        print(body)
        return 0
    number = pr_number(args.command)
    if not number:
        print("warning: コメント先の PR が見つかりませんでした。コメントせずに終了します", file=sys.stderr)
        return 0
    try:
        post(args.command, args.target, number, body)
    except urllib.error.HTTPError as exc:
        print(f"error: PR にコメントできませんでした: {exc} {exc.read().decode(errors='replace')}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
