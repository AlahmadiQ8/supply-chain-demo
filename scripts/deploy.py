#!/usr/bin/env python3
"""Deploy the supply-chain medallion demo to a Microsoft Fabric workspace.

Idempotent: items are resolved by display name and created or updated in place.
Authentication uses the Azure CLI (`az login`), no extra Python packages needed.

    python scripts/deploy.py --workspace supply-chain-demo            # deploy everything
    python scripts/deploy.py --workspace supply-chain-demo --run      # deploy + run the pipeline
    python scripts/deploy.py --only notebooks --run-notebooks nb_02_silver_transform
"""
from __future__ import annotations

import argparse
import base64
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
FABRIC_DIR = ROOT / "fabric"
API = "https://api.fabric.microsoft.com/v1"
EXTRA_HEADERS = {"x-ms-fabric-skill": "e2e-medallion-architecture"}

BRONZE_LH, SILVER_LH, GOLD_LH = "lh_bronze", "lh_silver", "lh_gold"
NOTEBOOKS = [  # (display name, default lakehouse)
    ("nb_02_silver_transform", SILVER_LH),
    ("nb_03_gold_star_schema", GOLD_LH),
    ("nb_04_refresh_semantic_model", GOLD_LH),
]
SEMANTIC_MODEL = "Supply Chain Trips"
REPORT = "Supply Chain Trips Overview"
PIPELINE = "pl_supply_chain_medallion"

_token: dict = {}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def token() -> str:
    if not _token or _token["exp"] - time.time() < 300:
        out = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
             "--query", "{t:accessToken,e:expires_on}", "-o", "json"],
            check=True, capture_output=True, text=True).stdout
        data = json.loads(out)
        _token.update(value=data["t"], exp=float(data["e"]))
    return _token["value"]


def call(method: str, url: str, body: dict | None = None, retries: int = 6):
    if not url.startswith("http"):
        url = API + url
    data = json.dumps(body).encode() if body is not None else (b"{}" if method == "POST" else None)
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token()}")
        req.add_header("Content-Type", "application/json")
        for key, value in EXTRA_HEADERS.items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return resp.status, dict(resp.headers), (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as err:
            raw = err.read().decode(errors="replace")
            transient = err.code == 429 or err.code >= 500 or "NotAvailableYet" in raw
            if transient and attempt < retries - 1:
                wait = int(err.headers.get("Retry-After") or 10 * (attempt + 1))
                log(f"  {err.code} on {method} {url} - retrying in {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"{method} {url} -> {err.code}: {raw}") from None
        except urllib.error.URLError as err:  # transient network/DNS failure
            if attempt == retries - 1:
                raise RuntimeError(f"{method} {url} -> {err.reason}") from None
            log(f"  network error on {method} {url} ({err.reason}) - retrying")
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed after {retries} attempts")


def wait_lro(status: int, headers: dict, body):
    """Poll a Fabric long-running operation until it finishes; return its result (if any)."""
    if status != 202:
        return body
    location = headers.get("Location")
    while True:
        time.sleep(int(headers.get("Retry-After") or 3))
        _, headers, op = call("GET", location)
        state = (op or {}).get("status")
        if state == "Succeeded":
            try:
                return call("GET", location + "/result")[2]
            except RuntimeError:
                return op
        if state in ("Failed", "Cancelled"):
            raise RuntimeError(f"LRO {state}: {json.dumps(op)}")


def b64(content: str | bytes) -> str:
    return base64.b64encode(content.encode() if isinstance(content, str) else content).decode()


# --------------------------------------------------------------------------- items
def find_workspace(name: str) -> dict:
    for ws in call("GET", "/workspaces")[2]["value"]:
        if name in (ws["displayName"], ws["id"]):
            return ws
    raise SystemExit(f"Workspace '{name}' not found")


def find_item(ws: str, item_type: str, name: str) -> dict | None:
    url = f"/workspaces/{ws}/items?type={item_type}"
    while url:
        page = call("GET", url)[2]
        for item in page["value"]:
            if item["displayName"] == name:
                return item
        url = page.get("continuationUri")
    return None


def upsert_item(ws: str, item_type: str, name: str, parts: list[dict] | None = None,
                fmt: str | None = None, creation_payload: dict | None = None) -> str:
    definition = {"parts": parts} if parts else None
    if definition and fmt:
        definition["format"] = fmt
    item = find_item(ws, item_type, name)
    if item is None:
        body: dict = {"displayName": name, "type": item_type}
        if definition:
            body["definition"] = definition
        if creation_payload:
            body["creationPayload"] = creation_payload
        result = wait_lro(*call("POST", f"/workspaces/{ws}/items", body))
        item = result if result and "id" in result else find_item(ws, item_type, name)
        log(f"  created {item_type} '{name}' ({item['id']})")
    elif definition:
        wait_lro(*call("POST", f"/workspaces/{ws}/items/{item['id']}/updateDefinition",
                       {"definition": definition}))
        log(f"  updated {item_type} '{name}' ({item['id']})")
    else:
        log(f"  exists  {item_type} '{name}' ({item['id']})")
    return item["id"]


def ensure_lakehouse(ws: str, name: str) -> dict:
    lh_id = upsert_item(ws, "Lakehouse", name, creation_payload={"enableSchemas": True})
    while True:
        lh = call("GET", f"/workspaces/{ws}/lakehouses/{lh_id}")[2]
        sql = lh["properties"].get("sqlEndpointProperties") or {}
        if sql.get("provisioningStatus") == "Success":
            return lh
        log(f"  waiting for {name} SQL endpoint ({sql.get('provisioningStatus')})")
        time.sleep(10)


def wait_job(location: str, label: str) -> None:
    start = time.time()
    while True:
        time.sleep(15)
        job = call("GET", location)[2]
        status = job.get("status")
        if status == "Completed":
            log(f"  {label}: Completed in {int(time.time() - start)}s")
            return
        if status in ("Failed", "Cancelled", "Deduped"):
            raise RuntimeError(f"{label}: {status} - {json.dumps(job.get('failureReason'))}")
        log(f"  {label}: {status} ({int(time.time() - start)}s)")


def folder_parts(folder: pathlib.Path, replacements: dict[str, str]) -> list[dict]:
    parts = []
    for path in sorted(p for p in folder.rglob("*") if p.is_file() and p.name != ".DS_Store"):
        rel = path.relative_to(folder).as_posix()
        if rel.startswith(".pbi/") or rel in (".platform", "item.metadata.json"):
            continue
        content = path.read_bytes()
        if path.suffix in (".tmdl", ".json", ".pbir", ".pbism"):
            text = content.decode()
            for key, value in replacements.items():
                text = text.replace(key, value)
            content = text.encode()
        parts.append({"path": rel, "payload": b64(content), "payloadType": "InlineBase64"})
    return parts


# --------------------------------------------------------------------------- deploy steps
def deploy_lakehouses(ctx: dict) -> None:
    log("Lakehouses")
    if not find_item(ctx["ws"], "Lakehouse", BRONZE_LH):
        raise SystemExit(f"{BRONZE_LH} must exist with the dbo.raw_data table shortcut (see README)")
    for name in (BRONZE_LH, SILVER_LH, GOLD_LH):
        ctx["lakehouses"][name] = ensure_lakehouse(ctx["ws"], name)


def deploy_notebooks(ctx: dict) -> None:
    log("Notebooks")
    for name, lh_name in NOTEBOOKS:
        nb = json.loads((FABRIC_DIR / "notebooks" / f"{name}.ipynb").read_text())
        lh_id = ctx["lakehouses"][lh_name]["id"]
        nb.setdefault("metadata", {})["dependencies"] = {"lakehouse": {
            "default_lakehouse": lh_id,
            "default_lakehouse_name": lh_name,
            "default_lakehouse_workspace_id": ctx["ws"],
            "known_lakehouses": [{"id": lh_id}],
        }}
        parts = [{"path": "notebook-content.ipynb", "payload": b64(json.dumps(nb)),
                  "payloadType": "InlineBase64"}]
        ctx["notebooks"][name] = upsert_item(ctx["ws"], "Notebook", name, parts, fmt="ipynb")


def run_notebook(ctx: dict, name: str) -> None:
    nb_id = ctx["notebooks"].get(name) or find_item(ctx["ws"], "Notebook", name)["id"]
    lh_name = dict(NOTEBOOKS)[name]
    body = {"executionData": {"configuration": {"defaultLakehouse": {
        "id": ctx["lakehouses"][lh_name]["id"], "name": lh_name, "workspaceId": ctx["ws"]}}}}
    _, headers, _ = call("POST", f"/workspaces/{ctx['ws']}/items/{nb_id}/jobs/instances?jobType=RunNotebook", body)
    wait_job(headers["Location"], name)


def deploy_semantic_model(ctx: dict) -> None:
    log("Semantic model")
    gold = ctx["lakehouses"][GOLD_LH]
    sql = gold["properties"]["sqlEndpointProperties"]
    replacements = {"{{WORKSPACE_ID}}": ctx["ws"], "{{GOLD_LAKEHOUSE_ID}}": gold["id"],
                    "{{GOLD_SQL_ENDPOINT}}": sql["connectionString"], "{{GOLD_SQL_ENDPOINT_ID}}": sql["id"]}
    parts = folder_parts(FABRIC_DIR / "semantic-model" / f"{SEMANTIC_MODEL}.SemanticModel", replacements)
    ctx["semantic_model"] = upsert_item(ctx["ws"], "SemanticModel", SEMANTIC_MODEL, parts)


def deploy_report(ctx: dict) -> None:
    log("Report")
    sm_id = ctx.get("semantic_model") or find_item(ctx["ws"], "SemanticModel", SEMANTIC_MODEL)["id"]
    parts = folder_parts(FABRIC_DIR / "report" / f"{REPORT}.Report",
                         {"{{SEMANTIC_MODEL_ID}}": sm_id, "{{WORKSPACE_NAME}}": ctx["ws_name"]})
    upsert_item(ctx["ws"], "Report", REPORT, parts)


def deploy_pipeline(ctx: dict) -> None:
    log("Pipeline")
    text = (FABRIC_DIR / "pipeline" / f"{PIPELINE}.json").read_text().replace("{{WORKSPACE_ID}}", ctx["ws"])
    for name, _ in NOTEBOOKS:
        nb_id = ctx["notebooks"].get(name) or find_item(ctx["ws"], "Notebook", name)["id"]
        text = text.replace("{{" + name + "}}", nb_id)
    parts = [{"path": "pipeline-content.json", "payload": b64(text), "payloadType": "InlineBase64"}]
    ctx["pipeline"] = upsert_item(ctx["ws"], "DataPipeline", PIPELINE, parts)


def run_pipeline(ctx: dict) -> None:
    log(f"Running pipeline {PIPELINE}")
    pl_id = ctx.get("pipeline") or find_item(ctx["ws"], "DataPipeline", PIPELINE)["id"]
    _, headers, _ = call("POST", f"/workspaces/{ctx['ws']}/items/{pl_id}/jobs/instances?jobType=Pipeline")
    wait_job(headers["Location"], PIPELINE)


STEPS = {"lakehouses": deploy_lakehouses, "notebooks": deploy_notebooks,
         "semantic-model": deploy_semantic_model, "report": deploy_report, "pipeline": deploy_pipeline}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workspace", default="supply-chain-demo", help="workspace name or id")
    ap.add_argument("--only", nargs="+", choices=list(STEPS), help="deploy only these steps")
    ap.add_argument("--run-notebooks", nargs="*", metavar="NAME",
                    help="run notebooks directly, in order (all if no names given)")
    ap.add_argument("--run", action="store_true", help="run the pipeline after deploying")
    args = ap.parse_args()

    ws = find_workspace(args.workspace)
    ctx: dict = {"ws": ws["id"], "ws_name": ws["displayName"], "lakehouses": {}, "notebooks": {}}
    log(f"Workspace {ctx['ws_name']} ({ctx['ws']})")
    deploy_lakehouses(ctx)  # always runs: every other step needs the lakehouse ids
    for step in args.only or list(STEPS):
        if step != "lakehouses":
            STEPS[step](ctx)
    if args.run_notebooks is not None:
        for name in args.run_notebooks or [n for n, _ in NOTEBOOKS]:
            log(f"Running {name}")
            run_notebook(ctx, name)
    if args.run:
        run_pipeline(ctx)
    log("Done")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        sys.exit(f"ERROR: {exc}")
