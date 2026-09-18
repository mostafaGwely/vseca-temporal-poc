import hashlib
import json
import yaml
from temporalio import activity
from es import ES

STATE_INDEX = ".vseca-temporal-orchestration-state"
MAIN_STAGING = "vseca-vgvg-iteh-asset-staging-list"
CLOUD_STAGING = "vseca-vgvg-iteh-cloud-asset-staging-list"
MAIN_REPORTING = "vseca-vgvg-iteh-assets-main-reporting"
CLOUD_REPORTING = "vseca-vgvg-iteh-assets-cloud-reporting"

def _first_index(v):
    return v[0] if isinstance(v, list) else v

def _classify(command):
    if command.get("stage"):
        return command["stage"]
    dest = _first_index(command.get("dest", {}).get("index"))
    if dest in (MAIN_STAGING, CLOUD_STAGING): return "backend-to-staging"
    if command.get("name") in ("iteh-main-reporting-index", "iteh-cloud-reporting-index"): return "staging-to-reporting"
    if command.get("name") in ("iteh-main-reporting-index-history", "iteh-cloud-reporting-index-history"): return "reporting-to-history"
    return "unmanaged"

@activity.defn
def load_plan(config_path: str):
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    enrich = [{"name": p["name"]} for p in cfg.get("enrichPolicies", [])]
    stages = {"backend-to-staging": [], "staging-to-reporting": [], "reporting-to-history": [], "unmanaged": []}
    for cmd in cfg.get("reindexCommands", []):
        stages[_classify(cmd)].append(cmd)
    return {"enrich": enrich, **stages}

def _policy_sources(es, name):
    data = es.request("GET", f"/_enrich/policy/{name}")
    policies = data.get("policies", [])
    if not policies:
        raise RuntimeError(f"Enrich policy not found: {name}")
    policy = policies[0].get("config", policies[0])
    for policy_type in ("match", "range", "geo_match"):
        if policy_type in policy:
            indices = policy[policy_type].get("indices", [])
            return indices if isinstance(indices, list) else [indices]
    raise RuntimeError(f"Cannot find source indices in enrich policy: {name}")

def _fingerprint(es, patterns):
    joined = ",".join(patterns)
    resolved = es.request("GET", f"/_resolve/index/{joined}")
    names = sorted(i["name"] for i in resolved.get("indices", []))
    if not names:
        raise RuntimeError(f"No concrete source indices resolved from: {patterns}")
    stats = es.request("GET", f"/{','.join(names)}/_stats/docs,seq_no")
    snapshot = []
    for name in names:
        s = stats["indices"][name]
        prim = s["primaries"]
        snapshot.append({
            "index": name,
            "uuid": s.get("uuid"),
            "docs": prim.get("docs", {}).get("count"),
            "max_seq_no": prim.get("seq_no", {}).get("max_seq_no")
        })
    raw = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest(), snapshot

@activity.defn
def inspect_enrich(inp):
    es = ES()
    sources = _policy_sources(es, inp["name"])
    fingerprint, snapshot = _fingerprint(es, sources)
    try:
        old = es.request("GET", f"/{STATE_INDEX}/_doc/enrich-{inp['name']}")
        previous = old.get("_source", {}).get("fingerprint")
    except RuntimeError as exc:
        if " 404 " not in str(exc): raise
        previous = None
    return {"name": inp["name"], "sources": sources, "fingerprint": fingerprint,
            "snapshot": snapshot, "changed": fingerprint != previous}

@activity.defn
def start_enrich(inp):
    if inp.get("dry_run"):
        return {"name": inp["name"], "dry_run": True, "completed": True}
    return ES().request("PUT", f"/_enrich/policy/{inp['name']}/_execute", params={"wait_for_completion": "false"})

@activity.defn
def save_enrich_checkpoint(inp):
    if inp.get("dry_run"): return {"saved": False, "dry_run": True}
    body = {"policy": inp["name"], "fingerprint": inp["fingerprint"],
            "sources": inp["sources"], "snapshot": inp["snapshot"]}
    return ES().request("PUT", f"/{STATE_INDEX}/_doc/enrich-{inp['name']}", body=body, params={"refresh": "true"})

@activity.defn
def start_reindex(inp):
    cmd = inp["command"]
    if inp.get("dry_run"):
        return {"command": cmd["name"], "dry_run": True, "completed": True}
    body = {k: cmd[k] for k in ("source", "dest", "script", "conflicts") if k in cmd}
    result = ES().request("POST", "/_reindex", body=body, params={"wait_for_completion": "false"})
    if "task" not in result:
        raise RuntimeError(f"No task ID returned for {cmd['name']}: {result}")
    return {"command": cmd["name"], "task": result["task"], "completed": False}

@activity.defn
def check_task(inp):
    data = ES().request("GET", f"/_tasks/{inp['task_id']}")
    if not data.get("completed"):
        return {"completed": False}
    if data.get("error"):
        raise RuntimeError(f"Elasticsearch task failed: {json.dumps(data['error'])}")
    response = data.get("response", {})
    failures = response.get("failures", [])
    if failures:
        raise RuntimeError(f"Elasticsearch task has failures: {json.dumps(failures[:10])}")
    return {"completed": True, "response": response}

@activity.defn
def validate_reindex(inp):
    cmd, result = inp["command"], inp.get("task_result", {})
    response = result.get("response", {})
    return {"command": cmd["name"], "destination": _first_index(cmd["dest"]["index"]),
            "created": response.get("created", 0), "updated": response.get("updated", 0),
            "deleted": response.get("deleted", 0), "version_conflicts": response.get("version_conflicts", 0),
            "failures": response.get("failures", []), "status": "SUCCESS"}
