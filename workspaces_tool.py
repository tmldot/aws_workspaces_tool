#!/usr/bin/env python3
"""
workspaces_tool.py - Amazon WorkSpaces bulk helper

Features
- Resolve targets to Workspace IDs from a file or --names CSV list
  * Fast first-pass matching on WorkspaceId, ComputerName, UserName
  * Optional tag-based match on Name tag via --include-tags (bounded by --max-tag-lookups)
  * Resolution table prints only for --action resolve
- Start / Stop workspaces in safe 25-size batches
  * Pre-filters by required state and prints a "skipping" table for those not in the correct state
  * Prints a failure table with error code/message for API-level failures
- List users table: ws_name | ws_id | ws_user
- Show status table: ws_name | ws_id | state
- Logging: YYYYMMDDHHMMSS-workspace-<action>.log
- Uses normal AWS credential chain; supports --profile and --region
- Robust error handling and clear exit codes

Exit codes:
 0 success
 2 partial success (some items unresolved or failed)
 3 invalid input or nothing matched
 4 AWS/API error

Requires: boto3
"""

import argparse
import datetime as dt
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    print("ERROR: boto3 is required. Install with: pip install boto3", file=sys.stderr)
    sys.exit(3)

# ---------- Logging ----------

def make_logger(action: str) -> logging.Logger:
    ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    # ensure logs/ directory exists
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)

    logfile = os.path.join(log_dir, f"{ts}-workspace-{action}.log")

    logger = logging.getLogger("workspaces_tool")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(logfile, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Log file: {logfile}")
    return logger

# ---------- Helpers ----------

def read_names_from_file(path: str) -> List[str]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",") if p.strip()]
            out.extend(parts)
    # de-dup preserving order (case-insensitive)
    seen = set()
    uniq = []
    for n in out:
        ln = n.lower()
        if ln not in seen:
            seen.add(ln)
            uniq.append(n)
    return uniq

def parse_targets(names_arg: Optional[str], file_arg: Optional[str]) -> List[str]:
    targets: List[str] = []
    if names_arg:
        targets.extend([n.strip() for n in names_arg.split(",") if n.strip()])
    if file_arg:
        targets.extend(read_names_from_file(file_arg))
    # de-dup preserving order (case-insensitive)
    seen = set()
    uniq = []
    for n in targets:
        ln = n.lower()
        if ln not in seen:
            seen.add(ln)
            uniq.append(n)
    return uniq

def build_ws_client(profile: Optional[str], region: Optional[str]):
    sess_kwargs = {}
    if profile:
        sess_kwargs["profile_name"] = profile
    session = boto3.Session(**sess_kwargs)
    return session.client("workspaces", region_name=region)

def paginate_describe_workspaces(client) -> List[dict]:
    workspaces = []
    next_token = None
    while True:
        params = {}
        if next_token:
            params["NextToken"] = next_token
        resp = client.describe_workspaces(**params)
        workspaces.extend(resp.get("Workspaces", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return workspaces

def safe_describe_tags(client, workspace_id: str) -> Dict[str, str]:
    """DescribeTags with small backoff for throttling; returns {} on errors."""
    backoff = 0.5
    for _ in range(5):
        try:
            resp = client.describe_tags(ResourceId=workspace_id)
            return {t["Key"]: t.get("Value", "") for t in resp.get("TagList", [])}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("Throttling", "ThrottlingException", "TooManyRequestsException"):
                time.sleep(backoff)
                backoff = min(backoff * 2, 4.0)
                continue
            return {}
        except BotoCoreError:
            return {}
    return {}

def best_name_for_ws(ws: dict, tags: Optional[Dict[str, str]] = None) -> str:
    if tags:
        t = tags.get("Name") or tags.get("name")
        if t:
            return t
    if ws.get("ComputerName"):
        return ws["ComputerName"]
    return ws.get("WorkspaceId", "UNKNOWN")

def build_index_without_tags(workspaces: List[dict]) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """
    by_id: WorkspaceId -> ws dict
    names_index: lower(name) -> WorkspaceId for WorkspaceId, ComputerName, UserName
    """
    by_id: Dict[str, dict] = {}
    names_index: Dict[str, str] = {}
    for ws in workspaces:
        wsid = ws.get("WorkspaceId")
        if not wsid:
            continue
        by_id[wsid] = ws
        keys = {wsid}
        if ws.get("ComputerName"):
            keys.add(ws["ComputerName"])
        if ws.get("UserName"):
            keys.add(ws["UserName"])
        for k in keys:
            names_index[k.lower()] = wsid
    return by_id, names_index

def print_table(headers: List[str], rows: List[Tuple]):
    if not rows:
        print("(no results)")
        return
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(str(cell)))
    sep = " | "
    header_line = sep.join(h.ljust(widths[i]) for i, h in enumerate(headers))
    underline = "-+-".join("-" * widths[i] for i in range(len(headers)))
    print(header_line)
    print(underline)
    for r in rows:
        print(sep.join(str(cell).ljust(widths[i]) for i, cell in enumerate(r)))

def chunked(iterable, size):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

# ---------- Resolution ----------

def resolve_targets(
    client,
    targets: List[str],
    logger: logging.Logger,
    include_tags: bool,
    max_tag_lookups: int,
    progress_every: int = 50,
    show_resolution: bool = False,  # only print for resolve action
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Resolve targets by WorkspaceId, ComputerName, UserName first.
    If include_tags=True, attempt Name-tag matching for unresolved inputs,
    bounded by max_tag_lookups to avoid long waits.

    If show_resolution=True, print the resolution table.
    """
    all_ws = paginate_describe_workspaces(client)
    by_id, names_index = build_index_without_tags(all_ws)

    resolved: List[Tuple[str, str]] = []
    unresolved: List[str] = []

    # fast first pass (no tags)
    for t in targets:
        wsid = names_index.get(t.lower())
        if wsid:
            resolved.append((t, wsid))
        else:
            unresolved.append(t)

    if include_tags and unresolved:
        logger.info(f"Attempting tag-based resolution for {len(unresolved)} input(s) with a cap of {max_tag_lookups} tag lookups.")
        unresolved_lc = {u.lower(): u for u in unresolved}
        matched_now: Dict[str, str] = {}
        tag_lookups = 0

        for ws in all_ws:
            if not unresolved_lc:
                break
            if tag_lookups >= max_tag_lookups:
                logger.warning(f"Reached --max-tag-lookups={max_tag_lookups} before resolving all names.")
                break
            wsid = ws.get("WorkspaceId")
            if not wsid:
                continue
            tags = safe_describe_tags(client, wsid)
            tag_lookups += 1
            if tag_lookups % progress_every == 0:
                logger.info(f"Tag lookups performed: {tag_lookups}")

            tag_name = (tags.get("Name") or tags.get("name") or "").lower().strip()
            if not tag_name:
                continue
            if tag_name in unresolved_lc:
                original = unresolved_lc.pop(tag_name)
                matched_now[original] = wsid

        for orig, wsid in matched_now.items():
            resolved.append((orig, wsid))
        unresolved = [u for u in unresolved if u not in matched_now]

    # Only print the resolution table if explicitly asked
    if show_resolution:
        if resolved:
            print_table(headers=["workspace_name", "workspace_id"], rows=resolved)
        else:
            logger.warning("No targets resolved.")
        if unresolved:
            logger.warning("Unresolved inputs: " + ", ".join(unresolved))

    return resolved, unresolved

# ---------- Actions ----------

def start_or_stop(client, pairs: List[Tuple[str, str]], action: str, logger: logging.Logger) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Returns (succeeded_ids, failed_pairs[(name, id)]) and prints a failure table (name, id, error code, message).
    """
    succeeded: List[str] = []
    failed: List[Tuple[str, str]] = []
    failures_detail: List[Tuple[str, str, str, str]] = []  # (name, wsid, code, message)

    if not pairs:
        return succeeded, failed

    for batch in chunked(pairs, 25):
        req = [{"WorkspaceId": wsid} for _, wsid in batch]
        try:
            if action == "start":
                resp = client.start_workspaces(StartWorkspaceRequests=req)
            else:
                resp = client.stop_workspaces(StopWorkspaceRequests=req)
            failed_list = resp.get("FailedRequests", [])
            failed_ids = {f.get("WorkspaceId") for f in failed_list if f.get("WorkspaceId")}
            name_by_id = {wsid: name for (name, wsid) in batch}

            for f in failed_list:
                wsid = f.get("WorkspaceId", "")
                code = f.get("ErrorCode", "") or f.get("Error", "")
                msg = f.get("ErrorMessage", "") or f.get("Message", "")
                name = name_by_id.get(wsid, wsid)
                failures_detail.append((name, wsid, code, msg))

            for name, wsid in batch:
                if wsid in failed_ids:
                    failed.append((name, wsid))
                else:
                    succeeded.append(wsid)

        except (ClientError, BotoCoreError) as e:
            logger.error(f"{action.title()}Workspaces failed: {e}")
            failed.extend(batch)

    if succeeded:
        logger.info(f"{action.title()} requested for {len(succeeded)} WorkSpaces.")
    if failed:
        logger.warning(f"{len(failed)} WorkSpaces failed to {action}.")
        print_table(
            headers=["ws_name", "ws_id", "error_code", "error_message"],
            rows=failures_detail if failures_detail else [(n, i, "", "") for (n, i) in failed],
        )
    return succeeded, failed

def list_users_table(client, pairs: List[Tuple[str, str]], logger: logging.Logger):
    """
    Prints: ws_name | ws_id | ws_user
    """
    if not pairs:
        logger.info("No workspaces to list.")
        print("(no results)")
        return
    rows = []
    for batch in chunked([wsid for _, wsid in pairs], 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
            for ws in resp.get("Workspaces", []):
                wsid = ws.get("WorkspaceId", "")
                user = ws.get("UserName", "")
                name = ws.get("ComputerName") or wsid  # fast; skip tags to keep it snappy
                rows.append((name, wsid, user))
        except (ClientError, BotoCoreError) as e:
            logger.error(f"DescribeWorkspaces failed: {e}")
    print_table(headers=["ws_name", "ws_id", "ws_user"], rows=rows)

def list_status_table(client, pairs: List[Tuple[str, str]], logger: logging.Logger):
    """
    Prints: ws_name | ws_id | state
    """
    if not pairs:
        logger.info("No workspaces to list.")
        print("(no results)")
        return
    rows = []
    for batch in chunked([wsid for _, wsid in pairs], 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
            for ws in resp.get("Workspaces", []):
                wsid = ws.get("WorkspaceId", "")
                state = ws.get("State", "")
                name = ws.get("ComputerName") or wsid
                rows.append((name, wsid, state))
        except (ClientError, BotoCoreError) as e:
            logger.error(f"DescribeWorkspaces failed: {e}")
    print_table(headers=["ws_name", "ws_id", "state"], rows=rows)

def get_workspace_states(client, wsids: List[str]) -> Dict[str, str]:
    """Return {wsid: state} for a list of ids."""
    states: Dict[str, str] = {}
    for batch in chunked(wsids, 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
            for ws in resp.get("Workspaces", []):
                states[ws.get("WorkspaceId", "")] = ws.get("State", "")
        except (ClientError, BotoCoreError):
            # Leave missing; the action may still try and fail if needed
            pass
    return states

# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Amazon WorkSpaces bulk helper")
    parser.add_argument("--names", help="Comma-separated workspace names/ids/usernames")
    parser.add_argument("--file", help="File with workspace names (comma-separated or one per line)")
    parser.add_argument("--action", required=True, choices=["resolve", "start", "stop", "users", "status"])
    parser.add_argument("--profile", help="AWS CLI profile")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without calling APIs")
    parser.add_argument("--include-tags", action="store_true", help="Also try to resolve by Name tag")
    parser.add_argument("--max-tag-lookups", type=int, default=500, help="Cap DescribeTags calls when --include-tags is set")
    args = parser.parse_args()

    logger = make_logger(args.action)

    try:
        targets = parse_targets(args.names, args.file)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(3)
    except Exception as e:
        logger.error(f"Failed to parse inputs: {e}")
        sys.exit(3)

    if not targets and args.action != "resolve":
        logger.error("You must provide at least one workspace target via --names or --file.")
        sys.exit(3)

    try:
        client = build_ws_client(args.profile, args.region)
    except Exception as e:
        logger.error(f"Failed to create AWS client: {e}")
        sys.exit(4)

    try:
        resolved, unresolved = resolve_targets(
            client,
            targets,
            logger,
            include_tags=args.include_tags,
            max_tag_lookups=args.max_tag_lookups,
            show_resolution=(args.action == "resolve"),  # print table only for resolve
        ) if targets else ([], [])
    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS error when resolving targets: {e}")
        sys.exit(4)
    except KeyboardInterrupt:
        logger.error("Interrupted during resolution.")
        sys.exit(3)
    except Exception as e:
        logger.error(f"Unexpected error during resolution: {e}")
        sys.exit(4)

    if args.action == "resolve":
        if unresolved and resolved:
            sys.exit(2)  # partial
        elif unresolved:
            sys.exit(3)  # none matched
        else:
            sys.exit(0)

    if not resolved:
        logger.error("No targets resolved to WorkSpaces. Nothing to do.")
        sys.exit(3)

    if args.dry_run:
        for name, wsid in resolved:
            logger.info(f"[DRY-RUN] {args.action.upper()} {name} ({wsid})")
        if unresolved:
            logger.warning(f"[DRY-RUN] Unresolved: {', '.join(unresolved)}")
        sys.exit(0)

    try:
        if args.action in ("start", "stop"):
            # Prefilter by state so we only call APIs on valid candidates
            id_list = [wsid for _, wsid in resolved]
            states = get_workspace_states(client, id_list)

            if args.action == "start":
                valid_state = "STOPPED"
                allowed = [(name, wsid) for (name, wsid) in resolved if states.get(wsid) == valid_state]
            else:  # stop
                valid_state = "AVAILABLE"
                allowed = [(name, wsid) for (name, wsid) in resolved if states.get(wsid) == valid_state]

            skipped = [(name, wsid, states.get(wsid, "UNKNOWN")) for (name, wsid) in resolved if (name, wsid) not in allowed]
            if skipped:
                logger.info(f"Skipping {len(skipped)} WorkSpaces not in {valid_state} state.")
                print_table(headers=["ws_name", "ws_id", "current_state"], rows=skipped)

            if not allowed:
                logger.warning("No WorkSpaces in the correct state for this action.")
                sys.exit(2 if unresolved else 3)

            succeeded, failed = start_or_stop(client, allowed, args.action, logger)
            if failed or unresolved:
                sys.exit(2 if succeeded else 3)
            sys.exit(0)

        elif args.action == "users":
            list_users_table(client, resolved, logger)
            sys.exit(2 if unresolved else 0)

        elif args.action == "status":
            list_status_table(client, resolved, logger)
            sys.exit(2 if unresolved else 0)

    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS error during '{args.action}': {e}")
        sys.exit(4)
    except KeyboardInterrupt:
        logger.error("Interrupted.")
        sys.exit(3)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(4)

if __name__ == "__main__":
    main()

