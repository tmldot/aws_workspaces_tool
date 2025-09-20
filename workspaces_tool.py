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
- Logging: logs/<timestamp>-workspace-<action>.log (logs/ auto-created)
- Uses normal AWS credential chain; supports --profile and --region
- Robust error handling and clear exit codes

Exit codes:
 0 success
 2 partial success (some items unresolved or failed)
 3 invalid input or nothing matched
 4 AWS/API error

Requires: boto3
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# ---------- Logging ----------


def make_logger(action: str) -> logging.Logger:
    """Create a console+file logger writing to logs/<timestamp>-workspace-<action>.log."""
    ts = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    # ensure logs/ directory exists next to this file
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    logfile = os.path.join(log_dir, f"{ts}-workspace-{action}.log")

    logger = logging.getLogger("workspaces_tool")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    stream_handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(fmt)
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.info("Log file: %s", logfile)
    return logger


# ---------- Helpers ----------


def read_names_from_file(path: str) -> List[str]:
    """Read workspace targets from a file (one per line or comma-separated), de-duplicated case-insensitively."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",") if p.strip()]
            out.extend(parts)
    # de-dup preserving order (case-insensitive)
    seen = set()
    uniq = []
    for name in out:
        name_lc = name.lower()
        if name_lc not in seen:
            seen.add(name_lc)
            uniq.append(name)
    return uniq


def parse_targets(names_arg: Optional[str], file_arg: Optional[str]) -> List[str]:
    """Combine targets from --names and --file, de-duplicated case-insensitively."""
    targets: List[str] = []
    if names_arg:
        targets.extend([n.strip() for n in names_arg.split(",") if n.strip()])
    if file_arg:
        targets.extend(read_names_from_file(file_arg))
    seen = set()
    uniq = []
    for name in targets:
        name_lc = name.lower()
        if name_lc not in seen:
            seen.add(name_lc)
            uniq.append(name)
    return uniq


def build_ws_client(profile: Optional[str], region: Optional[str]):
    """Return a boto3 WorkSpaces client using the normal credential chain, with optional profile and region."""
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    try:
        session = boto3.Session(**session_kwargs)
        return session.client("workspaces", region_name=region)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to create WorkSpaces client: {exc}") from exc


def paginate_describe_workspaces(client) -> List[dict]:
    """Return all WorkSpaces in the account/region, handling pagination."""
    workspaces = []
    next_token = None
    while True:
        params = {}
        if next_token:
            params["NextToken"] = next_token
        try:
            resp = client.describe_workspaces(**params)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"DescribeWorkspaces failed: {exc}") from exc
        workspaces.extend(resp.get("Workspaces", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return workspaces


def safe_describe_tags(client, workspace_id: str) -> Dict[str, str]:
    """Call DescribeTags with small backoff for throttling; return {} on permission/network errors."""
    backoff = 0.5
    for _ in range(5):
        try:
            resp = client.describe_tags(ResourceId=workspace_id)
            return {t["Key"]: t.get("Value", "") for t in resp.get("TagList", [])}
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("Throttling", "ThrottlingException", "TooManyRequestsException"):
                time.sleep(backoff)
                backoff = min(backoff * 2, 4.0)
                continue
            return {}
        except BotoCoreError:
            return {}
    return {}


def best_name_for_ws(ws: dict, tags: Optional[Dict[str, str]] = None) -> str:
    """Return a friendly name for a WorkSpace: Name tag > ComputerName > WorkspaceId."""
    if tags:
        tval = tags.get("Name") or tags.get("name")
        if tval:
            return tval
    comp = ws.get("ComputerName")
    if comp:
        return comp
    return ws.get("WorkspaceId", "UNKNOWN")


def build_index_without_tags(workspaces: Sequence[dict]) -> Tuple[Dict[str, dict], Dict[str, str]]:
    """Build (by_id, names_index) using WorkspaceId, ComputerName, UserName only (no tag sweeps)."""
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
        for key in keys:
            names_index[key.lower()] = wsid
    return by_id, names_index


def print_table(headers: List[str], rows: Iterable[Tuple]):
    """Print a simple fixed-width table; print '(no results)' if rows is empty."""
    rows = list(rows)
    if not rows:
        print("(no results)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(str(cell)))
    sep = " | "
    header_line = sep.join(h.ljust(widths[i]) for i, h in enumerate(headers))
    underline = "-+-".join("-" * widths[i] for i in range(len(headers)))
    print(header_line)
    print(underline)
    for row in rows:
        print(sep.join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    """Yield items from iterable in fixed-size chunks."""
    buf: List = []
    for item in iterable:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


# ---------- Resolution ----------


@dataclass
class ResolveOpts:
    """Options that govern how targets are resolved to Workspace IDs."""
    include_tags: bool = False
    max_tag_lookups: int = 500
    progress_every: int = 50
    show_resolution: bool = False


def resolve_targets(
    client,
    targets: List[str],
    logger: logging.Logger,
    opts: ResolveOpts,
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Resolve targets by ID/ComputerName/UserName, then optionally by Name tag (bounded)."""
    all_ws = paginate_describe_workspaces(client)
    _, names_index = build_index_without_tags(all_ws)

    resolved: List[Tuple[str, str]] = []
    unresolved: List[str] = []

    # fast first pass (no tags)
    for tgt in targets:
        wsid = names_index.get(tgt.lower())
        if wsid:
            resolved.append((tgt, wsid))
        else:
            unresolved.append(tgt)

    # optional Name-tag matching (bounded)
    if opts.include_tags and unresolved:
        logger.info(
            "Attempting tag-based resolution for %d input(s) with a cap of %d tag lookups.",
            len(unresolved),
            opts.max_tag_lookups,
        )
        unresolved_lc = {u.lower(): u for u in unresolved}
        matched_now: Dict[str, str] = {}
        tag_lookups = 0

        for ws in all_ws:
            if not unresolved_lc:
                break
            if tag_lookups >= opts.max_tag_lookups:
                logger.warning(
                    "Reached --max-tag-lookups=%d before resolving all names.",
                    opts.max_tag_lookups,
                )
                break
            wsid = ws.get("WorkspaceId")
            if not wsid:
                continue
            tags = safe_describe_tags(client, wsid)
            tag_lookups += 1
            if tag_lookups % opts.progress_every == 0:
                logger.info("Tag lookups performed: %d", tag_lookups)

            tag_name = (tags.get("Name") or tags.get("name") or "").lower().strip()
            if not tag_name:
                continue
            if tag_name in unresolved_lc:
                original = unresolved_lc.pop(tag_name)
                matched_now[original] = wsid

        for orig, wsid in matched_now.items():
            resolved.append((orig, wsid))
        unresolved = [u for u in unresolved if u not in matched_now]

    if opts.show_resolution:
        if resolved:
            print_table(headers=["workspace_name", "workspace_id"], rows=resolved)
        else:
            logger.warning("No targets resolved.")
        if unresolved:
            logger.warning("Unresolved inputs: %s", ", ".join(unresolved))

    return resolved, unresolved


# ---------- Actions ----------


def start_or_stop(
    client,
    pairs: List[Tuple[str, str]],
    action: str,
    logger: logging.Logger,
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Start or stop WorkSpaces in batches; print failure table; return (succeeded_ids, failed_pairs)."""
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
        except (BotoCoreError, ClientError) as exc:
            logger.error("%sWorkspaces failed: %s", action.title(), exc)
            failed.extend(batch)
            continue

        failed_list = resp.get("FailedRequests", [])
        failed_ids = {f.get("WorkspaceId") for f in failed_list if f.get("WorkspaceId")}
        name_by_id = {wsid: name for (name, wsid) in batch}

        for item in failed_list:
            wsid = item.get("WorkspaceId", "")
            code = item.get("ErrorCode", "") or item.get("Error", "")
            msg = item.get("ErrorMessage", "") or item.get("Message", "")
            name = name_by_id.get(wsid, wsid)
            failures_detail.append((name, wsid, code, msg))

        for name, wsid in batch:
            if wsid in failed_ids:
                failed.append((name, wsid))
            else:
                succeeded.append(wsid)

    if succeeded:
        logger.info("%s requested for %d WorkSpaces.", action.title(), len(succeeded))
    if failed:
        logger.warning("%d WorkSpaces failed to %s.", len(failed), action)
        rows = failures_detail if failures_detail else [(n, i, "", "") for (n, i) in failed]
        print_table(
            headers=["ws_name", "ws_id", "error_code", "error_message"],
            rows=rows,
        )
    return succeeded, failed


def list_users_table(client, pairs: List[Tuple[str, str]], logger: logging.Logger):
    """Print a table of ws_name | ws_id | ws_user for the resolved WorkSpaces."""
    if not pairs:
        logger.info("No workspaces to list.")
        print("(no results)")
        return
    rows = []
    for batch in chunked([wsid for _, wsid in pairs], 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
        except (BotoCoreError, ClientError) as exc:
            logger.error("DescribeWorkspaces failed: %s", exc)
            continue
        for ws in resp.get("Workspaces", []):
            wsid = ws.get("WorkspaceId", "")
            user = ws.get("UserName", "")
            name = ws.get("ComputerName") or wsid  # fast; skip tags to keep it snappy
            rows.append((name, wsid, user))
    print_table(headers=["ws_name", "ws_id", "ws_user"], rows=rows)


def list_status_table(client, pairs: List[Tuple[str, str]], logger: logging.Logger):
    """Print a table of ws_name | ws_id | state for the resolved WorkSpaces."""
    if not pairs:
        logger.info("No workspaces to list.")
        print("(no results)")
        return
    rows = []
    for batch in chunked([wsid for _, wsid in pairs], 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
        except (BotoCoreError, ClientError) as exc:
            logger.error("DescribeWorkspaces failed: %s", exc)
            continue
        for ws in resp.get("Workspaces", []):
            wsid = ws.get("WorkspaceId", "")
            state = ws.get("State", "")
            name = ws.get("ComputerName") or wsid
            rows.append((name, wsid, state))
    print_table(headers=["ws_name", "ws_id", "state"], rows=rows)


def get_workspace_states(client, wsids: List[str]) -> Dict[str, str]:
    """Return a mapping of WorkspaceId -> State for the given list of ids."""
    states: Dict[str, str] = {}
    for batch in chunked(wsids, 25):
        try:
            resp = client.describe_workspaces(WorkspaceIds=batch)
        except (BotoCoreError, ClientError):
            # Leave missing; the action may still try and fail if needed
            continue
        for ws in resp.get("Workspaces", []):
            states[ws.get("WorkspaceId", "")] = ws.get("State", "")
    return states


def filter_by_valid_state(
    client,
    resolved: List[Tuple[str, str]],
    action: str,
    logger: logging.Logger,
) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str, str]]]:
    """Return (allowed_pairs, skipped_rows[name,id,state]) based on required state for action."""
    id_list = [wsid for _, wsid in resolved]
    states = get_workspace_states(client, id_list)
    required = "STOPPED" if action == "start" else "AVAILABLE"
    allowed = [(name, wsid) for (name, wsid) in resolved if states.get(wsid) == required]
    skipped = [
        (name, wsid, states.get(wsid, "UNKNOWN"))
        for (name, wsid) in resolved
        if (name, wsid) not in allowed
    ]
    if skipped:
        logger.info("Skipping %d WorkSpaces not in %s state.", len(skipped), required)
    return allowed, skipped


# ---------- CLI ----------


def main():
    """CLI entrypoint for the WorkSpaces bulk helper."""
    parser = argparse.ArgumentParser(description="Amazon WorkSpaces bulk helper")
    parser.add_argument("--names", help="Comma-separated workspace names/ids/usernames")
    parser.add_argument("--file", help="File with workspace names (comma-separated or one per line)")
    parser.add_argument(
        "--action",
        required=True,
        choices=["resolve", "start", "stop", "users", "status"],
    )
    parser.add_argument("--profile", help="AWS CLI profile")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without calling APIs")
    parser.add_argument("--include-tags", action="store_true", help="Also try to resolve by Name tag")
    parser.add_argument(
        "--max-tag-lookups",
        type=int,
        default=500,
        help="Cap DescribeTags calls when --include-tags is set",
    )
    args = parser.parse_args()

    logger = make_logger(args.action)

    try:
        targets = parse_targets(args.names, args.file)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(3)
    except OSError as exc:
        logger.error("Failed to read inputs: %s", exc)
        sys.exit(3)

    if not targets and args.action != "resolve":
        logger.error("You must provide at least one workspace target via --names or --file.")
        sys.exit(3)

    try:
        client = build_ws_client(args.profile, args.region)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(4)

    opts = ResolveOpts(
        include_tags=args.include_tags,
        max_tag_lookups=args.max_tag_lookups,
        show_resolution=(args.action == "resolve"),
    )
    try:
        resolved, unresolved = resolve_targets(client, targets, logger, opts) if targets else ([], [])
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(4)

    if args.action == "resolve":
        if unresolved and resolved:
            sys.exit(2)  # partial
        if unresolved:
            sys.exit(3)  # none matched
        sys.exit(0)

    if not resolved:
        logger.error("No targets resolved to WorkSpaces. Nothing to do.")
        sys.exit(3)

    if args.dry_run:
        for name, wsid in resolved:
            logger.info("[DRY-RUN] %s %s (%s)", args.action.upper(), name, wsid)
        if unresolved:
            logger.warning("[DRY-RUN] Unresolved: %s", ", ".join(unresolved))
        sys.exit(0)

    try:
        if args.action in ("start", "stop"):
            allowed, skipped = filter_by_valid_state(client, resolved, args.action, logger)
            if skipped:
                print_table(headers=["ws_name", "ws_id", "current_state"], rows=skipped)
            if not allowed:
                logger.warning("No WorkSpaces in the correct state for this action.")
                sys.exit(2 if unresolved else 3)

            _, failed = start_or_stop(client, allowed, args.action, logger)  # succeeded not used
            sys.exit(2 if (failed or unresolved) else 0)

        if args.action == "users":
            list_users_table(client, resolved, logger)
            sys.exit(2 if unresolved else 0)

        if args.action == "status":
            list_status_table(client, resolved, logger)
            sys.exit(2 if unresolved else 0)

    except (BotoCoreError, ClientError) as exc:
        logger.error("AWS error during '%s': %s", args.action, exc)
        sys.exit(4)

    # Should not reach here
    sys.exit(0)


if __name__ == "__main__":
    main()
