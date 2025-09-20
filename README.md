# AWS WorkSpaces Tool

Python CLI tool for managing Amazon WorkSpaces — resolve by name, start/stop in batches, list users or status, with logging and error handling.

## Features

- Resolve WorkSpaces by:
  - WorkspaceId
  - ComputerName
  - UserName
  - Optional `Name` tag (`--include-tags`)
- Start/Stop WorkSpaces in safe batches (25 at a time)
  - Pre-filters by valid state (`STOPPED` for start, `AVAILABLE` for stop)
  - Prints skipped WorkSpaces with their current state
  - Prints failure table with error code and message
- List users: `ws_name | ws_id | ws_user`
- List status: `ws_name | ws_id | state`
- Logging to timestamped files (`YYYYMMDDHHMMSS-workspace-<action>.log`)
- Supports `--profile` and `--region` for AWS session control
- Clear exit codes for success, partial success, or errors

## Prerequisites

- Python 3.8+
- [boto3](https://pypi.org/project/boto3/)

Install dependencies:

```bash
pip install boto3
```

AWS credentials must be configured in one of the [standard ways](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) (environment variables, AWS CLI config, etc.).

## Usage

```bash
python workspaces_tool.py --file <workspace-list.txt> --action <resolve|start|stop|users|status>
```

### Options

- `--file` : File with WorkSpace names (one per line or comma-separated)
- `--names` : Comma-separated list of WorkSpace names/IDs/usernames
- `--action` : Action to perform (`resolve`, `start`, `stop`, `users`, `status`)
- `--profile` : AWS CLI profile name
- `--region` : AWS region
- `--include-tags` : Attempt to match against `Name` tag
- `--max-tag-lookups` : Limit number of DescribeTags API calls (default: 500)
- `--dry-run` : Show what would happen without making changes

## Examples

Resolve WorkSpaces from file:
```bash
python workspaces_tool.py --file danwslist.txt --action resolve
```

Resolve using Name tags:
```bash
python workspaces_tool.py --file danwslist.txt --action resolve --include-tags
```

Start all STOPPED WorkSpaces in list:
```bash
python workspaces_tool.py --file danwslist.txt --action start
```

Stop all AVAILABLE WorkSpaces in list:
```bash
python workspaces_tool.py --file danwslist.txt --action stop
```

List assigned users:
```bash
python workspaces_tool.py --file danwslist.txt --action users
```

Check status:
```bash
python workspaces_tool.py --file danwslist.txt --action status
```

## Exit Codes

- `0` = Success
- `2` = Partial success (some WorkSpaces unresolved or failed)
- `3` = Invalid input / no matches
- `4` = AWS/API error

## License

MIT License
