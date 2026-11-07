"""Apply an optional APP_NODE_ID=1001..1008 deployment override."""

import os

Import("env")

raw = os.environ.get("APP_NODE_ID")
if raw:
    try:
        node_id = int(raw)
    except ValueError as exc:
        raise ValueError("APP_NODE_ID must be an integer from 1001 to 1008") from exc
    if not 1001 <= node_id <= 1008:
        raise ValueError("APP_NODE_ID must be from 1001 to 1008")
    env.Append(CPPDEFINES=[("APP_NODE_ID", node_id)])
    print(f"Deployment node override: APP_NODE_ID={node_id}")
