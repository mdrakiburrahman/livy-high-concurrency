"""High-concurrency Fabric Livy demo.

Acquires one HC session per thread under a shared sessionTag while N
`SELECT 1` statements run concurrently.

Docs:
  https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy
  https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-high-concurrency-livy
"""

from __future__ import annotations

import argparse
import sys
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
import requests
from azure.identity import AzureCliCredential

AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_ENDPOINT = "https://api.fabric.microsoft.com/v1"
LIVY_API_VERSION = "2023-12-01"

HTTP_TIMEOUT = 60
ACQUIRE_TIMEOUT = 600
STATEMENT_TIMEOUT = 300
POLL_WAIT = 1.0

_ACQUIRING_STATES = frozenset({"NotStarted", "starting", "AcquiringHighConcurrencySession"})
_TERMINAL_BAD_STATES = frozenset({"Dead", "Killed", "Failed", "Error"})

_token_lock = threading.Lock()
_credential = AzureCliCredential()
_cached_token: Optional[Any] = None


def _now() -> float:
    return time.perf_counter()


def get_token() -> str:
    global _cached_token
    with _token_lock:
        if _cached_token is None or _cached_token.expires_on - time.time() < 300:
            _cached_token = _credential.get_token(AZURE_CREDENTIAL_SCOPE)
        return _cached_token.token


def headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_token()}",
    }


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
    max_retries = 5
    for attempt in range(max_retries):
        resp = requests.request(
            method, url, headers=headers(), timeout=HTTP_TIMEOUT, **kwargs
        )
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt < max_retries - 1:
                time.sleep(min(2**attempt, 15))
                continue
        return resp
    return resp


class LivyHC:
    """Thin client over the Fabric high-concurrency Livy REST surface."""

    def __init__(self, workspace_id: str, lakehouse_id: str, environment_id: str, session_tag: str):
        self.base = (
            f"{FABRIC_ENDPOINT}/workspaces/{workspace_id}"
            f"/lakehouses/{lakehouse_id}/livyapi/versions/{LIVY_API_VERSION}"
        )
        self.environment_id = environment_id
        self.session_tag = session_tag

    def acquire(self) -> str:
        payload: dict[str, Any] = {
            "name": self.session_tag,
            "sessionTag": self.session_tag,
            "conf": {
                "spark.fabric.environmentDetails": json.dumps({"id": self.environment_id})
            },
        }
        resp = _request(
            "POST", f"{self.base}/highConcurrencySessions", data=json.dumps(payload)
        )
        if resp.status_code not in (200, 201, 202):
            raise RuntimeError(f"HC acquire failed (HTTP {resp.status_code}): {resp.text}")
        hc_id = resp.json().get("id")
        if not hc_id:
            raise RuntimeError(f"HC acquire response missing 'id': {resp.text}")
        return hc_id

    def wait_until_idle(self, hc_id: str) -> tuple[str, str]:
        deadline = time.time() + ACQUIRE_TIMEOUT
        url = f"{self.base}/highConcurrencySessions/{hc_id}"
        while True:
            if time.time() > deadline:
                raise TimeoutError(f"HC session {hc_id} did not reach Idle in {ACQUIRE_TIMEOUT}s")
            resp = _request("GET", url)
            body = resp.json()
            state = body.get("state", "")
            if state in _TERMINAL_BAD_STATES:
                info = body.get("fabricSessionStateInfo", {}) or {}
                raise RuntimeError(f"HC session {hc_id} state={state}: {info.get('errorMessage', state)}")
            if state == "Idle" and body.get("sessionId") and body.get("replId"):
                return body["sessionId"], body["replId"]
            time.sleep(POLL_WAIT)

    def run_sql(self, session_id: str, repl_id: str, code: str) -> list:
        base = (
            f"{self.base}/highConcurrencySessions/{session_id}"
            f"/repls/{repl_id}/statements"
        )
        resp = _request("POST", base, data=json.dumps({"code": code, "kind": "sql"}))
        if resp.status_code >= 400:
            raise RuntimeError(f"Statement submit failed (HTTP {resp.status_code}): {resp.text}")
        statement_id = resp.json()["id"]

        deadline = time.time() + STATEMENT_TIMEOUT
        url = f"{base}/{statement_id}"
        while True:
            if time.time() > deadline:
                raise TimeoutError(f"Statement {statement_id} timed out after {STATEMENT_TIMEOUT}s")
            resp = _request("GET", url)
            if resp.status_code == 404:
                time.sleep(0.3)
                continue
            body = resp.json()
            state = body.get("state")
            if state == "available":
                return self._extract_rows(body)
            if state in ("error", "cancelled", "cancelling"):
                out = body.get("output", {}) or {}
                raise RuntimeError(f"Statement {statement_id} {state}: {out.get('evalue', out)}")
            time.sleep(0.3)

    @staticmethod
    def _extract_rows(body: dict) -> list:
        output = body.get("output", {}) or {}
        if output.get("status") == "error":
            raise RuntimeError(f"Statement error: {output.get('evalue', output)}")
        payload = (output.get("data", {}) or {}).get("application/json", {}) or {}
        return payload.get("data", [])

    def delete(self, hc_id: str) -> None:
        try:
            _request("DELETE", f"{self.base}/highConcurrencySessions/{hc_id}")
        except Exception:
            pass


@dataclass
class ThreadResult:
    index: int
    hc_id: str = ""
    session_id: str = ""
    repl_id: str = ""
    acquire_ms: float = 0.0
    query_ms: float = 0.0
    total_ms: float = 0.0
    start_offset_ms: float = 0.0
    finish_offset_ms: float = 0.0
    value: Any = None
    error: str = ""


def worker(client: LivyHC, index: int, barrier: threading.Barrier, t0_box: list) -> ThreadResult:
    r = ThreadResult(index=index)
    barrier.wait()
    fanout_t0 = t0_box[0]
    start = _now()
    r.start_offset_ms = (start - fanout_t0) * 1000
    try:
        a0 = _now()
        r.hc_id = client.acquire()
        r.session_id, r.repl_id = client.wait_until_idle(r.hc_id)
        r.acquire_ms = (_now() - a0) * 1000

        q0 = _now()
        rows = client.run_sql(r.session_id, r.repl_id, "SELECT 1")
        r.query_ms = (_now() - q0) * 1000
        r.value = _first_value(rows)
    except Exception as exc:  # noqa: BLE001
        r.error = str(exc)
    finally:
        end = _now()
        r.total_ms = (end - start) * 1000
        r.finish_offset_ms = (end - fanout_t0) * 1000
    return r


def _first_value(rows: list) -> Any:
    if not rows:
        return None
    row = rows[0]
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    if isinstance(row, (list, tuple)):
        return row[0] if row else None
    return row


def render_timeline(results: list[ThreadResult], width: int = 60) -> str:
    ok = [r for r in results if not r.error]
    if not ok:
        return "(no successful threads to plot)"
    max_finish = max(r.finish_offset_ms for r in ok)
    span = max_finish if max_finish > 0 else 1.0
    lines = ["", "Return timeline (dot = when SELECT 1 came back):", ""]
    for r in sorted(results, key=lambda x: x.index):
        label = f"T{r.index:02d}"
        if r.error:
            lines.append(f"{label} |{'':<{width}}|  ERROR")
            continue
        pos = int((r.finish_offset_ms / span) * (width - 1))
        track = [" "] * width
        track[pos] = "•"
        lines.append(f"{label} |{''.join(track)}|  {r.finish_offset_ms:8.1f} ms")
    lines.append("")
    lines.append(f"     0 ms{'':<{width - 12}}{span:8.1f} ms")
    return "\n".join(lines)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Fabric high-concurrency Livy demo")
    parser.add_argument("--workspace-id", default="3ff9e765-5d89-40e1-bd28-af3ca7128985")
    parser.add_argument("--lakehouse-id", default="18a3637f-d1cd-4b9c-ac7a-7fedf0cb44ef")
    parser.add_argument("--environment-id", default="07023309-c08c-48bf-9ce8-2df3b39c26d5")
    parser.add_argument("--concurrent-queries", type=int, default=16)
    args = parser.parse_args()

    session_tag = f"livy-hc-demo-{uuid.uuid4().hex[:12]}"
    client = LivyHC(args.workspace_id, args.lakehouse_id, args.environment_id, session_tag)

    stages: list[dict[str, Any]] = []
    created_hc_ids: list[str] = []

    print(f"Session tag: {session_tag}")
    print(f"Concurrent queries: {args.concurrent_queries}\n")

    t = _now()
    get_token()
    stages.append({"stage": "1. Azure CLI auth", "seconds": round(_now() - t, 3)})

    print("Acquiring primary HC session (cold start)...")
    t = _now()
    primary_hc = client.acquire()
    created_hc_ids.append(primary_hc)
    primary_session, primary_repl = client.wait_until_idle(primary_hc)
    stages.append({"stage": "2. Primary HC session ready", "seconds": round(_now() - t, 3)})
    print(f"  underlying Livy sessionId = {primary_session}\n")

    print("Warm-up: SELECT 1 ...")
    t = _now()
    warm_rows = client.run_sql(primary_session, primary_repl, "SELECT 1")
    warm_val = _first_value(warm_rows)
    stages.append({"stage": "3. Warm-up SELECT 1", "seconds": round(_now() - t, 3)})
    if str(warm_val) != "1":
        print(f"  WARNING: warm-up returned {warm_val!r}, expected 1")
    else:
        print(f"  got {warm_val} \u2713\n")

    print("Verifying attached Fabric environment on the live session...")
    try:
        env_rows = client.run_sql(
            primary_session, primary_repl, "SET spark.fabric.environmentDetails"
        )
        applied_env = _first_value([r[-1] if isinstance(r, (list, tuple)) else r for r in env_rows])
        print(f"  spark.fabric.environmentDetails = {applied_env}")
        if applied_env and args.environment_id in str(applied_env):
            print(f"  environment {args.environment_id} is attached \u2713\n")
        else:
            print(f"  WARNING: expected environment {args.environment_id} not reflected\n")
    except Exception as exc:  # noqa: BLE001
        print(f"  could not read environment back: {exc}\n")

    n = args.concurrent_queries
    print(f"Firing {n} concurrent SELECT 1 across {n} threads...")
    barrier = threading.Barrier(n + 1)
    t0_box: list = [0.0]
    results: list[ThreadResult] = []
    t = _now()
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(worker, client, i, barrier, t0_box) for i in range(n)]
        barrier.wait()
        t0_box[0] = _now()
        for fut in as_completed(futures):
            results.append(fut.result())
    stages.append({"stage": f"4. {n} concurrent queries", "seconds": round(_now() - t, 3)})
    created_hc_ids.extend(r.hc_id for r in results if r.hc_id)

    print(render_timeline(results))

    per_thread = pd.DataFrame(
        [
            {
                "thread": f"T{r.index:02d}",
                "acquire_ms": round(r.acquire_ms, 1),
                "query_ms": round(r.query_ms, 1),
                "total_ms": round(r.total_ms, 1),
                "finish_offset_ms": round(r.finish_offset_ms, 1),
                "value": r.value,
                "sessionId": r.session_id,
                "error": r.error,
            }
            for r in sorted(results, key=lambda x: x.index)
        ]
    )

    print("\nPer-thread results:")
    print(per_thread.to_string(index=False))

    print("\nStage timings:")
    print(pd.DataFrame(stages).to_string(index=False))

    all_session_ids = {r.session_id for r in results if r.session_id}
    all_session_ids.add(primary_session)
    print("\nUnderlying Livy sessionId(s) used across all queries:")
    for sid in sorted(all_session_ids):
        print(f"  {sid}")

    errors = [r for r in results if r.error]
    single = len(all_session_ids) == 1

    print()
    if single and not errors:
        print(f"\u2705 SINGLE high-concurrency Spark session served all {n} queries. No extra session spun up.")
    elif errors:
        print(f"\u274c {len(errors)} thread(s) failed \u2014 see 'error' column above.")
    if not single:
        print(f"\u274c Expected 1 underlying session, saw {len(all_session_ids)}.")

    print("\nCleaning up HC sessions...")
    for hc_id in dict.fromkeys(created_hc_ids):
        client.delete(hc_id)

    return 0 if (single and not errors) else 1


if __name__ == "__main__":
    raise SystemExit(main())
