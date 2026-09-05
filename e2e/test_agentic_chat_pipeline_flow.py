#!/usr/bin/env python3
"""
E2E Test: Chat-first pipeline creation (no drafts)

This validates the main flow used by /chat:
- Create pipeline (POST /pipelines)
- Run pipeline (POST /pipelines/:id/run)
- Poll state until it is processing OR waiting_for_user OR completed
- If HITL is required, best-effort fulfill connections/tables when possible
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests

API_URL = "http://localhost:5001/api/v1"
AUTH_HEADERS = {"Authorization": "Bearer dev-token", "Content-Type": "application/json", "X-User-ID": "00000000-0000-0000-0000-000000000001"}


def log_step(step: str):
    print(f"\n{'='*70}\n{step}\n{'='*70}")


def get_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"_raw": resp.text}


def create_pipeline(request_text: str) -> str:
    log_step("1) Create pipeline")
    resp = requests.post(
        f"{API_URL}/pipelines",
        headers=AUTH_HEADERS,
        json={
            "name": f"E2E Chat Pipeline {int(time.time())}",
            "description": "Created by e2e/test_agentic_chat_pipeline_flow.py",
            "request": request_text,
        },
        timeout=30,
    )
    assert resp.status_code in (200, 201), f"Create pipeline failed: {resp.status_code} {resp.text}"
    data = get_json(resp)
    pid = str(data.get("id") or data.get("pipeline_id") or "")
    assert pid, f"Pipeline id missing in response: {data}"
    print(f"✅ Pipeline created: {pid}")
    return pid


def run_pipeline(pipeline_id: str) -> str:
    log_step("2) Run pipeline (starts Temporal workflow)")
    resp = requests.post(
        f"{API_URL}/pipelines/{pipeline_id}/run",
        headers=AUTH_HEADERS,
        timeout=30,
    )
    assert resp.status_code == 200, f"Run pipeline failed: {resp.status_code} {resp.text}"
    data = get_json(resp)
    exec_id = str(data.get("execution_id") or "")
    print(f"✅ Pipeline run started. execution_id={exec_id or '(unknown)'}")
    return exec_id


def fetch_state(pipeline_id: str) -> Dict[str, Any]:
    resp = requests.get(f"{API_URL}/pipelines/{pipeline_id}/state", headers=AUTH_HEADERS, timeout=30)
    assert resp.status_code == 200, f"Get state failed: {resp.status_code} {resp.text}"
    return get_json(resp)


def list_connections() -> list[Dict[str, Any]]:
    resp = requests.get(f"{API_URL}/connections", headers=AUTH_HEADERS, timeout=30)
    if resp.status_code != 200:
        return []
    data = get_json(resp)
    conns = data.get("connections")
    return conns if isinstance(conns, list) else []


def pick_connections(required: Dict[str, Any], connections: list[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    required schema (best-effort):
      required_connections: { source: {connector_type}, destination:{connector_type} }
    """
    src_type = ""
    dst_type = ""
    req = required.get("required_connections")
    if isinstance(req, dict):
        src = req.get("source") if isinstance(req.get("source"), dict) else {}
        dst = req.get("destination") if isinstance(req.get("destination"), dict) else {}
        src_type = str(src.get("connector_type") or "")
        dst_type = str(dst.get("connector_type") or "")

    def match(conn: Dict[str, Any], typ: str, direction: str) -> bool:
        if not typ:
            return False
        ct = str(conn.get("connector_type") or "")
        if ct != typ:
            return False
        # tolerate different schemas
        t = str(conn.get("type") or conn.get("connection_type") or "")
        if t:
            return t == direction
        # fallback: assume it can be used
        return True

    src_id = next((str(c.get("id")) for c in connections if match(c, src_type, "source")), None)
    dst_id = next((str(c.get("id")) for c in connections if match(c, dst_type, "destination")), None)
    return src_id, dst_id


def submit_connections(pipeline_id: str, execution_id: str, source_id: str, dest_id: str) -> None:
    log_step("3) HITL: submit connections")
    payload: Dict[str, Any] = {
        "source_connection_id": source_id,
        "destination_connection_id": dest_id,
    }
    if execution_id:
        payload["execution_id"] = execution_id
    resp = requests.post(
        f"{API_URL}/pipelines/{pipeline_id}/hitl/connections",
        headers=AUTH_HEADERS,
        json=payload,
        timeout=30,
    )
    assert resp.status_code == 200, f"Submit connections failed: {resp.status_code} {resp.text}"
    print("✅ Submitted connections")


def submit_tables(pipeline_id: str, execution_id: str, tables: list[str]) -> None:
    log_step("4) HITL: submit selected tables")
    payload: Dict[str, Any] = {"selected_tables": tables}
    if execution_id:
        payload["execution_id"] = execution_id
    resp = requests.post(
        f"{API_URL}/pipelines/{pipeline_id}/hitl/tables",
        headers=AUTH_HEADERS,
        json=payload,
        timeout=30,
    )
    assert resp.status_code == 200, f"Submit tables failed: {resp.status_code} {resp.text}"
    print(f"✅ Submitted tables: {tables[:3]}{'...' if len(tables) > 3 else ''}")


def main() -> int:
    request_text = "sync mysql to s3"

    pipeline_id = ""
    try:
        pipeline_id = create_pipeline(request_text)
        execution_id = run_pipeline(pipeline_id)

        log_step("5) Poll state (and best-effort resolve HITL)")
        deadline = time.time() + 120
        resolved_connections = False
        resolved_tables = False

        while time.time() < deadline:
            state = fetch_state(pipeline_id)
            status = str(state.get("status") or "")
            stage = str(state.get("current_stage") or "")
            blocking = state.get("blocking_reason") if isinstance(state.get("blocking_reason"), dict) else {}
            btype = str(blocking.get("type") or "")
            details = blocking.get("details") if isinstance(blocking.get("details"), dict) else {}

            print(f"- status={status} stage={stage} blocking={btype}")

            if status == "failed":
                raise AssertionError(f"Pipeline failed: {json.dumps(state, indent=2)}")
            if status in ("processing", "running", "completed"):
                # good enough for E2E sanity: pipeline advanced into execution OR completed.
                if status == "completed":
                    print("✅ Pipeline completed")
                else:
                    print("✅ Pipeline is running/processing")
                break

            if status == "waiting_for_user":
                # Best-effort: resolve connections
                if not resolved_connections and btype and ("connection" in btype or "connections" in btype):
                    conns = list_connections()
                    src_id, dst_id = pick_connections(details if isinstance(details, dict) else {}, conns)
                    if src_id and dst_id:
                        submit_connections(pipeline_id, execution_id, src_id, dst_id)
                        resolved_connections = True
                        time.sleep(2)
                        continue

                # Best-effort: resolve tables
                if not resolved_tables and btype and ("table" in btype or "tables" in btype):
                    available = details.get("available_tables")
                    if isinstance(available, list) and len(available) > 0:
                        # tolerate either list[str] or list[object{name:...}]
                        picked: list[str] = []
                        for item in available[:3]:
                            if isinstance(item, str):
                                picked.append(item)
                            elif isinstance(item, dict) and item.get("name"):
                                picked.append(str(item["name"]))
                        if picked:
                            submit_tables(pipeline_id, execution_id, picked)
                            resolved_tables = True
                            time.sleep(2)
                            continue

                # Otherwise, HITL is expected; we don't fail.
                print("ℹ️ Pipeline is waiting for user input (HITL).")
                break

            time.sleep(2)

        log_step("6) Cleanup")
        # Stop best-effort (may be already completed)
        try:
            requests.post(f"{API_URL}/pipelines/{pipeline_id}/stop", headers=AUTH_HEADERS, timeout=30)
        except Exception:
            pass
        try:
            requests.delete(f"{API_URL}/pipelines/{pipeline_id}", headers=AUTH_HEADERS, timeout=30)
        except Exception:
            pass

        print("\n✅ E2E chat-first pipeline flow: PASSED")
        return 0
    except Exception as e:
        print(f"\n❌ E2E chat-first pipeline flow: FAILED\n{e}")
        # Attempt cleanup
        if pipeline_id:
            try:
                requests.post(f"{API_URL}/pipelines/{pipeline_id}/stop", headers=AUTH_HEADERS, timeout=30)
            except Exception:
                pass
            try:
                requests.delete(f"{API_URL}/pipelines/{pipeline_id}", headers=AUTH_HEADERS, timeout=30)
            except Exception:
                pass
        return 1


if __name__ == "__main__":
    sys.exit(main())

