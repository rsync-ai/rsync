#!/usr/bin/env python3
"""
Bulk Connector Regenerator
Regenerates existing connectors using the latest standards and Context7 documentation.

Usage:
    python scripts/regenerate_connectors.py [options]

Options:
    --filter <name>     Only regenerate connectors matching this name
    --only-api-saas     Only target connectors whose metadata category is api_saas
    --delete-first      Delete targeted connector directories before regeneration
    --dry-run           Don't actually regenerate, just list what would be done
    --no-context7       Disable Context7 (use internal knowledge/OpenAI only)
"""

import os
import sys
import argparse
import requests
import time
import json
import shutil
from pathlib import Path
from typing import Optional

# Configuration
TOOL_GENERATOR_URL = os.getenv("TOOL_GENERATOR_URL", "http://localhost:5010")
SHARED_CONNECTORS_DIR = Path(__file__).parent.parent / "shared/mcp-connectors"

def get_existing_connectors():
    """Find all existing connector directories"""
    connectors = []
    if not SHARED_CONNECTORS_DIR.exists():
        print(f"❌ Connectors directory not found: {SHARED_CONNECTORS_DIR}")
        return []
        
    for item in SHARED_CONNECTORS_DIR.iterdir():
        if item.is_dir() and (item / "connector.py").exists():
            connectors.append(item.name)
            
    return sorted(connectors)

def _load_connector_category(connector_dir: Path) -> Optional[str]:
    """Read metadata.json and return connector category (if available)."""
    meta_path = connector_dir / "metadata.json"
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
        return meta.get("category")
    except Exception:
        return None

def regenerate_connector(name: str, use_context7: bool = True, dry_run: bool = False):
    """Call tool-generator (agentic pipeline) to regenerate a connector"""
    print(f"🔄 Regenerating '{name}' {'(Dry Run)' if dry_run else ''}...")
    
    if dry_run:
        return True
        
    try:
        response = requests.post(
            f"{TOOL_GENERATOR_URL}/v1/generate",
            json={
                "api_name": name,
                "description": "",
                "developer_mode": False,
                # Chaos tests are valuable but slow for bulk regen; keep off by default.
                "enable_chaos": False,
                "output_dir": str(SHARED_CONNECTORS_DIR / name),
                # Optional knobs
                "use_context7": use_context7,
            },
            timeout=600  # Context7 + generation can take time
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success") is True:
                print(f"✅ Successfully regenerated '{name}'")
                return True
            else:
                print(f"❌ Failed to regenerate '{name}': {result.get('error_message') or result}")
                return False
        else:
            print(f"❌ HTTP {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error communicating with tool-generator: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Regenerate MCP connectors")
    parser.add_argument("--filter", help="Filter connectors by name")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--no-context7", action="store_true", help="Disable Context7")
    parser.add_argument("--only-api-saas", action="store_true", help="Only target category=api_saas connectors")
    parser.add_argument("--delete-first", action="store_true", help="Delete targeted connectors before regenerating")
    
    args = parser.parse_args()
    
    connectors = get_existing_connectors()
    
    if args.filter:
        connectors = [c for c in connectors if args.filter in c]

    if args.only_api_saas:
        filtered = []
        for name in connectors:
            cat = _load_connector_category(SHARED_CONNECTORS_DIR / name)
            if cat == "api_saas":
                filtered.append(name)
        connectors = filtered
        
    if not connectors:
        print("No connectors found to regenerate.")
        return
        
    print(f"Found {len(connectors)} connectors to regenerate: {', '.join(connectors)}")
    print(f"Mode: {'Context7 Enabled' if not args.no_context7 else 'Context7 Disabled'}")
    if args.only_api_saas:
        print("Filter: category=api_saas")
    if args.delete_first:
        print("Delete: enabled (will remove existing connector directories before regenerating)")
    
    success_count = 0
    for conn in connectors:
        if args.delete_first and not args.dry_run:
            target_dir = SHARED_CONNECTORS_DIR / conn
            print(f"🧹 Deleting connector directory: {target_dir}")
            try:
                shutil.rmtree(target_dir)
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"❌ Failed to delete {target_dir}: {e}")
                continue

        if regenerate_connector(conn, use_context7=not args.no_context7, dry_run=args.dry_run):
            success_count += 1
        time.sleep(1) # Polite delay
            
    print(f"\nSummary: {success_count}/{len(connectors)} regenerated successfully.")

if __name__ == "__main__":
    main()

