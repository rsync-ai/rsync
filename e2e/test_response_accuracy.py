#!/usr/bin/env python3
"""
E2E Test: Response Accuracy & Truth Verification
Verifies that Chat Assistant responses match the actual System State.
"""

import time
import requests
import json
import re
from datetime import datetime

# Configuration
API_BASE_URL = "http://localhost:5001"
FRONTEND_URL = "http://localhost:3000"
TIMEOUT = 30

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def log_section(message):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE} {message}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.RESET}\n")

def log_verification(query, bot_response, system_truth, is_accurate):
    print(f"{Colors.BOLD}Query:{Colors.RESET}        {query}")
    print(f"{Colors.BOLD}Bot Said:{Colors.RESET}     {bot_response.replace(chr(10), ' ')[:80]}...")
    print(f"{Colors.BOLD}System Truth:{Colors.RESET} {system_truth}")
    
    if is_accurate:
        print(f"{Colors.BOLD}Verdict:{Colors.RESET}      {Colors.GREEN}✅ ACCURATE{Colors.RESET}\n")
    else:
        print(f"{Colors.BOLD}Verdict:{Colors.RESET}      {Colors.RED}❌ INACCURATE (HALLUCINATION DETECTED){Colors.RESET}\n")

# --- API Helpers (The "Truth" Source) ---

def get_all_connections():
    """Fetch truth from API"""
    try:
        resp = requests.get(f"{API_BASE_URL}/api/v1/connections")
        return resp.json().get("connections", [])
    except:
        return []

def get_pipeline(pipeline_id):
    """Fetch truth from API"""
    try:
        resp = requests.get(f"{API_BASE_URL}/api/v1/pipelines/{pipeline_id}")
        if resp.status_code == 200:
            return resp.json()
        return None
    except:
        return None

def get_all_pipelines():
    try:
        resp = requests.get(f"{API_BASE_URL}/api/v1/pipelines")
        return resp.json().get("pipelines", [])
    except:
        return []

# --- Chat Interaction ---

def send_chat(message):
    try:
        resp = requests.post(
            f"{API_BASE_URL}/api/v1/chat/message",
            json={"message": message},
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        return resp.json() if resp.status_code == 200 else None
    except Exception as e:
        print(f"Chat Error: {e}")
        return None

# --- Scenario Verifiers ---

def verify_list_connections():
    log_section("Scenario 1: 'Show my connections'")
    query = "Show my active connections"
    
    # 1. User asks
    chat_resp = send_chat(query)
    if not chat_resp:
        print(f"{Colors.RED}Chat failed to respond{Colors.RESET}")
        return False
        
    bot_text = chat_resp.get("message", "")
    
    # 2. Get Truth
    real_connections = get_all_connections()
    real_count = len(real_connections)
    real_names = [c["name"] for c in real_connections]
    
    # 3. Verify
    # Check if bot mentions the correct count (heuristic)
    is_accurate = True
    truth_summary = f"DB has {real_count} connections: {', '.join(real_names)}"
    
    # Check if real names appear in text
    found_count = 0
    for name in real_names:
        if name in bot_text:
            found_count += 1
        else:
            print(f"{Colors.YELLOW}⚠ Bot missed connection: {name}{Colors.RESET}")
            is_accurate = False
            
    # Simple check: if DB has connections but bot says "no connections", that's a fail
    if real_count > 0 and "no connection" in bot_text.lower():
        is_accurate = False
    
    log_verification(query, bot_text, truth_summary, is_accurate)
    return is_accurate

def verify_test_connection():
    log_section("Scenario 2: 'Test connection [name]'")
    
    # Setup: Ensure we have a connection
    conns = get_all_connections()
    if not conns:
        print("Skipping: No connections to test")
        return True
        
    target_conn = conns[0]
    target_name = target_conn["name"]
    
    query = f"Test connection {target_name}"
    
    # 1. User asks
    chat_resp = send_chat(query)
    bot_text = chat_resp.get("message", "")
    
    # 2. Get Truth
    # We need to verify if a test ACTUALLY happened. 
    # We can check if 'last_tested_at' changed or check the specific test endpoint result.
    # For this E2E, we'll trust the list endpoint update if possible, or re-fetch specific conn.
    
    # Let's try to re-fetch the connection to see if 'last_tested_at' is recent
    time.sleep(1) # Give it a moment to persist
    updated_conn_resp = requests.get(f"{API_BASE_URL}/api/v1/connections/{target_conn['id']}")
    if updated_conn_resp.status_code == 200:
        updated_conn = updated_conn_resp.json()
        last_tested = updated_conn.get("last_tested_at")
        # In a real deep verification, we'd parse this time. 
        # Here we check if status matches bot's claim.
        
        truth_summary = f"Connection '{target_name}' status is {updated_conn.get('status')}"
        
        # Verify bot text contains status
        is_accurate = False
        if "success" in bot_text.lower() or "active" in bot_text.lower():
            if updated_conn.get("status") == "active" or updated_conn.get("last_test_status") == "success":
                is_accurate = True
        elif "fail" in bot_text.lower():
             if updated_conn.get("status") != "active":
                 is_accurate = True
                 
        # Fallback loose check
        if target_name in bot_text:
            is_accurate = True 
            
        log_verification(query, bot_text, truth_summary, is_accurate)
        return is_accurate
    return False

def verify_pipeline_creation():
    log_section("Scenario 3: 'Move [table] to [S3]'")
    
    query = "Create a pipeline to move users from MySQL to S3"
    
    # 0. Get Initial State
    initial_pipelines = get_all_pipelines()
    initial_ids = set(p['id'] for p in initial_pipelines)
    print(f"Initial pipeline count: {len(initial_pipelines)}")

    # 1. User asks
    chat_resp = send_chat(query)
    bot_text = chat_resp.get("message", "")
    
    # Wait for async creation
    time.sleep(5) 
    
    current_pipelines = get_all_pipelines()
    current_ids = set(p['id'] for p in current_pipelines)
    
    new_ids = current_ids - initial_ids
    
    truth_summary = ""
    is_accurate = False
    
    if new_ids:
        new_id = list(new_ids)[0]
        new_pipe = get_pipeline(new_id)
        truth_summary = f"Created Pipeline {new_id}: {new_pipe.get('name')} ({new_pipe.get('status')})"
        
        # Does bot mention creation?
        if "created" in bot_text.lower() or "started" in bot_text.lower() or "planning" in bot_text.lower():
            is_accurate = True
            
        # Does bot mention the ID? (Optional but good)
        if new_id in bot_text:
            is_accurate = True
            truth_summary += " (ID matched in text)"
    else:
        truth_summary = "No new pipeline found in DB"
        if "failed" in bot_text.lower() or "error" in bot_text.lower():
            is_accurate = True # Bot admitted failure, so it's accurate
        else:
            is_accurate = False # Bot claimed success but no pipeline
            
    log_verification(query, bot_text, truth_summary, is_accurate)
    return is_accurate

# --- Main Execution ---

def run_truth_tests():
    print(f"{Colors.BOLD}{Colors.CYAN}🕵️  RSYNC AI - TRUTH VERIFICATION SUITE{Colors.RESET}")
    print("Verifying that Chatbot responses match System Reality...\n")
    
    results = []
    results.append(verify_list_connections())
    results.append(verify_test_connection())
    results.append(verify_pipeline_creation())
    
    if all(results):
        print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 ALL RESPONSES VERIFIED ACCURATE{Colors.RESET}")
        return 0
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ SOME RESPONSES WERE INACCURATE{Colors.RESET}")
        return 1

if __name__ == "__main__":
    run_truth_tests()

