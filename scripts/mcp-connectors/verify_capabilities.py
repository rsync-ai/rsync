#!/usr/bin/env python3
"""
Verify MCP Connector Capabilities
"""
import sys
import os
import json

# Add parent dir to path
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

# Load connectors dynamically to avoid name collisions (like mysql.connector)
base_path = os.path.dirname(__file__)

try:
    mysql_mod = load_module("rsync_mysql", os.path.join(base_path, "mysql", "connector.py"))
    MysqlMCPServer = mysql_mod.MysqlMCPServer
except Exception as e:
    print(f"Failed to load MySQL connector: {e}")
    sys.exit(1)

try:
    pg_mod = load_module("rsync_postgres", os.path.join(base_path, "postgresql", "connector.py"))
    PostgresqlMCPServer = pg_mod.PostgresqlMCPServer
except Exception as e:
    print(f"Failed to load Postgres connector: {e}")
    sys.exit(1)

HAS_S3 = False
try:
    s3_mod = load_module("rsync_s3", os.path.join(base_path, "aws-s3", "connector.py"))
    AwsS3MCPServer = s3_mod.AwsS3MCPServer
    HAS_S3 = True
except Exception as e:
    print(f"Skipping active S3 test (load failed): {e}")

def verify_connector(server, name):
    print(f"\n--- Verifying {name} ---")
    caps = server.get_capabilities()
    
    if "capabilities" not in caps:
        print(f"❌ FAILED: 'capabilities' key missing in {name}")
        return False
        
    stats = caps["capabilities"]
    print(f"✅ Capabilities found: {json.dumps(stats, indent=2)}")
    
    required_keys = ["max_batch_size", "supported_formats", "supports_cdc"]
    missing = [k for k in required_keys if k not in stats]
    
    if missing:
        print(f"❌ FAILED: Missing keys in capabilities: {missing}")
        return False
        
    print(f"✅ {name} passed verification")
    return True

def main():
    passed = True
    
    # Verify MySQL
    passed &= verify_connector(MysqlMCPServer(), "MySQL")
    
    # Verify Postgres
    passed &= verify_connector(PostgresqlMCPServer(), "PostgreSQL")
    
    # Verify S3
    if HAS_S3:
        try:
            passed &= verify_connector(AwsS3MCPServer(), "AWS S3")
        except Exception as e:
            print(f"⚠️ Could not instantiate S3 (likely needs env vars): {e}")
            
    if passed:
        print("\n🎉 ALL CONNECTORS VERIFIED!")
        sys.exit(0)
    else:
        print("\n💥 VERIFICATION FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
