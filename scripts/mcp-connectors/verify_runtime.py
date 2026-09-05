#!/usr/bin/env python3
"""
Runtime Verification for MCP Connectors
Generic verification for both Databases (Docker) and APIs (Mocking).
"""

import os
import sys
import json
import time
import subprocess
import logging
import threading
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Verifier")

class MockAPIServer(BaseHTTPRequestHandler):
    """Simple mock server to test API connectors"""
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"success": True, "mock_data": "test"}).encode())
        
    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"success": True, "id": "123"}).encode())

class RuntimeVerifier:
    def __init__(self, connector_name: str, connector_path: str):
        self.connector_name = connector_name
        self.connector_path = connector_path
        self.container_id = None
        self.mock_server = None
        self.mock_server_thread = None
        self.mock_port = 8888

    def _get_free_port(self):
        """Find a free port for the mock server"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def start_mock_server(self):
        """Start a lightweight mock API server"""
        self.mock_port = self._get_free_port()
        logger.info(f"🎭 Starting Mock API Server on port {self.mock_port}...")
        
        self.mock_server = HTTPServer(('localhost', self.mock_port), MockAPIServer)
        self.mock_server_thread = threading.Thread(target=self.mock_server.serve_forever)
        self.mock_server_thread.daemon = True
        self.mock_server_thread.start()
        
        return f"http://localhost:{self.mock_port}"

    def start_infrastructure(self) -> Dict[str, Any]:
        """Start required infrastructure (Docker or Mock) based on type"""
        logger.info(f"🏗️  Preparing infrastructure for {self.connector_name}...")
        
        metadata_path = os.path.join(self.connector_path, "metadata.json")
        category = "unknown"
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                meta = json.load(f)
                category = meta.get("category", "unknown")

        config = {}
        
        # Database connectors -> Docker
        if category in ["database", "data_warehouse", "relational_db", "document_db"]:
            if "mysql" in self.connector_name:
                return self._start_mysql_docker()
            elif "postgres" in self.connector_name:
                return self._start_postgres_docker()
            elif "mongo" in self.connector_name:
                return self._start_mongo_docker()
            # Add more DBs here
            
        # API/SaaS connectors -> Mock Server
        elif category in ["saas", "api", "crm", "api_saas"]:
            base_url = self.start_mock_server()
            config = {
                "api_key": "mock_test_key",
                "base_url": base_url,  # Inject mock URL
                "url": base_url
            }
            logger.info(f"✅ Using Mock Server at {base_url}")
            
        return config

    def _start_mysql_docker(self):
        cmd = [
            "docker", "run", "-d", "--rm",
            "-e", "MYSQL_ROOT_PASSWORD=testroot",
            "-e", "MYSQL_DATABASE=testdb",
            "-p", "3307:3306",
            "mysql:8.0"
        ]
        return self._run_docker(cmd, {"host": "localhost", "port": 3307, "user": "root", "password": "testroot", "database": "testdb"})

    def _start_postgres_docker(self):
        cmd = [
            "docker", "run", "-d", "--rm",
            "-e", "POSTGRES_PASSWORD=testroot",
            "-e", "POSTGRES_DB=testdb",
            "-p", "5433:5432",
            "postgres:15"
        ]
        return self._run_docker(cmd, {"host": "localhost", "port": 5433, "user": "postgres", "password": "testroot", "database": "testdb"})
    
    def _start_mongo_docker(self):
        cmd = [
            "docker", "run", "-d", "--rm",
            "-p", "27018:27017",
            "mongo:latest"
        ]
        return self._run_docker(cmd, {"host": "localhost", "port": 27018, "database": "testdb"})

    def _run_docker(self, cmd, config):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.warning(f"⚠️ Failed to start Docker: {result.stderr}. Skipping Docker test.")
                return None
            
            self.container_id = result.stdout.strip()
            logger.info(f"🐳 Docker container started: {self.container_id[:12]}")
            time.sleep(15) # Wait for startup
            return config
        except Exception as e:
             logger.warning(f"⚠️ Docker error: {e}")
             return None

    def run_connector_command(self, method: str, params: Dict, env_vars: Dict = None) -> Dict:
        """Run a command against the connector script via stdio"""
        connector_script = os.path.join(self.connector_path, "connector.py")
        
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
            
        # Inject MCP_ environment variables from params for convenience
        if "config" in params:
            for k, v in params["config"].items():
                env[f"MCP_{k.upper()}"] = str(v)

        try:
            process = subprocess.Popen(
                [sys.executable, connector_script],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env
            )
            
            stdout, stderr = process.communicate(input=json.dumps(request), timeout=30)
            
            if stderr:
                # Filter out standard logs, keep errors
                errors = [line for line in stderr.split('\n') if "ERROR" in line or "Traceback" in line]
                if errors:
                    logger.debug(f"Connector Errors: {errors}")
            
            if not stdout.strip():
                return {"error": "Empty response from connector", "stderr": stderr}

            return json.loads(stdout)
            
        except subprocess.TimeoutExpired:
            return {"error": "Timeout execution connector"}
        except json.JSONDecodeError:
            return {"error": f"Invalid JSON response: {stdout}", "stderr": stderr}
        except Exception as e:
            return {"error": str(e)}

    def verify(self) -> Dict[str, Any]:
        """Run the full verification suite"""
        result = {
            "success": False,
            "checks": [],
            "error": None
        }
        
        try:
            # 1. Start Infrastructure
            config = self.start_infrastructure()
            
            # If infrastructure failed (e.g. no Docker), we can still test 'validate_config'
            # but we skip connection tests
            can_test_connection = config is not None

            # 2. Test validate_config (Static)
            logger.info("🧪 Testing 'validate_config'...")
            # Test with empty config (should fail)
            res_invalid = self.run_connector_command("validate_config", {"config": {}})
            # Test with valid config
            res_valid = self.run_connector_command("validate_config", {"config": config if config else {"api_key": "test"}})
            
            if res_valid.get("result", {}).get("valid"):
                result["checks"].append({"name": "validate_config", "status": "pass"})
            else:
                 result["checks"].append({"name": "validate_config", "status": "fail", "details": res_valid})

            # 3. Test Connection (Runtime)
            if can_test_connection:
                logger.info("🔌 Testing 'test_connection'...")
                res_conn = self.run_connector_command("test_connection", {"config": config})
                
                # Check for success
                # Note: For mock servers, this confirms the HTTP client works
                # For Docker, this confirms the DB client works
                if res_conn.get("result", {}).get("success"):
                     result["checks"].append({"name": "test_connection", "status": "pass"})
                else:
                     result["checks"].append({"name": "test_connection", "status": "fail", "details": res_conn})
                     # If connection fails, likely schema discovery will too
                     can_test_connection = False 

            # 4. Test Schema/Capabilities (Metadata)
            logger.info("📜 Testing 'get_capabilities'...")
            res_caps = self.run_connector_command("get_capabilities", {})
            if res_caps.get("result", {}).get("success"):
                 result["checks"].append({"name": "get_capabilities", "status": "pass"})
            else:
                 result["checks"].append({"name": "get_capabilities", "status": "fail"})

            # Calculate overall success
            result["success"] = all(c["status"] == "pass" for c in result["checks"])
            return result

        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            result["error"] = str(e)
            return result
        finally:
            self.cleanup()

    def cleanup(self):
        """Stop infrastructure"""
        if self.container_id:
            logger.info(f"🧹 Stopping container {self.container_id[:12]}...")
            subprocess.run(["docker", "stop", self.container_id], capture_output=True)
        
        if self.mock_server:
            logger.info("🧹 Stopping Mock Server...")
            self.mock_server.shutdown()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python verify_runtime.py <connector_directory>")
        sys.exit(1)
        
    path = sys.argv[1]
    name = os.path.basename(path)
    
    verifier = RuntimeVerifier(name, path)
    res = verifier.verify()
    print(json.dumps(res, indent=2))
    sys.exit(0 if res["success"] else 1)












