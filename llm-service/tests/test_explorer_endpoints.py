"""
Tests for Explorer LLM endpoints - table linking, column linking, and SQL generation.
"""

import json
import re
import pytest
from fastapi.testclient import TestClient
from pathlib import Path

# Import the FastAPI app
import sys
# Ensure we import *this repo's* `src/...` package (not any site-packages `src`).
# Add the llm-service root to sys.path so `import src.*` resolves correctly, and
# purge any previously-imported foreign `src` modules (some deps use `src` too).
repo_root = str(Path(__file__).parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
for k in list(sys.modules.keys()):
    if k == "src" or k.startswith("src."):
        del sys.modules[k]
from src.gateway.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


# ---------------------------------------------------------------------------
# Offline LLM stub.
#
# Every endpoint below calls `<client>.chat.completions.create` on a module-level
# AsyncOpenAI client. Unpatched, this file issues REAL /chat/completions requests:
# 13 of its 15 tests fail on "Connection error." with no LLM backend reachable,
# which is why the whole file sat behind an `--ignore` in ci.yml (#784) instead of
# being gated. Faking only the network hop keeps request validation, prompt
# rendering, JSON extraction, response parsing and pydantic coercion under test --
# which is the part that actually regresses.
#
# The three explorer endpoints share ONE client and all three default to the SAME
# model name, so the stub cannot dispatch on `model`. It instead returns a single
# JSON object carrying every key the three handlers read; each handler picks out
# its own subset via result.get(...) and ignores the rest. /api/v1/sql/generate
# uses a different client (gateway `sql_client`), so it gets its own responder.
# ---------------------------------------------------------------------------

_EXPLORER_REPLY = json.dumps({
    # read by /explorer/nl/resolve-tables
    "candidates": [
        {"table": "users", "schema_name": "public", "confidence": 0.93,
         "reason": "Question names users directly"},
        {"table": "orders", "schema_name": "public", "confidence": 0.71,
         "reason": "Joined for per-user order facts"},
    ],
    "confidence_overall": 0.86,
    "needs_hitl": False,
    "hitl_reason": None,
    "suggested_join_keys": ["orders.user_id = users.id"],
    # read by /explorer/nl/resolve-columns
    "columns": {
        "select_cols": ["users.name", "users.email"],
        "where_cols": [],
        "group_by_cols": [],
        "order_by_cols": [],
    },
    "join_plan": [
        {"join_type": "INNER", "left_table": "orders", "right_table": "users",
         "condition": "orders.user_id = users.id"},
    ],
    "confidence": 0.9,
    "ambiguous_columns": None,
    # read by /explorer/nl/next-steps
    "suggestions": [
        {"action_type": "metabase", "title": "Create dashboard",
         "description": "Visualize these results in Metabase",
         "confidence": 0.9, "required_inputs": ["dashboard_name"], "cta": "Create"},
        {"action_type": "download_csv", "title": "Download CSV",
         "description": "Export the result set",
         "confidence": 0.8, "required_inputs": [], "cta": "Download"},
    ],
})


def _explorer_reply(_kwargs):
    return _EXPLORER_REPLY


def _sql_reply(kwargs):
    """Answer /api/v1/sql/generate with SELECT over a table the prompt names.

    The rendered prompt lists the confirmed schema slice as
    `- <schema>.<table> AS <alias>` (prompts/explorer/sql_generate.yaml), so the
    stub echoes back a table the caller actually asked about instead of a
    hardcoded one -- otherwise the dialect/${schema} tests would assert against
    SQL referencing a table absent from their own request.
    """
    text = "\n".join(
        m.get("content", "") for m in (kwargs.get("messages") or []) if isinstance(m, dict)
    )
    m = re.search(r"^\s*-\s+(\S+)\.(\S+)\s+AS\s+", text, re.M)
    table = f"{m.group(1)}.{m.group(2)}" if m else "public.users"
    return json.dumps({
        "sql": f"SELECT * FROM {table} LIMIT 100",
        "explanation": f"Returns rows from {table}.",
        "confidence": 0.9,
        "warnings": [],
    })


class _StubCompletions:
    def __init__(self, responder):
        self._responder = responder

    async def create(self, **kwargs):
        return _StubCompletion(self._responder(kwargs))


class _StubCompletion:
    def __init__(self, content):
        self.choices = [_StubChoice(content)]


class _StubChoice:
    def __init__(self, content):
        self.message = _StubMessage(content)
        self.text = content          # /v1/completions shape (ollama fallback path)


class _StubMessage:
    def __init__(self, content):
        self.content = content


class _StubClient:
    """Stands in for AsyncOpenAI: `.chat.completions.create` and `.completions.create`."""

    def __init__(self, responder):
        self.chat = type("_Chat", (), {"completions": _StubCompletions(responder)})()
        self.completions = _StubCompletions(responder)


@pytest.fixture(autouse=True)
def stub_llm(monkeypatch):
    """Patch both module-level LLM clients for every test in this file.

    monkeypatch (not a global assignment) so the real clients are restored even
    if a test fails -- these modules stay imported for the rest of the session.
    """
    from src.agents.explorer import api as explorer_api
    from src.gateway import main as gateway_main

    monkeypatch.setattr(explorer_api, "explorer_client", _StubClient(_explorer_reply))
    monkeypatch.setattr(gateway_main, "sql_client", _StubClient(_sql_reply))


@pytest.fixture
def sample_schema():
    """Sample database schema for testing."""
    return [
        {
            "name": "users",
            "schema": "public",
            "row_count": 10000,
            "columns": [
                {"name": "id", "type": "integer", "is_primary_key": True},
                {"name": "name", "type": "varchar", "is_primary_key": False},
                {"name": "email", "type": "varchar", "is_primary_key": False},
                {"name": "created_at", "type": "timestamp", "is_primary_key": False},
                {"name": "active", "type": "boolean", "is_primary_key": False},
            ]
        },
        {
            "name": "orders",
            "schema": "public",
            "row_count": 50000,
            "columns": [
                {"name": "id", "type": "integer", "is_primary_key": True},
                {"name": "user_id", "type": "integer", "is_primary_key": False},
                {"name": "total", "type": "decimal", "is_primary_key": False},
                {"name": "created_at", "type": "timestamp", "is_primary_key": False},
                {"name": "status", "type": "varchar", "is_primary_key": False},
            ]
        },
        {
            "name": "products",
            "schema": "public",
            "row_count": 1000,
            "columns": [
                {"name": "id", "type": "integer", "is_primary_key": True},
                {"name": "name", "type": "varchar", "is_primary_key": False},
                {"name": "price", "type": "decimal", "is_primary_key": False},
                {"name": "category", "type": "varchar", "is_primary_key": False},
            ]
        }
    ]


class TestTableLinkEndpoint:
    """Tests for POST /api/v1/explorer/nl/resolve-tables"""
    
    def test_simple_table_match(self, client, sample_schema):
        """Test that a simple question matches the right table."""
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={
                "question": "Show me all users",
                "tables": sample_schema
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "candidates" in data
        assert len(data["candidates"]) > 0
        
        # The users table should be in the candidates
        table_names = [c["table"] for c in data["candidates"]]
        assert "users" in table_names
        
    def test_join_detection(self, client, sample_schema):
        """Test that a join query identifies multiple tables."""
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={
                "question": "Show me orders with user names",
                "tables": sample_schema
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        table_names = [c["table"] for c in data["candidates"]]
        # Should identify both users and orders
        assert "users" in table_names or "orders" in table_names
        
    def test_confidence_scores(self, client, sample_schema):
        """Test that confidence scores are returned."""
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={
                "question": "Count all orders",
                "tables": sample_schema
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        for candidate in data["candidates"]:
            assert "confidence" in candidate
            assert 0 <= candidate["confidence"] <= 1
            
        assert "confidence_overall" in data
        assert 0 <= data["confidence_overall"] <= 1
        
    def test_needs_hitl_flag(self, client, sample_schema):
        """Test that ambiguous questions trigger HITL."""
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={
                "question": "Show me some data",  # Very ambiguous
                "tables": sample_schema
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have needs_hitl field
        assert "needs_hitl" in data
        
    def test_empty_question_handling(self, client, sample_schema):
        """Test handling of empty questions."""
        response = client.post(
            "/api/v1/explorer/nl/resolve-tables",
            json={
                "question": "",
                "tables": sample_schema
            }
        )
        
        # Should either return 400 or return results with low confidence
        assert response.status_code in [200, 400, 422]


class TestColumnLinkEndpoint:
    """Tests for POST /api/v1/explorer/nl/resolve-columns"""
    
    def test_column_selection(self, client, sample_schema):
        """Test that columns are selected for a query."""
        selected_tables = [sample_schema[0]]  # users table
        
        response = client.post(
            "/api/v1/explorer/nl/resolve-columns",
            json={
                "question": "Show me user names and emails",
                "selected_tables": selected_tables
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "columns" in data
        assert "select_cols" in data["columns"] or len(data["columns"]) > 0
        
    def test_join_plan_generation(self, client, sample_schema):
        """Test that join plans are generated for multi-table queries."""
        selected_tables = sample_schema[:2]  # users and orders
        
        response = client.post(
            "/api/v1/explorer/nl/resolve-columns",
            json={
                "question": "Show me orders with user names",
                "selected_tables": selected_tables
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Should have join_plan or detect the need for joins
        assert "join_plan" in data


class TestSQLGenerateEndpoint:
    """Tests for POST /api/v1/sql/generate"""
    
    def test_simple_sql_generation(self, client):
        """Test basic SQL generation."""
        response = client.post(
            "/api/v1/sql/generate",
            json={
                "question": "Count all users",
                "dialect": "postgresql",
                "schema": "users(id, name, email, created_at)"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "sql" in data
        assert data["sql"]  # Should not be empty
        
        # SQL should be SELECT only
        sql_upper = data["sql"].upper()
        assert sql_upper.strip().startswith("SELECT") or sql_upper.strip().startswith("WITH")
        
    def test_sql_safety_instructions(self, client):
        """Test that generated SQL is SELECT-only."""
        response = client.post(
            "/api/v1/sql/generate",
            json={
                "question": "Delete all users",  # Malicious intent
                "dialect": "postgresql",
                "schema": "users(id, name, email)"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Even with malicious intent, should generate SELECT or refuse
        if data.get("sql"):
            sql_upper = data["sql"].upper()
            assert "DELETE" not in sql_upper
            assert "DROP" not in sql_upper
            assert "TRUNCATE" not in sql_upper
            
    def test_dialect_support(self, client):
        """Test that dialect is respected."""
        response = client.post(
            "/api/v1/sql/generate",
            json={
                "question": "Monthly revenue for 2024",
                "dialect": "postgresql",
                "schema": "orders(id, total, created_at)"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "sql" in data


class TestNextStepsEndpoint:
    """Tests for POST /api/v1/explorer/nl/next-steps"""
    
    def test_suggestions_returned(self, client):
        """Test that suggestions are returned."""
        response = client.post(
            "/api/v1/explorer/nl/next-steps",
            json={
                "question": "Show me total revenue by month",
                "sql": "SELECT date_trunc('month', created_at) as month, SUM(total) FROM orders GROUP BY 1",
                "result_profile": {
                    "row_count": 12,
                    "columns": ["month", "sum"]
                }
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "suggestions" in data
        assert len(data["suggestions"]) > 0
        
    def test_suggestion_types(self, client):
        """Test that suggestions are from allowed types."""
        response = client.post(
            "/api/v1/explorer/nl/next-steps",
            json={
                "question": "User list",
                "sql": "SELECT * FROM users LIMIT 100",
                "result_profile": {
                    "row_count": 100,
                    "columns": ["id", "name", "email"]
                }
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        allowed_types = {"metabase", "download_csv", "slack", "email"}
        for suggestion in data["suggestions"]:
            assert suggestion["action_type"] in allowed_types


class TestHealthEndpoint:
    """Test the health check endpoint."""
    
    def test_health(self, client):
        """Test health endpoint returns OK."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"


# Load NL test set if available
def load_nl_test_set():
    """Load the NL test set for parameterized testing."""
    test_file = Path(__file__).parent / "explorer_nl_test_set.json"
    if test_file.exists():
        with open(test_file) as f:
            return json.load(f)
    return None


@pytest.fixture
def nl_test_set():
    """Fixture to provide the NL test set."""
    return load_nl_test_set()


class TestNLTestSet:
    """Tests using the NL test set."""
    
    def test_nl_test_set_exists(self, nl_test_set):
        """Verify the test set exists and is valid."""
        if nl_test_set is None:
            pytest.skip("NL test set not found")
            
        assert "test_cases" in nl_test_set
        assert len(nl_test_set["test_cases"]) > 0
        
    def test_single_table_queries(self, client, nl_test_set, sample_schema):
        """Test single table queries from the test set."""
        if nl_test_set is None:
            pytest.skip("NL test set not found")
            
        single_table_cases = [
            tc for tc in nl_test_set["test_cases"]
            if tc.get("category") == "single_table"
        ]
        
        for case in single_table_cases:
            response = client.post(
                "/api/v1/explorer/nl/resolve-tables",
                json={
                    "question": case["question"],
                    "tables": sample_schema
                }
            )
            
            assert response.status_code == 200, f"Failed for case: {case['id']}"
