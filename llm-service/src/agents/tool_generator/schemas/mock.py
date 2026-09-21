"""
Mock Server Profile Schema

Defines the behavior of the target API for Digital Twin simulation.
Used by the QA Agent to verify generated connectors without hitting live APIs.

Usage:
    profile = MockProfile(
        name="stripe-mock",
        base_url="http://localhost:9999",
        endpoints=[
            MockEndpoint(
                path="/v1/customers",
                method="GET",
                response=MockResponse(status=200, body={"data": [...]}),
            )
        ],
        chaos_config=ChaosConfig(error_rate=0.1, latency_range=(100, 500)),
    )

VERSION: 1.0.0
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Tuple, Union
from pydantic import BaseModel, Field, field_validator
import re
import random


class HttpMethod(str, Enum):
    """HTTP methods"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ResponseType(str, Enum):
    """Types of mock responses"""
    STATIC = "static"          # Fixed response
    TEMPLATE = "template"      # Response with placeholders
    DYNAMIC = "dynamic"        # Generated based on request
    ECHO = "echo"              # Echo back request body
    ERROR = "error"            # Simulate error


class ErrorType(str, Enum):
    """Simulated error types for chaos testing"""
    TIMEOUT = "timeout"
    CONNECTION_REFUSED = "connection_refused"
    MALFORMED_JSON = "malformed_json"
    RATE_LIMITED = "rate_limited"
    AUTH_FAILED = "auth_failed"
    NOT_FOUND = "not_found"
    SERVER_ERROR = "server_error"


class ChaosConfig(BaseModel):
    """
    Configuration for chaos/fault injection during testing.
    
    Enables testing connector robustness against real-world API issues.
    """
    enabled: bool = Field(default=True, description="Whether chaos mode is active")
    
    # Random latency injection
    latency_enabled: bool = Field(default=True)
    latency_range_ms: Tuple[int, int] = Field(
        default=(50, 200),
        description="Range of random latency to add (min_ms, max_ms)"
    )
    
    # Random error injection
    error_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of injecting an error (0.0 to 1.0)"
    )
    error_types: List[ErrorType] = Field(
        default_factory=lambda: [ErrorType.TIMEOUT, ErrorType.SERVER_ERROR],
        description="Types of errors to randomly inject"
    )
    
    # Rate limiting simulation
    rate_limit_enabled: bool = Field(default=False)
    rate_limit_requests: int = Field(default=100, description="Max requests per window")
    rate_limit_window_seconds: int = Field(default=60, description="Rate limit window")
    
    # Malformed response simulation
    malformed_response_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability of returning malformed JSON"
    )
    
    def should_inject_error(self) -> bool:
        """Determine if an error should be injected this request"""
        return self.enabled and random.random() < self.error_rate
    
    def get_random_error(self) -> ErrorType:
        """Get a random error type to inject"""
        return random.choice(self.error_types)
    
    def get_random_latency_ms(self) -> int:
        """Get random latency to inject"""
        if not self.latency_enabled:
            return 0
        return random.randint(self.latency_range_ms[0], self.latency_range_ms[1])
    
    def should_malform_response(self) -> bool:
        """Determine if response should be malformed"""
        return self.enabled and random.random() < self.malformed_response_rate


class MockResponse(BaseModel):
    """Definition of a mock response"""
    status_code: int = Field(default=200, ge=100, le=599)
    headers: Dict[str, str] = Field(default_factory=lambda: {"Content-Type": "application/json"})
    body: Union[Dict[str, Any], List[Any], str, None] = Field(default=None)
    response_type: ResponseType = Field(default=ResponseType.STATIC)
    
    # For template responses - placeholders like {{id}}, {{timestamp}}
    template_vars: Dict[str, Any] = Field(default_factory=dict)
    
    # For dynamic responses - Python expression to evaluate
    generator_expr: Optional[str] = Field(
        default=None,
        description="Python expression to generate response (use with caution)"
    )
    
    # Delay before responding (in addition to chaos latency)
    delay_ms: int = Field(default=0, ge=0, description="Fixed delay before responding")
    
    def render_body(self, request_data: Dict[str, Any] = None) -> Any:
        """Render the response body, applying templates if needed"""
        if self.response_type == ResponseType.STATIC:
            return self.body
        
        if self.response_type == ResponseType.ECHO:
            return request_data or {}
        
        if self.response_type == ResponseType.TEMPLATE and isinstance(self.body, (dict, str)):
            return self._render_template(self.body, request_data)
        
        return self.body
    
    def _render_template(self, template: Any, request_data: Dict[str, Any] = None) -> Any:
        """Simple template rendering with {{var}} placeholders"""
        import json
        from datetime import datetime
        
        # Build context
        context = {
            "timestamp": datetime.now().isoformat(),
            "id": str(random.randint(10000, 99999)),
            **(self.template_vars or {}),
            **(request_data or {}),
        }
        
        if isinstance(template, str):
            result = template
            for key, value in context.items():
                result = result.replace(f"{{{{{key}}}}}", str(value))
            return result
        
        if isinstance(template, dict):
            json_str = json.dumps(template)
            for key, value in context.items():
                json_str = json_str.replace(f"{{{{{key}}}}}", str(value))
            return json.loads(json_str)
        
        return template


class RequestMatcher(BaseModel):
    """Conditions for matching incoming requests"""
    # Path matching
    path_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern for path matching"
    )
    path_params: Dict[str, str] = Field(
        default_factory=dict,
        description="Expected path parameters (e.g., {id: '[0-9]+'})"
    )
    
    # Query parameter matching
    query_params: Dict[str, Optional[str]] = Field(
        default_factory=dict,
        description="Expected query params (None means any value)"
    )
    
    # Header matching
    headers: Dict[str, Optional[str]] = Field(
        default_factory=dict,
        description="Expected headers"
    )
    
    # Body matching
    body_contains: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON fields that must be present in body"
    )
    
    def matches(self, path: str, method: str, headers: Dict, query: Dict, body: Any) -> bool:
        """Check if request matches this matcher"""
        # Path matching
        if self.path_pattern:
            if not re.match(self.path_pattern, path):
                return False
        
        # Query param matching
        for key, expected in self.query_params.items():
            if key not in query:
                return False
            if expected is not None and query[key] != expected:
                return False
        
        # Header matching
        for key, expected in self.headers.items():
            if key.lower() not in {k.lower() for k in headers}:
                return False
            if expected is not None:
                actual = next((v for k, v in headers.items() if k.lower() == key.lower()), None)
                if actual != expected:
                    return False
        
        # Body matching
        if self.body_contains and isinstance(body, dict):
            for key, value in self.body_contains.items():
                if key not in body or body[key] != value:
                    return False
        
        return True


class MockEndpoint(BaseModel):
    """Definition of a single mock endpoint"""
    path: str = Field(..., description="URL path pattern (can include {param} placeholders)")
    methods: List[HttpMethod] = Field(
        default_factory=lambda: [HttpMethod.GET],
        description="Allowed HTTP methods"
    )
    description: str = Field(default="", description="What this endpoint simulates")
    
    # Request matching (optional - for conditional responses)
    matcher: Optional[RequestMatcher] = Field(default=None)
    
    # Response configuration
    response: MockResponse = Field(default_factory=MockResponse)
    
    # Error responses for specific conditions
    error_responses: Dict[str, MockResponse] = Field(
        default_factory=dict,
        description="Error responses keyed by condition (e.g., 'not_found', 'invalid_auth')"
    )
    
    # Validate incoming requests against schema
    request_validation: bool = Field(
        default=False,
        description="Whether to validate incoming request structure"
    )
    expected_request_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON Schema for expected request body"
    )
    
    @field_validator('path')
    @classmethod
    def validate_path(cls, v: str) -> str:
        """Ensure path starts with /"""
        if not v.startswith('/'):
            v = '/' + v
        return v
    
    def get_path_regex(self) -> str:
        """Convert path with {param} to regex pattern"""
        # Convert {param} to named capture groups
        pattern = self.path
        pattern = re.sub(r'\{(\w+)\}', r'(?P<\1>[^/]+)', pattern)
        return f'^{pattern}$'
    
    def matches_path(self, request_path: str) -> Tuple[bool, Dict[str, str]]:
        """Check if request path matches, return captured params"""
        pattern = self.get_path_regex()
        match = re.match(pattern, request_path)
        if match:
            return True, match.groupdict()
        return False, {}


class AuthSimulation(BaseModel):
    """Configuration for simulating authentication"""
    type: str = Field(..., description="Auth type to simulate: 'bearer', 'api_key', 'basic', 'oauth'")
    
    # For bearer/api_key - list of valid tokens
    valid_tokens: List[str] = Field(
        default_factory=lambda: ["test-token-123", "mock-api-key"],
        description="Tokens that will be accepted"
    )
    
    # Header to check for auth
    auth_header: str = Field(default="Authorization")
    
    # Expected prefix (e.g., "Bearer ")
    token_prefix: str = Field(default="Bearer ")
    
    # Response when auth fails
    unauthorized_response: MockResponse = Field(
        default_factory=lambda: MockResponse(
            status_code=401,
            body={"error": "Unauthorized", "message": "Invalid or missing authentication"}
        )
    )

    # Headers that must NOT be present (used to detect duplicate auth methods)
    forbidden_headers: List[str] = Field(
        default_factory=list,
        description="If any of these headers are present, reject the request (e.g., forbid Authorization when using X-API-Key).",
    )

    forbidden_response: MockResponse = Field(
        default_factory=lambda: MockResponse(
            status_code=400,
            body={"error": "Bad Request", "message": "Duplicate or forbidden authentication headers present"}
        )
    )
    
    def validate_request(self, headers: Dict[str, str]) -> Tuple[bool, Optional[MockResponse]]:
        """Validate auth header, return (is_valid, error_response if invalid)"""
        # First, reject forbidden/duplicate auth headers.
        if self.forbidden_headers:
            present = {k.lower() for k in headers.keys()}
            for h in self.forbidden_headers:
                if (h or "").lower() in present:
                    return False, self.forbidden_response

        auth_value = None
        for key, value in headers.items():
            if key.lower() == self.auth_header.lower():
                auth_value = value
                break
        
        if not auth_value:
            return False, self.unauthorized_response
        
        # Strip prefix
        if self.token_prefix and auth_value.startswith(self.token_prefix):
            token = auth_value[len(self.token_prefix):]
        else:
            token = auth_value
        
        if token not in self.valid_tokens:
            return False, self.unauthorized_response
        
        return True, None


class MockProfile(BaseModel):
    """
    Complete profile for a mock API server (Digital Twin).
    
    Defines all endpoints, authentication, and chaos configuration
    needed to simulate a real API for connector testing.
    """
    
    name: str = Field(..., description="Profile identifier")
    description: str = Field(default="", description="What API this simulates")
    base_url: str = Field(
        default="http://localhost:9999",
        description="Base URL where mock server will run"
    )
    
    # Endpoints
    endpoints: List[MockEndpoint] = Field(
        default_factory=list,
        description="List of mock endpoints"
    )
    
    # Default response for unmatched requests
    default_response: MockResponse = Field(
        default_factory=lambda: MockResponse(
            status_code=404,
            body={"error": "Not Found", "message": "Endpoint not found"}
        )
    )
    
    # Authentication simulation
    auth: Optional[AuthSimulation] = Field(default=None)
    
    # Chaos/fault injection
    chaos_config: ChaosConfig = Field(default_factory=ChaosConfig)
    
    # Global headers to add to all responses
    global_response_headers: Dict[str, str] = Field(
        default_factory=lambda: {
            "X-Mock-Server": "true",
            "Content-Type": "application/json",
        }
    )
    
    # Request logging
    log_requests: bool = Field(default=True, description="Log all incoming requests")
    
    # Spec version this profile validates
    spec_version: str = Field(default="1.0")
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.match(r'^[a-z][a-z0-9_-]*$', v):
            raise ValueError(f"Profile name must be lowercase alphanumeric: {v}")
        return v
    
    def find_endpoint(self, path: str, method: str) -> Tuple[Optional[MockEndpoint], Dict[str, str]]:
        """Find matching endpoint for a request"""
        for endpoint in self.endpoints:
            if HttpMethod(method.upper()) not in endpoint.methods:
                continue
            matches, params = endpoint.matches_path(path)
            if matches:
                return endpoint, params
        return None, {}
    
    def get_port(self) -> int:
        """Extract port from base_url"""
        from urllib.parse import urlparse
        parsed = urlparse(self.base_url)
        return parsed.port or 80


# =============================================================================
# FACTORY FUNCTIONS FOR COMMON MOCK PATTERNS
# =============================================================================

def create_database_mock_profile(name: str, tables: List[str] = None) -> MockProfile:
    """Create a mock profile for simulating database connector testing"""
    tables = tables or ["users", "orders"]
    
    # Database connectors typically don't need HTTP mocking
    # but we can simulate metadata/health endpoints
    return MockProfile(
        name=f"{name}-mock",
        description=f"Mock profile for {name} database connector",
        endpoints=[
            MockEndpoint(
                path="/health",
                methods=[HttpMethod.GET],
                response=MockResponse(status_code=200, body={"status": "healthy"}),
            ),
        ],
        chaos_config=ChaosConfig(enabled=False),
    )


def create_api_mock_profile(
    name: str,
    resources: List[Dict[str, Any]] = None,
    auth_type: str = "bearer"
) -> MockProfile:
    """Create a mock profile for API/SaaS connector testing"""
    endpoints = []
    
    # Add endpoints for each resource
    resources = resources or [
        {"name": "contacts", "endpoint": "/crm/v3/objects/contacts"},
        {"name": "companies", "endpoint": "/crm/v3/objects/companies"},
    ]
    
    for resource in resources:
        res_name = resource["name"]
        res_endpoint = resource.get("endpoint", f"/{res_name}")
        
        # List endpoint
        endpoints.append(MockEndpoint(
            path=res_endpoint,
            methods=[HttpMethod.GET],
            description=f"List {res_name}",
            response=MockResponse(
                status_code=200,
                response_type=ResponseType.TEMPLATE,
                body={
                    "results": [
                        {"id": "{{id}}", "name": f"Test {res_name} 1", "created_at": "{{timestamp}}"},
                        {"id": "{{id}}", "name": f"Test {res_name} 2", "created_at": "{{timestamp}}"},
                    ],
                    "paging": {"next": None}
                }
            ),
        ))
        
        # Get single endpoint
        endpoints.append(MockEndpoint(
            path=f"{res_endpoint}/{{id}}",
            methods=[HttpMethod.GET],
            description=f"Get single {res_name}",
            response=MockResponse(
                status_code=200,
                response_type=ResponseType.TEMPLATE,
                body={"id": "{{id}}", "name": f"Test {res_name}", "created_at": "{{timestamp}}"}
            ),
        ))
    
    return MockProfile(
        name=f"{name}-mock",
        description=f"Mock profile for {name} API connector",
        endpoints=endpoints,
        auth=AuthSimulation(
            type=auth_type,
            valid_tokens=["test-token-123", "mock-api-key"],
        ),
        chaos_config=ChaosConfig(
            enabled=True,
            error_rate=0.05,  # 5% chance of random errors
            latency_range_ms=(50, 200),
        ),
    )


def create_storage_mock_profile(name: str, bucket: str = "test-bucket") -> MockProfile:
    """Create a mock profile for cloud storage connector testing"""
    return MockProfile(
        name=f"{name}-mock",
        description=f"Mock profile for {name} storage connector",
        endpoints=[
            # List buckets
            MockEndpoint(
                path="/",
                methods=[HttpMethod.GET],
                response=MockResponse(
                    status_code=200,
                    body={"buckets": [{"name": bucket, "creation_date": "2024-01-01"}]}
                ),
            ),
            # List objects in bucket
            MockEndpoint(
                path=f"/{bucket}",
                methods=[HttpMethod.GET],
                response=MockResponse(
                    status_code=200,
                    body={
                        "contents": [
                            {"key": "data/file1.json", "size": 1024, "last_modified": "2024-01-01"},
                            {"key": "data/file2.json", "size": 2048, "last_modified": "2024-01-02"},
                        ]
                    }
                ),
            ),
            # Get object
            MockEndpoint(
                path=f"/{bucket}/{{key}}",
                methods=[HttpMethod.GET],
                response=MockResponse(
                    status_code=200,
                    body={"data": [{"id": 1, "value": "test"}]}
                ),
            ),
            # Put object
            MockEndpoint(
                path=f"/{bucket}/{{key}}",
                methods=[HttpMethod.PUT],
                response=MockResponse(
                    status_code=200,
                    response_type=ResponseType.TEMPLATE,
                    body={"etag": "\"{{id}}\"", "key": "{{key}}"}
                ),
            ),
        ],
        auth=AuthSimulation(
            type="aws_sig",
            valid_tokens=["test-access-key"],
            auth_header="Authorization",
        ),
        chaos_config=ChaosConfig(enabled=True, error_rate=0.02),
    )

