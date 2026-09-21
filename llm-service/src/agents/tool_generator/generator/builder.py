"""
Connector Builder

Renders Jinja2 templates to generate connector code.
This is a deterministic process - no LLM involved.

Usage:
    from generator.builder import ConnectorBuilder
    
    builder = ConnectorBuilder()
    result = builder.build(spec)
    
    # result.code contains connector.py
    # result.metadata contains metadata.json
    # result.requirements contains requirements.txt

VERSION: 1.0.0
"""

import os
import ast
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, TemplateError, select_autoescape

# Import validation package
try:
    from validation import PreGenerationValidator, PostGenerationValidator
    from validation.models import ValidationResult
    from validation.security_lint import lint_connector_code, sanitize_spec_for_dockerfile
except ImportError:
    from ..validation import PreGenerationValidator, PostGenerationValidator
    from ..validation.models import ValidationResult
    from ..validation.security_lint import lint_connector_code, sanitize_spec_for_dockerfile

# JSON literals (`true/false/null`) sometimes leak into generated Python code via
# embedded API examples. They parse but crash at runtime (NameError). Sanitize
# them deterministically before QA.
try:
    from ..contracts.python_literal_sanitizer import sanitize_python_json_literals
except Exception:
    from contracts.python_literal_sanitizer import sanitize_python_json_literals

def _get_logo_downloader():
    """
    Lazily import logo downloader to handle various import contexts.
    Returns the download_logo function or None if unavailable.
    """
    import sys
    
    # Try direct import first
    try:
        from utils.logo_downloader import download_logo
        return download_logo
    except ImportError:
        pass
    
    # Try with computed path
    try:
        src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)
        from utils.logo_downloader import download_logo
        return download_logo
    except ImportError:
        pass
    
    # Try with absolute Docker path
    try:
        if '/app/src' not in sys.path:
            sys.path.insert(0, '/app/src')
        from utils.logo_downloader import download_logo
        return download_logo
    except ImportError:
        pass
    
    return None

# Import schemas (package-first to avoid duplicate class definitions)
import sys
try:
    from src.agents.tool_generator.schemas.spec import ConnectorSpec, OperationType
except Exception:
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from schemas.spec import ConnectorSpec, OperationType


logger = logging.getLogger(__name__)

# Template directory
TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")


@dataclass
class GeneratedConnector:
    """Container for all generated connector artifacts"""
    name: str
    code: str                           # connector.py content
    metadata: str                       # metadata.json content
    requirements: str                   # requirements.txt content
    dockerfile: Optional[str] = None    # Dockerfile content (optional)
    
    # Validation results
    is_valid: bool = True
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    # Generation metadata
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    spec_version: str = "1.0"
    generator_version: str = "1.0.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "code": self.code,
            "metadata": self.metadata,
            "requirements": self.requirements,
            "dockerfile": self.dockerfile,
            "is_valid": self.is_valid,
            "validation_errors": self.validation_errors,
            "validation_warnings": self.validation_warnings,
            "generated_at": self.generated_at,
            "spec_version": self.spec_version,
        }
    
    def save_to_directory(self, output_dir: str) -> Dict[str, str]:
        """
        Save all artifacts to a directory.

        Refuses to write when the GeneratedConnector is not valid OR has
        an empty code body. Both conditions are how ConnectorBuilder.build()
        signals a hard fail (security lint reject, manifest validation
        reject, template error). Without this guard, callers that don't
        explicitly check is_valid (session_fast_path, orchestrator, qa)
        would silently write empty connector.py / metadata.json to disk.

        Returns dict mapping filename -> filepath.
        """
        if not self.is_valid:
            raise ValueError(
                "Refusing to persist invalid connector "
                f"({self.name}): {self.validation_errors!r}"
            )
        if not self.code or not self.code.strip():
            raise ValueError(
                f"Refusing to persist empty connector body ({self.name})"
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        files = {}

        # Save connector.py
        connector_path = output_path / "connector.py"
        connector_path.write_text(self.code)
        files["connector.py"] = str(connector_path)
        
        # Save metadata.json
        metadata_path = output_path / "metadata.json"
        metadata_path.write_text(self.metadata)
        files["metadata.json"] = str(metadata_path)
        
        # Save requirements.txt
        requirements_path = output_path / "requirements.txt"
        requirements_path.write_text(self.requirements)
        files["requirements.txt"] = str(requirements_path)
        
        # Save Dockerfile if present
        if self.dockerfile:
            dockerfile_path = output_path / "Dockerfile"
            dockerfile_path.write_text(self.dockerfile)
            files["Dockerfile"] = str(dockerfile_path)
        
        # Download logo for the connector
        logo_downloader = _get_logo_downloader()
        if logo_downloader is not None:
            try:
                # Extract connector name from metadata
                metadata_dict = json.loads(self.metadata)
                connector_name = (
                    metadata_dict.get("id")
                    or metadata_dict.get("connector_type")
                    or metadata_dict.get("name", "")
                )
                
                if connector_name:
                    logger.info(f"🎨 Downloading logo for {connector_name}...")
                    success, logo_filename = logo_downloader(connector_name, str(output_path))
                    if success:
                        logo_path = output_path / logo_filename
                        files[logo_filename] = str(logo_path)
                        logger.info(f"✅ Logo downloaded: {logo_filename}")
                    else:
                        logger.warning(f"⚠️ Could not download logo for {connector_name}")
            except Exception as e:
                logger.warning(f"⚠️ Logo download failed: {e}")
        else:
            logger.warning("⚠️ Logo downloader not available")
        
        logger.info(f"Saved connector artifacts to {output_dir}")
        return files


class ConnectorBuilder:
    """
    Builds MCP connectors from specs using Jinja2 templates.
    
    This is the core of the "Constitution" approach:
    - Templates define the structure (hardcoded, tested)
    - Specs provide the configuration (dynamic, from agents)
    - No LLM involved in code generation
    """
    
    def __init__(self, templates_dir: str = None):
        """
        Initialize the builder.
        
        Args:
            templates_dir: Path to templates directory. Defaults to ./templates/
        """
        self.templates_dir = templates_dir or TEMPLATES_DIR
        
        # Initialize Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(self.templates_dir),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )
        
        # Add custom filters
        self._register_filters()
        
        logger.info(f"ConnectorBuilder initialized (templates: {self.templates_dir})")
    
    def _register_filters(self):
        """Register custom Jinja2 filters"""
        
        def to_snake_case(value: str) -> str:
            """
            Convert arbitrary strings to a safe snake_case identifier.

            IMPORTANT: This is used to generate Python method names from resource/action names.
            It must always return a valid identifier matching: ^[a-z][a-z0-9_]*$
            """
            import re
            s = str(value or "")
            s = s.strip()
            if not s:
                return "op"

            # Convert CamelCase boundaries first
            s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
            s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)

            # Normalize separators and remove query/fragment patterns
            s2 = s2.replace("-", "_").replace(" ", "_").replace("/", "_")

            # Replace any non-alphanumeric/underscore with underscore
            s2 = re.sub(r"[^a-zA-Z0-9_]+", "_", s2)

            # Collapse multiple underscores and trim
            s2 = re.sub(r"_+", "_", s2).strip("_")
            s2 = s2.lower()

            # Ensure starts with a letter
            if not s2 or not re.match(r"^[a-z]", s2):
                s2 = f"op_{s2}" if s2 else "op"

            # Final safety: strip any remaining invalid chars
            s2 = re.sub(r"[^a-z0-9_]+", "_", s2)
            s2 = re.sub(r"_+", "_", s2).strip("_")
            if not s2:
                s2 = "op"
            if not re.match(r"^[a-z][a-z0-9_]*$", s2):
                # As last resort, hard-normalize
                s2 = re.sub(r"^[^a-z]+", "op_", s2)
                s2 = re.sub(r"[^a-z0-9_]+", "_", s2)
                s2 = re.sub(r"_+", "_", s2).strip("_")
                if not s2:
                    s2 = "op"
            return s2
        
        def to_pascal_case(value: str) -> str:
            """Convert string to PascalCase"""
            return ''.join(word.capitalize() for word in value.replace('-', '_').split('_'))
        
        def singularize(value: str) -> str:
            """Simple singularization (removes trailing 's')"""
            if value.endswith('ies'):
                return value[:-3] + 'y'
            if value.endswith('s') and not value.endswith('ss'):
                return value[:-1]
            return value
        
        self.env.filters['snake_case'] = to_snake_case
        self.env.filters['pascal_case'] = to_pascal_case
        self.env.filters['singularize'] = singularize
    
    def _select_connector_template(self, spec) -> str:
        """
        Select appropriate connector template based on protocol, category, and type.

        Routing precedence:
          1. protocol == GRAPHQL                → connector_graphql.py.j2
          2. category in database_categories    → connector_database.py.j2
          3. fallback                           → connector.py.j2
        """
        connector_type = spec.connector_type.lower()
        category = spec.category.value if hasattr(spec.category, 'value') else str(spec.category)
        protocol = getattr(spec, "protocol", None)
        protocol_value = (
            protocol.value if hasattr(protocol, "value")
            else (str(protocol).lower() if protocol else "rest")
        )

        logger.info(f"Template selection: connector_type={connector_type}, category={category}, protocol={protocol_value}")

        # GraphQL connectors get their own lean template — REST/DB code paths
        # don't apply (single endpoint, JSON body, errors[] envelope).
        if protocol_value == "graphql":
            logger.info(f"Using GraphQL template connector_graphql.py.j2 for {connector_type}")
            return 'connector_graphql.py.j2'

        # All database categories use the generic database template
        database_categories = [
            'relational_db', 'RELATIONAL_DB',
            'document_db', 'DOCUMENT_DB',
            'data_warehouse', 'DATA_WAREHOUSE',
            'key_value', 'KEY_VALUE'
        ]

        if category in database_categories:
            logger.info(f"Using generic database template for {connector_type} ({category})")
            return 'connector_database.py.j2'

        # API/SaaS and other connectors use the standard template
        logger.info(f"Using standard template connector.py.j2 for {connector_type} ({category})")
        return 'connector.py.j2'
    
    def build(self, spec: ConnectorSpec, include_dockerfile: bool = True) -> GeneratedConnector:
        """
        Build a complete connector from a spec.

        Args:
            spec: The connector specification
            include_dockerfile: Whether to render Dockerfile.j2. Default True
                because every callable persistence path needs a Dockerfile —
                without one the compose generator silently skips the connector
                and runtime deploy fails with a build-context error. Callers
                that genuinely don't need a Dockerfile (e.g. in-process QA
                that only inspects connector.py) pass False explicitly.

        Returns:
            GeneratedConnector with all artifacts
        """
        logger.info(f"Building connector: {spec.name}")
        
        # Run pre-generation validation
        pre_validator = PreGenerationValidator()
        pre_validation = pre_validator.validate(spec)
        
        if pre_validation.has_errors:
            logger.error(f"Pre-generation validation failed: {len(pre_validation.errors)} errors")
            return GeneratedConnector(
                name=spec.name,
                code="",
                metadata="",
                requirements="",
                is_valid=False,
                validation_errors=[str(e) for e in pre_validation.errors],
                validation_warnings=[str(w) for w in pre_validation.warnings],
            )
        
        if pre_validation.has_warnings:
            logger.warning(f"Pre-generation validation warnings: {len(pre_validation.warnings)}")
        
        # Log pre-generation warnings (but don't block)
        validation_errors = [str(e) for e in pre_validation.errors]
        validation_warnings = [str(w) for w in pre_validation.warnings]
        
        # Generate timestamp for all templates
        generation_timestamp = datetime.now().isoformat()
        
        # Build template context
        context = {
            "spec": spec,
            "generation_timestamp": generation_timestamp,
        }
        
        # DEBUG: Log spec details for troubleshooting
        logger.info(f"Spec category: {spec.category} (value: {spec.category.value if hasattr(spec.category, 'value') else 'N/A'})")
        logger.info(f"Spec supports_source: {spec.supports_source}")
        logger.info(f"Spec supports_destination: {spec.supports_destination}")
        
        # Select appropriate template for the connector type
        connector_template = self._select_connector_template(spec)
        logger.info(f"Using template: {connector_template}")
        
        # Render all templates
        try:
            code = self._render_template(connector_template, context)
            code = sanitize_python_json_literals(code)
            metadata = self._render_template("metadata.json.j2", context)
            requirements = self._render_template("requirements.txt.j2", context)
            dockerfile = None
            if include_dockerfile:
                # Block the build if any Dockerfile-bound spec field has a
                # newline / quote / metacharacter that would let an LLM-
                # controlled string inject extra Dockerfile instructions.
                # Done BEFORE Jinja2 render so a rejected spec never has
                # the chance to produce a malicious build context.
                dockerfile_findings = sanitize_spec_for_dockerfile(spec)
                if dockerfile_findings:
                    msgs = [str(f) for f in dockerfile_findings]
                    logger.error(f"Dockerfile field validation failed: {msgs}")
                    return GeneratedConnector(
                        name=spec.name,
                        code="",
                        metadata="",
                        requirements="",
                        is_valid=False,
                        validation_errors=msgs,
                    )
                dockerfile = self._render_template("Dockerfile.j2", context)
        except TemplateError as e:
            logger.error(f"Template rendering failed: {e}")
            return GeneratedConnector(
                name=spec.name,
                code="",
                metadata="",
                requirements="",
                is_valid=False,
                validation_errors=[f"Template error: {str(e)}"],
            )

        # Security lint of connector.py — fail closed on anything that would
        # give the runtime container arbitrary code execution beyond what the
        # connector contract requires (os.system, subprocess shell=True,
        # eval/exec/compile, dynamic __import__). The connector receives the
        # user's data-source credentials, so a malicious code path here is a
        # direct exfiltration vector.
        security_findings = lint_connector_code(code)
        if security_findings:
            msgs = [str(f) for f in security_findings]
            logger.error(f"Connector security lint failed: {msgs}")
            return GeneratedConnector(
                name=spec.name,
                code="",
                metadata="",
                requirements="",
                is_valid=False,
                validation_errors=msgs,
            )

        # Hard-fail metadata validation. `validate_metadata` (defined below
        # in this file) used to exist but was never called from `build()`,
        # so malformed metadata.json could persist with is_draft=True and
        # downstream consumers (api-gateway, server-manager) either crashed
        # or silently ignored fields depending on their tolerance. Refuse
        # to persist a connector whose manifest fails the schema check —
        # callers can still see the warnings further down for non-fatal
        # lints.
        metadata_errors, metadata_warnings = validate_metadata(metadata)
        if metadata_errors:
            prefixed = [f"metadata: {m}" for m in metadata_errors]
            logger.error(f"Connector metadata validation failed: {prefixed}")
            return GeneratedConnector(
                name=spec.name,
                code="",
                metadata="",
                requirements="",
                is_valid=False,
                validation_errors=prefixed,
            )

        # Run post-generation validation with new category-aware validators
        post_validator = PostGenerationValidator()
        post_validation = post_validator.validate(code, spec)
        
        # Convert validation results to lists
        code_errors = [str(e) for e in post_validation.errors]
        code_warnings = [str(w) for w in post_validation.warnings]
        
        # Also run legacy validation for backward compatibility (warn-only)
        legacy_errors, legacy_warnings = validate_generated_code(code, spec)
        if legacy_errors:
            logger.warning(f"Legacy validator found additional errors: {legacy_errors}")
        
        # Create result. Manifest warnings (best-effort lints that did not
        # cause a hard fail above) surface here so callers see them even on
        # a successful build.
        result = GeneratedConnector(
            name=spec.name,
            code=code,
            metadata=metadata,
            requirements=requirements,
            dockerfile=dockerfile,
            is_valid=len(code_errors) == 0,
            validation_errors=validation_errors + code_errors,
            validation_warnings=validation_warnings + code_warnings + [f"metadata: {w}" for w in metadata_warnings],
            spec_version=spec.spec_version,
        )
        
        if result.is_valid:
            logger.info(f"✅ Successfully built connector: {spec.name}")
        else:
            logger.error(f"❌ Connector build failed: {spec.name} - {result.validation_errors}")
        
        return result
    
    def _render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render a single template"""
        template = self.env.get_template(template_name)
        return template.render(**context)
    
    def _validate_spec(self, spec: ConnectorSpec) -> List[str]:
        """
        Validate spec against requirements.
        
        Returns list of error/warning messages.
        """
        errors = []
        
        # Check required operations
        operation_names = {op.name for op in spec.operations}
        required = {'test_connection', 'validate_config', 'discover_schema', 'get_capabilities'}
        missing = required - operation_names
        
        if missing:
            errors.append(f"Missing required operations: {missing}")
        
        # Check category-specific requirements
        if spec.category.value == 'api_saas':
            if not spec.base_url:
                errors.append("API connectors require base_url")
        
        # Check auth configuration
        if spec.auth.type.value == 'oauth2':
            if not spec.auth.oauth_provider:
                errors.append("OAuth2 auth requires oauth_provider")
        
        return errors
    
    def build_from_dict(self, spec_dict: Dict[str, Any], **kwargs) -> GeneratedConnector:
        """
        Build from a dictionary (e.g., loaded from JSON/YAML).
        
        Args:
            spec_dict: Spec as dictionary
            **kwargs: Additional arguments passed to build()
        
        Returns:
            GeneratedConnector
        """
        spec = ConnectorSpec(**spec_dict)
        return self.build(spec, **kwargs)


# =============================================================================
# STANDALONE FUNCTIONS
# =============================================================================

def generate_connector_code(spec: ConnectorSpec) -> str:
    """
    Generate connector.py code from spec.
    
    This is the main function for code generation.
    Returns the Python code as a string.
    """
    builder = ConnectorBuilder()
    result = builder.build(spec)
    return result.code


def generate_metadata(spec: ConnectorSpec) -> str:
    """Generate metadata.json from spec"""
    builder = ConnectorBuilder()
    context = {
        "spec": spec,
        "generation_timestamp": datetime.now().isoformat(),
    }
    return builder._render_template("metadata.json.j2", context)


def generate_requirements(spec: ConnectorSpec) -> str:
    """Generate requirements.txt from spec"""
    builder = ConnectorBuilder()
    context = {
        "spec": spec,
        "generation_timestamp": datetime.now().isoformat(),
    }
    return builder._render_template("requirements.txt.j2", context)


def generate_dockerfile(spec: ConnectorSpec) -> str:
    """Generate Dockerfile from spec"""
    builder = ConnectorBuilder()
    context = {
        "spec": spec,
        "generation_timestamp": datetime.now().isoformat(),
    }
    return builder._render_template("Dockerfile.j2", context)


def validate_generated_code(code: str, spec: Optional[ConnectorSpec] = None) -> Tuple[List[str], List[str]]:
    """
    Validate generated Python code with category-aware checks.
    
    Performs:
    1. Syntax validation (ast.parse)
    2. Structure validation (required methods present)
    3. Import validation
    4. Category-aware stub detection (prevents AWS S3 class failures)
    5. I/O operation validation for destinations
    
    Args:
        code: Generated connector code
        spec: Optional ConnectorSpec for category-aware validation
    
    Returns:
        (errors, warnings) tuple of lists
    """
    errors = []
    warnings = []
    
    # 1. Syntax validation
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        errors.append(f"Syntax error at line {e.lineno}: {e.msg}")
        return errors, warnings
    
    # 2. Find class definition
    class_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith('MCPServer'):
            class_def = node
            break
    
    if not class_def:
        errors.append("No MCPServer class found in generated code")
        return errors, warnings
    
    # 3. Check required methods
    methods = {node.name for node in class_def.body if isinstance(node, ast.FunctionDef)}
    required_methods = {'__init__', 'get_capabilities', 'validate_config', 'test_connection', 'discover_schema'}
    missing = required_methods - methods
    
    if missing:
        errors.append(f"Missing required methods: {missing}")
    
    # 3a. Check CRITICAL abstract methods from BaseMCPConnector
    abstract_methods = {'export', 'import_data'}
    missing_abstract = abstract_methods - methods
    
    if missing_abstract:
        errors.append(f"CRITICAL: Missing abstract methods required by BaseMCPConnector: {missing_abstract}")
        errors.append("These methods MUST be implemented in every connector, even if they return an error.")
    
    # Warn if export or import_data are missing (even if not in class body, check in code string)
    if 'export' not in methods and 'def export(' not in code:
        errors.append("CRITICAL: export() method not found in generated code")
    if 'import_data' not in methods and 'def import_data(' not in code:
        errors.append("CRITICAL: import_data() method not found in generated code")
    
    # 3b. CATEGORY-AWARE STUB DETECTION (prevents AWS S3 class failures)
    if spec:
        category = spec.category.value if hasattr(spec.category, 'value') else str(spec.category)
        
        # Check destination connectors for stub implementations
        if spec.supports_destination:
            import_data_method = next((n for n in class_def.body if isinstance(n, ast.FunctionDef) and n.name == 'import_data'), None)
            
            if import_data_method:
                method_code = ast.unparse(import_data_method)
                method_code_lower = method_code.lower()
                
                # HARD FAIL: Stub patterns that indicate non-functional code
                stub_patterns = [
                    ('TODO', 'TODO comment found in import_data'),
                    ('NotImplementedError', 'NotImplementedError raised in import_data'),
                    ('raise NotImplemented', 'NotImplemented error in import_data'),
                ]
                
                for pattern, error_msg in stub_patterns:
                    if pattern in method_code:
                        errors.append(f"STUB DETECTED: {error_msg} - connector will not work")
                
                # HARD FAIL: Success without doing work (AWS S3 class failure)
                # Check if method returns success but has no I/O operations
                has_io_operation = False
                
                # Category-specific I/O operation checks
                if category in ['cloud_storage', 'CLOUD_STORAGE']:
                    # Must have S3/cloud storage write operations
                    s3_operations = [
                        'put_object', 'upload_file', 'upload_fileobj', 'upload_part',
                        'create_multipart_upload', 's3.put', 's3.upload',
                        'blob.upload', 'bucket.upload', 'client.upload'
                    ]
                    has_io_operation = any(op in method_code_lower for op in s3_operations)
                    
                    if not has_io_operation:
                        errors.append(
                            f"CRITICAL: Cloud storage connector import_data must include actual upload operations "
                            f"(e.g., put_object, upload_file, upload_fileobj). Found none."
                        )
                
                elif category in ['relational_db', 'RELATIONAL_DB', 'document_db', 'DOCUMENT_DB', 'data_warehouse', 'DATA_WAREHOUSE']:
                    # Must have database write operations
                    db_operations = [
                        'execute(', 'executemany(', 'commit()', 'insert_many', 'insert_one',
                        'bulk_write', 'bulk_insert', 'cursor.execute', 'session.execute',
                        'connection.execute', 'db.execute'
                    ]
                    has_io_operation = any(op in method_code_lower for op in db_operations)
                    
                    if not has_io_operation:
                        errors.append(
                            f"CRITICAL: Database connector import_data must include actual write operations "
                            f"(e.g., execute, executemany, commit, insert). Found none."
                        )

                elif category in ['api_saas', 'API_SAAS']:
                    # Must have HTTP write operations (POST/PUT/PATCH/DELETE) or helper that performs them.
                    api_write_ops = [
                        # Common helper in our generated connectors
                        '_make_request', 'self._make_request',
                        # requests/httpx usage
                        'requests.', 'httpx.',
                        # direct verbs
                        '.post(', '.put(', '.patch(', '.delete(',
                        'request(',
                    ]
                    has_io_operation = any(op in method_code_lower for op in api_write_ops)

                    if not has_io_operation:
                        errors.append(
                            f"CRITICAL: API/SaaS connector import_data must include actual HTTP write operations "
                            f"(e.g., POST/PUT/PATCH/DELETE via requests/httpx). Found none."
                        )
                
                # Check for "return success without doing work" pattern
                # This catches cases where method returns {"success": True} immediately
                has_early_return_success = False
                for node in ast.walk(import_data_method):
                    if isinstance(node, ast.Return) and node.value:
                        if isinstance(node.value, ast.Dict):
                            # Check if it's a dict with "success": True
                            for key, val in zip(node.value.keys, node.value.values):
                                if isinstance(key, ast.Constant) and key.value == 'success':
                                    if isinstance(val, ast.Constant) and val.value is True:
                                        # Check if this return is NOT in an if/try block (early return)
                                        # For simplicity, if we find this pattern and no I/O, it's suspicious
                                        has_early_return_success = True
                
                if has_early_return_success and not has_io_operation:
                    errors.append(
                        f"CRITICAL STUB: import_data returns success=True but performs no I/O operations. "
                        f"This is the AWS S3 stub pattern that caused production failures."
                    )
        
        # Check source connectors for stub implementations
        if spec.supports_source:
            export_method = next((n for n in class_def.body if isinstance(n, ast.FunctionDef) and n.name == 'export'), None)
            
            if export_method:
                method_code = ast.unparse(export_method)
                
                # Check for stub patterns
                if 'TODO' in method_code:
                    errors.append(f"STUB DETECTED: TODO comment found in export - connector will not work")
                if 'NotImplementedError' in method_code:
                    errors.append(f"STUB DETECTED: NotImplementedError in export - connector will not work")
    
    # 4. Check for BaseMCPConnector inheritance
    base_classes = []
    for base in class_def.bases:
        if isinstance(base, ast.Name):
            base_classes.append(base.id)
        elif isinstance(base, ast.Attribute):
            base_classes.append(base.attr)
    
    if 'BaseMCPConnector' not in base_classes:
        warnings.append("Class does not inherit from BaseMCPConnector")
    
    # 5. Check __init__ calls super()
    init_method = next((n for n in class_def.body if isinstance(n, ast.FunctionDef) and n.name == '__init__'), None)
    if init_method:
        has_super_call = False
        for node in ast.walk(init_method):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and node.func.attr == '__init__':
                    if isinstance(node.func.value, ast.Call):
                        if isinstance(node.func.value.func, ast.Name) and node.func.value.func.id == 'super':
                            has_super_call = True
        if not has_super_call:
            warnings.append("__init__ may not call super().__init__()")
    
    # 6. Check for common anti-patterns
    code_lower = code.lower()
    if 'eval(' in code_lower or 'exec(' in code_lower:
        warnings.append("Code contains eval() or exec() - potential security risk")
    
    if 'password' in code and 'os.getenv' not in code:
        warnings.append("Password may be hardcoded instead of using environment variables")
    
    return errors, warnings


def validate_metadata(metadata_str: str) -> Tuple[List[str], List[str]]:
    """
    Validate generated metadata.json.
    
    Returns:
        (errors, warnings) tuple of lists
    """
    errors = []
    warnings = []
    
    try:
        metadata = json.loads(metadata_str)
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON: {e}")
        return errors, warnings
    
    # Required fields
    required = ['name', 'version', 'connector_type', 'category', 'operations']
    for field in required:
        if field not in metadata:
            errors.append(f"Missing required field: {field}")
    
    # Validate operations
    if 'operations' in metadata:
        for op in metadata['operations']:
            if 'name' not in op:
                errors.append("Operation missing 'name' field")
            if 'type' not in op:
                warnings.append(f"Operation '{op.get('name', 'unknown')}' missing 'type' field")
    
    return errors, warnings


# =============================================================================
# BATCH GENERATION
# =============================================================================

def build_connectors_batch(specs: List[ConnectorSpec], output_dir: str) -> Dict[str, GeneratedConnector]:
    """
    Build multiple connectors in batch.
    
    Args:
        specs: List of connector specs
        output_dir: Base output directory
    
    Returns:
        Dict mapping connector name -> GeneratedConnector
    """
    builder = ConnectorBuilder()
    results = {}
    
    for spec in specs:
        try:
            result = builder.build(spec, include_dockerfile=True)
            
            # Save to subdirectory
            connector_dir = os.path.join(output_dir, spec.name)
            result.save_to_directory(connector_dir)
            
            results[spec.name] = result
            
        except Exception as e:
            logger.error(f"Failed to build {spec.name}: {e}")
            results[spec.name] = GeneratedConnector(
                name=spec.name,
                code="",
                metadata="",
                requirements="",
                is_valid=False,
                validation_errors=[str(e)],
            )
    
    # Summary
    valid_count = sum(1 for r in results.values() if r.is_valid)
    logger.info(f"Batch build complete: {valid_count}/{len(specs)} valid")
    
    return results

