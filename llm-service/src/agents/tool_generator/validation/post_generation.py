"""
Post-Generation Validator

Validates generated connector code after Jinja2 rendering.

Checks:
- Code compiles (valid Python syntax)
- Required methods exist and aren't stubs
- Category-specific I/O operations present
- No obvious anti-patterns

VERSION: 1.0.0
"""

import ast
import logging
import os
from pathlib import Path
from typing import Optional

try:
    from ..schemas.spec import ConnectorSpec, ConnectorCategory
    from .models import ValidationResult
    from .category_validators import validate_category_fast
except ImportError:
    # Fallback for different execution context
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from schemas.spec import ConnectorSpec, ConnectorCategory
    from validation.models import ValidationResult
    from validation.category_validators import validate_category_fast

logger = logging.getLogger(__name__)


class PostGenerationValidator:
    """
    Validates generated connector code.
    
    This runs after code generation and checks that the generated
    code is valid, complete, and meets category-specific requirements.
    """
    
    def validate(self, code: str, spec: ConnectorSpec) -> ValidationResult:
        """
        Run all post-generation validations.
        
        Args:
            code: Generated connector code
            spec: ConnectorSpec that was used to generate the code
            
        Returns:
            ValidationResult with errors/warnings
        """
        result = ValidationResult(passed=True)
        # Carry spec for best-effort debug dumping in validators
        # (ValidationResult doesn't formally include this, so keep it as an attribute).
        try:
            setattr(result, "spec", spec)
        except Exception:
            pass
        
        # Run all validation checks
        self._validate_syntax(code, result)
        self._validate_class_structure(code, spec, result)
        self._validate_required_methods(code, spec, result)
        self._validate_category_specific(code, spec, result)
        
        logger.info(
            f"Post-generation validation for {spec.name}: "
            f"{'PASSED' if result.passed else 'FAILED'} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
        
        return result
    
    def _validate_syntax(self, code: str, result: ValidationResult) -> None:
        """Validate that code is syntactically correct Python"""
        try:
            ast.parse(code)
        except SyntaxError as e:
            # Best-effort: dump the generated code to disk for debugging.
            # This is especially important when generation fails before artifacts are written.
            try:
                tools_dir = os.getenv("TOOLS_DIR", "/app/shared/mcp-connectors")
                # Avoid writing outside our expected tree even if TOOLS_DIR is mis-set.
                base = Path(tools_dir).resolve()
                failed_dir = (base / "_failed" / "post_generation_syntax").resolve()
                if str(failed_dir).startswith(str(base)):
                    failed_dir.mkdir(parents=True, exist_ok=True)
                    fname = f"{getattr(getattr(result, 'spec', None), 'name', None) or 'unknown'}-connector.py"
                    # If the ValidationResult doesn't carry spec, use a generic name and write both.
                    (failed_dir / fname).write_text(code, encoding="utf-8")
                    (failed_dir / "last_error.txt").write_text(
                        f"lineno={e.lineno} offset={getattr(e, 'offset', None)} msg={e.msg}\n",
                        encoding="utf-8",
                    )
            except Exception:
                # Never fail validation because debug dumping failed
                pass

            # Log context lines (best-effort) to make debugging from logs possible.
            try:
                lineno = int(e.lineno or 0)
                if lineno > 0:
                    lines = code.splitlines()
                    start = max(0, lineno - 4)
                    end = min(len(lines), lineno + 3)
                    snippet = "\n".join(f"{i+1}: {lines[i]}" for i in range(start, end))
                    logger.error("Generated code syntax error context:\n%s", snippet)
            except Exception:
                pass

            result.add_error(
                f"Generated code has syntax error: {e}",
                location=f"connector.py:{e.lineno}",
                code="SYNTAX_ERROR"
            )
        except Exception as e:
            result.add_error(
                f"Failed to parse generated code: {e}",
                location="connector.py",
                code="PARSE_ERROR"
            )
    
    def _validate_class_structure(
        self,
        code: str,
        spec: ConnectorSpec,
        result: ValidationResult
    ) -> None:
        """Validate basic class structure.

        The runtime contract is that every connector defines a class that
        inherits from ``BaseMCPConnector`` — the orchestrator's MCP server
        manager imports the module and instantiates whichever subclass it
        finds. The class **name** is irrelevant at runtime: every
        production connector on disk today uses the ``<Name>Connector``
        suffix (e.g. ``ShopifyConnector``, ``PostgreSQLConnector``), and
        the legacy ``<Name>MCPServer`` naming was a hand-written
        convention that no shipped connector actually uses.

        Previously this validator required the literal substring
        ``MCPServer`` in the generated code, which broke every
        template-generated connector and caused builder.build() to mark
        them invalid even though they'd run correctly.
        """
        # Accept both single-line and multi-line imports, e.g.:
        # - from base_connector import BaseMCPConnector
        # - from base_connector import (BaseMCPConnector, ...)
        if ("from base_connector import" not in code) or ("BaseMCPConnector" not in code):
            result.add_error(
                "Generated code must import BaseMCPConnector",
                location="connector.py",
                code="MISSING_BASE_IMPORT"
            )

        # Accept any class that inherits from BaseMCPConnector. Match
        # both ``class X(BaseMCPConnector)`` and ``class X(BaseMCPConnector,``
        # (multi-inheritance) — that's the actual runtime contract.
        if "(BaseMCPConnector)" not in code and "(BaseMCPConnector," not in code:
            result.add_error(
                "Generated code must define a class inheriting from BaseMCPConnector",
                location="connector.py",
                code="MISSING_CLASS"
            )
    
    def _validate_required_methods(
        self,
        code: str,
        spec: ConnectorSpec,
        result: ValidationResult
    ) -> None:
        """Validate that required methods are implemented"""
        required_methods = [
            "test_connection",
            "validate_config",
            "get_capabilities",
            "discover_schema",
        ]
        
        # Add operation-specific methods
        if spec.supports_source:
            required_methods.append("export")
        
        if spec.supports_destination:
            required_methods.append("import_data")
        
        for method_name in required_methods:
            if f"def {method_name}" not in code:
                result.add_error(
                    f"Required method '{method_name}' not found in generated code",
                    location="connector.py",
                    code="MISSING_METHOD",
                    fix_suggestion=f"Add {method_name} method to connector class"
                )
                continue
            
            # Check for stub implementations
            self._check_method_stub(code, method_name, result)
    
    def _check_method_stub(
        self,
        code: str,
        method_name: str,
        result: ValidationResult
    ) -> None:
        """Check if a method is just a stub"""
        # Try to extract the method
        import re
        
        pattern = rf"def {method_name}\(.*?\):.*?(?=\n    def |\nclass |\Z)"
        match = re.search(pattern, code, re.DOTALL)
        
        if not match:
            return
        
        method_code = match.group(0).lower()
        
        # Check for stub patterns
        stub_indicators = [
            "raise notimplementederror",
            "return {'success': false, 'error': 'not implemented'",
            "pass  # todo",
            "pass  # stub",
        ]
        
        for indicator in stub_indicators:
            if indicator in method_code:
                result.add_error(
                    f"Method '{method_name}' appears to be a stub implementation",
                    location=f"connector.py:{method_name}",
                    code="STUB_DETECTED",
                    fix_suggestion=f"Implement actual logic for {method_name}"
                )
                break
    
    def _validate_category_specific(
        self,
        code: str,
        spec: ConnectorSpec,
        result: ValidationResult
    ) -> None:
        """Run category-specific validations"""
        try:
            fast = validate_category_fast(code, spec)
            result.merge(fast)
        except Exception as e:
            logger.warning(f"Fast category validation failed unexpectedly: {e}", exc_info=True)

