"""
Validation Models

Defines data structures for validation results and errors.

VERSION: 1.0.0
"""

from enum import Enum
from typing import List, Optional
from dataclasses import dataclass, field


class ValidationSeverity(str, Enum):
    """Severity levels for validation messages"""
    ERROR = "error"  # Blocks generation/deployment
    WARNING = "warning"  # Doesn't block but should be reviewed
    INFO = "info"  # Informational only


@dataclass
class ValidationError:
    """Represents a single validation error or warning"""
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    location: Optional[str] = None  # e.g., "spec.operations[2]", "connector.py:45"
    code: Optional[str] = None  # e.g., "MISSING_OPERATION", "STUB_DETECTED"
    fix_suggestion: Optional[str] = None
    
    def __str__(self) -> str:
        loc = f" [{self.location}]" if self.location else ""
        return f"{self.severity.value.upper()}{loc}: {self.message}"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    passed: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    info: List[ValidationError] = field(default_factory=list)
    
    @property
    def has_errors(self) -> bool:
        """Check if there are blocking errors"""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if there are warnings"""
        return len(self.warnings) > 0
    
    @property
    def all_messages(self) -> List[ValidationError]:
        """Get all validation messages (errors + warnings + info)"""
        return self.errors + self.warnings + self.info
    
    def add_error(
        self,
        message: str,
        location: Optional[str] = None,
        code: Optional[str] = None,
        fix_suggestion: Optional[str] = None,
    ) -> None:
        """Add an error (blocks generation)"""
        self.errors.append(ValidationError(
            message=message,
            severity=ValidationSeverity.ERROR,
            location=location,
            code=code,
            fix_suggestion=fix_suggestion,
        ))
        self.passed = False
    
    def add_warning(
        self,
        message: str,
        location: Optional[str] = None,
        code: Optional[str] = None,
        fix_suggestion: Optional[str] = None,
    ) -> None:
        """Add a warning (doesn't block generation)"""
        self.warnings.append(ValidationError(
            message=message,
            severity=ValidationSeverity.WARNING,
            location=location,
            code=code,
            fix_suggestion=fix_suggestion,
        ))
    
    def add_info(
        self,
        message: str,
        location: Optional[str] = None,
    ) -> None:
        """Add informational message"""
        self.info.append(ValidationError(
            message=message,
            severity=ValidationSeverity.INFO,
            location=location,
        ))
    
    def merge(self, other: 'ValidationResult') -> None:
        """Merge another validation result into this one"""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.info.extend(other.info)
        if other.has_errors:
            self.passed = False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging/serialization"""
        return {
            "passed": self.passed,
            "errors": [str(e) for e in self.errors],
            "warnings": [str(w) for w in self.warnings],
            "info": [str(i) for i in self.info],
        }

