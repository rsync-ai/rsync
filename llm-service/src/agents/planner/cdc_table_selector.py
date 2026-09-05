#!/usr/bin/env python3
"""
HITL Table Selection and Validation
Implements table selection with:
- PK/replica identity validation
- PII detection
- Table size estimation
- Prerequisites checking
- Diff generation for "add tables" operations
"""

import logging
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .cdc_pipeline_model import TableMetadata, SourceType

logger = logging.getLogger(__name__)


class PIISeverity(Enum):
    """PII detection severity levels"""
    CRITICAL = "critical"  # SSN, CC, passwords - should block
    HIGH = "high"  # Email, phone - warn
    MEDIUM = "medium"  # Address, IP - inform


@dataclass
class PIIFinding:
    """PII detection finding"""
    column_name: str
    pii_type: str  # "email", "ssn", "credit_card", etc.
    severity: PIISeverity
    confidence: str  # "high" or "medium"
    pattern_matched: str


@dataclass
class TableValidationResult:
    """Result of table validation"""
    table_name: str
    is_valid: bool
    has_primary_key: bool = False
    primary_key_columns: List[str] = field(default_factory=list)
    
    # PII findings
    pii_findings: List[PIIFinding] = field(default_factory=list)
    has_critical_pii: bool = False
    
    # Size estimation
    estimated_rows: int = 0
    data_size_mb: float = 0.0
    change_rate_per_hour: int = 0
    
    # Validation errors/warnings
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Block reasons
    should_block: bool = False
    block_reason: Optional[str] = None


@dataclass
class TableSelectionDiff:
    """
    Diff for adding/removing tables from CDC pipeline.
    Used for HITL confirmation before applying changes.
    """
    tables_to_add: List[str] = field(default_factory=list)
    tables_to_remove: List[str] = field(default_factory=list)
    
    # Validation results for new tables
    validation_results: Dict[str, TableValidationResult] = field(default_factory=dict)
    
    # Summary for UI display
    total_new_tables: int = 0
    total_removed_tables: int = 0
    total_estimated_rows: int = 0
    total_size_mb: float = 0.0
    blocked_tables: List[str] = field(default_factory=list)
    requires_approval: bool = True
    
    def has_critical_issues(self) -> bool:
        """Check if diff has any critical issues that require attention"""
        return len(self.blocked_tables) > 0
    
    def get_summary(self) -> str:
        """Get human-readable summary for HITL display"""
        lines = []
        
        if self.tables_to_add:
            lines.append(f"📊 Adding {len(self.tables_to_add)} table(s):")
            for table in self.tables_to_add:
                validation = self.validation_results.get(table)
                if validation:
                    size_str = f"{validation.data_size_mb:.1f} MB" if validation.data_size_mb > 0 else "unknown size"
                    rows_str = f"~{validation.estimated_rows:,} rows" if validation.estimated_rows > 0 else "unknown rows"
                    status = "✅" if validation.is_valid and not validation.should_block else "⚠️"
                    lines.append(f"  {status} {table} ({rows_str}, {size_str})")
                    
                    if validation.pii_findings:
                        pii_types = [f.pii_type for f in validation.pii_findings]
                        lines.append(f"      ⚠️  PII detected: {', '.join(set(pii_types))}")
        
        if self.tables_to_remove:
            lines.append(f"🗑️  Removing {len(self.tables_to_remove)} table(s):")
            for table in self.tables_to_remove:
                lines.append(f"  ❌ {table}")
        
        if self.blocked_tables:
            lines.append(f"⛔ {len(self.blocked_tables)} table(s) blocked:")
            for table in self.blocked_tables:
                validation = self.validation_results.get(table)
                if validation and validation.block_reason:
                    lines.append(f"  ⛔ {table}: {validation.block_reason}")
        
        lines.append(f"\n📈 Total impact: ~{self.total_estimated_rows:,} rows, {self.total_size_mb:.1f} MB")
        
        return "\n".join(lines)


class PIIDetector:
    """
    Heuristic PII detector for table columns.
    Uses pattern matching on column names and types.
    """
    
    # PII patterns (from plan)
    PII_PATTERNS = {
        "email": {
            "keywords": ["email", "mail_address", "e_mail", "email_addr"],
            "severity": PIISeverity.HIGH,
        },
        "phone": {
            "keywords": ["phone", "mobile", "cell", "telephone", "phone_number"],
            "severity": PIISeverity.HIGH,
        },
        "ssn": {
            "keywords": ["ssn", "social_security", "national_id", "tax_id"],
            "severity": PIISeverity.CRITICAL,
        },
        "credit_card": {
            "keywords": ["cc", "credit_card", "card_number", "pan", "card_num"],
            "severity": PIISeverity.CRITICAL,
        },
        "password": {
            "keywords": ["password", "passwd", "pwd", "hash", "secret", "pass_hash"],
            "severity": PIISeverity.CRITICAL,
        },
        "address": {
            "keywords": ["address", "street", "zip", "postal", "zip_code", "postal_code"],
            "severity": PIISeverity.MEDIUM,
        },
        "ip_address": {
            "keywords": ["ip_address", "ip_addr", "client_ip", "remote_addr"],
            "severity": PIISeverity.MEDIUM,
        },
    }
    
    def scan_table_columns(self, columns: List[Dict[str, Any]]) -> List[PIIFinding]:
        """
        Scan table columns for potential PII.
        
        Args:
            columns: List of column metadata dicts with 'name' and 'type'
            
        Returns:
            List of PII findings
        """
        findings = []
        
        for column in columns:
            col_name = column.get("name", "").lower()
            col_type = column.get("type", "").lower()
            
            for pii_type, config in self.PII_PATTERNS.items():
                for keyword in config["keywords"]:
                    if keyword in col_name:
                        confidence = "high" if keyword == col_name else "medium"
                        findings.append(PIIFinding(
                            column_name=column.get("name"),
                            pii_type=pii_type,
                            severity=config["severity"],
                            confidence=confidence,
                            pattern_matched=keyword,
                        ))
                        break  # Only report first match per column
        
        return findings


class TableValidator:
    """
    Validates tables for CDC eligibility.
    Implements all validation rules from the plan:
    - PK/replica identity requirement
    - PII detection
    - Size estimation
    """
    
    def __init__(
        self,
        require_pk: bool = True,
        enable_pii_detection: bool = True,
        block_critical_pii: bool = True,
    ):
        self.require_pk = require_pk
        self.enable_pii_detection = enable_pii_detection
        self.block_critical_pii = block_critical_pii
        self.pii_detector = PIIDetector()
    
    def validate_table(
        self,
        table_name: str,
        table_schema: Dict[str, Any],
        source_type: SourceType,
    ) -> TableValidationResult:
        """
        Validate a single table for CDC.
        
        Args:
            table_name: Fully qualified table name
            table_schema: Schema metadata including columns, PK, size
            source_type: Database type
            
        Returns:
            TableValidationResult with validation status and findings
        """
        result = TableValidationResult(
            table_name=table_name,
            is_valid=True,
        )
        
        # Extract metadata
        columns = table_schema.get("columns", [])
        # discover_schema returns "primary_keys" (plural); "primary_key" (singular) is a legacy key
        # used by some older CDC planner paths. Check both to stay backward-compatible.
        primary_keys = table_schema.get("primary_keys") or table_schema.get("primary_key") or []
        replica_identity = table_schema.get("replica_identity")  # Postgres-specific
        
        # Check primary key
        if source_type in (SourceType.POSTGRESQL, SourceType.POSTGRES):
            # Postgres: check replica identity
            if replica_identity and replica_identity != "default":
                result.has_primary_key = True
                result.primary_key_columns = table_schema.get("replica_identity_columns", primary_keys)
            elif primary_keys:
                result.has_primary_key = True
                result.primary_key_columns = primary_keys
        elif source_type == SourceType.MONGODB:
            # MongoDB documents always carry a mandatory _id, which is the implicit
            # primary key of the packed destination table (_id + document JSONB).
            # A collection can therefore never lack a PK — do NOT block it just
            # because schema discovery didn't populate a relational primary_keys
            # field (collections have no relational PK metadata).
            result.has_primary_key = True
            result.primary_key_columns = primary_keys or ["_id"]
        else:
            # Other DBs: check primary key
            if primary_keys:
                result.has_primary_key = True
                result.primary_key_columns = primary_keys
        
        if self.require_pk and not result.has_primary_key:
            result.is_valid = False
            result.should_block = True
            result.block_reason = "No primary key or replica identity"
            result.errors.append("Table must have a primary key for CDC")
        
        # PII detection
        if self.enable_pii_detection:
            pii_findings = self.pii_detector.scan_table_columns(columns)
            result.pii_findings = pii_findings
            
            critical_findings = [f for f in pii_findings if f.severity == PIISeverity.CRITICAL]
            if critical_findings:
                result.has_critical_pii = True
                if self.block_critical_pii:
                    result.should_block = True
                    result.block_reason = f"Critical PII detected: {', '.join([f.pii_type for f in critical_findings])}"
                    result.errors.append("⛔ CRITICAL: Table contains sensitive data (passwords, SSN, credit cards). Manual approval required.")
                else:
                    result.warnings.append("⚠️  WARNING: Critical PII detected. Review before enabling CDC.")
            
            high_findings = [f for f in pii_findings if f.severity == PIISeverity.HIGH]
            if high_findings:
                result.warnings.append(f"⚠️  PII detected: {', '.join(set([f.pii_type for f in high_findings]))}")
        
        # Size estimation
        result.estimated_rows = table_schema.get("estimated_rows", 0)
        result.data_size_mb = table_schema.get("data_size_mb", 0.0)
        result.change_rate_per_hour = table_schema.get("change_rate_per_hour", 0)
        
        # Large table warnings
        if result.estimated_rows > 100_000_000:
            result.warnings.append(f"⚠️  Large table: {result.estimated_rows:,} rows. Initial snapshot may take hours.")
        
        if result.data_size_mb > 10_000:
            result.warnings.append(f"⚠️  Large data size: {result.data_size_mb:.1f} MB. Review snapshot strategy.")
        
        return result


class TableSelector:
    """
    HITL table selection service.
    Generates diffs for "add tables" operations with full validation.
    """
    
    def __init__(
        self,
        validator: Optional[TableValidator] = None,
    ):
        self.validator = validator or TableValidator()
    
    def create_table_diff(
        self,
        current_tables: List[str],
        requested_tables: List[str],
        table_schemas: Dict[str, Dict[str, Any]],
        source_type: SourceType,
    ) -> TableSelectionDiff:
        """
        Create a diff for adding/removing tables with HITL validation.
        
        Args:
            current_tables: Currently selected tables in pipeline
            requested_tables: Requested table list (add or full replacement)
            table_schemas: Schema metadata for all tables
            source_type: Database type
            
        Returns:
            TableSelectionDiff for HITL confirmation
        """
        current_set = set(current_tables)
        requested_set = set(requested_tables)
        
        tables_to_add = list(requested_set - current_set)
        tables_to_remove = list(current_set - requested_set)
        
        diff = TableSelectionDiff(
            tables_to_add=tables_to_add,
            tables_to_remove=tables_to_remove,
            total_new_tables=len(tables_to_add),
            total_removed_tables=len(tables_to_remove),
        )
        
        # Validate new tables
        for table in tables_to_add:
            schema = table_schemas.get(table, {})
            if not schema:
                logger.warning(f"No schema found for table {table}")
                continue
            
            validation = self.validator.validate_table(table, schema, source_type)
            diff.validation_results[table] = validation
            
            if validation.should_block:
                diff.blocked_tables.append(table)
            
            diff.total_estimated_rows += validation.estimated_rows
            diff.total_size_mb += validation.data_size_mb
        
        # Determine if manual approval is required
        diff.requires_approval = (
            len(tables_to_add) > 0
            or len(tables_to_remove) > 0
            or diff.has_critical_issues()
        )
        
        return diff
    
    def suggest_tables(
        self,
        all_tables: List[str],
        table_schemas: Dict[str, Dict[str, Any]],
        source_type: SourceType,
        intent: str = "",
    ) -> List[str]:
        """
        AI-suggested tables based on intent and schema analysis.
        
        Args:
            all_tables: All available tables in source DB
            table_schemas: Schema metadata
            source_type: Database type
            intent: User intent (e.g., "replicate user data", "analytics")
            
        Returns:
            List of suggested table names (unvalidated)
        """
        # This is a placeholder for AI-based table suggestion
        # In a full implementation, this would use LLM to suggest tables
        # based on intent, schema, foreign keys, naming patterns, etc.
        
        suggested = []
        
        # Simple heuristics as fallback
        if "user" in intent.lower():
            suggested.extend([t for t in all_tables if "user" in t.lower()])
        
        if "order" in intent.lower():
            suggested.extend([t for t in all_tables if "order" in t.lower()])
        
        if "product" in intent.lower():
            suggested.extend([t for t in all_tables if "product" in t.lower()])
        
        # Remove duplicates
        suggested = list(set(suggested))
        
        logger.info(f"Suggested {len(suggested)} tables for intent: {intent}")
        
        return suggested

