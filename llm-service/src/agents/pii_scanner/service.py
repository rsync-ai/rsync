"""PII Scanner Service - HTTP API for ML-based PII detection."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .ml_detector import (
    MLPIIDetector, 
    PIIDetectionRequest, 
    PIIDetectionResponse,
    PIIEntity,
    PIIType
)
from .column_names import DETECTION_METHOD, NAME_MATCH_CONFIDENCE, pii_type_for_column_name

logger = logging.getLogger(__name__)


@dataclass
class ColumnScanRequest:
    """Request to scan a column for PII."""
    column_name: str
    samples: List[Any]
    data_type: Optional[str] = None


@dataclass
class TableScanRequest:
    """Request to scan a table for PII."""
    table_name: str
    columns: List[ColumnScanRequest]
    

@dataclass  
class SchemaScanRequest:
    """Request to scan an entire schema for PII."""
    connection_id: Optional[str] = None
    tables: List[TableScanRequest] = None
    include_ml: bool = True
    score_threshold: float = 0.5
    

@dataclass
class ColumnScanResult:
    """Result of scanning a column for PII."""
    column_name: str
    is_pii: bool
    pii_type: Optional[str]
    confidence: float
    detection_method: str
    sample_count: int
    match_count: int
    suggested_masking: str


@dataclass
class TableScanResult:
    """Result of scanning a table for PII."""
    table_name: str
    columns: List[ColumnScanResult]
    pii_columns_found: int


@dataclass
class SchemaScanResult:
    """Result of scanning a schema for PII."""
    tables: List[TableScanResult]
    total_columns_scanned: int
    total_pii_columns_found: int
    scan_method: str
    errors: List[str]


class PIIScannerService:
    """Service for ML-based PII detection."""
    
    def __init__(self, models_path: Optional[str] = None, max_workers: int = 4):
        """Initialize the PII scanner service.
        
        Args:
            models_path: Optional path to custom NER models
            max_workers: Maximum number of concurrent workers for scanning
        """
        self.detector = MLPIIDetector(models_path=models_path)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Suggested masking actions per PII type
        self.masking_suggestions = {
            PIIType.EMAIL: "hash",
            PIIType.PHONE: "partial_mask",
            PIIType.SSN: "hash",
            PIIType.CREDIT_CARD: "hash",
            PIIType.NAME: "hash",
            PIIType.ADDRESS: "redact",
            PIIType.IP_ADDRESS: "hash",
            PIIType.DATE_OF_BIRTH: "redact",
            PIIType.BANK_ACCOUNT: "hash",
            PIIType.PASSPORT: "hash",
            PIIType.DRIVER_LICENSE: "hash",
            PIIType.MEDICAL_LICENSE: "hash",
            PIIType.URL: "redact",
            PIIType.LOCATION: "redact",
            PIIType.DATE_TIME: "redact",
            PIIType.PERSON: "hash",
            PIIType.ORGANIZATION: "redact",
            PIIType.UNKNOWN: "hash",
        }
    
    def is_available(self) -> bool:
        """Check if ML detection is available."""
        return self.detector.is_available()
    
    def get_status(self) -> Dict[str, Any]:
        """Get service status."""
        return {
            "service": "pii_scanner",
            "ml_available": self.detector.is_available(),
            "supported_entities": self.detector.get_supported_entities() if self.detector.is_available() else [],
            "version": "1.0.0"
        }
    
    def detect_texts(self, request: PIIDetectionRequest) -> PIIDetectionResponse:
        """Detect PII in text samples.
        
        Args:
            request: Detection request
            
        Returns:
            Detection response with results
        """
        return self.detector.detect(request)
    
    def scan_column(self, request: ColumnScanRequest, 
                    score_threshold: float = 0.5) -> ColumnScanResult:
        """Scan a column for PII.
        
        Args:
            request: Column scan request
            score_threshold: Minimum confidence threshold
            
        Returns:
            Column scan result
        """
        if not request.samples:
            return self._scan_column_name(request)

        result = self.detector.analyze_column_samples(
            column_name=request.column_name,
            samples=request.samples,
            score_threshold=score_threshold
        )
        
        pii_type = None
        suggested_masking = "hash"
        
        if result.get("pii_type"):
            pii_type = result["pii_type"]
            try:
                pii_type_enum = PIIType(pii_type)
                suggested_masking = self.masking_suggestions.get(pii_type_enum, "hash")
            except ValueError:
                suggested_masking = "hash"
        
        return ColumnScanResult(
            column_name=request.column_name,
            is_pii=result.get("is_pii", False),
            pii_type=pii_type,
            confidence=result.get("confidence", 0.0),
            detection_method="ml" if result.get("is_pii") else "none",
            sample_count=result.get("sample_count", 0),
            match_count=result.get("match_count", 0),
            suggested_masking=suggested_masking
        )
    
    def _scan_column_name(self, request: ColumnScanRequest) -> ColumnScanResult:
        """Classify a column with no samples by its name and declared type.

        Without samples the detector has nothing to read and answers "not PII"
        for every column, which the gateway's prune would take as a clean scan.
        """
        pii_type = pii_type_for_column_name(request.column_name, request.data_type)
        masking = "hash"
        if pii_type:
            try:
                masking = self.masking_suggestions.get(PIIType(pii_type), "hash")
            except ValueError:
                masking = "hash"
        return ColumnScanResult(
            column_name=request.column_name,
            is_pii=pii_type is not None,
            pii_type=pii_type,
            confidence=NAME_MATCH_CONFIDENCE if pii_type else 0.0,
            detection_method=DETECTION_METHOD if pii_type else "none",
            sample_count=0,
            match_count=0,
            suggested_masking=masking,
        )

    def scan_table(self, request: TableScanRequest,
                   score_threshold: float = 0.5) -> TableScanResult:
        """Scan a table for PII.
        
        Args:
            request: Table scan request
            score_threshold: Minimum confidence threshold
            
        Returns:
            Table scan result
        """
        column_results = []
        
        for column in request.columns:
            result = self.scan_column(column, score_threshold)
            column_results.append(result)
        
        pii_count = sum(1 for c in column_results if c.is_pii)
        
        return TableScanResult(
            table_name=request.table_name,
            columns=column_results,
            pii_columns_found=pii_count
        )
    
    def scan_schema(self, request: SchemaScanRequest) -> SchemaScanResult:
        """Scan a schema for PII.
        
        Args:
            request: Schema scan request
            
        Returns:
            Schema scan result
        """
        errors = []
        table_results = []

        tables = request.tables or []
        # With no sample anywhere there is nothing for the ML detector to read,
        # so it is not loaded (a cold Presidio start is seconds) and the result
        # says what the scan actually was.
        has_samples = any(c.samples for t in tables for c in (t.columns or []))

        if has_samples and not self.detector.is_available() and request.include_ml:
            errors.append("ML detection not available, using pattern-based detection only")

        for table in tables:
            # A table with no columns was not scanned. Listing it in `tables`
            # would tell the gateway it came back clean, and the gateway prunes
            # every stored finding of a table reported clean.
            if not table.columns:
                errors.append(f"Table {table.table_name}: no columns were supplied, so it was not scanned")
                continue
            try:
                result = self.scan_table(table, request.score_threshold)
                table_results.append(result)
            except Exception as e:
                logger.error(f"Error scanning table {table.table_name}: {e}")
                errors.append(f"Table {table.table_name}: {str(e)}")

        total_columns = sum(len(t.columns) for t in table_results)
        total_pii = sum(t.pii_columns_found for t in table_results)

        if not has_samples:
            scan_method = DETECTION_METHOD
        elif request.include_ml and self.detector.is_available():
            scan_method = "ml"
        else:
            scan_method = "pattern"

        return SchemaScanResult(
            tables=table_results,
            total_columns_scanned=total_columns,
            total_pii_columns_found=total_pii,
            scan_method=scan_method,
            errors=errors
        )
    
    async def scan_schema_async(self, request: SchemaScanRequest) -> SchemaScanResult:
        """Asynchronously scan a schema for PII.
        
        Args:
            request: Schema scan request
            
        Returns:
            Schema scan result
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.scan_schema, request)
    
    def to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert dataclass to dictionary."""
        if hasattr(obj, '__dataclass_fields__'):
            return asdict(obj)
        return obj
