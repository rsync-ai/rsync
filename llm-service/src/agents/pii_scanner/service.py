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
        
        if not self.detector.is_available() and request.include_ml:
            errors.append("ML detection not available, using pattern-based detection only")
        
        tables = request.tables or []
        
        for table in tables:
            try:
                result = self.scan_table(table, request.score_threshold)
                table_results.append(result)
            except Exception as e:
                logger.error(f"Error scanning table {table.table_name}: {e}")
                errors.append(f"Table {table.table_name}: {str(e)}")
        
        total_columns = sum(len(t.columns) for t in table_results)
        total_pii = sum(t.pii_columns_found for t in table_results)
        
        return SchemaScanResult(
            tables=table_results,
            total_columns_scanned=total_columns,
            total_pii_columns_found=total_pii,
            scan_method="ml" if request.include_ml and self.detector.is_available() else "pattern",
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


# FastAPI router for the PII scanner service
def create_router():
    """Create FastAPI router for PII scanner endpoints."""
    try:
        from fastapi import APIRouter, HTTPException
        from pydantic import BaseModel
        from typing import List, Optional
    except ImportError:
        logger.warning("FastAPI not available, router not created")
        return None
    
    router = APIRouter(prefix="/pii", tags=["PII Scanner"])
    service = PIIScannerService()
    
    # Pydantic models for API
    class TextDetectionRequest(BaseModel):
        texts: List[str]
        language: str = "en"
        entities: Optional[List[str]] = None
        score_threshold: float = 0.5
    
    class ColumnScanRequestModel(BaseModel):
        column_name: str
        samples: List[Any]
        data_type: Optional[str] = None
    
    class TableScanRequestModel(BaseModel):
        table_name: str
        columns: List[ColumnScanRequestModel]
    
    class SchemaScanRequestModel(BaseModel):
        connection_id: Optional[str] = None
        tables: List[TableScanRequestModel]
        include_ml: bool = True
        score_threshold: float = 0.5
    
    @router.get("/status")
    async def get_status():
        """Get PII scanner service status."""
        return service.get_status()
    
    @router.post("/detect")
    async def detect_pii(request: TextDetectionRequest):
        """Detect PII in text samples."""
        req = PIIDetectionRequest(
            texts=request.texts,
            language=request.language,
            entities=request.entities,
            score_threshold=request.score_threshold
        )
        response = service.detect_texts(req)
        
        # Convert to JSON-serializable format
        return {
            "results": [[e.to_dict() for e in entities] for entities in response.results],
            "language": response.language,
            "model_version": response.model_version,
            "errors": response.errors
        }
    
    @router.post("/scan/column")
    async def scan_column(request: ColumnScanRequestModel):
        """Scan a single column for PII."""
        req = ColumnScanRequest(
            column_name=request.column_name,
            samples=request.samples,
            data_type=request.data_type
        )
        result = service.scan_column(req)
        return asdict(result)
    
    @router.post("/scan/table")
    async def scan_table(request: TableScanRequestModel):
        """Scan a table for PII."""
        req = TableScanRequest(
            table_name=request.table_name,
            columns=[
                ColumnScanRequest(
                    column_name=c.column_name,
                    samples=c.samples,
                    data_type=c.data_type
                )
                for c in request.columns
            ]
        )
        result = service.scan_table(req)
        return {
            "table_name": result.table_name,
            "columns": [asdict(c) for c in result.columns],
            "pii_columns_found": result.pii_columns_found
        }
    
    @router.post("/scan/schema")
    async def scan_schema(request: SchemaScanRequestModel):
        """Scan an entire schema for PII."""
        req = SchemaScanRequest(
            connection_id=request.connection_id,
            tables=[
                TableScanRequest(
                    table_name=t.table_name,
                    columns=[
                        ColumnScanRequest(
                            column_name=c.column_name,
                            samples=c.samples,
                            data_type=c.data_type
                        )
                        for c in t.columns
                    ]
                )
                for t in request.tables
            ],
            include_ml=request.include_ml,
            score_threshold=request.score_threshold
        )
        result = await service.scan_schema_async(req)
        return {
            "tables": [
                {
                    "table_name": t.table_name,
                    "columns": [asdict(c) for c in t.columns],
                    "pii_columns_found": t.pii_columns_found
                }
                for t in result.tables
            ],
            "total_columns_scanned": result.total_columns_scanned,
            "total_pii_columns_found": result.total_pii_columns_found,
            "scan_method": result.scan_method,
            "errors": result.errors
        }
    
    return router

