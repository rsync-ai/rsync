"""ML-based PII detection using Microsoft Presidio and custom models."""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


class PIIType(str, Enum):
    """Supported PII types for detection."""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    NAME = "name"
    ADDRESS = "address"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    BANK_ACCOUNT = "bank_account"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"
    MEDICAL_LICENSE = "medical_license"
    URL = "url"
    LOCATION = "location"
    DATE_TIME = "date_time"
    PERSON = "person"
    ORGANIZATION = "organization"
    UNKNOWN = "unknown"


# Mapping from Presidio entity types to our PII types
PRESIDIO_TO_PII_TYPE: Dict[str, PIIType] = {
    "EMAIL_ADDRESS": PIIType.EMAIL,
    "PHONE_NUMBER": PIIType.PHONE,
    "US_SSN": PIIType.SSN,
    "US_PASSPORT": PIIType.PASSPORT,
    "US_DRIVER_LICENSE": PIIType.DRIVER_LICENSE,
    "CREDIT_CARD": PIIType.CREDIT_CARD,
    "IBAN_CODE": PIIType.BANK_ACCOUNT,
    "US_BANK_NUMBER": PIIType.BANK_ACCOUNT,
    "IP_ADDRESS": PIIType.IP_ADDRESS,
    "DATE_TIME": PIIType.DATE_OF_BIRTH,
    "PERSON": PIIType.NAME,
    "LOCATION": PIIType.ADDRESS,
    "NRP": PIIType.NAME,  # Nationality, Religion, Political group
    "MEDICAL_LICENSE": PIIType.MEDICAL_LICENSE,
    "URL": PIIType.URL,
    "CRYPTO": PIIType.BANK_ACCOUNT,  # Cryptocurrency addresses
    "UK_NHS": PIIType.SSN,  # UK National Health Service number
    "SG_NRIC_FIN": PIIType.SSN,  # Singapore NRIC/FIN
    "AU_ABN": PIIType.BANK_ACCOUNT,  # Australian Business Number
    "AU_ACN": PIIType.BANK_ACCOUNT,  # Australian Company Number
    "AU_TFN": PIIType.SSN,  # Australian Tax File Number
    "AU_MEDICARE": PIIType.SSN,  # Australian Medicare number
}


@dataclass
class PIIEntity:
    """Represents a detected PII entity."""
    entity_type: PIIType
    text: str
    start: int
    end: int
    score: float  # Confidence score 0.0 - 1.0
    recognition_metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": self.entity_type.value,
            "text": self.text,
            "start": self.start,
            "end": self.end,
            "score": self.score,
            "recognition_metadata": self.recognition_metadata
        }


@dataclass
class PIIDetectionRequest:
    """Request for PII detection."""
    texts: List[str]
    language: str = "en"
    entities: Optional[List[str]] = None  # Specific entities to detect
    score_threshold: float = 0.5
    return_decision_process: bool = False


@dataclass
class PIIDetectionResponse:
    """Response from PII detection."""
    results: List[List[PIIEntity]]
    language: str
    model_version: str
    errors: List[str] = field(default_factory=list)


class MLPIIDetector:
    """ML-based PII detector using Microsoft Presidio."""
    
    def __init__(self, models_path: Optional[str] = None):
        """Initialize the ML PII detector.
        
        Args:
            models_path: Optional path to custom NER models
        """
        self.analyzer = None
        self.models_path = models_path
        self._initialized = False
        self._initialization_error: Optional[str] = None
        
    def _lazy_init(self) -> bool:
        """Lazily initialize Presidio analyzer.

        IMPORTANT: this MUST fail fast if the spaCy `en_core_web_sm` model is
        not pre-installed (the Dockerfile bakes it in). The default
        `AnalyzerEngine()` constructor will otherwise trigger a runtime
        download of `en_core_web_lg` (~400 MB) from GitHub on the first call,
        which historically hung the suggestions endpoint for 60+ seconds and
        caused intermittent "Failed to generate suggestions" errors during
        demos. We'd rather skip ML enrichment (heuristics still run) than
        block the request.
        """
        if self._initialized:
            return True

        if self._initialization_error:
            return False

        try:
            from presidio_analyzer import AnalyzerEngine
            from presidio_analyzer.nlp_engine import NlpEngineProvider

            # Pre-check: the spaCy model must already be importable. If not,
            # bail out before Presidio tries to auto-download a larger model.
            try:
                import spacy
                spacy.load("en_core_web_sm")
            except Exception as e:
                self._initialization_error = (
                    f"spaCy model en_core_web_sm not available: {e}. "
                    "Build the llm-service image with the model baked in "
                    "(see llm-service/Dockerfile)."
                )
                logger.warning(
                    f"ML PII detection unavailable, falling back to heuristics: "
                    f"{self._initialization_error}"
                )
                return False

            configuration = {
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}]
            }
            provider = NlpEngineProvider(nlp_configuration=configuration)
            nlp_engine = provider.create_engine()
            self.analyzer = AnalyzerEngine(nlp_engine=nlp_engine)

            self._initialized = True
            logger.info("Presidio analyzer initialized successfully")
            return True

        except ImportError as e:
            self._initialization_error = f"Presidio not installed: {e}"
            logger.warning(f"ML PII detection unavailable: {self._initialization_error}")
            return False
        except Exception as e:
            self._initialization_error = f"Failed to initialize Presidio: {e}"
            logger.error(self._initialization_error)
            return False
    
    def is_available(self) -> bool:
        """Check if ML detection is available."""
        return self._lazy_init()
    
    def detect(self, request: PIIDetectionRequest) -> PIIDetectionResponse:
        """Detect PII in the given texts using ML models.
        
        Args:
            request: Detection request with texts and configuration
            
        Returns:
            PIIDetectionResponse with detected entities
        """
        if not self._lazy_init():
            return PIIDetectionResponse(
                results=[[] for _ in request.texts],
                language=request.language,
                model_version="unavailable",
                errors=[self._initialization_error or "ML detection not available"]
            )
        
        all_results: List[List[PIIEntity]] = []
        errors: List[str] = []
        
        # Define entities to detect
        entities_to_detect = request.entities
        if not entities_to_detect:
            # Default entities to detect
            entities_to_detect = [
                "EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD",
                "US_SSN", "PERSON", "LOCATION", "DATE_TIME",
                "IBAN_CODE", "US_BANK_NUMBER", "IP_ADDRESS",
                "US_PASSPORT", "US_DRIVER_LICENSE", "MEDICAL_LICENSE",
                "URL", "CRYPTO", "NRP"
            ]
        
        for text in request.texts:
            try:
                if not text or not text.strip():
                    all_results.append([])
                    continue
                
                # Run Presidio analyzer
                results = self.analyzer.analyze(
                    text=text,
                    language=request.language,
                    entities=entities_to_detect,
                    score_threshold=request.score_threshold,
                    return_decision_process=request.return_decision_process
                )
                
                # Convert to our entity format
                entities = []
                for result in results:
                    pii_type = PRESIDIO_TO_PII_TYPE.get(
                        result.entity_type, PIIType.UNKNOWN
                    )
                    
                    metadata = None
                    if request.return_decision_process and hasattr(result, 'analysis_explanation'):
                        metadata = {
                            "recognizer": result.analysis_explanation.recognizer if result.analysis_explanation else None,
                            "pattern_name": result.analysis_explanation.pattern_name if result.analysis_explanation else None,
                        }
                    
                    entity = PIIEntity(
                        entity_type=pii_type,
                        text=text[result.start:result.end],
                        start=result.start,
                        end=result.end,
                        score=result.score,
                        recognition_metadata=metadata
                    )
                    entities.append(entity)
                
                all_results.append(entities)
                
            except Exception as e:
                logger.error(f"Error detecting PII in text: {e}")
                errors.append(str(e))
                all_results.append([])
        
        return PIIDetectionResponse(
            results=all_results,
            language=request.language,
            model_version="presidio-2.2",
            errors=errors
        )
    
    def detect_single(self, text: str, language: str = "en", 
                      score_threshold: float = 0.5) -> List[PIIEntity]:
        """Convenience method to detect PII in a single text.
        
        Args:
            text: Text to analyze
            language: Language code
            score_threshold: Minimum confidence score
            
        Returns:
            List of detected PII entities
        """
        request = PIIDetectionRequest(
            texts=[text],
            language=language,
            score_threshold=score_threshold
        )
        response = self.detect(request)
        return response.results[0] if response.results else []
    
    def get_supported_entities(self) -> List[str]:
        """Get list of supported entity types."""
        if not self._lazy_init():
            return []
        
        return self.analyzer.get_supported_entities()
    
    def analyze_column_samples(self, column_name: str, samples: List[Any],
                                score_threshold: float = 0.5) -> Dict[str, Any]:
        """Analyze a column's sample values for PII.
        
        This method aggregates results across multiple samples to determine
        if a column likely contains PII.
        
        Args:
            column_name: Name of the column
            samples: Sample values from the column
            score_threshold: Minimum confidence score
            
        Returns:
            Dictionary with analysis results including:
            - is_pii: Whether the column likely contains PII
            - pii_type: Most likely PII type
            - confidence: Aggregated confidence score
            - sample_count: Number of samples analyzed
            - match_count: Number of samples with PII detected
        """
        if not samples:
            return {
                "column_name": column_name,
                "is_pii": False,
                "pii_type": None,
                "confidence": 0.0,
                "sample_count": 0,
                "match_count": 0
            }
        
        # Convert samples to strings
        text_samples = []
        for sample in samples:
            if sample is not None:
                text_samples.append(str(sample))
        
        if not text_samples:
            return {
                "column_name": column_name,
                "is_pii": False,
                "pii_type": None,
                "confidence": 0.0,
                "sample_count": 0,
                "match_count": 0
            }
        
        # Detect PII in all samples
        request = PIIDetectionRequest(
            texts=text_samples,
            score_threshold=score_threshold
        )
        response = self.detect(request)
        
        # Aggregate results
        type_counts: Dict[PIIType, int] = {}
        type_scores: Dict[PIIType, List[float]] = {}
        match_count = 0
        
        for entities in response.results:
            if entities:
                match_count += 1
                for entity in entities:
                    type_counts[entity.entity_type] = type_counts.get(entity.entity_type, 0) + 1
                    if entity.entity_type not in type_scores:
                        type_scores[entity.entity_type] = []
                    type_scores[entity.entity_type].append(entity.score)
        
        if not type_counts:
            return {
                "column_name": column_name,
                "is_pii": False,
                "pii_type": None,
                "confidence": 0.0,
                "sample_count": len(text_samples),
                "match_count": 0
            }
        
        # Find the most common PII type
        most_common_type = max(type_counts.keys(), key=lambda t: type_counts[t])
        avg_score = sum(type_scores[most_common_type]) / len(type_scores[most_common_type])
        
        # Calculate overall confidence based on match rate and score
        match_rate = match_count / len(text_samples)
        overall_confidence = avg_score * match_rate
        
        return {
            "column_name": column_name,
            "is_pii": overall_confidence >= 0.3,  # Threshold for considering as PII
            "pii_type": most_common_type.value,
            "confidence": overall_confidence,
            "avg_score": avg_score,
            "sample_count": len(text_samples),
            "match_count": match_count,
            "match_rate": match_rate,
            "type_distribution": {t.value: c for t, c in type_counts.items()}
        }

