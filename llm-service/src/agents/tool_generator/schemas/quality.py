"""
ConnectorQualityTier

Computes and represents connector quality tiers (Gold/Silver/Bronze) based on:
- Context7 authoritativeness
- Number of safe resources produced
- Offline QA checks (auth mocks + contracts)
- Error-rate signals (optional; not implemented in v1)

VERSION: 1.0.0
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class QualityTier(str, Enum):
    """Connector quality tier"""
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"
    DRAFT = "draft"


@dataclass
class QualityScore:
    """Quality scoring breakdown"""
    tier: QualityTier
    score: float  # 0-100
    authoritativeness_score: float
    resource_count_score: float
    qa_score: float
    reasons: List[str]
    capabilities: Dict[str, bool]  # What's enabled


class ConnectorQualityTierCalculator:
    """
    Calculates connector quality tier based on multiple signals.
    
    Tier definitions:
    - Gold: High authoritativeness + curated resources + curated actions + QA passing
    - Silver: Medium authoritativeness + curated resources + QA passing
    - Bronze: Actions-only OR low authoritativeness with minimal resources
    - Draft: Failed QA or insufficient coverage
    """
    
    # Tier thresholds
    GOLD_THRESHOLD = 80.0
    SILVER_THRESHOLD = 60.0
    BRONZE_THRESHOLD = 40.0
    
    # Scoring weights
    WEIGHT_AUTH = 0.4
    WEIGHT_RESOURCES = 0.3
    WEIGHT_QA = 0.3
    
    def __init__(self):
        """Initialize calculator"""
        pass
    
    def calculate(
        self,
        authoritativeness_score: float,  # 0-1
        resource_count: int,
        has_actions: bool,
        qa_passed: bool,
        qa_details: Optional[Dict[str, Any]] = None,
    ) -> QualityScore:
        """
        Calculate quality tier.
        
        Args:
            authoritativeness_score: Context7 authoritativeness (0-1)
            resource_count: Number of curated resources generated
            has_actions: Whether connector has curated actions
            qa_passed: Whether QA tests passed
            qa_details: Optional QA test details
        
        Returns:
            QualityScore with tier, score, and reasons
        """
        qa_details = qa_details or {}
        reasons = []
        
        # Component 1: Authoritativeness (0-100)
        auth_score = authoritativeness_score * 100
        reasons.append(f"Authoritativeness: {auth_score:.1f}/100")
        
        # Component 2: Resource coverage (0-100)
        # Score based on resource count (diminishing returns after 15)
        resource_score = min(100.0, (resource_count / 15.0) * 100)
        reasons.append(f"Resource coverage: {resource_score:.1f}/100 ({resource_count} resources)")
        
        # Component 3: QA score (0-100)
        qa_score = 100.0 if qa_passed else 0.0
        if qa_passed:
            reasons.append(f"QA: PASSED ({qa_score:.1f}/100)")
        else:
            reasons.append(f"QA: FAILED ({qa_score:.1f}/100)")
            if qa_details.get("failures"):
                for failure in qa_details["failures"][:3]:  # Show top 3
                    reasons.append(f"  - {failure}")
        
        # Weighted score
        final_score = (
            self.WEIGHT_AUTH * auth_score +
            self.WEIGHT_RESOURCES * resource_score +
            self.WEIGHT_QA * qa_score
        )
        
        # Determine tier
        tier, tier_reason, capabilities = self._determine_tier(
            score=final_score,
            authoritativeness_score=authoritativeness_score,
            resource_count=resource_count,
            has_actions=has_actions,
            qa_passed=qa_passed,
        )
        
        reasons.insert(0, tier_reason)
        reasons.insert(1, f"Overall score: {final_score:.1f}/100")
        
        return QualityScore(
            tier=tier,
            score=final_score,
            authoritativeness_score=auth_score,
            resource_count_score=resource_score,
            qa_score=qa_score,
            reasons=reasons,
            capabilities=capabilities,
        )
    
    def _determine_tier(
        self,
        score: float,
        authoritativeness_score: float,
        resource_count: int,
        has_actions: bool,
        qa_passed: bool,
    ) -> tuple[QualityTier, str, Dict[str, bool]]:
        """
        Determine tier based on score and additional constraints.
        
        Returns:
            (tier, reason, capabilities)
        """
        # QA must pass for any non-draft tier.
        # If QA passed but resources are empty, we still mark BRONZE (not DRAFT),
        # because the connector is usable with manual resource selection/actions.
        if not qa_passed:
            return (
                QualityTier.DRAFT,
                "DRAFT: QA tests failed",
                {"sync_collections": False, "actions": False},
            )

        # Gold: strong docs + enough safe resources + actions
        if authoritativeness_score >= 0.7 and resource_count >= 5 and has_actions:
            return (
                QualityTier.GOLD,
                "GOLD: High authoritativeness + curated resources + curated actions",
                {"sync_collections": True, "actions": True},
            )

        # Silver: decent docs + enough resources (actions optional)
        if authoritativeness_score >= 0.4 and resource_count >= 2:
            return (
                QualityTier.SILVER,
                "SILVER: Adequate authoritativeness + curated resources",
                {"sync_collections": True, "actions": has_actions},
            )

        # Bronze: QA passed but resources/actions are limited or missing.
        # This is the default safe tier when the connector is usable but not well-modeled.
        return (
            QualityTier.BRONZE,
            "BRONZE: QA passed but limited curated collections; manual configuration may be required",
            {"sync_collections": resource_count > 0, "actions": has_actions},
        )

