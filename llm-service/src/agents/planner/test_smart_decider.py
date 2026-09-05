#!/usr/bin/env python3
"""
Test suite for SmartPipelineDecider (Phase 2)

Run with: python -m pytest test_smart_decider.py -v
Or directly: python test_smart_decider.py
"""

import unittest
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.agents.planner.strategies import (
    SmartPipelineDecider,
    SmartStrategyDecision,
    DataStatistics,
    TableStats,
    PipelinePhase,
    get_smart_pipeline_decider,
)


class TestSmartPipelineDecider(unittest.TestCase):
    """Test cases for SmartPipelineDecider"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.decider = SmartPipelineDecider()
    
    def _create_stats(self, total_rows: int, size_mb: float, table_count: int = 1) -> DataStatistics:
        """Helper to create DataStatistics"""
        stats = DataStatistics()
        rows_per_table = total_rows // table_count
        size_per_table = size_mb / table_count
        
        for i in range(table_count):
            table = TableStats(
                name=f"table_{i}",
                row_count=rows_per_table,
                estimated_size_mb=size_per_table,
            )
            stats.add_table(table)
        
        return stats
    
    def test_small_table_uses_cdc_snapshot(self):
        """Small tables (< 100K rows) should use standard CDC snapshot"""
        stats = self._create_stats(total_rows=50000, size_mb=25)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="mysql",
        )
        
        self.assertEqual(decision.strategy, "cdc_snapshot")
        self.assertFalse(decision.requires_user_confirmation)
        self.assertEqual(len(decision.phases), 1)
        self.assertEqual(decision.phases[0].phase_type, "cdc_snapshot")
    
    def test_medium_table_uses_optimized_cdc_snapshot(self):
        """Medium tables (100K - 10M rows) should use CDC snapshot with optimizations"""
        stats = self._create_stats(total_rows=5_000_000, size_mb=2500)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="postgresql",
        )
        
        self.assertEqual(decision.strategy, "cdc_snapshot")
        self.assertEqual(len(decision.phases), 1)
        # Should have optimization config
        self.assertIn("snapshot_fetch_size", decision.phases[0].config)
    
    def test_large_table_uses_hybrid(self):
        """Large tables (> 10M rows) should recommend hybrid approach"""
        stats = self._create_stats(total_rows=50_000_000, size_mb=25000)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="mysql",
        )
        
        self.assertEqual(decision.strategy, "hybrid")
        self.assertTrue(decision.requires_user_confirmation)
        self.assertGreater(len(decision.phases), 1)
        
        # Should have batch_export and cdc_catchup phases
        phase_types = [p.phase_type for p in decision.phases]
        self.assertIn("batch_export", phase_types)
        self.assertIn("cdc_catchup", phase_types)
    
    def test_huge_table_by_size_uses_hybrid(self):
        """Tables > 100GB should recommend hybrid regardless of row count"""
        # 100GB+ with relatively few rows (wide rows)
        stats = self._create_stats(total_rows=5_000_000, size_mb=120 * 1024)  # 120GB
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="postgresql",
        )
        
        self.assertEqual(decision.strategy, "hybrid")
        self.assertTrue(decision.requires_user_confirmation)
    
    def test_batch_mode_uses_batch(self):
        """Batch sync mode should use batch strategy"""
        stats = self._create_stats(total_rows=1_000_000, size_mb=500)
        
        decision = self.decider.decide_strategy(
            sync_mode="batch",
            data_stats=stats,
            source_type="mysql",
        )
        
        self.assertEqual(decision.strategy, "batch")
        self.assertFalse(decision.requires_user_confirmation)
    
    def test_streaming_only_mode(self):
        """streaming_only CDC mode should skip snapshot"""
        stats = self._create_stats(total_rows=10_000_000, size_mb=5000)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="mysql",
            cdc_mode="streaming_only",
        )
        
        self.assertEqual(decision.strategy, "cdc_streaming_only")
        self.assertEqual(len(decision.phases), 1)
        self.assertEqual(decision.phases[0].config.get("snapshot_mode"), "never")
    
    def test_hybrid_has_correct_phases(self):
        """Hybrid strategy should have all required phases"""
        stats = self._create_stats(total_rows=100_000_000, size_mb=50000)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="postgresql",
        )
        
        self.assertEqual(decision.strategy, "hybrid")
        
        # Check phase order
        phases = decision.phases
        self.assertGreaterEqual(len(phases), 3)
        
        # First phase should capture position
        self.assertEqual(phases[0].phase_type, "capture_position")
        
        # Should have batch_export
        batch_phases = [p for p in phases if p.phase_type == "batch_export"]
        self.assertEqual(len(batch_phases), 1)
        
        # Should have cdc_catchup
        cdc_phases = [p for p in phases if p.phase_type == "cdc_catchup"]
        self.assertEqual(len(cdc_phases), 1)
        
        # CDC catchup should use snapshot_mode: never
        self.assertEqual(cdc_phases[0].config.get("snapshot_mode"), "never")
    
    def test_recommendation_has_content(self):
        """All strategies should have meaningful recommendations"""
        for total_rows in [10000, 5_000_000, 100_000_000]:
            stats = self._create_stats(total_rows=total_rows, size_mb=total_rows / 100)
            
            decision = self.decider.decide_strategy(
                sync_mode="cdc",
                data_stats=stats,
                source_type="mysql",
            )
            
            self.assertIsNotNone(decision.recommendation)
            self.assertGreater(len(decision.recommendation), 50)
            self.assertGreater(len(decision.benefits), 0)
    
    def test_data_stats_calculation(self):
        """DataStatistics should correctly aggregate table stats"""
        stats = DataStatistics()
        
        stats.add_table(TableStats(name="users", row_count=100000, estimated_size_mb=50))
        stats.add_table(TableStats(name="orders", row_count=500000, estimated_size_mb=250))
        stats.add_table(TableStats(name="products", row_count=10000, estimated_size_mb=5))
        
        self.assertEqual(stats.total_rows, 610000)
        self.assertEqual(stats.estimated_size_mb, 305)
        self.assertEqual(stats.table_count, 3)
        self.assertEqual(stats.largest_table, "orders")
        self.assertEqual(stats.largest_table_rows, 500000)
    
    def test_estimate_from_discovery(self):
        """Should correctly estimate stats from discovery response"""
        tables = [
            {"name": "users", "row_count": 100000, "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "email", "type": "varchar"},
                {"name": "created_at", "type": "timestamp"},
            ]},
            {"name": "orders", "row_count": 500000, "columns": [
                {"name": "id", "type": "bigint"},
                {"name": "user_id", "type": "bigint"},
                {"name": "total", "type": "decimal"},
            ]},
        ]
        
        stats = self.decider.estimate_data_size_from_discovery(tables)
        
        self.assertEqual(stats.total_rows, 600000)
        self.assertEqual(stats.table_count, 2)
        self.assertGreater(stats.estimated_size_mb, 0)
    
    def test_to_dict_serialization(self):
        """SmartStrategyDecision should serialize to dict correctly"""
        stats = self._create_stats(total_rows=50_000_000, size_mb=25000)
        
        decision = self.decider.decide_strategy(
            sync_mode="cdc",
            data_stats=stats,
            source_type="mysql",
        )
        
        as_dict = decision.to_dict()
        
        self.assertIn("strategy", as_dict)
        self.assertIn("phases", as_dict)
        self.assertIn("recommendation", as_dict)
        self.assertIn("data_stats", as_dict)
        
        # Phases should be dicts too
        for phase in as_dict["phases"]:
            self.assertIn("phase", phase)
            self.assertIn("type", phase)
            self.assertIn("description", phase)


class TestSmartPipelineDeciderSingleton(unittest.TestCase):
    """Test the singleton pattern for SmartPipelineDecider"""
    
    def test_get_smart_pipeline_decider_singleton(self):
        """get_smart_pipeline_decider should return the same instance"""
        decider1 = get_smart_pipeline_decider()
        decider2 = get_smart_pipeline_decider()
        self.assertIs(decider1, decider2)


if __name__ == "__main__":
    print("=" * 60)
    print("SmartPipelineDecider Test Suite (Phase 2)")
    print("=" * 60)
    
    unittest.main(verbosity=2)
