#!/usr/bin/env python3
"""
Test script for Connector Resolver Agent

Tests the AI agent's ability to resolve connector names with:
- Exact names
- Typos
- Brand variations
- Abbreviations
- Compound names
- Natural language
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from agents.connector_resolver.agent import ConnectorResolverAgent

def test_resolver():
    """Test the connector resolver with various inputs"""
    
    print("=" * 80)
    print("CONNECTOR RESOLVER AGENT - TEST SUITE")
    print("=" * 80)
    print()
    
    agent = ConnectorResolverAgent()
    
    # Test cases: (input, expected_canonical, description)
    test_cases = [
        # Exact names
        ("mysql", "mysql", "Exact name"),
        ("postgresql", "postgresql", "Exact name (full)"),
        
        # Typos
        ("postgre", "postgresql", "Typo - missing 'sql'"),
        ("mysq", "mysql", "Typo - missing 'l'"),
        ("amazons3", "aws_s3", "Typo - compound without separator"),
        ("elsticsearch", None, "Typo - elasticsearch"),
        
        # Brand variations
        ("amazon s3", "aws_s3", "Brand variation - Amazon S3"),
        ("AWS S3", "aws_s3", "Brand variation - AWS S3 (caps)"),
        ("google bigquery", None, "Brand variation - Google BigQuery"),
        
        # Abbreviations
        ("s3", "aws_s3", "Abbreviation - S3"),
        ("pg", "postgresql", "Abbreviation - PostgreSQL"),
        ("bq", None, "Abbreviation - BigQuery"),
        
        # Natural language
        ("amazon's object storage", "aws_s3", "Natural language - S3"),
        ("google's data warehouse", None, "Natural language - BigQuery"),
        ("stripe payment api", "stripe", "Natural language - Stripe"),
        
        # Unknown connectors
        ("my-custom-db", None, "Unknown - should suggest or allow new"),
        ("shopify", "shopify", "Known API - Shopify"),
    ]
    
    results = {
        "passed": 0,
        "failed": 0,
        "uncertain": 0,
        "total": len(test_cases)
    }
    
    for i, (input_name, expected, description) in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"TEST {i}/{len(test_cases)}: {description}")
        print(f"Input: '{input_name}'")
        if expected:
            print(f"Expected: '{expected}'")
        print("-" * 80)
        
        try:
            result = agent.resolve(input_name)
            
            print(f"Canonical: {result.canonical}")
            print(f"Confidence: {result.confidence:.2f}")
            print(f"Reasoning: {result.reasoning}")
            
            if result.suggestions:
                print(f"\nSuggestions ({len(result.suggestions)}):")
                for j, sug in enumerate(result.suggestions[:3], 1):
                    print(f"  {j}. {sug.get('name')} - {sug.get('reason', 'N/A')}")
            
            # Determine test result
            if result.confidence >= 0.9:
                if expected and result.canonical == expected:
                    print("\n✅ PASS - Correct match with high confidence")
                    results["passed"] += 1
                elif expected and result.canonical != expected:
                    print(f"\n❌ FAIL - Expected '{expected}', got '{result.canonical}'")
                    results["failed"] += 1
                elif not expected:
                    print(f"\n✅ PASS - High confidence result: {result.canonical}")
                    results["passed"] += 1
            elif result.confidence >= 0.7:
                print("\n⚠️  UNCERTAIN - Medium confidence, would show suggestions")
                results["uncertain"] += 1
            else:
                print("\n⚠️  LOW CONFIDENCE - Would show suggestions + generate new")
                results["uncertain"] += 1
                
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            results["failed"] += 1
    
    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total Tests: {results['total']}")
    print(f"✅ Passed: {results['passed']}")
    print(f"⚠️  Uncertain: {results['uncertain']}")
    print(f"❌ Failed: {results['failed']}")
    print(f"Success Rate: {(results['passed'] / results['total'] * 100):.1f}%")
    print()

if __name__ == "__main__":
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set it before running tests:")
        print("  export OPENAI_API_KEY='your-key-here'")
        sys.exit(1)
    
    test_resolver()

