#!/usr/bin/env python3
"""
Performance Benchmarks for UI

Tests load times, rendering performance, and resource usage
for all major UI components and pages.
"""

import json
import time
import requests
import statistics
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
API_URL = "http://localhost:5001"
FRONTEND_URL = "http://localhost:3000"
ITERATIONS = 5


@dataclass
class BenchmarkResult:
    """Benchmark result data class."""
    name: str
    passed: bool
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    std_dev_ms: float
    threshold_ms: float
    iterations: int
    timestamp: str


class PerformanceBenchmarks:
    """Performance benchmark suite."""
    
    def __init__(self, api_url: str = API_URL, frontend_url: str = FRONTEND_URL):
        self.api_url = api_url
        self.frontend_url = frontend_url
        self.results: List[BenchmarkResult] = []
    
    def _measure_api_latency(self, endpoint: str, iterations: int = ITERATIONS) -> List[float]:
        """Measure API endpoint latency."""
        times = []
        for _ in range(iterations):
            try:
                start = time.time()
                response = requests.get(f"{self.api_url}{endpoint}", timeout=10)
                elapsed = (time.time() - start) * 1000
                if response.status_code == 200:
                    times.append(elapsed)
            except Exception:
                pass
        return times
    
    def _measure_page_load(self, path: str, iterations: int = ITERATIONS) -> List[float]:
        """Measure page load time."""
        times = []
        for _ in range(iterations):
            try:
                start = time.time()
                response = requests.get(f"{self.frontend_url}{path}", timeout=30)
                elapsed = (time.time() - start) * 1000
                if response.status_code == 200:
                    times.append(elapsed)
            except Exception:
                pass
        return times
    
    def _record_benchmark(
        self,
        name: str,
        times: List[float],
        threshold_ms: float
    ):
        """Record benchmark results."""
        if not times:
            result = BenchmarkResult(
                name=name,
                passed=False,
                avg_time_ms=0,
                min_time_ms=0,
                max_time_ms=0,
                std_dev_ms=0,
                threshold_ms=threshold_ms,
                iterations=0,
                timestamp=datetime.now().isoformat()
            )
            self.results.append(result)
            print(f"❌ {name}: No measurements collected")
            return
        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0
        passed = avg_time <= threshold_ms
        
        result = BenchmarkResult(
            name=name,
            passed=passed,
            avg_time_ms=avg_time,
            min_time_ms=min_time,
            max_time_ms=max_time,
            std_dev_ms=std_dev,
            threshold_ms=threshold_ms,
            iterations=len(times),
            timestamp=datetime.now().isoformat()
        )
        self.results.append(result)
        
        status = "✅" if passed else "❌"
        print(f"{status} {name}:")
        print(f"   Avg: {avg_time:.1f}ms | Min: {min_time:.1f}ms | Max: {max_time:.1f}ms | StdDev: {std_dev:.1f}ms")
        print(f"   Threshold: {threshold_ms}ms | {'PASS' if passed else 'FAIL'}")
    
    # ==========================================
    # API LATENCY BENCHMARKS
    # ==========================================
    
    def benchmark_pipelines_api(self):
        """Benchmark pipelines list API."""
        times = self._measure_api_latency("/api/v1/pipelines")
        self._record_benchmark("API: List Pipelines", times, threshold_ms=200)
    
    def benchmark_connections_api(self):
        """Benchmark connections list API."""
        times = self._measure_api_latency("/api/v1/connections")
        self._record_benchmark("API: List Connections", times, threshold_ms=200)
    
    def benchmark_agents_api(self):
        """Benchmark agents health API."""
        times = self._measure_api_latency("/api/v1/agents/health")
        self._record_benchmark("API: Agents Health", times, threshold_ms=300)
    
    def benchmark_cdc_api(self):
        """Benchmark CDC connectors API."""
        times = self._measure_api_latency("/api/v1/cdc/connectors")
        self._record_benchmark("API: CDC Connectors", times, threshold_ms=500)
    
    def benchmark_chat_api(self):
        """Benchmark chat endpoint."""
        times = []
        for _ in range(3):  # Fewer iterations for chat
            try:
                start = time.time()
                response = requests.post(
                    f"{self.api_url}/api/v1/chat",
                    json={"message": "hello", "session_id": "benchmark"},
                    timeout=30
                )
                elapsed = (time.time() - start) * 1000
                if response.status_code in [200, 201]:
                    times.append(elapsed)
            except Exception:
                pass
        self._record_benchmark("API: Chat Response", times, threshold_ms=5000)
    
    # ==========================================
    # PAGE LOAD BENCHMARKS
    # ==========================================
    
    def benchmark_dashboard_page(self):
        """Benchmark dashboard page load."""
        times = self._measure_page_load("/dashboard")
        self._record_benchmark("Page: Dashboard", times, threshold_ms=1000)
    
    def benchmark_pipelines_page(self):
        """Benchmark pipelines page load."""
        times = self._measure_page_load("/dashboard/pipelines")
        self._record_benchmark("Page: Pipelines List", times, threshold_ms=1500)
    
    def benchmark_connections_page(self):
        """Benchmark connections page load."""
        times = self._measure_page_load("/dashboard/connections")
        self._record_benchmark("Page: Connections", times, threshold_ms=1500)
    
    def benchmark_agents_page(self):
        """Benchmark agents page load."""
        times = self._measure_page_load("/dashboard/agents")
        self._record_benchmark("Page: Agents Activity", times, threshold_ms=1500)
    
    def benchmark_topology_page(self):
        """Benchmark topology page load."""
        times = self._measure_page_load("/dashboard/topology")
        self._record_benchmark("Page: Topology Map", times, threshold_ms=2000)
    
    def benchmark_chat_page(self):
        """Benchmark chat page load."""
        times = self._measure_page_load("/")
        self._record_benchmark("Page: Chat", times, threshold_ms=1000)
    
    # ==========================================
    # CONCURRENT LOAD BENCHMARKS
    # ==========================================
    
    def benchmark_concurrent_api_calls(self):
        """Benchmark multiple concurrent API calls."""
        endpoints = [
            "/api/v1/pipelines",
            "/api/v1/connections",
            "/api/v1/agents/health",
        ]
        
        def call_endpoint(endpoint):
            try:
                start = time.time()
                response = requests.get(f"{self.api_url}{endpoint}", timeout=10)
                return (time.time() - start) * 1000 if response.status_code == 200 else None
            except:
                return None
        
        times = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in range(3):
                start = time.time()
                futures = [executor.submit(call_endpoint, ep) for ep in endpoints * 3]
                for future in as_completed(futures):
                    pass
                total_time = (time.time() - start) * 1000
                times.append(total_time)
        
        self._record_benchmark("Concurrent: 9 API Calls", times, threshold_ms=3000)
    
    def benchmark_parallel_page_loads(self):
        """Benchmark parallel page loads."""
        pages = [
            "/dashboard",
            "/dashboard/pipelines",
            "/dashboard/connections",
        ]
        
        def load_page(path):
            try:
                start = time.time()
                response = requests.get(f"{self.frontend_url}{path}", timeout=30)
                return (time.time() - start) * 1000 if response.status_code == 200 else None
            except:
                return None
        
        times = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for _ in range(3):
                start = time.time()
                futures = [executor.submit(load_page, p) for p in pages]
                for future in as_completed(futures):
                    pass
                total_time = (time.time() - start) * 1000
                times.append(total_time)
        
        self._record_benchmark("Concurrent: 3 Page Loads", times, threshold_ms=5000)
    
    # ==========================================
    # DATA SIZE BENCHMARKS
    # ==========================================
    
    def benchmark_large_pipeline_list(self):
        """Benchmark API response time with large data."""
        # First, get current pipeline count
        try:
            response = requests.get(f"{self.api_url}/api/v1/pipelines")
            if response.status_code == 200:
                count = len(response.json().get("pipelines", []))
                print(f"   Current pipeline count: {count}")
        except:
            pass
        
        # Measure with pagination
        times = []
        for _ in range(ITERATIONS):
            try:
                start = time.time()
                response = requests.get(
                    f"{self.api_url}/api/v1/pipelines",
                    params={"limit": 100, "offset": 0}
                )
                elapsed = (time.time() - start) * 1000
                if response.status_code == 200:
                    times.append(elapsed)
            except:
                pass
        
        self._record_benchmark("API: Large Pipeline List (limit=100)", times, threshold_ms=500)
    
    def benchmark_filtered_data(self):
        """Benchmark filtered API responses."""
        times = []
        for _ in range(ITERATIONS):
            try:
                start = time.time()
                response = requests.get(
                    f"{self.api_url}/api/v1/pipelines",
                    params={"status": "running"}
                )
                elapsed = (time.time() - start) * 1000
                if response.status_code == 200:
                    times.append(elapsed)
            except:
                pass
        
        self._record_benchmark("API: Filtered Pipelines", times, threshold_ms=300)
    
    # ==========================================
    # MEMORY AND PAYLOAD BENCHMARKS
    # ==========================================
    
    def benchmark_response_size(self):
        """Benchmark API response sizes."""
        endpoints = {
            "Pipelines": "/api/v1/pipelines",
            "Connections": "/api/v1/connections",
            "Agents Health": "/api/v1/agents/health",
        }
        
        print("\n📦 Response Size Analysis:")
        for name, endpoint in endpoints.items():
            try:
                response = requests.get(f"{self.api_url}{endpoint}")
                if response.status_code == 200:
                    size_kb = len(response.content) / 1024
                    print(f"   {name}: {size_kb:.2f} KB")
            except:
                print(f"   {name}: Error")
    
    # ==========================================
    # TIME TO FIRST BYTE (TTFB) BENCHMARKS
    # ==========================================
    
    def benchmark_ttfb(self):
        """Benchmark Time to First Byte for critical endpoints."""
        endpoints = [
            ("/api/v1/pipelines", "Pipelines API"),
            ("/dashboard", "Dashboard Page"),
        ]
        
        for endpoint, name in endpoints:
            times = []
            url = self.api_url if endpoint.startswith("/api") else self.frontend_url
            
            for _ in range(ITERATIONS):
                try:
                    start = time.time()
                    response = requests.get(
                        f"{url}{endpoint}",
                        stream=True,
                        timeout=30
                    )
                    # Read first chunk
                    next(response.iter_content(chunk_size=1024), None)
                    ttfb = (time.time() - start) * 1000
                    times.append(ttfb)
                except:
                    pass
            
            threshold = 200 if endpoint.startswith("/api") else 500
            self._record_benchmark(f"TTFB: {name}", times, threshold_ms=threshold)
    
    # ==========================================
    # RUN ALL BENCHMARKS
    # ==========================================
    
    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Run all performance benchmarks."""
        print("\n" + "=" * 60)
        print("⚡ PERFORMANCE BENCHMARKS")
        print("=" * 60 + "\n")
        
        print("📡 API Latency Benchmarks")
        print("-" * 40)
        self.benchmark_pipelines_api()
        self.benchmark_connections_api()
        self.benchmark_agents_api()
        self.benchmark_cdc_api()
        self.benchmark_chat_api()
        
        print("\n📄 Page Load Benchmarks")
        print("-" * 40)
        self.benchmark_dashboard_page()
        self.benchmark_pipelines_page()
        self.benchmark_connections_page()
        self.benchmark_agents_page()
        self.benchmark_topology_page()
        self.benchmark_chat_page()
        
        print("\n🔄 Concurrent Load Benchmarks")
        print("-" * 40)
        self.benchmark_concurrent_api_calls()
        self.benchmark_parallel_page_loads()
        
        print("\n📊 Data Size Benchmarks")
        print("-" * 40)
        self.benchmark_large_pipeline_list()
        self.benchmark_filtered_data()
        self.benchmark_response_size()
        
        print("\n⏱️ TTFB Benchmarks")
        print("-" * 40)
        self.benchmark_ttfb()
        
        # Summary
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        
        # Calculate aggregate stats
        all_times = [r.avg_time_ms for r in self.results if r.avg_time_ms > 0]
        avg_overall = statistics.mean(all_times) if all_times else 0
        
        print("\n" + "=" * 60)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 60)
        print(f"Total Benchmarks: {total}")
        print(f"Passed: {passed} ✅")
        print(f"Failed: {failed} ❌")
        print(f"Pass Rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")
        print(f"Average Response Time: {avg_overall:.1f}ms")
        print("=" * 60 + "\n")
        
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": passed/total*100 if total > 0 else 0,
            "average_response_time_ms": avg_overall,
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "avg_time_ms": r.avg_time_ms,
                    "min_time_ms": r.min_time_ms,
                    "max_time_ms": r.max_time_ms,
                    "std_dev_ms": r.std_dev_ms,
                    "threshold_ms": r.threshold_ms,
                    "iterations": r.iterations,
                    "timestamp": r.timestamp
                }
                for r in self.results
            ]
        }


def main():
    """Run performance benchmarks."""
    benchmarks = PerformanceBenchmarks()
    summary = benchmarks.run_all_benchmarks()
    
    # Save results
    with open("performance_benchmark_results.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    # Generate markdown report
    report = generate_markdown_report(summary)
    with open("performance_benchmark_report.md", "w") as f:
        f.write(report)
    
    exit(0 if summary["failed"] == 0 else 1)


def generate_markdown_report(summary: Dict[str, Any]) -> str:
    """Generate a markdown report from benchmark results."""
    report = f"""# Performance Benchmark Report

Generated: {datetime.now().isoformat()}

## Summary

| Metric | Value |
|--------|-------|
| Total Benchmarks | {summary['total']} |
| Passed | {summary['passed']} |
| Failed | {summary['failed']} |
| Pass Rate | {summary['pass_rate']:.1f}% |
| Avg Response Time | {summary['average_response_time_ms']:.1f}ms |

## Results

| Benchmark | Avg (ms) | Min (ms) | Max (ms) | Threshold | Status |
|-----------|----------|----------|----------|-----------|--------|
"""
    
    for r in summary['results']:
        status = "✅ Pass" if r['passed'] else "❌ Fail"
        report += f"| {r['name']} | {r['avg_time_ms']:.1f} | {r['min_time_ms']:.1f} | {r['max_time_ms']:.1f} | {r['threshold_ms']} | {status} |\n"
    
    report += """
## Recommendations

Based on the benchmark results:

1. **API Latency**: Ensure all API endpoints respond under 200ms
2. **Page Load**: Target under 1.5s for all pages
3. **Concurrent Load**: Monitor performance under load
4. **TTFB**: Keep Time to First Byte under 500ms

## Next Steps

- Monitor trends over time
- Set up automated alerting for threshold breaches
- Profile slow endpoints for optimization opportunities
"""
    
    return report


if __name__ == "__main__":
    main()

