#!/usr/bin/env python3
"""
Round 6 Deep Test - 长时间运行稳定性、极端负载、边界条件
Focus: Memory leaks, unbounded growth, edge cases, extreme load
"""

import asyncio
import sys
import os
import time
import json
import traceback
import tempfile
import shutil
import tracemalloc
import gc
from collections import deque
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ============================================================
# Test Infrastructure
# ============================================================

@dataclass
class TestResult:
    test_id: str
    name: str
    passed: bool
    bug_found: bool = False
    bug_description: str = ""
    exception: str = ""
    details: str = ""

results: List[TestResult] = []

def report(result: TestResult):
    results.append(result)
    status = "✅ PASS" if result.passed else ("🐛 BUG" if result.bug_found else "❌ FAIL")
    print(f"\n{'='*60}")
    print(f"{status} | {result.test_id}: {result.name}")
    if result.bug_found:
        print(f"  BUG: {result.bug_description}")
    if result.exception:
        print(f"  Exception: {result.exception[:500]}")
    if result.details:
        print(f"  Details: {result.details[:500]}")
    print(f"{'='*60}")


def make_mock_knowledge_engine():
    mock = MagicMock()
    mock.record_experience = MagicMock()
    mock.query_experience = MagicMock(return_value=[])
    mock.get_statistics = MagicMock(return_value={})
    mock.add_error_pattern = MagicMock(return_value=asyncio.coroutine(lambda: None)())
    return mock


async def make_real_state_manager(tmpdir):
    from storage.persistent_state_manager import PersistentStateManager
    mgr = PersistentStateManager(tmpdir)
    await mgr.initialize()
    return mgr


# ============================================================
# TEST A: Knowledge Graph Unbounded Growth (High Priority)
# ============================================================

async def test_A1_knowledge_graph_unbounded_growth():
    """BUG-KG-1: Non-execution nodes (optimization, error, pattern) are never cleaned"""
    test_id = "A1"
    name = "Knowledge graph unbounded node growth"

    try:
        from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine

        tmpdir = tempfile.mkdtemp()
        try:
            kg = KnowledgeGraphIntegrationEngine(storage_path=tmpdir)
            await kg.initialize()

            # Simulate continuous operation - add many nodes of different types
            initial_stats = kg.get_statistics()
            initial_node_count = initial_stats.get("total_nodes", 0)

            # Add 500 optimization nodes (these should be cleaned but aren't)
            for i in range(500):
                kg.add_optimization_record(
                    rule_id=f"rule_{i}",
                    target_component="test",
                    parameters={"param": i},
                    confidence=0.8
                )

            # Add 500 error pattern nodes
            for i in range(500):
                kg.add_error_pattern(
                    error_type="test_error",
                    context={"index": i},
                    resolution="test"
                )

            # Add 500 pattern recognition nodes
            for i in range(500):
                kg.record_pattern(
                    pattern_type="test_pattern",
                    entities=[f"entity_{i}"],
                    confidence=0.9
                )

            # Trigger cleanup
            await kg._do_cleanup()

            final_stats = kg.get_statistics()
            final_node_count = final_stats.get("total_nodes", 0)
            node_types = final_stats.get("node_types", {})

            await kg.shutdown()

            # Check if non-execution nodes were cleaned
            non_execution_nodes = sum(
                count for ntype, count in node_types.items()
                if ntype != "execution"
            )

            if non_execution_nodes > 1000:  # Most should have been cleaned
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"BUG-KG-1 confirmed: {non_execution_nodes} non-execution nodes accumulated. "
                                   f"Cleanup only removes 'execution' type nodes. Other types (optimization, error, pattern) "
                                   f"grow unbounded and will eventually cause memory exhaustion.",
                    details=f"Nodes: {initial_node_count} -> {final_node_count}, Types: {node_types}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Node growth controlled. Final: {final_node_count}, Types: {node_types}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST B: Optimization Rules Unbounded Accumulation (High Priority)
# ============================================================

async def test_B1_optimization_rules_unbounded():
    """BUG-OPT-1: Optimization rules accumulate without limit"""
    test_id = "B1"
    name = "Optimization rules unbounded accumulation"

    try:
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer

        mock_ke = make_mock_knowledge_engine()
        mock_sm = MagicMock()
        mock_sm.load_optimization_rules = MagicMock(return_value=asyncio.coroutine(lambda: [])())
        mock_sm.save_optimization_rules = MagicMock(return_value=asyncio.coroutine(lambda: None)())

        optimizer = IntelligenceOptimizationAnalyzer(knowledge_engine=mock_ke, state_manager=mock_sm)
        await optimizer.initialize()

        # Simulate continuous operation with many bottlenecks
        # Each bottleneck generates rules
        for cycle in range(100):
            for i in range(10):
                optimizer.feed_execution_data({
                    "component": f"component_{i}",
                    "metric_type": "bottleneck",
                    "details": {
                        "avg_execution_time": 5.0 + cycle * 0.1,
                        "error_rate": 0.3,
                        "queue_depth": 100,
                        "throughput": 10
                    }
                })

            # Trigger analysis
            await optimizer._analyze_execution_data()

        rule_count = len(optimizer._optimization_rules)
        effectiveness_count = len(optimizer._rule_effectiveness)

        await optimizer.shutdown()

        # Check if rules are accumulating
        if rule_count > 500:  # Should have cleanup mechanism
            report(TestResult(
                test_id=test_id, name=name, passed=False, bug_found=True,
                bug_description=f"BUG-OPT-1 confirmed: {rule_count} optimization rules accumulated without limit. "
                               f"Rules are only deleted when success_rate < 0.3 AND applied_count > 20. "
                               f"Inactive rules accumulate forever, causing memory exhaustion.",
                details=f"Rules: {rule_count}, Effectiveness histories: {effectiveness_count}"
            ))
        else:
            report(TestResult(
                test_id=test_id, name=name, passed=True,
                details=f"Rule count controlled: {rule_count}"
            ))
    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST C: Task Execution Worker Memory Leak (High Priority)
# ============================================================

async def test_C1_task_worker_memory_leak():
    """BUG-EXEC-1: _tasks dict grows if cleanup can't keep up"""
    test_id = "C1"
    name = "Task worker memory leak under high load"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_knowledge_engine()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_real_state_manager(tmpdir)

            # Create worker with small cleanup interval to test race condition
            worker = TaskExecutionWorker(
                knowledge_engine=mock_ke,
                state_manager=state_mgr,
                max_task_retention=100  # Small limit
            )
            await worker.initialize()

            # Rapidly submit many tasks
            for i in range(200):
                await worker.submit_task(
                    name=f"task_{i}",
                    payload={"index": i},
                    task_type="default"
                )

            # Wait a bit for processing
            await asyncio.sleep(0.5)

            # Check _tasks size
            tasks_count = len(worker._tasks)

            # Force cleanup
            await worker._cleanup_loop()

            tasks_after_cleanup = len(worker._tasks)

            await worker.shutdown()
            await state_mgr.shutdown()

            if tasks_count > 150:  # Should have been cleaned
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"BUG-EXEC-1 confirmed: {tasks_count} tasks in _tasks dict. "
                                   f"Cleanup loop runs every 10s, but under high load tasks accumulate faster. "
                                   f"After forced cleanup: {tasks_after_cleanup}. Memory will grow unbounded.",
                    details=f"Tasks before cleanup: {tasks_count}, after: {tasks_after_cleanup}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Task count controlled: {tasks_count} -> {tasks_after_cleanup}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST D: Floating Point Precision Loss in Statistics (Medium Priority)
# ============================================================

async def test_D1_float_precision_loss():
    """BUG-STAT-1: Average execution time calculation loses precision over time"""
    test_id = "D1"
    name = "Floating point precision loss in statistics"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_knowledge_engine()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_real_state_manager(tmpdir)
            worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            await worker.initialize()

            # Simulate many completed tasks with varying durations
            # This tests the incremental average calculation
            durations = []
            for i in range(10000):
                duration = 100.0 + (i % 50)  # Varying durations around 100-150ms
                durations.append(duration)

                # Simulate task completion stats update
                total_completed = i + 1
                worker._stats["avg_execution_time"] = (
                    (worker._stats["avg_execution_time"] * (total_completed - 1) + duration)
                    / total_completed
                )

            calculated_avg = worker._stats["avg_execution_time"]
            true_avg = sum(durations) / len(durations)
            error = abs(calculated_avg - true_avg)

            await worker.shutdown()
            await state_mgr.shutdown()

            # Check precision loss
            if error > 1.0:  # More than 1ms error is significant
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"BUG-STAT-1 confirmed: Average calculation error = {error:.2f}ms. "
                                   f"Incremental average formula loses precision with large n. "
                                   f"Calculated: {calculated_avg:.2f}, True: {true_avg:.2f}. "
                                   f"After millions of tasks, statistics become meaningless.",
                    details=f"Error: {error:.4f}ms after {len(durations)} tasks"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Precision acceptable. Error: {error:.4f}ms"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST E: Guardian Recovery Loop (High Priority)
# ============================================================

async def test_E1_guardian_recovery_loop():
    """BUG-GUARD-1: Recovery failure can cause infinite loop"""
    test_id = "E1"
    name = "Guardian recovery infinite loop risk"

    try:
        from core.resident_guardian_daemon import ResidentGuardianDaemon, HealthStatus

        shutdown_event = asyncio.Event()
        guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event)
        await guardian.initialize()

        recovery_count = [0]

        async def always_failing_recovery():
            recovery_count[0] += 1
            raise RuntimeError("Simulated recovery failure")

        # Register component with failing recovery
        guardian.register_component(
            "test_component",
            heartbeat_callback=lambda: {"status": "unhealthy"},  # Always unhealthy
            recovery_callback=always_failing_recovery
        )

        # Manually trigger health check that will fail and trigger recovery
        health = guardian._component_health["test_component"]
        health.error_count = 3  # Trigger recovery threshold
        health.status = HealthStatus.UNHEALTHY

        # Try recovery multiple times
        for i in range(5):
            await guardian._trigger_recovery("test_component")
            # Check if error_count is reset (it shouldn't be after failed recovery)

        await guardian.shutdown()

        # Check recovery attempts
        if recovery_count[0] >= 5 and health.status != HealthStatus.CRITICAL:
            report(TestResult(
                test_id=test_id, name=name, passed=False, bug_found=True,
                bug_description=f"BUG-GUARD-1 confirmed: {recovery_count[0]} recovery attempts without reaching CRITICAL. "
                               f"Failed recovery doesn't increment recovery_attempts properly. "
                               f"Component stuck in UNHEALTHY/RECOVERING loop, causing infinite recovery attempts.",
                details=f"Recovery attempts: {recovery_count[0]}, Final status: {health.status}"
            ))
        else:
            report(TestResult(
                test_id=test_id, name=name, passed=True,
                details=f"Recovery loop controlled. Attempts: {recovery_count[0]}, Status: {health.status}"
            ))

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST F: Orchestrator Layer Tasks Leak (High Priority)
# ============================================================

async def test_F1_orchestrator_layer_tasks_leak():
    """BUG-ORCH-2: _layer_tasks list grows without cleanup"""
    test_id = "F1"
    name = "Orchestrator layer tasks list leak"

    try:
        from core.dual_process_orchestrator import DualProcessOrchestrator
        from modules.task_execution_worker import TaskExecutionWorker
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
        from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
        from core.resident_guardian_daemon import ResidentGuardianDaemon

        tmpdir = tempfile.mkdtemp()
        try:
            mock_ke = make_mock_knowledge_engine()
            state_mgr = await make_real_state_manager(tmpdir)
            shutdown_event = asyncio.Event()

            task_worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            optimizer = IntelligenceOptimizationAnalyzer(knowledge_engine=mock_ke, state_manager=state_mgr)
            deployer = SynchronizationDeploymentPipeline(task_worker=task_worker, optimization_analyzer=optimizer)
            guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event)

            await task_worker.initialize()
            await optimizer.initialize()
            await deployer.initialize()
            await guardian.initialize()

            orchestrator = DualProcessOrchestrator(
                task_worker=task_worker,
                optimization_analyzer=optimizer,
                deployment_pipeline=deployer,
                guardian_daemon=guardian,
                knowledge_engine=mock_ke
            )

            # Simulate multiple recovery cycles
            initial_count = len(orchestrator._layer_tasks)

            for i in range(10):
                await orchestrator._recover_task_executor()
                await orchestrator._recover_optimizer()
                await orchestrator._recover_deployer()

            final_count = len(orchestrator._layer_tasks)

            await task_worker.shutdown()
            await optimizer.shutdown()
            await deployer.shutdown()
            await guardian.shutdown()
            await state_mgr.shutdown()

            # Check if _layer_tasks is growing
            if final_count > initial_count + 20:  # Should not grow this much
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"BUG-ORCH-2 confirmed: _layer_tasks grew from {initial_count} to {final_count}. "
                                   f"Recovery adds new tasks but old dead task references are never removed. "
                                   f"List grows unbounded, causing memory leak and shutdown issues.",
                    details=f"Initial: {initial_count}, Final: {final_count}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Layer tasks controlled: {initial_count} -> {final_count}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST G: Concurrent Write Conflict (Medium Priority)
# ============================================================

async def test_G1_concurrent_write_conflict():
    """BUG-STATE-1: Concurrent writes to same key may cause inconsistency"""
    test_id = "G1"
    name = "State manager concurrent write conflict"

    try:
        from storage.persistent_state_manager import PersistentStateManager

        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = PersistentStateManager(tmpdir)
            await state_mgr.initialize()

            # Simulate concurrent writes to same key
            async def write_value(value):
                await state_mgr.save_key_value("test_key", {"value": value})
                return value

            # Launch concurrent writes
            tasks = [write_value(i) for i in range(10)]
            values = await asyncio.gather(*tasks)

            # Read final value
            final_value = await state_mgr.load_key_value("test_key")

            await state_mgr.shutdown()

            # Check if all writes were processed (last write should win)
            if final_value is None:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description="BUG-STATE-1 confirmed: Concurrent writes caused data loss. "
                                   "Final value is None. File writes are not atomic/locked.",
                    details=f"Final value: {final_value}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Concurrent writes handled. Final value: {final_value}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST H: Metrics Buffer Unused (Low Priority)
# ============================================================

async def test_H1_metrics_buffer_unused():
    """BUG-METRICS-1: _metrics_buffer is filled but never read"""
    test_id = "H1"
    name = "Metrics buffer memory waste"

    try:
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer

        mock_ke = make_mock_knowledge_engine()
        mock_sm = MagicMock()
        mock_sm.load_optimization_rules = MagicMock(return_value=asyncio.coroutine(lambda: [])())

        optimizer = IntelligenceOptimizationAnalyzer(knowledge_engine=mock_ke, state_manager=mock_sm)
        await optimizer.initialize()

        # Fill metrics buffer
        for i in range(1000):
            optimizer.feed_metrics_data({"metric": i, "value": i * 10})

        buffer_size = len(optimizer._metrics_buffer)

        # Check if buffer is ever processed (it shouldn't be - this is the bug)
        has_consumer = hasattr(optimizer, '_process_metrics') or \
                      any('metrics' in method for method in dir(optimizer) if 'analyze' in method)

        await optimizer.shutdown()

        if buffer_size > 0 and not has_consumer:
            report(TestResult(
                test_id=test_id, name=name, passed=False, bug_found=True,
                bug_description=f"BUG-METRICS-1 confirmed: {buffer_size} metrics in buffer but no consumer. "
                               f"_metrics_buffer is filled by feed_metrics_data() but never read. "
                               f"Memory is wasted on data that is never used. Buffer should be removed or utilized.",
                details=f"Buffer size: {buffer_size}, Has consumer: {has_consumer}"
            ))
        else:
            report(TestResult(
                test_id=test_id, name=name, passed=True,
                details=f"Metrics buffer properly utilized"
            ))
    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST I: Extreme Load Test (High Priority)
# ============================================================

async def test_I1_extreme_load():
    """Test system behavior under extreme load"""
    test_id = "I1"
    name = "Extreme load stress test"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_knowledge_engine()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_real_state_manager(tmpdir)
            worker = TaskExecutionWorker(
                knowledge_engine=mock_ke,
                state_manager=state_mgr,
                max_concurrent_tasks=100,
                task_queue_size=1000
            )
            await worker.initialize()
            await worker.start_execution_loop()

            # Submit many tasks rapidly
            start_time = time.time()
            success_count = 0
            fail_count = 0

            for i in range(500):
                try:
                    await worker.submit_task(
                        name=f"load_test_{i}",
                        payload={"data": "x" * 1000},  # 1KB payload
                        task_type="default"
                    )
                    success_count += 1
                except Exception as e:
                    fail_count += 1

            # Wait for processing
            await asyncio.sleep(2)

            elapsed = time.time() - start_time
            stats = worker.get_statistics()

            await worker.shutdown()
            await state_mgr.shutdown()

            # Check if system handled load gracefully
            if fail_count > 100:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"System failed under load: {fail_count}/{500} task submissions failed. "
                                   f"Backpressure or queue management issues.",
                    details=f"Success: {success_count}, Failed: {fail_count}, Time: {elapsed:.2f}s, Stats: {stats}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Load handled. Success: {success_count}, Failed: {fail_count}, Time: {elapsed:.2f}s"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST J: Edge Case - Empty/None Inputs (Medium Priority)
# ============================================================

async def test_J1_edge_cases():
    """Test system behavior with edge case inputs"""
    test_id = "J1"
    name = "Edge case input handling"

    bugs_found = []

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_knowledge_engine()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_real_state_manager(tmpdir)
            worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            await worker.initialize()

            # Test 1: Empty payload
            try:
                await worker.submit_task(name="empty_payload", payload={}, task_type="default")
            except Exception as e:
                bugs_found.append(f"Empty payload failed: {e}")

            # Test 2: None in payload (should handle gracefully)
            try:
                await worker.submit_task(name="none_payload", payload={"data": None}, task_type="default")
            except Exception as e:
                bugs_found.append(f"None in payload failed: {e}")

            # Test 3: Very long task name
            try:
                long_name = "x" * 10000
                await worker.submit_task(name=long_name, payload={"test": 1}, task_type="default")
            except Exception as e:
                bugs_found.append(f"Long name failed: {e}")

            # Test 4: Special characters in name
            try:
                special_name = "test<>&\"'\n\t"
                await worker.submit_task(name=special_name, payload={"test": 1}, task_type="default")
            except Exception as e:
                bugs_found.append(f"Special chars failed: {e}")

            await worker.shutdown()
            await state_mgr.shutdown()

            if bugs_found:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"Edge case handling issues: {len(bugs_found)} failures. "
                                   f"System may crash or behave unexpectedly with unusual inputs.",
                    details="; ".join(bugs_found[:3])
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details="All edge cases handled gracefully"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# Main Test Runner
# ============================================================

async def run_all_tests():
    print("=" * 70)
    print("  ROUND 6 DEEP TEST - Long-term Stability, Extreme Load, Edge Cases")
    print("  Focus: Memory leaks, unbounded growth, boundary conditions")
    print("=" * 70)

    tests = [
        ("A1", "Knowledge graph unbounded growth", test_A1_knowledge_graph_unbounded_growth),
        ("B1", "Optimization rules unbounded", test_B1_optimization_rules_unbounded),
        ("C1", "Task worker memory leak", test_C1_task_worker_memory_leak),
        ("D1", "Float precision loss", test_D1_float_precision_loss),
        ("E1", "Guardian recovery loop", test_E1_guardian_recovery_loop),
        ("F1", "Orchestrator layer tasks leak", test_F1_orchestrator_layer_tasks_leak),
        ("G1", "Concurrent write conflict", test_G1_concurrent_write_conflict),
        ("H1", "Metrics buffer unused", test_H1_metrics_buffer_unused),
        ("I1", "Extreme load test", test_I1_extreme_load),
        ("J1", "Edge case handling", test_J1_edge_cases),
    ]

    for test_id, test_name, test_func in tests:
        print(f"\n>>> Running {test_id}: {test_name}...")
        try:
            await test_func()
        except Exception as e:
            report(TestResult(
                test_id=test_id, name=test_name, passed=False,
                exception=str(e), details=traceback.format_exc()
            ))

    # Summary
    print("\n" + "=" * 70)
    print("  ROUND 6 TEST SUMMARY")
    print("=" * 70)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed and not r.bug_found)
    bugs = sum(1 for r in results if r.bug_found)

    print(f"  Total: {total} | Passed: {passed} | Failed: {failed} | Bugs Found: {bugs}")
    print()

    if bugs > 0:
        print("  🐛 BUGS FOUND:")
        for r in results:
            if r.bug_found:
                print(f"    [{r.test_id}] {r.name}")
                print(f"      → {r.bug_description[:200]}")
                print()
    else:
        print("  ✅ No bugs found in this round!")

    # Save results
    summary = {
        "round": 6,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total": total,
        "passed": passed,
        "failed": failed,
        "bugs_found": bugs,
        "results": [
            {
                "test_id": r.test_id,
                "name": r.name,
                "passed": r.passed,
                "bug_found": r.bug_found,
                "bug_description": r.bug_description,
                "exception": r.exception,
            }
            for r in results
        ]
    }

    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "round6_test_results.json")
    with open(results_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"\n  Results saved to: {results_path}")

    return bugs


if __name__ == "__main__":
    bugs = asyncio.run(run_all_tests())
    sys.exit(1 if bugs > 0 else 0)
