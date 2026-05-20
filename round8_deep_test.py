#!/usr/bin/env python3
"""
Round 8 Deep Test - 混乱场景：故障组合、时序交错、资源竞争、异常传播链
Focus: Shutdown timing, exception chains, resource contention, data flow edge cases
"""

import asyncio
import sys
import os
import time
import json
import traceback
import tempfile
import shutil
import math
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock, patch
from typing import Dict, Any, Optional, List

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


def make_mock_ke():
    mock = MagicMock()
    mock.add_error_pattern = AsyncMock()
    return mock


async def make_state_mgr(tmpdir):
    from storage.persistent_state_manager import PersistentStateManager
    mgr = PersistentStateManager(tmpdir)
    await mgr.initialize()
    return mgr


# ============================================================
# TEST A: R8-MISC-01 - Initialize failure cleanup logic error (CONFIRMED BUG)
# ============================================================

async def test_A1_init_failure_cleanup():
    """R8-MISC-01: initialize_system cleanup logic has wrong break condition"""
    test_id = "A1"
    name = "Initialize failure cleanup logic error"

    try:
        from main_perpetual_execution_engine import MainPerpetualExecutionEngine

        engine = MainPerpetualExecutionEngine()

        # Track which shutdown methods were called
        shutdown_calls = []
        original_shutdowns = {}

        for comp_name in ["config_loader", "state_manager", "knowledge_engine",
                          "guardian_daemon", "task_worker", "optimization_analyzer",
                          "deployment_pipeline"]:
            comp = getattr(engine, comp_name)
            original_shutdowns[comp_name] = comp.shutdown

            async def tracked_shutdown(orig=comp.shutdown, name=comp_name):
                shutdown_calls.append(name)
                return await orig()

            comp.shutdown = tracked_shutdown

        # Mock optimization_analyzer.initialize() to fail (4th component, index 3)
        async def failing_init():
            raise RuntimeError("Simulated optimization analyzer init failure")

        engine.optimization_analyzer.initialize = failing_init

        # Call initialize_system
        result = await engine.initialize_system()

        # Check which components were cleaned up
        # Expected: config_loader, state_manager, knowledge_engine, guardian_daemon (indices 0-3)
        # Bug: break condition is wrong, so most won't be cleaned
        expected_cleanup = ["config_loader", "state_manager", "knowledge_engine", "guardian_daemon"]
        actually_cleaned = shutdown_calls

        missing_cleanup = [c for c in expected_cleanup if c not in actually_cleaned]

        # Manual cleanup
        for comp_name in ["config_loader", "state_manager", "knowledge_engine", "guardian_daemon"]:
            try:
                await original_shutdowns[comp_name]()
            except:
                pass

        if missing_cleanup:
            report(TestResult(
                test_id=test_id, name=name, passed=False, bug_found=True,
                bug_description=f"R8-MISC-01 confirmed: Initialize failure cleanup missed: {missing_cleanup}. "
                               f"The break condition in initialize_system compares against the FAILED component "
                               f"name, but should compare against already-initialized components. "
                               f"Components cleaned: {actually_cleaned}, Missing: {missing_cleanup}.",
                details=f"Result: {result}, Shutdown calls: {shutdown_calls}"
            ))
        else:
            report(TestResult(
                test_id=test_id, name=name, passed=True,
                details=f"All components cleaned. Calls: {shutdown_calls}"
            ))

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST B: R8-EP-01 - Double exception loses original error
# ============================================================

async def test_B1_double_exception_loses_original():
    """R8-EP-01: handler exception + knowledge_engine failure loses original error"""
    test_id = "B1"
    name = "Double exception loses original error"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_ke()

        # Make knowledge_engine.add_execution_record raise a different error
        async def failing_record(*args, **kwargs):
            raise RuntimeError("Knowledge engine disk full")

        mock_ke.add_execution_record = failing_record

        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_state_mgr(tmpdir)
            worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            await worker.initialize()
            await worker.start_execution_loop()

            # Register a handler that fails
            async def bad_handler(payload):
                raise ValueError("Original handler error: bad data")

            worker.register_handler("bad_type", bad_handler)

            # Submit failing task
            task_id = await worker.submit_task(name="bad_task", payload={"x": 1}, task_type="bad_type")
            await asyncio.sleep(1.0)

            # Check task error_message
            task = worker._tasks.get(task_id)
            error_msg = task.error_message if task else "task not found"

            await worker.shutdown()
            await state_mgr.shutdown()

            # The original error "bad data" should be preserved in task.error_message
            if "bad data" in error_msg:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Original error preserved: {error_msg}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"R8-EP-01 confirmed: Original error 'bad data' lost. "
                                   f"Task error_message: '{error_msg}'. "
                                   f"Knowledge engine failure masked the original handler error.",
                    details=f"Error message: {error_msg}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST C: R8-RC-04 - Data flow pipeline permanently stops
# ============================================================

async def test_C1_data_flow_permanent_stop():
    """R8-RC-04: last_processed_count exceeds history length, pipeline stops forever"""
    test_id = "C1"
    name = "Data flow pipeline permanently stops feeding data"

    try:
        from core.dual_process_orchestrator import DualProcessOrchestrator
        from modules.task_execution_worker import TaskExecutionWorker
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
        from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
        from core.resident_guardian_daemon import ResidentGuardianDaemon

        tmpdir = tempfile.mkdtemp()
        try:
            mock_ke = make_mock_ke()
            state_mgr = await make_state_mgr(tmpdir)
            shutdown_event = asyncio.Event()

            # Use small execution_history_size to trigger the bug
            task_worker = TaskExecutionWorker(
                knowledge_engine=mock_ke, state_manager=state_mgr,
                execution_history_size=50  # Small!
            )
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

            # Submit tasks to fill history beyond maxlen
            for i in range(100):
                await task_worker.submit_task(name=f"task_{i}", payload={"i": i}, task_type="default")

            await task_worker.start_execution_loop()
            await asyncio.sleep(2.0)
            await task_worker.shutdown()

            # Simulate multiple data flow iterations
            total_fed = 0
            for iteration in range(5):
                history = task_worker.get_execution_history(limit=10000)
                # This is what _execution_to_optimizer_flow does internally
                if iteration == 0:
                    last_count = 0
                current_count = len(history)
                new_records = history[last_count:]
                for record in new_records:
                    optimizer.feed_execution_data(record)
                    total_fed += 1
                last_count = current_count

            buffer_size = len(optimizer._execution_data_buffer)

            await optimizer.shutdown()
            await deployer.shutdown()
            await guardian.shutdown()
            await state_mgr.shutdown()

            # After history wraps around (maxlen=50), last_count=50 but history is always 50 items
            # So new_records = history[50:] = [] (empty!) -> pipeline stops feeding
            if total_fed < 50 and buffer_size < 50:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"R8-RC-04 confirmed: Data flow stopped after {total_fed} records. "
                                   f"Buffer size: {buffer_size}. Once last_processed_count exceeds "
                                   f"execution_history deque length, new_records is always empty.",
                    details=f"Total fed: {total_fed}, Buffer: {buffer_size}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Data flow working. Fed: {total_fed}, Buffer: {buffer_size}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST D: R8-SD-03 - graceful_shutdown doesn't call component shutdown
# ============================================================

async def test_D1_graceful_shutdown_no_component_cleanup():
    """R8-SD-03: graceful_shutdown cancels tasks but doesn't call component shutdown"""
    test_id = "D1"
    name = "Graceful shutdown skips component cleanup"

    try:
        from core.dual_process_orchestrator import DualProcessOrchestrator
        from modules.task_execution_worker import TaskExecutionWorker
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
        from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
        from core.resident_guardian_daemon import ResidentGuardianDaemon

        tmpdir = tempfile.mkdtemp()
        try:
            mock_ke = make_mock_ke()
            state_mgr = await make_state_mgr(tmpdir)
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

            # Start some tasks
            await task_worker.start_execution_loop()
            await asyncio.sleep(0.5)

            # Record state before graceful_shutdown
            worker_running_before = task_worker._is_running
            optimizer_running_before = optimizer._is_running

            # Call graceful_shutdown (NOT graceful_teardown)
            await orchestrator.graceful_shutdown()

            # Check if component shutdown was called
            worker_running_after = task_worker._is_running
            optimizer_running_after = optimizer._is_running

            # Check if thread pool was shut down
            thread_pool_after = task_worker._thread_pool

            # Manual cleanup
            await task_worker.shutdown()
            await optimizer.shutdown()
            await deployer.shutdown()
            await guardian.shutdown()
            await state_mgr.shutdown()

            if worker_running_after or optimizer_running_after:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"R8-SD-03 confirmed: graceful_shutdown didn't call component shutdown. "
                                   f"Worker running: {worker_running_before}->{worker_running_after}, "
                                   f"Optimizer running: {optimizer_running_before}->{optimizer_running_after}. "
                                   f"ThreadPool alive: {thread_pool_after is not None and not thread_pool_after._shutdown}. "
                                   f"State not saved, thread pool not closed.",
                    details=f"Worker: {worker_running_after}, Optimizer: {optimizer_running_after}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Components properly shut down"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST E: R8-EP-04 - Error recording failure kills orchestrator loop
# ============================================================

async def test_E1_error_recording_kills_loop():
    """R8-EP-04: knowledge_engine.add_error_pattern failure terminates orchestrator loop"""
    test_id = "E1"
    name = "Error recording failure terminates orchestrator loop"

    try:
        from core.dual_process_orchestrator import DualProcessOrchestrator, CycleMetrics
        from modules.task_execution_worker import TaskExecutionWorker
        from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
        from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
        from core.resident_guardian_daemon import ResidentGuardianDaemon

        tmpdir = tempfile.mkdtemp()
        try:
            mock_ke = make_mock_ke()

            # Make add_error_pattern raise
            async def failing_error_pattern(*args, **kwargs):
                raise RuntimeError("Error pattern recording failed")

            mock_ke.add_error_pattern = failing_error_pattern

            state_mgr = await make_state_mgr(tmpdir)
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

            # Make _execution_phase fail
            async def failing_phase(cycle_metrics):
                raise RuntimeError("Injected execution failure")

            original_phase = orchestrator._execution_phase
            orchestrator._execution_phase = failing_phase

            # Run multiple cycles - the loop should survive
            loop_survived = True
            for i in range(3):
                try:
                    cycle_metrics = CycleMetrics(cycle_number=i, start_time=time.time())
                    await orchestrator._execution_phase(cycle_metrics)
                except RuntimeError as e:
                    if "Error pattern recording failed" in str(e):
                        # The second exception (from add_error_pattern) propagated out
                        # This means the except block in the main loop didn't catch it
                        loop_survived = False
                    # First exception is expected
                    pass

            await task_worker.shutdown()
            await optimizer.shutdown()
            await deployer.shutdown()
            await guardian.shutdown()
            await state_mgr.shutdown()

            if not loop_survived:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description="R8-EP-04 confirmed: knowledge_engine.add_error_pattern failure "
                                   "propagated out of the except block, terminating the loop. "
                                   "The except block should catch ALL exceptions including ones from "
                                   "error recording.",
                    details="Error pattern recording failure propagated to caller"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details="Loop survived double exception"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST F: R8-EP-02 - NaN/Inf pollution in statistics
# ============================================================

async def test_F1_nan_pollution():
    """R8-EP-02: NaN in avg_execution_time permanently pollutes statistics"""
    test_id = "F1"
    name = "NaN pollution in average execution time"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_ke()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_state_mgr(tmpdir)
            worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            await worker.initialize()

            # Pollute avg_execution_time with NaN
            worker._stats["avg_execution_time"] = float('nan')
            worker._stats["total_completed"] = 10
            worker._stats["total_failed"] = 0

            # Simulate a normal task completion
            from modules.task_execution_worker import Task, TaskStatus
            task = Task(
                task_id="test",
                name="test",
                payload={"x": 1},
                task_type="default",
                priority=5
            )
            task.status = TaskStatus.COMPLETED
            task.metrics.duration_ms = 100.0

            # Call _record_execution
            worker._record_execution(task, success=True)

            avg = worker._stats["avg_execution_time"]

            await worker.shutdown()
            await state_mgr.shutdown()

            import math
            if math.isnan(avg) or math.isinf(avg):
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"R8-EP-02 confirmed: avg_execution_time is {avg} after normal task. "
                                   f"NaN/Inf permanently pollutes incremental average calculation. "
                                   f"All subsequent statistics become meaningless.",
                    details=f"avg_execution_time: {avg}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details=f"Statistics recovered. avg: {avg}"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST G: R8-DF-02 - payload=None crashes submit_task
# ============================================================

async def test_G1_payload_none():
    """R8-DF-02: payload=None crashes submit_task"""
    test_id = "G1"
    name = "payload=None crashes submit_task"

    try:
        from modules.task_execution_worker import TaskExecutionWorker

        mock_ke = make_mock_ke()
        tmpdir = tempfile.mkdtemp()
        try:
            state_mgr = await make_state_mgr(tmpdir)
            worker = TaskExecutionWorker(knowledge_engine=mock_ke, state_manager=state_mgr)
            await worker.initialize()

            crashed = False
            error_msg = ""

            try:
                await worker.submit_task(name="none_payload", payload=None, task_type="default")
            except TypeError as e:
                crashed = True
                error_msg = str(e)

            await worker.shutdown()
            await state_mgr.shutdown()

            if crashed:
                report(TestResult(
                    test_id=test_id, name=name, passed=False, bug_found=True,
                    bug_description=f"R8-DF-02 confirmed: payload=None crashed submit_task with: {error_msg}. "
                                   f"No input validation on payload parameter.",
                    details=f"Error: {error_msg}"
                ))
            else:
                report(TestResult(
                    test_id=test_id, name=name, passed=True,
                    details="payload=None handled gracefully"
                ))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# TEST H: R8-SD-04 - Shutdown during recovery creates orphan tasks
# ============================================================

async def test_H1_shutdown_during_recovery():
    """R8-SD-04: Shutdown during recovery creates orphan tasks"""
    test_id = "H1"
    name = "Shutdown during recovery creates orphan tasks"

    try:
        from core.resident_guardian_daemon import ResidentGuardianDaemon, HealthStatus

        shutdown_event = asyncio.Event()
        guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event)
        await guardian.initialize()

        # Create a slow recovery that we can interrupt
        recovery_started = asyncio.Event()
        async def slow_recovery():
            recovery_started.set()
            # Simulate slow recovery (re-initialize + start)
            await asyncio.sleep(5.0)  # Long enough to trigger shutdown
            return True

        guardian.register_component(
            "test_component",
            heartbeat_callback=lambda: {"healthy": False},
            recovery_callback=slow_recovery
        )

        health = guardian._component_health["test_component"]
        health.error_count = 3
        health.status = HealthStatus.UNHEALTHY

        # Start recovery in background
        recovery_task = asyncio.create_task(guardian._trigger_recovery("test_component"))

        # Wait for recovery to start
        await recovery_started.wait()

        # Now shutdown during recovery
        await guardian.shutdown()

        # Check if recovery task completed cleanly
        try:
            await asyncio.wait_for(recovery_task, timeout=2.0)
            recovery_result = "completed"
        except asyncio.TimeoutError:
            recovery_result = "still_running"
            recovery_task.cancel()
        except asyncio.CancelledError:
            recovery_result = "cancelled"

        # Check for orphan tasks
        all_tasks = asyncio.all_tasks()
        current = asyncio.current_task()
        orphan_tasks = [t for t in all_tasks if t is not current and not t.done()]

        if orphan_tasks:
            report(TestResult(
                test_id=test_id, name=name, passed=False, bug_found=True,
                bug_description=f"R8-SD-04 confirmed: {len(orphan_tasks)} orphan tasks after shutdown during recovery. "
                               f"Recovery result: {recovery_result}. "
                               f"Tasks created during recovery are not tracked for cancellation.",
                details=f"Orphan tasks: {len(orphan_tasks)}, Recovery: {recovery_result}"
            ))
        else:
            report(TestResult(
                test_id=test_id, name=name, passed=True,
                details=f"No orphan tasks. Recovery: {recovery_result}"
            ))

    except Exception as e:
        report(TestResult(test_id=test_id, name=name, passed=False, exception=str(e),
                         details=traceback.format_exc()))


# ============================================================
# Main Test Runner
# ============================================================

async def run_all_tests():
    print("=" * 70)
    print("  ROUND 8 DEEP TEST - Chaos Scenarios")
    print("  Focus: Shutdown timing, exception chains, resource contention")
    print("=" * 70)

    tests = [
        ("A1", "Init failure cleanup logic", test_A1_init_failure_cleanup),
        ("B1", "Double exception loses error", test_B1_double_exception_loses_original),
        ("C1", "Data flow permanent stop", test_C1_data_flow_permanent_stop),
        ("D1", "Graceful shutdown no cleanup", test_D1_graceful_shutdown_no_component_cleanup),
        ("E1", "Error recording kills loop", test_E1_error_recording_kills_loop),
        ("F1", "NaN pollution in stats", test_F1_nan_pollution),
        ("G1", "payload=None crash", test_G1_payload_none),
        ("H1", "Shutdown during recovery", test_H1_shutdown_during_recovery),
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
    print("  ROUND 8 TEST SUMMARY")
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

    summary = {
        "round": 8,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total": total, "passed": passed, "failed": failed, "bugs_found": bugs,
        "results": [
            {"test_id": r.test_id, "name": r.name, "passed": r.passed,
             "bug_found": r.bug_found, "bug_description": r.bug_description,
             "exception": r.exception}
            for r in results
        ]
    }

    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "round8_test_results.json")
    with open(results_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"\n  Results saved to: {results_path}")

    return bugs


if __name__ == "__main__":
    bugs = asyncio.run(run_all_tests())
    sys.exit(1 if bugs > 0 else 0)
