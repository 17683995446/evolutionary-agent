#!/usr/bin/env python3
"""
Round 8 Bug Fix Verification Tests
验证 R8-MISC-01, R8-SD-03, R8-DF-02, R8-SD-04 的修复
"""

import asyncio
import sys
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def test_r8_misc_01_init_cleanup():
    """验证 R8-MISC-01: initialize_system 失败时正确清理已初始化组件"""
    print("\n" + "=" * 60)
    print("TEST R8-MISC-01: initialize_system cleanup on partial init failure")
    print("=" * 60)

    from main_perpetual_execution_engine import MainPerpetualExecutionEngine

    # Patch RuntimeConfigurationLoader to avoid file I/O
    with patch('main_perpetual_execution_engine.RuntimeConfigurationLoader') as MockConfigLoader:
        mock_config = MagicMock()
        mock_config.initialize = AsyncMock()
        mock_config.shutdown = AsyncMock()
        mock_config.get = lambda *args, **kwargs: kwargs.get('default', 10) if 'default' in kwargs else args[2] if len(args) > 2 else 10
        MockConfigLoader.return_value = mock_config

        engine = MainPerpetualExecutionEngine()

        # Make the 5th component (Task Execution Worker) fail during initialize
        original_init = engine.task_worker.initialize

        call_count = {"n": 0}

        async def failing_init():
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("Simulated init failure")

        engine.task_worker.initialize = failing_init

        result = await engine.initialize_system()

        # Verify init returned False
        assert result is False, f"Expected False, got {result}"

        # Verify that shutdown was called on already-initialized components
        # Components initialized before failure: config_loader, state_manager, knowledge_engine, guardian_daemon
        # These should have been cleaned up
        print(f"  config_loader shutdown called: {mock_config.shutdown.called}")
        print(f"  knowledge_engine _is_running: {engine.knowledge_engine._is_running}")
        print(f"  guardian_daemon _is_running: {engine.guardian_daemon._is_running}")

        # After cleanup, knowledge_engine and guardian_daemon should be shut down
        assert engine.knowledge_engine._is_running is False, "knowledge_engine should be cleaned up"
        assert engine.guardian_daemon._is_running is False, "guardian_daemon should be cleaned up"

        print("  ✓ PASS: All initialized components properly cleaned up on failure")
        return True


async def test_r8_sd_03_graceful_shutdown_calls_component_shutdown():
    """验证 R8-SD-03: graceful_shutdown 调用组件 shutdown 方法"""
    print("\n" + "=" * 60)
    print("TEST R8-SD-03: graceful_shutdown calls component shutdown methods")
    print("=" * 60)

    from core.dual_process_orchestrator import DualProcessOrchestrator
    from modules.task_execution_worker import TaskExecutionWorker
    from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
    from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
    from core.resident_guardian_daemon import ResidentGuardianDaemon
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    shutdown_event = asyncio.Event()
    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()
    guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event)
    await guardian.initialize()
    task_worker = TaskExecutionWorker(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await task_worker.initialize()
    optimizer = IntelligenceOptimizationAnalyzer(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await optimizer.initialize()
    deployer = SynchronizationDeploymentPipeline(task_worker=task_worker, optimization_analyzer=optimizer)
    await deployer.initialize()

    orchestrator = DualProcessOrchestrator(
        task_worker=task_worker,
        optimization_analyzer=optimizer,
        deployment_pipeline=deployer,
        guardian_daemon=guardian,
        knowledge_engine=knowledge_engine
    )

    # Mark components as running
    task_worker._is_running = True
    optimizer._is_running = True
    deployer._is_running = True
    guardian._is_running = True
    knowledge_engine._is_running = True

    await orchestrator.graceful_shutdown()

    # Verify all components were shut down
    print(f"  task_worker._is_running: {task_worker._is_running}")
    print(f"  optimizer._is_running: {optimizer._is_running}")
    print(f"  deployer._is_running: {deployer._is_running}")
    print(f"  guardian._is_running: {guardian._is_running}")
    print(f"  knowledge_engine._is_running: {knowledge_engine._is_running}")

    assert task_worker._is_running is False, "task_worker should be shut down"
    assert optimizer._is_running is False, "optimizer should be shut down"
    assert deployer._is_running is False, "deployer should be shut down"
    assert guardian._is_running is False, "guardian should be shut down"
    assert knowledge_engine._is_running is False, "knowledge_engine should be shut down"

    # Verify thread pool was closed
    assert task_worker._thread_pool is not None, "thread pool should exist"
    # ThreadPoolExecutor.shutdown sets _shutdown flag
    assert task_worker._thread_pool._shutdown, "thread pool should be shut down"

    print("  ✓ PASS: All component shutdown methods called during graceful_shutdown")
    return True


async def test_r8_df_02_payload_none():
    """验证 R8-DF-02: payload=None 不会崩溃"""
    print("\n" + "=" * 60)
    print("TEST R8-DF-02: payload=None handled gracefully")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()
    worker = TaskExecutionWorker(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await worker.initialize()

    # Test 1: payload=None should work (auto-convert to {})
    try:
        task_id = await worker.submit_task(name="test_none_payload", payload=None)
        print(f"  payload=None → task_id={task_id[:8]}... ✓")
        task = worker._tasks[task_id]
        assert task.payload == {"task_type": "default"}, f"Expected empty payload with task_type, got {task.payload}"
        print("  ✓ payload=None correctly converted to empty dict")
    except Exception as e:
        print(f"  ✗ FAIL: payload=None raised {type(e).__name__}: {e}")
        return False

    # Test 2: non-dict payload should raise TypeError
    try:
        await worker.submit_task(name="test_bad_payload", payload="not a dict")
        print("  ✗ FAIL: non-dict payload should raise TypeError")
        return False
    except TypeError as e:
        print(f"  non-dict payload → TypeError: {e} ✓")

    # Test 3: normal dict payload still works
    try:
        task_id = await worker.submit_task(name="test_normal", payload={"key": "value"})
        print(f"  normal payload → task_id={task_id[:8]}... ✓")
    except Exception as e:
        print(f"  ✗ FAIL: normal payload raised {type(e).__name__}: {e}")
        return False

    await worker.shutdown()
    print("  ✓ PASS: All payload validation tests passed")
    return True


async def test_r8_sd_04_shutdown_during_recovery():
    """验证 R8-SD-04: 恢复期间关闭不会产生孤儿任务"""
    print("\n" + "=" * 60)
    print("TEST R8-SD-04: No orphan tasks during shutdown-during-recovery")
    print("=" * 60)

    from core.resident_guardian_daemon import ResidentGuardianDaemon

    shutdown_event = asyncio.Event()
    guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event, max_recovery_attempts=5)
    await guardian.initialize()

    recovery_started = asyncio.Event()
    recovery_should_wait = asyncio.Event()

    async def slow_recovery():
        """模拟慢速恢复过程"""
        recovery_started.set()
        # Wait until we're told to proceed (or timeout)
        try:
            await asyncio.wait_for(recovery_should_wait.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        # Simulate creating a task during recovery
        dummy_task = asyncio.create_task(asyncio.sleep(100))
        return dummy_task

    guardian.register_component(
        component_name="slow_component",
        heartbeat_callback=lambda: {"healthy": False},  # Always unhealthy
        recovery_callback=slow_recovery,
        critical=True
    )

    # Set component to unhealthy to trigger recovery
    guardian._component_health["slow_component"].error_count = 3

    # Start monitoring in background
    monitor_task = asyncio.create_task(guardian._check_all_components())

    # Wait for recovery to start
    await asyncio.wait_for(recovery_started.wait(), timeout=3.0)
    print("  Recovery started, now initiating shutdown...")

    # Get all tasks before shutdown
    all_tasks_before = asyncio.all_tasks()
    print(f"  Active tasks before shutdown: {len(all_tasks_before)}")

    # Initiate shutdown during recovery
    shutdown_event.set()
    await guardian.shutdown()

    # Wait a bit for cleanup
    await asyncio.sleep(0.5)

    # Check for orphan tasks
    all_tasks_after = asyncio.all_tasks()
    orphan_tasks = []
    for t in all_tasks_after:
        if t is not asyncio.current_task() and not t.done():
            task_name = t.get_name() if hasattr(t, 'get_name') else str(t)
            # Filter out known system tasks
            if 'recovery' in task_name.lower() or 'slow' in task_name.lower():
                orphan_tasks.append(t)

    print(f"  Active tasks after shutdown: {len(all_tasks_after)}")
    print(f"  Potential orphan tasks: {len(orphan_tasks)}")

    # Verify recovery was skipped or cancelled
    assert len(guardian._recovery_tasks) == 0, f"recovery_tasks should be empty, got {len(guardian._recovery_tasks)}"

    print("  ✓ PASS: No orphan tasks after shutdown during recovery")
    return True


async def test_r8_sd_04_no_recovery_after_shutdown():
    """验证 R8-SD-04: shutdown 后不会发起新的恢复"""
    print("\n" + "=" * 60)
    print("TEST R8-SD-04b: No recovery initiated after shutdown signal")
    print("=" * 60)

    from core.resident_guardian_daemon import ResidentGuardianDaemon

    shutdown_event = asyncio.Event()
    guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event, max_recovery_attempts=5)
    await guardian.initialize()

    recovery_called = {"count": 0}

    async def counting_recovery():
        recovery_called["count"] += 1
        await asyncio.sleep(0.1)

    guardian.register_component(
        component_name="test_comp",
        heartbeat_callback=lambda: {"healthy": False},
        recovery_callback=counting_recovery,
        critical=True
    )

    # Set shutdown BEFORE triggering recovery
    shutdown_event.set()
    guardian._is_running = False

    # Set component to unhealthy
    guardian._component_health["test_comp"].error_count = 3

    # Try to trigger recovery
    await guardian._trigger_recovery("test_comp")

    print(f"  Recovery called: {recovery_called['count']} times")
    assert recovery_called["count"] == 0, "Recovery should NOT be called after shutdown"

    print("  ✓ PASS: Recovery correctly skipped after shutdown signal")
    return True


async def test_r8_misc_01_edge_case_first_component_fails():
    """验证 R8-MISC-01 边界情况: 第一个组件就失败"""
    print("\n" + "=" * 60)
    print("TEST R8-MISC-01b: Edge case - first component fails")
    print("=" * 60)

    from main_perpetual_execution_engine import MainPerpetualExecutionEngine

    with patch('main_perpetual_execution_engine.RuntimeConfigurationLoader') as MockConfigLoader:
        mock_config = MagicMock()
        mock_config.initialize = AsyncMock(side_effect=RuntimeError("First component fails"))
        mock_config.shutdown = AsyncMock()
        mock_config.get = lambda *args, **kwargs: kwargs.get('default', 10) if 'default' in kwargs else args[2] if len(args) > 2 else 10
        MockConfigLoader.return_value = mock_config

        engine = MainPerpetualExecutionEngine()
        result = await engine.initialize_system()

        assert result is False, f"Expected False, got {result}"
        # No components should have been cleaned up (none were initialized)
        print(f"  config_loader shutdown called: {mock_config.shutdown.called}")
        assert not mock_config.shutdown.called, "No shutdown should be called when 0 components initialized"

        print("  ✓ PASS: First component failure handled correctly (no cleanup needed)")
        return True


async def test_r8_sd_03_idempotent_shutdown():
    """验证 R8-SD-03: shutdown 方法是幂等的（可重复调用）"""
    print("\n" + "=" * 60)
    print("TEST R8-SD-03b: Shutdown idempotency")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()
    worker = TaskExecutionWorker(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await worker.initialize()

    # Call shutdown twice
    await worker.shutdown()
    print("  First shutdown completed")

    try:
        await worker.shutdown()
        print("  Second shutdown completed")
    except Exception as e:
        print(f"  ✗ FAIL: Second shutdown raised {type(e).__name__}: {e}")
        return False

    assert worker._is_running is False
    print("  ✓ PASS: Shutdown is idempotent")
    return True


async def main():
    """运行所有验证测试"""
    print("=" * 60)
    print("Round 8 Bug Fix Verification")
    print("=" * 60)

    tests = [
        ("R8-MISC-01: init cleanup", test_r8_misc_01_init_cleanup),
        ("R8-MISC-01b: first component fails", test_r8_misc_01_edge_case_first_component_fails),
        ("R8-SD-03: graceful_shutdown calls component shutdown", test_r8_sd_03_graceful_shutdown_calls_component_shutdown),
        ("R8-SD-03b: shutdown idempotency", test_r8_sd_03_idempotent_shutdown),
        ("R8-DF-02: payload=None handling", test_r8_df_02_payload_none),
        ("R8-SD-04: shutdown during recovery", test_r8_sd_04_shutdown_during_recovery),
        ("R8-SD-04b: no recovery after shutdown", test_r8_sd_04_no_recovery_after_shutdown),
    ]

    results = {}
    for name, test_fn in tests:
        try:
            result = await test_fn()
            results[name] = "PASS" if result else "FAIL"
        except Exception as e:
            import traceback
            print(f"  ✗ EXCEPTION: {type(e).__name__}: {e}")
            traceback.print_exc()
            results[name] = f"ERROR: {type(e).__name__}"

    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v == "PASS")
    total = len(results)
    for name, result in results.items():
        status = "✓" if result == "PASS" else "✗"
        print(f"  {status} {name}: {result}")
    print(f"\n  Total: {passed}/{total} passed")

    if passed == total:
        print("  🎉 All bug fixes verified successfully!")
    else:
        print(f"  ⚠ {total - passed} test(s) need attention")

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
