#!/usr/bin/env python3
"""
Round 10 Extreme Chaos Tests
极端混沌测试 - 长时间、大规模、高并发、资源耗尽、级联故障

测试目标：
1. 高吞吐量下的内存爆炸 (R10-THROUGHPUT-01)
2. 重试风暴级联故障 (R10-CASCADE-02)
3. 线程池无界队列增长 (R10-RESOURCE-01)
4. 知识图谱并发竞态 (R10-CONCURRENT-02)
5. 主循环无超时阻塞 (R10-CASCADE-01)
6. 长时间运行浮点精度丢失 (R10-LONG-01/02)
7. 规则ID冲突 (R10-LONG-03)
8. 部署队列无限增长 (R10-THROUGHPUT-03)
"""

import asyncio
import sys
import os
import time
import gc
import tracemalloc
from collections import deque
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 启动内存追踪
tracemalloc.start()


# ============================================================
# TEST A: R10-THROUGHPUT-01 - 无限制Task创建导致内存爆炸
# ============================================================
async def test_A_unbounded_task_creation():
    """
    模拟极高任务提交速率，观察是否无限制创建asyncio.Task对象
    """
    print("\n" + "=" * 60)
    print("TEST A: R10-THROUGHPUT-01 - Unbounded task creation memory explosion")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()

    # 使用高并发配置
    worker = TaskExecutionWorker(
        knowledge_engine=knowledge_engine,
        state_manager=state_manager,
        max_concurrent_tasks=100,
        task_queue_size=10000,
        max_task_retention=50000
    )
    await worker.initialize()

    # 记录初始内存
    gc.collect()
    snapshot1 = tracemalloc.take_snapshot()

    # 极速提交任务（模拟10,000任务/秒）
    print("  Submitting 5000 tasks at high speed...")
    start_time = time.time()

    tasks = []
    for i in range(5000):
        try:
            tid = await worker.submit_task(
                name=f"flood_task_{i}",
                payload={"data": list(range(100)), "operation": "sum"}
            )
            tasks.append(tid)
        except RuntimeError as e:
            print(f"  Backpressure triggered at task {i}: {e}")
            break

    submit_time = time.time() - start_time
    print(f"  Submitted {len(tasks)} tasks in {submit_time:.2f}s ({len(tasks)/submit_time:.0f} tasks/s)")

    # 等待一小段时间让任务开始执行
    await asyncio.sleep(2)

    # 检查内存增长
    gc.collect()
    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')

    total_memory_increase = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)
    print(f"  Memory increase: {total_memory_increase / 1024 / 1024:.2f} MB")

    # 检查正在运行的任务数
    running_count = len(worker._running_tasks)
    print(f"  Running tasks: {running_count}")

    # 检查是否有过多的Task对象（可能泄露）
    all_tasks = asyncio.all_tasks()
    print(f"  Total asyncio tasks: {len(all_tasks)}")

    # 判断是否有问题
    if len(all_tasks) > 500:  # 如果有超过500个asyncio任务，说明可能有无限制创建
        print(f"  ✗ BUG CONFIRMED: Too many asyncio tasks ({len(all_tasks)}), possible unbounded creation")
        await worker.shutdown()
        return False, f"R10-THROUGHPUT-01: {len(all_tasks)} asyncio tasks, unbounded creation"

    if total_memory_increase > 100 * 1024 * 1024:  # 超过100MB增长
        print(f"  ⚠ High memory usage: {total_memory_increase / 1024 / 1024:.2f} MB")

    print(f"  ✓ Task creation appears bounded")
    await worker.shutdown()
    return True, ""


# ============================================================
# TEST B: R10-CASCADE-02 - 重试风暴级联故障
# ============================================================
async def test_B_retry_storm():
    """
    模拟所有任务都失败的情况，观察是否产生重试风暴
    """
    print("\n" + "=" * 60)
    print("TEST B: R10-CASCADE-02 - Retry storm cascade failure")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker, TaskStatus
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()

    worker = TaskExecutionWorker(
        knowledge_engine=knowledge_engine,
        state_manager=state_manager,
        max_concurrent_tasks=50,
        task_timeout_seconds=0.1  # 极短超时确保任务失败
    )
    await worker.initialize()

    # 注册一个总是失败的任务处理器
    async def always_failing_handler(payload):
        raise RuntimeError("Simulated failure")

    worker.register_handler("failing", always_failing_handler)

    # 提交一批注定失败的任务
    print("  Submitting 100 tasks that will always fail...")
    for i in range(100):
        await worker.submit_task(
            name=f"doomed_task_{i}",
            payload={"data": "x"},
            task_type="failing"
        )

    # 等待任务执行和重试
    print("  Waiting for execution and retries...")
    await asyncio.sleep(5)

    # 检查统计
    stats = worker._stats
    print(f"  Total submitted: {stats['total_submitted']}")
    print(f"  Total completed: {stats['total_completed']}")
    print(f"  Total failed: {stats['total_failed']}")
    print(f"  Total retried: {stats['total_retried']}")

    # 每个任务最多重试3次，所以重试次数应该约等于提交数的2-3倍
    expected_max_retries = 100 * 2  # 每个任务最多重试2次（第1次失败+2次重试）

    if stats['total_retried'] > expected_max_retries * 1.5:
        print(f"  ✗ BUG CONFIRMED: Retry storm detected!")
        print(f"     Expected max ~{expected_max_retries} retries, got {stats['total_retried']}")
        await worker.shutdown()
        return False, f"R10-CASCADE-02: Retry storm - {stats['total_retried']} retries for 100 tasks"

    # 检查队列是否被重试任务填满
    queue_size = worker._task_queue.qsize()
    print(f"  Queue size after 5s: {queue_size}")

    if queue_size > 200:
        print(f"  ⚠ Queue backlog: {queue_size} tasks waiting")

    print(f"  ✓ Retry behavior appears controlled")
    await worker.shutdown()
    return True, ""


# ============================================================
# TEST C: R10-RESOURCE-01 - 线程池无界队列
# ============================================================
async def test_C_threadpool_unbounded_queue():
    """
    测试ThreadPoolExecutor内部队列是否无界增长
    """
    print("\n" + "=" * 60)
    print("TEST C: R10-RESOURCE-01 - ThreadPool unbounded queue growth")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()

    worker = TaskExecutionWorker(
        knowledge_engine=knowledge_engine,
        state_manager=state_manager,
        max_concurrent_tasks=5  # 很小的线程池
    )
    await worker.initialize()

    # 提交大量CPU密集型任务（会用到线程池）
    print("  Submitting 200 CPU-intensive tasks to small thread pool...")

    # 使用大数据量的compute任务，强制使用线程池
    for i in range(200):
        await worker.submit_task(
            name=f"cpu_task_{i}",
            payload={
                "operation": "sum",
                "data": list(range(10000))  # 大数据量强制使用线程池
            },
            task_type="compute"
        )

    # 检查线程池状态
    thread_pool = worker._thread_pool
    if hasattr(thread_pool, '_work_queue'):
        queue_size = thread_pool._work_queue.qsize()
        print(f"  ThreadPool work queue size: {queue_size}")

        if queue_size > 100:
            print(f"  ✗ BUG CONFIRMED: ThreadPool queue has {queue_size} pending tasks!")
            print(f"     This indicates unbounded queue growth")
            await worker.shutdown()
            return False, f"R10-RESOURCE-01: ThreadPool queue unbounded - {queue_size} pending"

    # 等待一段时间
    await asyncio.sleep(3)

    # 检查任务完成情况
    completed = worker._stats['total_completed']
    print(f"  Tasks completed after 3s: {completed}")

    print(f"  ✓ ThreadPool queue appears bounded")
    await worker.shutdown()
    return True, ""


# ============================================================
# TEST D: R10-CONCURRENT-02 - 知识图谱并发更新竞态
# ============================================================
async def test_D_knowledge_graph_race_condition():
    """
    高并发下同时向知识图谱添加记录，检查是否有竞态条件
    """
    print("\n" + "=" * 60)
    print("TEST D: R10-CONCURRENT-02 - Knowledge graph concurrent update race")
    print("=" * 60)

    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    import tempfile

    # 使用临时目录避免之前测试的数据干扰
    temp_dir = tempfile.mkdtemp(prefix="kg_test_")
    knowledge_engine = KnowledgeGraphIntegrationEngine(storage_path=temp_dir)
    await knowledge_engine.initialize()

    # 并发添加大量记录
    print("  Concurrently adding 1000 records from 10 coroutines...")

    async def add_records_batch(batch_id, count):
        for i in range(count):
            await knowledge_engine.add_execution_record(
                task_type=f"batch_{batch_id}",
                action="test",
                data={"index": i, "batch": batch_id}
            )

    # 10个协程同时添加记录
    tasks = [add_records_batch(i, 100) for i in range(10)]
    await asyncio.gather(*tasks)

    # 检查统计
    stats = knowledge_engine.get_statistics()
    print(f"  Total nodes: {stats['total_nodes']}")
    print(f"  Expected: 1000")

    # 检查是否有丢失的更新
    if stats['total_nodes'] != 1000:
        print(f"  ✗ BUG CONFIRMED: Node count mismatch!")
        print(f"     Expected 1000, got {stats['total_nodes']}")
        print(f"     This indicates lost updates due to race conditions")
        await knowledge_engine.shutdown()
        return False, f"R10-CONCURRENT-02: Lost updates - expected 1000 nodes, got {stats['total_nodes']}"

    print(f"  ✓ All records accounted for")
    await knowledge_engine.shutdown()

    # 清理临时目录
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

    return True, ""


# ============================================================
# TEST E: R10-CASCADE-01 - 主循环无超时阻塞
# ============================================================
async def test_E_main_loop_no_timeout():
    """
    测试主循环阶段是否有超时保护
    """
    print("\n" + "=" * 60)
    print("TEST E: R10-CASCADE-01 - Main loop phase timeout protection")
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

    # 检查代码中是否有超时保护
    import inspect
    source = inspect.getsource(orchestrator.run_dual_process_loop)

    has_timeout_protection = 'wait_for' in source or 'timeout' in source.lower()

    print(f"  Checking run_dual_process_loop for timeout protection...")

    # 检查 gather 是否有超时
    if 'asyncio.gather(execution_task, optimization_task)' in source:
        if 'wait_for' not in source or 'timeout' not in source:
            print(f"  ✗ BUG CONFIRMED: Main loop gather has no timeout protection!")
            print(f"     If execution_task or optimization_task hangs, the loop blocks forever")

            await orchestrator.graceful_shutdown()
            return False, "R10-CASCADE-01: Main loop gather has no timeout protection"

    print(f"  ✓ Timeout protection appears present")
    await orchestrator.graceful_shutdown()
    return True, ""


# ============================================================
# TEST F: R10-LONG-03 - 规则ID冲突
# ============================================================
async def test_F_rule_id_collision():
    """
    快速生成多个规则，检查是否有ID冲突
    """
    print("\n" + "=" * 60)
    print("TEST F: R10-LONG-03 - Rule ID collision under rapid generation")
    print("=" * 60)

    from modules.intelligence_optimization_analyzer import (
        IntelligenceOptimizationAnalyzer, OptimizationRule, OptimizationType
    )
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()
    analyzer = IntelligenceOptimizationAnalyzer(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await analyzer.initialize()

    # 快速生成100个规则（模拟每秒多个规则）
    print("  Rapidly generating 100 rules...")

    rule_ids = []
    for i in range(100):
        rule = OptimizationRule(
            rule_id=f"test_rule_{int(time.time())}_{i}",  # 使用当前时间戳
            optimization_type=OptimizationType.BATCH_SIZE,
            target_component="compute",
            parameters={"batch_size": i},
            confidence=0.8,
            created_at=time.time()
        )
        analyzer._optimization_rules[rule.rule_id] = rule
        rule_ids.append(rule.rule_id)

    # 检查是否有重复ID
    unique_ids = set(rule_ids)
    print(f"  Total rules generated: {len(rule_ids)}")
    print(f"  Unique rule IDs: {len(unique_ids)}")

    if len(unique_ids) < len(rule_ids):
        duplicates = len(rule_ids) - len(unique_ids)
        print(f"  ✗ BUG CONFIRMED: {duplicates} duplicate rule IDs detected!")
        print(f"     Time-based ID generation caused collisions")
        await analyzer.shutdown()
        return False, f"R10-LONG-03: {duplicates} duplicate rule IDs"

    print(f"  ✓ All rule IDs are unique")
    await analyzer.shutdown()
    return True, ""


# ============================================================
# TEST G: R10-THROUGHPUT-03 - 部署队列无限增长
# ============================================================
async def test_G_deployment_queue_unbounded():
    """
    测试部署队列是否会无限增长
    """
    print("\n" + "=" * 60)
    print("TEST G: R10-THROUGHPUT-03 - Deployment queue unbounded growth")
    print("=" * 60)

    from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
    from modules.task_execution_worker import TaskExecutionWorker
    from modules.intelligence_optimization_analyzer import (
        IntelligenceOptimizationAnalyzer, OptimizationRule, OptimizationType
    )
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()
    worker = TaskExecutionWorker(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await worker.initialize()
    optimizer = IntelligenceOptimizationAnalyzer(knowledge_engine=knowledge_engine, state_manager=state_manager)
    await optimizer.initialize()

    # 使用极短的冷却时间
    pipeline = SynchronizationDeploymentPipeline(
        task_worker=worker,
        optimization_analyzer=optimizer,
        deployment_cooldown_seconds=0.1  # 极短冷却
    )
    await pipeline.initialize()

    # 快速生成大量规则
    print("  Rapidly generating 500 optimization rules...")
    for i in range(500):
        rule = OptimizationRule(
            rule_id=f"flood_rule_{i}_{time.time()}",
            optimization_type=OptimizationType.BATCH_SIZE,
            target_component=f"component_{i % 10}",
            parameters={"batch_size": i},
            confidence=0.9,
            created_at=time.time()
        )
        optimizer._optimization_rules[rule.rule_id] = rule

    # 启动部署循环（短时间）
    await pipeline.start_deployment_loop()

    print("  Letting deployment loop run for 3 seconds...")
    await asyncio.sleep(3)

    # 检查队列大小
    queue_size = pipeline._deployment_queue.qsize()
    print(f"  Deployment queue size: {queue_size}")

    # 检查部署记录数量（有maxlen=1000限制）
    print(f"  Deployment records: {len(pipeline._deployments)}")
    print(f"  Active rules: {len(pipeline._active_rules)}")

    # asyncio.Queue 没有内置大小限制，可能无限增长
    if queue_size > 1000:
        print(f"  ✗ BUG CONFIRMED: Deployment queue has grown to {queue_size}!")
        print(f"     asyncio.Queue has no maxlen, can grow unbounded")
        await pipeline.shutdown()
        return False, f"R10-THROUGHPUT-03: Deployment queue unbounded - {queue_size} items"

    print(f"  ✓ Deployment queue appears bounded")
    await pipeline.shutdown()
    await worker.shutdown()
    return True, ""


# ============================================================
# TEST H: 长时间运行稳定性测试（简化版）
# ============================================================
async def test_H_long_running_stability():
    """
    模拟长时间运行，检查内存增长和状态一致性
    """
    print("\n" + "=" * 60)
    print("TEST H: Long-running stability test (60 seconds)")
    print("=" * 60)

    from modules.task_execution_worker import TaskExecutionWorker
    from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
    from storage.persistent_state_manager import PersistentStateManager

    state_manager = PersistentStateManager()
    await state_manager.initialize()
    knowledge_engine = KnowledgeGraphIntegrationEngine()
    await knowledge_engine.initialize()

    worker = TaskExecutionWorker(
        knowledge_engine=knowledge_engine,
        state_manager=state_manager,
        max_concurrent_tasks=20,
        max_task_retention=5000
    )
    await worker.initialize()

    # 记录初始状态
    gc.collect()
    snapshot1 = tracemalloc.take_snapshot()
    initial_tasks = worker._stats['total_submitted']

    print("  Running for 60 seconds with continuous task submission...")
    start_time = time.time()
    iteration = 0

    while time.time() - start_time < 60:
        iteration += 1

        # 每轮提交一批任务
        for i in range(50):
            try:
                await worker.submit_task(
                    name=f"long_run_{iteration}_{i}",
                    payload={"data": list(range(50))}
                )
            except RuntimeError:
                pass  # 背压触发，正常

        # 每10秒报告一次状态
        if iteration % 10 == 0:
            elapsed = time.time() - start_time
            stats = worker._stats
            print(f"  [{elapsed:.0f}s] Submitted: {stats['total_submitted']}, "
                  f"Completed: {stats['total_completed']}, "
                  f"Failed: {stats['total_failed']}, "
                  f"Tasks in memory: {len(worker._tasks)}")

        await asyncio.sleep(1)

    # 最终检查
    gc.collect()
    snapshot2 = tracemalloc.take_snapshot()
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')
    total_increase = sum(stat.size_diff for stat in top_stats if stat.size_diff > 0)

    final_stats = worker._stats
    total_submitted = final_stats['total_submitted'] - initial_tasks

    print(f"\n  Final results after 60s:")
    print(f"    Total submitted: {total_submitted}")
    print(f"    Memory increase: {total_increase / 1024 / 1024:.2f} MB")
    print(f"    Tasks in memory: {len(worker._tasks)}")

    # 检查内存增长是否合理（每小时运行不应超过100MB）
    memory_per_minute = total_increase / 60 / 1024 / 1024
    if memory_per_minute > 5:  # 超过5MB/分钟可能有问题
        print(f"  ⚠ High memory growth rate: {memory_per_minute:.2f} MB/minute")

    print(f"  ✓ Long-running test completed")
    await worker.shutdown()
    return True, ""


# ============================================================
# Main
# ============================================================
async def main():
    print("=" * 60)
    print("Round 10 Extreme Chaos Tests")
    print("Focus: Long-running, High-throughput, Resource exhaustion, Cascade failures")
    print("=" * 60)

    tests = [
        ("A: Unbounded task creation", test_A_unbounded_task_creation),
        ("B: Retry storm", test_B_retry_storm),
        ("C: ThreadPool unbounded queue", test_C_threadpool_unbounded_queue),
        ("D: Knowledge graph race condition", test_D_knowledge_graph_race_condition),
        ("E: Main loop timeout protection", test_E_main_loop_no_timeout),
        ("F: Rule ID collision", test_F_rule_id_collision),
        ("G: Deployment queue unbounded", test_G_deployment_queue_unbounded),
        ("H: Long-running stability (60s)", test_H_long_running_stability),
    ]

    results = {}
    bugs_found = []

    for name, test_fn in tests:
        try:
            passed, bug_info = await test_fn()
            if passed:
                results[name] = "PASS"
            else:
                results[name] = f"BUG: {bug_info}"
                bugs_found.append(bug_info)
        except Exception as e:
            import traceback
            print(f"  ✗ EXCEPTION: {type(e).__name__}: {e}")
            traceback.print_exc()
            results[name] = f"ERROR: {type(e).__name__}: {e}"

    print("\n" + "=" * 60)
    print("ROUND 10 TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v == "PASS")
    total = len(results)
    for name, result in results.items():
        status = "✓" if result == "PASS" else "✗"
        print(f"  {status} {name}: {result[:80]}")

    print(f"\n  Total: {passed}/{total} passed, {len(bugs_found)} bug(s) found")

    if bugs_found:
        print("\n  🐛 BUGS FOUND:")
        for bug in bugs_found:
            print(f"    - {bug}")

    # 保存结果
    import json
    with open("/workspace/evolutionary_agent/round10_test_results.json", "w") as f:
        json.dump({
            "timestamp": time.time(),
            "total": total,
            "passed": passed,
            "bugs_found": bugs_found,
            "results": {k: str(v) for k, v in results.items()}
        }, f, indent=2)

    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
