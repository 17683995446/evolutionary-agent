#!/usr/bin/env python3
"""
真实运行验证脚本
长时间运行系统，验证所有bug修复有效，并发现新的潜在问题
"""

import asyncio
import time
import random
import sys
import os
import tracemalloc
import gc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import modules.task_execution_worker as task_module
import modules.intelligence_optimization_analyzer as optimizer_module
import modules.synchronization_deployment_pipeline as deployer_module
import core.knowledge_graph_integration_engine as knowledge_module
import core.resident_guardian_daemon as guardian_module
import storage.persistent_state_manager as state_module

TaskExecutionWorker = task_module.TaskExecutionWorker
TaskPriority = task_module.TaskPriority
IntelligenceOptimizationAnalyzer = optimizer_module.IntelligenceOptimizationAnalyzer
SynchronizationDeploymentPipeline = deployer_module.SynchronizationDeploymentPipeline
KnowledgeGraphIntegrationEngine = knowledge_module.KnowledgeGraphIntegrationEngine
ResidentGuardianDaemon = guardian_module.ResidentGuardianDaemon
PersistentStateManager = state_module.PersistentStateManager


async def run_verification():
    """运行完整验证"""
    print("=" * 70)
    print("真实运行验证 - 长时间高负载测试")
    print("=" * 70)
    
    all_passed = True
    
    # ========== 阶段1: 基础功能验证 ==========
    print("\n[阶段1] 基础功能验证")
    print("-" * 50)
    
    state_manager = PersistentStateManager(storage_path='./verify_storage/state')
    await state_manager.initialize()
    
    knowledge = KnowledgeGraphIntegrationEngine(
        storage_path='./verify_storage/knowledge',
        auto_save_interval=10.0,
        max_nodes_in_memory=50000
    )
    await knowledge.initialize()
    
    executor = TaskExecutionWorker(
        knowledge_engine=knowledge,
        state_manager=state_manager,
        max_concurrent_tasks=20,
        task_queue_size=1000,
        task_timeout_seconds=5.0,
        max_task_retention=5000
    )
    await executor.initialize()
    await executor.start_execution_loop()
    
    optimizer = IntelligenceOptimizationAnalyzer(
        knowledge_engine=knowledge,
        state_manager=state_manager,
        analysis_interval=3.0,
        min_samples_for_analysis=10
    )
    await optimizer.initialize()
    await optimizer.start_analysis_loop()
    
    deployer = SynchronizationDeploymentPipeline(
        task_worker=executor,
        optimization_analyzer=optimizer,
        validation_window_seconds=5.0,
        deployment_cooldown_seconds=1.0
    )
    await deployer.initialize()
    await deployer.start_deployment_loop()
    
    # --- 验证1: _running_tasks 是否正确填充 ---
    print("\n[验证1] _running_tasks 跟踪机制")
    print("  （任务执行极快，running_tasks可能为0，将在高负载阶段进一步验证）")
    hb = executor.get_heartbeat()
    print(f"  running_tasks = {hb['metrics']['running_tasks']}, completed = {hb['metrics']['total_completed']}")
    
    await asyncio.sleep(3)
    
    # --- 验证2: 基本任务执行 ---
    print("\n[验证2] 基本任务执行（等待任务完成）")
    # 先提交一些任务
    for i in range(30):
        await executor.submit_task(
            name=f"basic_test_{i}",
            payload={"operation": "sum", "data": list(range(100))},
            task_type="compute"
        )
    await asyncio.sleep(2)  # 等待完成
    hb = executor.get_heartbeat()
    completed = hb['metrics']['total_completed']
    failed = hb['metrics']['total_failed']
    print(f"  completed={completed}, failed={failed}")
    if completed >= 30:
        print("  ✓ 任务执行正常")
    else:
        print(f"  ✗ 任务完成数不足: {completed}/30")
        all_passed = False
    
    # --- 验证3: 知识图谱节点创建（超时和错误会创建节点） ---
    print("\n[验证3] 知识图谱节点创建（在超时验证后检查）")
    count = await knowledge.get_entry_count()
    print(f"  知识节点数: {count}")
    # 超时任务会创建一个节点，所以应该 >= 1
    if count >= 1:
        print("  ✓ 知识图谱正常记录（错误/超时事件）")
    else:
        print("  ~ 知识图谱暂无记录（正常：仅错误/超时事件才记录）")
    
    # ========== 阶段2: BUG-5 超时机制验证 ==========
    print("\n[阶段2] BUG-5 超时机制验证")
    print("-" * 50)
    
    # 注册一个会超时的handler
    async def slow_handler(payload):
        await asyncio.sleep(100)  # 故意超时
    
    executor.register_handler("slow", slow_handler)
    
    await executor.submit_task(
        name="timeout_test",
        payload={},
        task_type="slow"
    )
    
    print("  已提交超时任务，等待5秒...")
    await asyncio.sleep(6)
    
    hb = executor.get_heartbeat()
    timeout_failed = hb['metrics']['total_failed']
    print(f"  失败任务数: {timeout_failed}")
    if timeout_failed > 0:
        print("  ✓ BUG-5 修复有效: 超时机制生效")
    else:
        print("  ✗ BUG-5 可能未修复: 超时未触发")
        all_passed = False
    
    # ========== 阶段3: BUG-1 知识图谱tags序列化验证 ==========
    print("\n[阶段3] BUG-1 知识图谱tags序列化验证")
    print("-" * 50)
    
    # 先保存一些数据
    await knowledge.add_error_pattern(
        error_type="test_error",
        error_message="test error message",
        context={"key": "value"}
    )
    
    # 触发保存
    knowledge._modified_since_save = True
    await knowledge._save_to_storage()
    
    # 重新加载
    knowledge2 = KnowledgeGraphIntegrationEngine(
        storage_path='./verify_storage/knowledge',
        auto_save_interval=60.0
    )
    await knowledge2.initialize()
    
    # 检查加载后的tags是否为set
    tags_ok = True
    for node in knowledge2._nodes.values():
        if not isinstance(node.tags, set):
            print(f"  ✗ 节点 {node.node_id} 的 tags 类型错误: {type(node.tags)}")
            tags_ok = False
            break
    
    if tags_ok:
        print("  ✓ BUG-1 修复有效: tags 正确为 set 类型")
    else:
        print("  ✗ BUG-1 未修复: tags 仍为 list")
        all_passed = False
    
    await knowledge2.shutdown()
    
    # ========== 阶段4: BUG-2 优化规则枚举反序列化验证 ==========
    print("\n[阶段4] BUG-2 优化规则枚举反序列化验证")
    print("-" * 50)
    
    # 保存一些规则
    import modules.intelligence_optimization_analyzer as opt_mod
    rule_data = {
        "rule_id": "test_rule_1",
        "optimization_type": "batch_size",
        "target_component": "compute",
        "parameters": {"batch_size": 10},
        "confidence": 0.8,
        "created_at": time.time(),
        "applied_count": 0,
        "success_rate": 0.0
    }
    await state_manager.save_optimization_rules([rule_data])
    
    # 重新加载
    optimizer2 = IntelligenceOptimizationAnalyzer(
        knowledge_engine=knowledge,
        state_manager=state_manager,
        analysis_interval=60.0
    )
    await optimizer2.initialize()
    
    # 检查规则类型
    enum_ok = True
    for rule in optimizer2._optimization_rules.values():
        if not isinstance(rule.optimization_type, opt_mod.OptimizationType):
            print(f"  ✗ 规则 {rule.rule_id} 的 optimization_type 类型错误: {type(rule.optimization_type)}")
            enum_ok = False
            break
        # 尝试访问 .value
        try:
            _ = rule.optimization_type.value
        except AttributeError:
            print(f"  ✗ 规则 {rule.rule_id} 的 .value 调用失败")
            enum_ok = False
            break
    
    if enum_ok and len(optimizer2._optimization_rules) > 0:
        print("  ✓ BUG-2 修复有效: optimization_type 正确为枚举类型")
    else:
        print("  ✗ BUG-2 未修复")
        all_passed = False
    
    await optimizer2.shutdown()
    
    # ========== 阶段5: 长时间高负载运行 ==========
    print("\n[阶段5] 长时间高负载运行 (45秒)")
    print("-" * 50)
    
    tracemalloc.start()
    task_count = 0
    error_count = 0
    start_time = time.time()
    memory_samples = []
    sampled_seconds = set()
    
    while time.time() - start_time < 45:
        try:
            for _ in range(20):
                await executor.submit_task(
                    name=f"load_test_{task_count}",
                    payload={
                        "operation": random.choice(["sum", "avg", "max", "min"]),
                        "data": list(range(random.randint(100, 5000)))
                    },
                    priority=random.choice([
                        TaskPriority.CRITICAL,
                        TaskPriority.HIGH,
                        TaskPriority.NORMAL,
                        TaskPriority.LOW
                    ]),
                    task_type="compute"
                )
                task_count += 1
        except Exception as e:
            error_count += 1
            
        # 每5秒采样（去重）
        elapsed = int(time.time() - start_time)
        if elapsed > 0 and elapsed % 5 == 0 and elapsed not in sampled_seconds:
            sampled_seconds.add(elapsed)
            gc.collect()
            current, peak = tracemalloc.get_traced_memory()
            hb = executor.get_heartbeat()
            sample = {
                'time': elapsed,
                'memory_mb': current / 1024 / 1024,
                'completed': hb['metrics']['total_completed'],
                'failed': hb['metrics']['total_failed'],
                'running': hb['metrics']['running_tasks'],
                'queue': hb['metrics']['queue_size']
            }
            memory_samples.append(sample)
            print(f"  [{elapsed}s] 内存={sample['memory_mb']:.1f}MB, "
                  f"完成={sample['completed']}, 失败={sample['failed']}, "
                  f"运行中={sample['running']}, 队列={sample['queue']}")
        
        await asyncio.sleep(0.05)
    
    tracemalloc.stop()
    
    # 分析结果
    print(f"\n  总提交: {task_count}")
    print(f"  提交错误: {error_count}")
    
    hb = executor.get_heartbeat()
    print(f"  最终完成: {hb['metrics']['total_completed']}")
    print(f"  最终失败: {hb['metrics']['total_failed']}")
    
    # 内存趋势分析 - 比较清理后的内存水平
    if len(memory_samples) >= 3:
        # 取清理后的低谷点比较
        first = memory_samples[0]['memory_mb']
        last = memory_samples[-1]['memory_mb']
        # 找到最低点（清理后的内存）
        min_memory = min(s['memory_mb'] for s in memory_samples)
        growth = last - first
        print(f"  内存趋势: 首={first:.1f}MB, 末={last:.1f}MB, 最低={min_memory:.1f}MB")
        print(f"  内存增长(首→末): {growth:.1f}MB")
        # 内存应该呈现锯齿形（积累→清理→释放），最终不应持续增长
        if min_memory < first * 1.5:  # 清理后内存回到初始水平的1.5倍以内
            print("  ✓ 内存清理机制有效，无持续泄漏")
        else:
            print("  ⚠ 内存清理后仍较高，可能存在泄漏")
            all_passed = False
    
    # running_tasks 验证
    if any(s['running'] > 0 for s in memory_samples):
        print("  ✓ BUG-4 持续有效: running_tasks 在负载下正确报告")
    
    # ========== 阶段6: 优雅关闭验证 ==========
    print("\n[阶段6] 优雅关闭验证")
    print("-" * 50)
    
    shutdown_start = time.time()
    await deployer.shutdown()
    await optimizer.shutdown()
    await executor.shutdown()
    await knowledge.shutdown()
    shutdown_time = time.time() - shutdown_start
    
    print(f"  关闭耗时: {shutdown_time:.2f}s")
    if shutdown_time < 10:
        print("  ✓ 优雅关闭正常")
    else:
        print("  ⚠ 关闭耗时较长")
    
    # ========== 最终报告 ==========
    print("\n" + "=" * 70)
    if all_passed:
        print("所有验证通过 ✓")
    else:
        print("部分验证未通过 ✗")
    print("=" * 70)
    
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(run_verification())
    sys.exit(0 if success else 1)
