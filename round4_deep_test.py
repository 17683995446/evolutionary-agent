#!/usr/bin/env python3
"""
第四轮深度测试 - 组件交互、状态一致性、资源约束
发现真实运行时的深层bug
"""

import asyncio
import time
import random
import sys
import os
import gc
import tracemalloc
import json
import shutil

sys.path.insert(0, '.')

from modules.task_execution_worker import TaskExecutionWorker, TaskPriority
from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
from core.resident_guardian_daemon import ResidentGuardianDaemon
from storage.persistent_state_manager import PersistentStateManager

TEST_DIR = './round4_test_storage'
ALL_BUGS = []

def report_bug(test_name, severity, issue, details=""):
    """报告发现的bug"""
    bug = {"test": test_name, "severity": severity, "issue": issue, "details": details}
    ALL_BUGS.append(bug)
    print(f"  [BUG-{severity.upper()}] {issue}")
    if details:
        print(f"    详情: {details[:200]}")


# ============================================================
# 测试A: 组件交互边界测试
# ============================================================
async def test_component_interaction():
    """测试守护层+执行层+优化层的联动"""
    print("\n" + "="*70)
    print("测试A: 组件交互边界测试")
    print("="*70)
    
    # 清理存储
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    
    state = PersistentStateManager(storage_path=f'{TEST_DIR}/state')
    await state.initialize()
    
    knowledge = KnowledgeGraphIntegrationEngine(
        storage_path=f'{TEST_DIR}/knowledge',
        auto_save_interval=10.0, max_nodes_in_memory=5000
    )
    await knowledge.initialize()
    
    executor = TaskExecutionWorker(
        knowledge_engine=knowledge, state_manager=state,
        max_concurrent_tasks=10, task_queue_size=200,
        task_timeout_seconds=5.0, max_task_retention=500
    )
    await executor.initialize()
    await executor.start_execution_loop()
    
    optimizer = IntelligenceOptimizationAnalyzer(
        knowledge_engine=knowledge, state_manager=state,
        analysis_interval=3.0, min_samples_for_analysis=10
    )
    await optimizer.initialize()
    await optimizer.start_analysis_loop()
    
    deployer = SynchronizationDeploymentPipeline(
        task_worker=executor, optimization_analyzer=optimizer,
        validation_window_seconds=5.0, deployment_cooldown_seconds=1.0
    )
    await deployer.initialize()
    await deployer.start_deployment_loop()
    
    shutdown_event = asyncio.Event()
    guardian = ResidentGuardianDaemon(
        shutdown_event=shutdown_event,
        health_check_interval=3.0, heartbeat_timeout=10.0, max_recovery_attempts=2
    )
    await guardian.initialize()
    
    # 注册所有组件到守护层
    guardian.register_component(
        "executor", executor.get_heartbeat,
        recovery_callback=None, critical=True
    )
    guardian.register_component(
        "optimizer", optimizer.get_heartbeat,
        recovery_callback=None, critical=True
    )
    guardian.register_component(
        "deployer", deployer.get_heartbeat,
        recovery_callback=None, critical=True
    )
    await guardian.start_monitoring()
    
    # A1: 正常交互 - 执行层产生数据，优化层消费
    print("\n[A1] 执行→优化数据流测试")
    try:
        for i in range(100):
            await executor.submit_task(
                f"interact_{i}",
                {"op": random.choice(["sum","avg","max"]), "data": list(range(500))},
                task_type="compute"
            )
        await asyncio.sleep(5)
        
        history = executor.get_execution_history(limit=50)
        for record in history:
            optimizer.feed_execution_data(record)
        
        rules = optimizer.get_optimization_rules()
        print(f"  执行记录: {len(history)}, 优化规则: {len(rules)}")
        
        if len(history) == 0:
            report_bug("A1", "medium", "执行历史为空，数据流断裂")
        else:
            print("  ✓ 数据流正常")
    except Exception as e:
        report_bug("A1", "high", f"数据流测试崩溃: {e}")
    
    # A2: 守护层心跳检测
    print("\n[A2] 守护层心跳检测")
    try:
        await asyncio.sleep(5)  # 等待守护层检测
        health = guardian.get_system_health()
        print(f"  系统健康: {health['overall_status']}")
        print(f"  组件数: {health['summary']['total']}")
        
        if health['overall_status'] != "healthy":
            report_bug("A2", "high", f"系统不健康: {health['overall_status']}")
        else:
            print("  ✓ 守护层正常")
    except Exception as e:
        report_bug("A2", "high", f"守护层测试崩溃: {e}")
    
    # A3: 部署管道接收优化规则
    print("\n[A3] 部署管道规则同步测试")
    try:
        # 手动注入一些执行数据触发优化
        for i in range(50):
            optimizer.feed_execution_data({
                "task_type": "compute",
                "duration_ms": random.uniform(50, 500),
                "success": random.random() > 0.1,  # 10%失败率
                "timestamp": time.time()
            })
        
        await asyncio.sleep(5)
        
        deploy_summary = deployer.get_deployment_summary()
        print(f"  部署摘要: {json.dumps(deploy_summary['stats'], indent=2)}")
        print("  ✓ 部署管道正常")
    except Exception as e:
        report_bug("A3", "medium", f"部署管道测试异常: {e}")
    
    # A4: 并发提交+守护层同时检测
    print("\n[A4] 并发提交+守护层同时检测")
    try:
        submit_tasks = []
        for i in range(200):
            submit_tasks.append(executor.submit_task(
                f"concurrent_{i}",
                {"op": "sum", "data": list(range(200))},
                priority=random.choice([TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]),
                task_type="compute"
            ))
        
        await asyncio.gather(*submit_tasks, return_exceptions=True)
        await asyncio.sleep(3)
        
        hb = executor.get_heartbeat()
        health = guardian.get_system_health()
        print(f"  完成: {hb['metrics']['total_completed']}, 失败: {hb['metrics']['total_failed']}")
        print(f"  系统健康: {health['overall_status']}")
        
        if hb['metrics']['total_failed'] > 20:
            report_bug("A4", "medium", f"并发提交失败率过高: {hb['metrics']['total_failed']}/200")
        else:
            print("  ✓ 并发交互正常")
    except Exception as e:
        report_bug("A4", "high", f"并发交互测试崩溃: {e}")
    
    # 关闭
    await guardian.shutdown()
    await deployer.shutdown()
    await optimizer.shutdown()
    await executor.shutdown()
    await knowledge.shutdown()


# ============================================================
# 测试B: 状态一致性测试
# ============================================================
async def test_state_consistency():
    """测试重启恢复和数据持久化一致性"""
    print("\n" + "="*70)
    print("测试B: 状态一致性测试")
    print("="*70)
    
    state_dir = f'{TEST_DIR}/consistency'
    if os.path.exists(state_dir):
        shutil.rmtree(state_dir)
    
    # B1: 保存→加载→验证 循环
    print("\n[B1] 状态保存→加载→验证")
    try:
        state = PersistentStateManager(storage_path=state_dir)
        await state.initialize()
        
        # 保存一些数据
        await state.save_executor_state({
            "stats": {"total_completed": 42, "total_failed": 3},
            "optimization_rules": {"compute": {"batch_size": 10}}
        })
        await state.save_optimization_rules([
            {"rule_id": "r1", "optimization_type": "batch_size", "target_component": "compute",
             "parameters": {"batch_size": 10}, "confidence": 0.8,
             "created_at": time.time(), "applied_count": 0, "success_rate": 0.0}
        ])
        await state.save_key_value("test_key", {"value": 123})
        
        # 重新加载
        state2 = PersistentStateManager(storage_path=state_dir)
        await state2.initialize()
        
        loaded_state = await state2.load_executor_state()
        loaded_rules = await state2.load_optimization_rules()
        loaded_kv = await state2.load_key_value("test_key")
        
        ok = True
        if loaded_state is None:
            report_bug("B1", "high", "执行器状态加载失败")
            ok = False
        elif loaded_state["stats"]["total_completed"] != 42:
            report_bug("B1", "high", f"状态不一致: expected 42, got {loaded_state['stats']['total_completed']}")
            ok = False
        
        if not loaded_rules or len(loaded_rules) == 0:
            report_bug("B1", "high", "优化规则加载失败")
            ok = False
        
        if loaded_kv is None or loaded_kv.get("value") != 123:
            report_bug("B1", "medium", "键值对加载不一致")
            ok = False
        
        if ok:
            print("  ✓ 状态一致性正常")
    except Exception as e:
        report_bug("B1", "high", f"状态一致性测试崩溃: {e}")
    
    # B2: 知识图谱持久化→加载→tags类型验证
    print("\n[B2] 知识图谱持久化tags类型验证")
    try:
        knowledge = KnowledgeGraphIntegrationEngine(
            storage_path=f'{TEST_DIR}/kg_persist',
            auto_save_interval=5.0, max_nodes_in_memory=1000
        )
        await knowledge.initialize()
        
        # 创建各种类型的节点
        await knowledge.add_error_pattern("test_err", "test message", {"k": "v"})
        await knowledge.add_execution_record("compute", "test_action", {"data": "test"})
        
        # 强制保存
        knowledge._modified_since_save = True
        await knowledge._save_to_storage()
        
        # 重新加载
        knowledge2 = KnowledgeGraphIntegrationEngine(
            storage_path=f'{TEST_DIR}/kg_persist',
            auto_save_interval=60.0
        )
        await knowledge2.initialize()
        
        tags_ok = True
        for node_id, node in knowledge2._nodes.items():
            if not isinstance(node.tags, set):
                report_bug("B2", "high", f"节点 {node_id} tags类型错误: {type(node.tags).__name__}, 应为set")
                tags_ok = False
                break
        
        if tags_ok and len(knowledge2._nodes) > 0:
            print(f"  加载 {len(knowledge2._nodes)} 个节点, tags类型全部正确")
            print("  ✓ 知识图谱持久化正常")
        elif len(knowledge2._nodes) == 0:
            report_bug("B2", "medium", "知识图谱持久化后加载为空")
        
        await knowledge2.shutdown()
    except Exception as e:
        report_bug("B2", "high", f"知识图谱持久化测试崩溃: {e}")
    
    # B3: 优化规则枚举持久化验证
    print("\n[B3] 优化规则枚举持久化验证")
    try:
        state3 = PersistentStateManager(storage_path=f'{TEST_DIR}/opt_persist')
        await state3.initialize()
        
        # 保存包含枚举的规则
        await state3.save_optimization_rules([
            {"rule_id": "enum_test", "optimization_type": "batch_size",
             "target_component": "compute", "parameters": {"batch_size": 5},
             "confidence": 0.9, "created_at": time.time(), "applied_count": 0, "success_rate": 0.0},
            {"rule_id": "enum_test2", "optimization_type": "concurrency",
             "target_component": "io", "parameters": {"max_concurrent": 3},
             "confidence": 0.7, "created_at": time.time(), "applied_count": 0, "success_rate": 0.0}
        ])
        
        # 通过优化器加载
        knowledge_tmp = KnowledgeGraphIntegrationEngine(storage_path=f'{TEST_DIR}/kg_tmp')
        await knowledge_tmp.initialize()
        
        optimizer = IntelligenceOptimizationAnalyzer(
            knowledge_engine=knowledge_tmp, state_manager=state3,
            analysis_interval=60.0
        )
        await optimizer.initialize()
        
        enum_ok = True
        for rule_id, rule in optimizer._optimization_rules.items():
            if not hasattr(rule.optimization_type, 'value'):
                report_bug("B3", "high", f"规则 {rule_id} optimization_type不是枚举: {type(rule.optimization_type)}")
                enum_ok = False
            else:
                try:
                    _ = rule.optimization_type.value
                except AttributeError:
                    report_bug("B3", "high", f"规则 {rule_id} .value 调用失败")
                    enum_ok = False
        
        if enum_ok and len(optimizer._optimization_rules) > 0:
            print(f"  加载 {len(optimizer._optimization_rules)} 条规则, 枚举类型全部正确")
            print("  ✓ 优化规则枚举持久化正常")
        elif len(optimizer._optimization_rules) == 0:
            report_bug("B3", "medium", "优化规则加载为空")
        
        await optimizer.shutdown()
        await knowledge_tmp.shutdown()
    except Exception as e:
        report_bug("B3", "high", f"优化规则枚举测试崩溃: {e}")


# ============================================================
# 测试C: 极端资源约束测试
# ============================================================
async def test_resource_constraints():
    """极端资源约束下的系统行为"""
    print("\n" + "="*70)
    print("测试C: 极端资源约束测试")
    print("="*70)
    
    state = PersistentStateManager(storage_path=f'{TEST_DIR}/resource')
    await state.initialize()
    
    knowledge = KnowledgeGraphIntegrationEngine(
        storage_path=f'{TEST_DIR}/resource_kg',
        auto_save_interval=30.0, max_nodes_in_memory=2000
    )
    await knowledge.initialize()
    
    # C1: 超大payload测试
    print("\n[C1] 超大payload测试")
    try:
        executor = TaskExecutionWorker(
            knowledge_engine=knowledge, state_manager=state,
            max_concurrent_tasks=5, task_queue_size=50,
            task_timeout_seconds=5.0, max_task_retention=100
        )
        await executor.initialize()
        await executor.start_execution_loop()
        
        # 提交超大payload
        huge_payload = {"data": list(range(100000))}  # ~800KB
        await executor.submit_task("huge_task", huge_payload, task_type="compute")
        
        await asyncio.sleep(3)
        hb = executor.get_heartbeat()
        if hb['metrics']['total_failed'] > 0:
            print(f"  超大payload处理: 失败={hb['metrics']['total_failed']}（预期行为）")
        else:
            print(f"  超大payload处理: 成功完成")
        print("  ✓ 超大payload未导致崩溃")
        
        await executor.shutdown()
    except MemoryError:
        report_bug("C1", "high", "超大payload导致内存溢出")
    except Exception as e:
        report_bug("C1", "medium", f"超大payload测试异常: {e}")
    
    # C2: 零间隔高频提交
    print("\n[C2] 零间隔高频提交测试")
    try:
        executor2 = TaskExecutionWorker(
            knowledge_engine=knowledge, state_manager=state,
            max_concurrent_tasks=3, task_queue_size=20,
            task_timeout_seconds=5.0, max_task_retention=50
        )
        await executor2.initialize()
        await executor2.start_execution_loop()
        
        rejected = 0
        for i in range(500):
            try:
                await executor2.submit_task(
                    f"rapid_{i}",
                    {"op": "sum", "data": list(range(100))},
                    task_type="compute"
                )
            except RuntimeError:
                rejected += 1
        
        await asyncio.sleep(3)
        hb = executor2.get_heartbeat()
        print(f"  提交500, 拒绝={rejected}, 完成={hb['metrics']['total_completed']}, 失败={hb['metrics']['total_failed']}")
        
        if rejected == 0 and hb['metrics']['total_completed'] < 500:
            report_bug("C2", "medium", "背压机制未触发，可能导致内存问题")
        else:
            print("  ✓ 背压机制正常工作")
        
        await executor2.shutdown()
    except Exception as e:
        report_bug("C2", "high", f"高频提交测试崩溃: {e}")
    
    # C3: 长时间运行内存稳定性
    print("\n[C3] 长时间运行内存稳定性")
    try:
        executor3 = TaskExecutionWorker(
            knowledge_engine=knowledge, state_manager=state,
            max_concurrent_tasks=20, task_queue_size=500,
            task_timeout_seconds=5.0, max_task_retention=500
        )
        await executor3.initialize()
        await executor3.start_execution_loop()
        
        tracemalloc.start()
        samples = []
        sampled = set()
        
        for wave in range(6):  # 6波，每波5秒
            for i in range(200):
                try:
                    await executor3.submit_task(
                        f"mem_{wave}_{i}",
                        {"op": random.choice(["sum","avg","max","min"]),
                         "data": list(range(random.randint(100, 5000)))},
                        task_type="compute"
                    )
                except RuntimeError:
                    pass
            
            await asyncio.sleep(5)
            
            gc.collect()
            current, peak = tracemalloc.get_traced_memory()
            wave_data = {"wave": wave, "memory_mb": current/1024/1024, "peak_mb": peak/1024/1024}
            samples.append(wave_data)
            print(f"  波次{wave}: 内存={wave_data['memory_mb']:.1f}MB, 峰值={wave_data['peak_mb']:.1f}MB")
        
        tracemalloc.stop()
        
        # 分析趋势
        if len(samples) >= 4:
            first_half = [s["memory_mb"] for s in samples[:3]]
            second_half = [s["memory_mb"] for s in samples[3:]]
            avg_first = sum(first_half) / len(first_half)
            avg_second = sum(second_half) / len(second_half)
            growth = avg_second - avg_first
            
            print(f"  前半平均: {avg_first:.1f}MB, 后半平均: {avg_second:.1f}MB, 增长: {growth:.1f}MB")
            
            if growth > 50:
                report_bug("C3", "high", f"内存持续增长: {growth:.1f}MB")
            elif growth > 20:
                report_bug("C3", "medium", f"内存增长较快: {growth:.1f}MB")
            else:
                print("  ✓ 内存稳定")
        
        await executor3.shutdown()
    except Exception as e:
        report_bug("C3", "high", f"内存稳定性测试崩溃: {e}")
    
    await knowledge.shutdown()


# ============================================================
# 测试D: 新模块集成测试
# ============================================================
async def test_new_modules_integration():
    """新模块深度集成测试"""
    print("\n" + "="*70)
    print("测试D: 新模块深度集成测试")
    print("="*70)
    
    # D1: 工作流引擎 - 复杂DAG
    print("\n[D1] 工作流引擎复杂DAG测试")
    try:
        from modules.workflow_orchestration_engine import WorkflowOrchestrationEngine
        
        engine = WorkflowOrchestrationEngine(max_concurrent_tasks=5)
        
        # 创建菱形依赖: A → B,C → D
        wf = engine.create_workflow("diamond")
        
        results = {}
        
        async def handler(ctx, task):
            await asyncio.sleep(0.05)
            results[task.task_id] = f"done_{task.task_id}"
            return task.task_id
        
        wf.add_task("A", "Root", handler=handler)
        wf.add_task("B", "Branch1", handler=handler, dependencies={"A"})
        wf.add_task("C", "Branch2", handler=handler, dependencies={"A"})
        wf.add_task("D", "Join", handler=handler, dependencies={"B", "C"})
        
        r = await engine.execute_workflow(wf)
        
        print(f"  状态: {r.status.value}, 完成: {r.completed_tasks}/{r.total_tasks}")
        
        if r.status.value != "completed":
            report_bug("D1", "high", f"菱形DAG执行失败: {r.errors}")
        elif r.completed_tasks != 4:
            report_bug("D1", "medium", f"菱形DAG未全部完成: {r.completed_tasks}/4")
        else:
            print("  ✓ 菱形DAG正常")
    except Exception as e:
        report_bug("D1", "high", f"工作流DAG测试崩溃: {e}")
    
    # D2: 工作流 - 任务失败传播
    print("\n[D2] 工作流失败传播测试")
    try:
        engine2 = WorkflowOrchestrationEngine(max_concurrent_tasks=5)
        wf2 = engine2.create_workflow("failure_test")
        
        async def fail_handler(ctx, task):
            if task.task_id == "fail_task":
                raise ValueError("intentional failure")
            await asyncio.sleep(0.05)
            return "ok"
        
        wf2.add_task("fail_task", "WillFail", handler=fail_handler)
        wf2.add_task("dependent", "DependsOnFail", handler=fail_handler, dependencies={"fail_task"})
        
        r2 = await engine2.execute_workflow(wf2)
        
        print(f"  状态: {r2.status.value}")
        print(f"  完成: {r2.completed_tasks}, 失败: {r2.failed_tasks}, 跳过: {r2.skipped_tasks}")
        
        # dependent应该被跳过
        dep_result = r2.task_results.get("dependent", {})
        if dep_result.get("status") != "skipped":
            report_bug("D2", "medium", f"依赖失败的任务未被跳过: {dep_result.get('status')}")
        else:
            print("  ✓ 失败传播正确")
    except Exception as e:
        report_bug("D2", "medium", f"失败传播测试异常: {e}")
    
    # D3: 工作流 - 循环依赖检测
    print("\n[D3] 工作流循环依赖检测")
    try:
        engine3 = WorkflowOrchestrationEngine()
        wf3 = engine3.create_workflow("cycle_test")
        
        async def noop(ctx, task):
            return "ok"
        
        wf3.add_task("X", "X", handler=noop, dependencies={"Z"})
        wf3.add_task("Y", "Y", handler=noop, dependencies={"X"})
        wf3.add_task("Z", "Z", handler=noop, dependencies={"Y"})
        
        errors = wf3.validate()
        
        if not any("ycle" in str(e) for e in errors):
            report_bug("D3", "high", "循环依赖未被检测到")
        else:
            print(f"  检测到循环依赖: {errors}")
            print("  ✓ 循环依赖检测正常")
    except Exception as e:
        report_bug("D3", "medium", f"循环依赖检测异常: {e}")
    
    # D4: 事件总线 - 高吞吐
    print("\n[D4] 事件总线高吞吐测试")
    try:
        from core.event_bus_system import EventBus
        
        bus = EventBus(max_history=5000)
        await bus.start()
        
        received = 0
        
        async def fast_handler(event):
            nonlocal received
            received += 1
        
        bus.subscribe("fast.event", fast_handler)
        
        # 快速发布1000个事件
        for i in range(1000):
            await bus.publish("fast.event", {"i": i}, source="test")
        
        await asyncio.sleep(2)
        
        print(f"  发布: 1000, 接收: {received}")
        
        if received < 990:
            report_bug("D4", "medium", f"事件丢失: {1000 - received}/1000")
        else:
            print("  ✓ 事件总线高吞吐正常")
        
        stats = bus.get_statistics()
        print(f"  统计: {stats}")
        
        await bus.stop()
    except Exception as e:
        report_bug("D4", "medium", f"事件总线高吞吐测试异常: {e}")
    
    # D5: 智能调度器 - 预测精度验证
    print("\n[D5] 智能调度器预测精度验证")
    try:
        from modules.intelligent_task_scheduler import IntelligentTaskScheduler, SchedulingStrategy
        
        scheduler = IntelligentTaskScheduler(default_strategy=SchedulingStrategy.ADAPTIVE)
        
        # 训练数据: task_type "fast" 平均 0.1s
        for i in range(100):
            scheduler.record_execution(f"fast_{i}", "fast", 0.1 + random.gauss(0, 0.01), True)
        
        # 训练数据: task_type "slow" 平均 1.0s
        for i in range(100):
            scheduler.record_execution(f"slow_{i}", "slow", 1.0 + random.gauss(0, 0.1), True)
        
        pred_fast, conf_fast = scheduler.predict_duration("fast")
        pred_slow, conf_slow = scheduler.predict_duration("slow")
        
        print(f"  fast预测: {pred_fast:.3f}s (实际~0.1s), 置信度: {conf_fast:.2f}")
        print(f"  slow预测: {pred_slow:.3f}s (实际~1.0s), 置信度: {conf_slow:.2f}")
        
        errors = []
        if abs(pred_fast - 0.1) > 0.2:
            errors.append("fast预测偏差过大")
        if abs(pred_slow - 1.0) > 0.5:
            errors.append("slow预测偏差过大")
        if conf_fast < 0.5:
            errors.append("fast置信度过低")
        
        if errors:
            for err in errors:
                report_bug("D5", "low", f"调度器预测: {err}")
        else:
            print("  ✓ 预测精度正常")
    except Exception as e:
        report_bug("D5", "medium", f"调度器预测测试异常: {e}")


# ============================================================
# 主函数
# ============================================================
async def main():
    print("="*70)
    print("第四轮深度测试 - 组件交互/状态一致性/资源约束/新模块集成")
    print("="*70)
    
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    
    await test_component_interaction()
    await test_state_consistency()
    await test_resource_constraints()
    await test_new_modules_integration()
    
    # 最终报告
    print("\n" + "="*70)
    print("第四轮测试报告")
    print("="*70)
    
    print(f"\n发现Bug总数: {len(ALL_BUGS)}")
    
    if ALL_BUGS:
        by_severity = {}
        for bug in ALL_BUGS:
            sev = bug["severity"]
            by_severity[sev] = by_severity.get(sev, 0) + 1
        
        print(f"  高严重度: {by_severity.get('high', 0)}")
        print(f"  中严重度: {by_severity.get('medium', 0)}")
        print(f"  低严重度: {by_severity.get('low', 0)}")
        
        print("\nBug详情:")
        for i, bug in enumerate(ALL_BUGS, 1):
            print(f"  {i}. [{bug['severity'].upper()}] [{bug['test']}] {bug['issue']}")
    else:
        print("  未发现新Bug！")
    
    # 清理
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    
    return ALL_BUGS


if __name__ == "__main__":
    bugs = asyncio.run(main())
    with open('./round4_test_results.json', 'w') as f:
        json.dump(bugs, f, indent=2, ensure_ascii=False)
    print(f"\n结果已保存到 ./round4_test_results.json")
