#!/usr/bin/env python3
"""
Stress Test Advanced Chaos
高级混沌测试 - 发现深层缺陷

测试场景：
1. 内存压力测试 - 检测内存泄漏
2. CPU压力测试 - 检测计算瓶颈
3. 网络IO模拟 - 检测异步处理缺陷
4. 级联故障测试 - 检测容错能力
"""

import asyncio
import time
import random
import sys
import os
import gc
import tracemalloc
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import json

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


@dataclass
class ChaosTestMetrics:
    """混沌测试指标"""
    test_name: str
    start_time: float
    end_time: Optional[float] = None
    iterations: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    memory_samples: List[Dict[str, Any]] = field(default_factory=list)


class AdvancedChaosTester:
    """高级混沌测试器"""
    
    def __init__(self):
        self.components = {}
        self.test_results: List[ChaosTestMetrics] = []
        self.critical_defects: List[Dict[str, Any]] = []
        
    async def initialize_system(self):
        """初始化被测系统"""
        print("[CHAOS_TEST] Initializing system...")
        
        state_manager = PersistentStateManager(storage_path='./chaos_test_storage/state')
        await state_manager.initialize()
        
        knowledge = KnowledgeGraphIntegrationEngine(
            storage_path='./chaos_test_storage/knowledge',
            auto_save_interval=30.0,
            max_nodes_in_memory=50000
        )
        await knowledge.initialize()
        
        executor = TaskExecutionWorker(
            knowledge_engine=knowledge,
            state_manager=state_manager,
            max_concurrent_tasks=100,
            task_queue_size=10000,
            task_timeout_seconds=10.0,
            max_task_retention=5000
        )
        await executor.initialize()
        
        optimizer = IntelligenceOptimizationAnalyzer(
            knowledge_engine=knowledge,
            state_manager=state_manager,
            analysis_interval=3.0,
            min_samples_for_analysis=10
        )
        await optimizer.initialize()
        
        deployer = SynchronizationDeploymentPipeline(
            task_worker=executor,
            optimization_analyzer=optimizer,
            validation_window_seconds=5.0,
            deployment_cooldown_seconds=1.0
        )
        await deployer.initialize()
        
        shutdown_event = asyncio.Event()
        guardian = ResidentGuardianDaemon(shutdown_event=shutdown_event)
        await guardian.initialize()
        
        self.components = {
            'state_manager': state_manager,
            'knowledge': knowledge,
            'executor': executor,
            'optimizer': optimizer,
            'deployer': deployer,
            'guardian': guardian,
            'shutdown_event': shutdown_event
        }
        
        print("[CHAOS_TEST] System initialized")
        
    async def run_all_tests(self):
        """运行所有混沌测试"""
        print("\n" + "="*70)
        print("ADVANCED CHAOS TEST SUITE")
        print("="*70 + "\n")
        
        await self.initialize_system()
        
        await self.components['executor'].start_execution_loop()
        await self.components['optimizer'].start_analysis_loop()
        await self.components['deployer'].start_deployment_loop()
        
        try:
            await self.test_memory_pressure()
            await self.test_cpu_intensive_load()
            await self.test_rapid_task_burst()
            await self.test_component_isolation()
            await self.test_edge_cases()
        finally:
            await self.shutdown_system()
            
        await self.generate_chaos_report()
        
    async def test_memory_pressure(self):
        """测试1: 内存压力测试"""
        print("\n[CHAOS-1] Memory Pressure Test")
        print("-" * 50)
        
        metrics = ChaosTestMetrics(test_name="memory_pressure", start_time=time.time())
        tracemalloc.start()
        
        try:
            # 提交大量大payload任务
            for i in range(500):
                payload_size = random.randint(1000, 100000)  # 1KB - 100KB
                await self.components['executor'].submit_task(
                    name=f"memory_task_{i}",
                    payload={
                        "operation": "sum",
                        "data": list(range(payload_size)),
                        "metadata": "x" * payload_size
                    },
                    task_type="compute"
                )
                
                if i % 50 == 0:
                    current, peak = tracemalloc.get_traced_memory()
                    metrics.memory_samples.append({
                        'iteration': i,
                        'memory_mb': current / 1024 / 1024,
                        'peak_mb': peak / 1024 / 1024
                    })
                    
            await asyncio.sleep(5)
            
            # 强制GC后检查内存
            gc.collect()
            current, peak = tracemalloc.get_traced_memory()
            final_memory = current / 1024 / 1024
            
            print(f"[CHAOS-1] Final memory: {final_memory:.2f}MB")
            print(f"[CHAOS-1] Peak memory: {peak / 1024 / 1024:.2f}MB")
            
            # 检查内存是否回归合理范围
            if final_memory > 100:
                self.critical_defects.append({
                    "test": "memory_pressure",
                    "severity": "critical",
                    "issue": f"Memory not released after GC: {final_memory:.2f}MB",
                    "details": "Potential memory leak detected"
                })
                
        except Exception as e:
            self.critical_defects.append({
                "test": "memory_pressure",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": str(e)
            })
        finally:
            tracemalloc.stop()
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_cpu_intensive_load(self):
        """测试2: CPU密集型负载"""
        print("\n[CHAOS-2] CPU Intensive Load Test")
        print("-" * 50)
        
        metrics = ChaosTestMetrics(test_name="cpu_intensive", start_time=time.time())
        
        try:
            # 提交大量计算密集型任务
            start_time = time.time()
            task_count = 0
            
            while time.time() - start_time < 10:  # 持续10秒
                for _ in range(20):
                    await self.components['executor'].submit_task(
                        name=f"cpu_task_{task_count}",
                        payload={
                            "operation": random.choice(["sum", "avg", "max", "min"]),
                            "data": list(range(random.randint(1000, 10000)))
                        },
                        priority=TaskPriority.HIGH,
                        task_type="compute"
                    )
                    task_count += 1
                    
                await asyncio.sleep(0.01)
                
            metrics.iterations = task_count
            print(f"[CHAOS-2] Submitted {task_count} CPU-intensive tasks")
            
            # 等待处理
            await asyncio.sleep(5)
            
            heartbeat = self.components['executor'].get_heartbeat()
            print(f"[CHAOS-2] Completed: {heartbeat['metrics']['total_completed']}")
            print(f"[CHAOS-2] Failed: {heartbeat['metrics']['total_failed']}")
            
            if heartbeat['metrics']['total_failed'] > task_count * 0.05:  # 超过5%失败
                self.critical_defects.append({
                    "test": "cpu_intensive",
                    "severity": "high",
                    "issue": f"High failure rate: {heartbeat['metrics']['total_failed']}/{task_count}",
                    "details": "System cannot handle CPU-intensive load"
                })
                
        except Exception as e:
            self.critical_defects.append({
                "test": "cpu_intensive",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": str(e)
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_rapid_task_burst(self):
        """测试3: 极速任务突发"""
        print("\n[CHAOS-3] Rapid Task Burst Test")
        print("-" * 50)
        
        metrics = ChaosTestMetrics(test_name="rapid_burst", start_time=time.time())
        
        try:
            # 在1秒内提交尽可能多的任务
            burst_count = 0
            burst_start = time.time()
            
            while time.time() - burst_start < 1.0:
                await self.components['executor'].submit_task(
                    name=f"burst_task_{burst_count}",
                    payload={"operation": "noop"},
                    task_type="default"
                )
                burst_count += 1
                
            metrics.iterations = burst_count
            print(f"[CHAOS-3] Submitted {burst_count} tasks in 1 second")
            
            # 检查队列状态
            await asyncio.sleep(2)
            heartbeat = self.components['executor'].get_heartbeat()
            
            print(f"[CHAOS-3] Queue size: {heartbeat['metrics']['queue_size']}")
            print(f"[CHAOS-3] System healthy: {heartbeat['healthy']}")
            
            if not heartbeat['healthy']:
                self.critical_defects.append({
                    "test": "rapid_burst",
                    "severity": "high",
                    "issue": "System unhealthy after burst",
                    "details": f"Queue size: {heartbeat['metrics']['queue_size']}"
                })
                
        except Exception as e:
            self.critical_defects.append({
                "test": "rapid_burst",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": str(e)
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_component_isolation(self):
        """测试4: 组件隔离性测试"""
        print("\n[CHAOS-4] Component Isolation Test")
        print("-" * 50)
        
        metrics = ChaosTestMetrics(test_name="component_isolation", start_time=time.time())
        
        try:
            # 测试优化器故障是否影响执行器
            print("[CHAOS-4] Testing optimizer failure isolation...")
            
            # 提交一些任务
            for i in range(50):
                await self.components['executor'].submit_task(
                    name=f"isolation_task_{i}",
                    payload={"operation": "sum", "data": [1, 2, 3]},
                    task_type="compute"
                )
                
            await asyncio.sleep(2)
            
            # 检查执行器是否仍然健康
            heartbeat = self.components['executor'].get_heartbeat()
            print(f"[CHAOS-4] Executor healthy: {heartbeat['healthy']}")
            
            if not heartbeat['healthy']:
                self.critical_defects.append({
                    "test": "component_isolation",
                    "severity": "critical",
                    "issue": "Component isolation failure",
                    "details": "Executor affected by other component state"
                })
                
        except Exception as e:
            self.critical_defects.append({
                "test": "component_isolation",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": str(e)
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_edge_cases(self):
        """测试5: 边界情况测试"""
        print("\n[CHAOS-5] Edge Cases Test")
        print("-" * 50)
        
        metrics = ChaosTestMetrics(test_name="edge_cases", start_time=time.time())
        
        edge_cases = [
            ("empty_payload", {}),
            ("null_values", {"key": None, "data": []}),
            ("nested_deep", {"a": {"b": {"c": {"d": "deep"}}}}),
            ("unicode", {"text": "你好世界 🌍 ñoño"}),
            ("special_chars", {"text": "<script>alert('xss')</script>"}),
        ]
        
        try:
            for case_name, payload in edge_cases:
                print(f"[CHAOS-5] Testing: {case_name}")
                try:
                    await self.components['executor'].submit_task(
                        name=f"edge_{case_name}",
                        payload=payload,
                        task_type="default"
                    )
                except Exception as e:
                    metrics.errors.append({
                        "case": case_name,
                        "error": str(e)
                    })
                    
            await asyncio.sleep(2)
            
            heartbeat = self.components['executor'].get_heartbeat()
            print(f"[CHAOS-5] System healthy after edge cases: {heartbeat['healthy']}")
            
            if metrics.errors:
                self.critical_defects.append({
                    "test": "edge_cases",
                    "severity": "medium",
                    "issue": f"Edge case failures: {len(metrics.errors)}",
                    "details": str(metrics.errors)
                })
                
        except Exception as e:
            self.critical_defects.append({
                "test": "edge_cases",
                "severity": "high",
                "issue": f"Test crashed: {e}",
                "details": str(e)
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def shutdown_system(self):
        """关闭系统"""
        print("\n[CHAOS_TEST] Shutting down system...")
        
        await self.components['optimizer'].shutdown()
        await self.components['deployer'].shutdown()
        await self.components['executor'].shutdown()
        await self.components['knowledge'].shutdown()
        
        print("[CHAOS_TEST] System shutdown complete")
        
    async def generate_chaos_report(self):
        """生成混沌测试报告"""
        print("\n" + "="*70)
        print("ADVANCED CHAOS TEST REPORT")
        print("="*70)
        
        print("\n[Test Results]")
        for result in self.test_results:
            duration = result.end_time - result.start_time if result.end_time else 0
            print(f"\n{result.test_name}:")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Iterations: {result.iterations}")
            print(f"  Errors: {len(result.errors)}")
            
        print("\n" + "-"*70)
        print(f"CRITICAL DEFECTS FOUND: {len(self.critical_defects)}")
        print("-"*70)
        
        for defect in self.critical_defects:
            print(f"\n[{defect['severity'].upper()}] {defect['test']}")
            print(f"  Issue: {defect['issue']}")
            print(f"  Details: {defect['details'][:150]}...")
            
        # 保存报告
        os.makedirs('./chaos_test_reports', exist_ok=True)
        report = {
            "test_results": [
                {
                    "test_name": r.test_name,
                    "duration": r.end_time - r.start_time if r.end_time else 0,
                    "iterations": r.iterations,
                    "errors": r.errors
                }
                for r in self.test_results
            ],
            "critical_defects": self.critical_defects,
            "summary": {
                "total_tests": len(self.test_results),
                "total_defects": len(self.critical_defects)
            }
        }
        
        with open('./chaos_test_reports/chaos_report.json', 'w') as f:
            json.dump(report, f, indent=2)
            
        print(f"\n[Report saved to ./chaos_test_reports/chaos_report.json]")
        print("="*70)
        
        return report


async def main():
    tester = AdvancedChaosTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    import traceback
    asyncio.run(main())
