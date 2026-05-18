#!/usr/bin/env python3
"""
Stress Test Comprehensive Framework
综合压力测试框架 - 发现系统缺陷的完整测试套件

测试维度：
1. 高并发压力测试 - 检测并发瓶颈
2. 长时间稳定性测试 - 检测内存泄漏、资源耗尽
3. 故障注入测试 - 检测容错能力
4. 极端负载测试 - 检测系统极限
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

# Use absolute imports
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
class TestMetrics:
    """测试指标"""
    test_name: str
    start_time: float
    end_time: Optional[float] = None
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    avg_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    throughput_per_sec: float = 0.0
    memory_peak_mb: float = 0.0
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "duration_sec": self.end_time - self.start_time if self.end_time else None,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.completed_tasks / max(self.total_tasks, 1),
            "avg_latency_ms": self.avg_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "throughput_per_sec": self.throughput_per_sec,
            "memory_peak_mb": self.memory_peak_mb,
            "error_count": len(self.errors)
        }


class ComprehensiveStressTester:
    """综合压力测试器"""
    
    def __init__(self):
        self.components = {}
        self.test_results: List[TestMetrics] = []
        self.defects_found: List[Dict[str, Any]] = []
        
    async def initialize_system(self):
        """初始化被测系统"""
        print("[STRESS_TEST] Initializing system under test...")
        
        state_manager = PersistentStateManager(storage_path='./stress_test_storage/state')
        await state_manager.initialize()
        
        knowledge = KnowledgeGraphIntegrationEngine(
            storage_path='./stress_test_storage/knowledge',
            auto_save_interval=30.0
        )
        await knowledge.initialize()
        
        executor = TaskExecutionWorker(
            knowledge_engine=knowledge,
            state_manager=state_manager,
            max_concurrent_tasks=50,
            task_queue_size=5000
        )
        await executor.initialize()
        
        optimizer = IntelligenceOptimizationAnalyzer(
            knowledge_engine=knowledge,
            state_manager=state_manager,
            analysis_interval=5.0,
            min_samples_for_analysis=20
        )
        await optimizer.initialize()
        
        deployer = SynchronizationDeploymentPipeline(
            task_worker=executor,
            optimization_analyzer=optimizer,
            validation_window_seconds=10.0,
            deployment_cooldown_seconds=2.0
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
        
        print("[STRESS_TEST] System initialized")
        
    async def run_all_tests(self):
        """运行所有测试"""
        print("\n" + "="*70)
        print("COMPREHENSIVE STRESS TEST SUITE")
        print("="*70 + "\n")
        
        await self.initialize_system()
        
        # 启动所有组件
        await self.components['executor'].start_execution_loop()
        await self.components['optimizer'].start_analysis_loop()
        await self.components['deployer'].start_deployment_loop()
        
        try:
            # 测试1: 高并发压力测试
            await self.test_high_concurrency()
            
            # 测试2: 长时间稳定性测试
            await self.test_long_term_stability()
            
            # 测试3: 故障注入测试
            await self.test_fault_injection()
            
            # 测试4: 极端负载测试
            await self.test_extreme_load()
            
        finally:
            await self.shutdown_system()
            
        # 生成测试报告
        await self.generate_test_report()
        
    async def test_high_concurrency(self):
        """测试1: 高并发压力测试"""
        print("\n[TEST-1] High Concurrency Stress Test")
        print("-" * 50)
        
        metrics = TestMetrics(
            test_name="high_concurrency",
            start_time=time.time()
        )
        
        # 启动内存追踪
        tracemalloc.start()
        
        try:
            # 快速提交大量任务
            task_count = 1000
            latencies = []
            
            print(f"[TEST-1] Submitting {task_count} tasks rapidly...")
            submit_start = time.time()
            
            for i in range(task_count):
                task_start = time.time()
                await self.components['executor'].submit_task(
                    name=f"concurrent_task_{i}",
                    payload={
                        "operation": random.choice(["sum", "avg", "max", "min"]),
                        "data": list(range(random.randint(10, 1000)))
                    },
                    priority=random.choice([
                        TaskPriority.CRITICAL,
                        TaskPriority.HIGH,
                        TaskPriority.NORMAL,
                        TaskPriority.LOW
                    ]),
                    task_type="compute"
                )
                latencies.append((time.time() - task_start) * 1000)
                
            submit_duration = time.time() - submit_start
            
            # 等待任务完成
            print(f"[TEST-1] Waiting for tasks to complete...")
            await asyncio.sleep(5)
            
            # 收集指标
            heartbeat = self.components['executor'].get_heartbeat()
            metrics.total_tasks = task_count
            metrics.completed_tasks = heartbeat['metrics']['total_completed']
            metrics.failed_tasks = heartbeat['metrics']['total_failed']
            metrics.avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
            metrics.p99_latency_ms = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
            metrics.throughput_per_sec = task_count / submit_duration if submit_duration > 0 else 0
            
            # 检查内存
            current, peak = tracemalloc.get_traced_memory()
            metrics.memory_peak_mb = peak / 1024 / 1024
            
            print(f"[TEST-1] Results:")
            print(f"  - Submitted: {metrics.total_tasks}")
            print(f"  - Completed: {metrics.completed_tasks}")
            print(f"  - Failed: {metrics.failed_tasks}")
            print(f"  - Success rate: {metrics.completed_tasks/max(metrics.total_tasks,1)*100:.1f}%")
            print(f"  - Avg submit latency: {metrics.avg_latency_ms:.2f}ms")
            print(f"  - P99 submit latency: {metrics.p99_latency_ms:.2f}ms")
            print(f"  - Throughput: {metrics.throughput_per_sec:.1f} tasks/sec")
            print(f"  - Memory peak: {metrics.memory_peak_mb:.2f}MB")
            
            # 检查缺陷
            if metrics.failed_tasks > 0:
                self.defects_found.append({
                    "test": "high_concurrency",
                    "severity": "high",
                    "issue": f"Task failures detected: {metrics.failed_tasks}",
                    "details": "Some tasks failed under high concurrency"
                })
                
            if metrics.p99_latency_ms > 100:
                self.defects_found.append({
                    "test": "high_concurrency",
                    "severity": "medium",
                    "issue": f"High P99 latency: {metrics.p99_latency_ms:.2f}ms",
                    "details": "Task submission latency is too high"
                })
                
        except Exception as e:
            metrics.errors.append(str(e))
            self.defects_found.append({
                "test": "high_concurrency",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": traceback.format_exc()
            })
        finally:
            tracemalloc.stop()
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_long_term_stability(self):
        """测试2: 长时间稳定性测试"""
        print("\n[TEST-2] Long-term Stability Test")
        print("-" * 50)
        
        metrics = TestMetrics(
            test_name="long_term_stability",
            start_time=time.time()
        )
        
        test_duration = 30  # 30秒持续测试
        memory_samples = []
        throughput_samples = []
        
        print(f"[TEST-2] Running for {test_duration} seconds...")
        
        # 启动内存追踪
        tracemalloc.start()
        
        try:
            start_time = time.time()
            task_counter = 0
            
            while time.time() - start_time < test_duration:
                # 持续提交任务
                for _ in range(10):
                    await self.components['executor'].submit_task(
                        name=f"stability_task_{task_counter}",
                        payload={
                            "operation": "sum",
                            "data": list(range(100))
                        },
                        task_type="compute"
                    )
                    task_counter += 1
                
                # 每5秒采样
                elapsed = int(time.time() - start_time)
                if elapsed > 0 and elapsed % 5 == 0 and elapsed not in [s.get('time') for s in memory_samples]:
                    heartbeat = self.components['executor'].get_heartbeat()
                    throughput_samples.append(heartbeat['metrics']['total_completed'])
                    
                    # 强制垃圾回收后检查内存 (使用tracemalloc替代psutil)
                    gc.collect()
                    current, peak = tracemalloc.get_traced_memory()
                    memory_mb = current / 1024 / 1024
                    memory_samples.append({'time': elapsed, 'memory': memory_mb})
                    print(f"  [Sample @ {elapsed}s] Memory: {memory_mb:.2f}MB, Completed: {heartbeat['metrics']['total_completed']}")
                    
                await asyncio.sleep(0.1)
            
            metrics.total_tasks = task_counter
            metrics.end_time = time.time()
            
            # 分析内存趋势
            if len(memory_samples) >= 3:
                mem_values = [s['memory'] for s in memory_samples]
                first_half_avg = sum(mem_values[:len(mem_values)//2]) / (len(mem_values)//2)
                second_half_avg = sum(mem_values[len(mem_values)//2:]) / (len(mem_values)//2)
                memory_growth = second_half_avg - first_half_avg
                
                print(f"[TEST-2] Results:")
                print(f"  - Total tasks submitted: {task_counter}")
                print(f"  - Memory samples: {len(memory_samples)}")
                print(f"  - Memory growth: {memory_growth:.2f}MB")
                print(f"  - Avg memory: {sum(mem_values)/len(mem_values):.2f}MB")
                
                if memory_growth > 10:
                    self.defects_found.append({
                        "test": "long_term_stability",
                        "severity": "critical",
                        "issue": f"Memory leak detected: {memory_growth:.2f}MB growth",
                        "details": f"Memory grew from {first_half_avg:.2f}MB to {second_half_avg:.2f}MB"
                    })
                    
        except Exception as e:
            metrics.errors.append(str(e))
            self.defects_found.append({
                "test": "long_term_stability",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": traceback.format_exc()
            })
        finally:
            tracemalloc.stop()
            if not metrics.end_time:
                metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_fault_injection(self):
        """测试3: 故障注入测试"""
        print("\n[TEST-3] Fault Injection Test")
        print("-" * 50)
        
        metrics = TestMetrics(
            test_name="fault_injection",
            start_time=time.time()
        )
        
        try:
            # 测试1: 队列溢出
            print("[TEST-3] Testing queue overflow handling...")
            overflow_tasks = 10000
            for i in range(overflow_tasks):
                try:
                    await asyncio.wait_for(
                        self.components['executor'].submit_task(
                            name=f"overflow_task_{i}",
                            payload={"data": "x" * 1000},
                            task_type="io"
                        ),
                        timeout=0.01
                    )
                except asyncio.TimeoutError:
                    metrics.errors.append(f"Queue full at task {i}")
                    break
                    
            # 测试2: 无效任务类型
            print("[TEST-3] Testing invalid task type handling...")
            try:
                await self.components['executor'].submit_task(
                    name="invalid_task",
                    payload={},
                    task_type="nonexistent_type"
                )
            except Exception as e:
                self.defects_found.append({
                    "test": "fault_injection",
                    "severity": "medium",
                    "issue": "Invalid task type not handled gracefully",
                    "details": str(e)
                })
                
            # 测试3: 超大payload
            print("[TEST-3] Testing large payload handling...")
            try:
                await self.components['executor'].submit_task(
                    name="large_payload_task",
                    payload={"data": "x" * 10000000},  # 10MB
                    task_type="compute"
                )
            except Exception as e:
                self.defects_found.append({
                    "test": "fault_injection",
                    "severity": "low",
                    "issue": "Large payload handling issue",
                    "details": str(e)
                })
                
            await asyncio.sleep(2)
            
            heartbeat = self.components['executor'].get_heartbeat()
            print(f"[TEST-3] System health after fault injection: {heartbeat['healthy']}")
            
        except Exception as e:
            metrics.errors.append(str(e))
            self.defects_found.append({
                "test": "fault_injection",
                "severity": "high",
                "issue": f"Test crashed: {e}",
                "details": traceback.format_exc()
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def test_extreme_load(self):
        """测试4: 极端负载测试"""
        print("\n[TEST-4] Extreme Load Test")
        print("-" * 50)
        
        metrics = TestMetrics(
            test_name="extreme_load",
            start_time=time.time()
        )
        
        try:
            # 短时间内提交大量高优先级任务
            print("[TEST-4] Submitting burst of high-priority tasks...")
            burst_size = 500
            
            for i in range(burst_size):
                await self.components['executor'].submit_task(
                    name=f"burst_task_{i}",
                    payload={
                        "operation": "complex",
                        "data": list(range(random.randint(100, 10000)))
                    },
                    priority=TaskPriority.CRITICAL,
                    task_type="compute"
                )
                
            print(f"[TEST-4] Submitted {burst_size} critical tasks")
            
            # 立即检查系统响应
            await asyncio.sleep(1)
            
            heartbeat = self.components['executor'].get_heartbeat()
            print(f"[TEST-4] Queue size: {heartbeat['metrics']['queue_size']}")
            print(f"[TEST-4] Running tasks: {heartbeat['metrics']['running_tasks']}")
            
            # 等待处理完成
            await asyncio.sleep(10)
            
            final_heartbeat = self.components['executor'].get_heartbeat()
            metrics.total_tasks = burst_size
            metrics.completed_tasks = final_heartbeat['metrics']['total_completed']
            metrics.failed_tasks = final_heartbeat['metrics']['total_failed']
            
            print(f"[TEST-4] Results:")
            print(f"  - Completed: {metrics.completed_tasks}")
            print(f"  - Failed: {metrics.failed_tasks}")
            
            if metrics.failed_tasks > burst_size * 0.1:  # 超过10%失败
                self.defects_found.append({
                    "test": "extreme_load",
                    "severity": "critical",
                    "issue": f"High failure rate under extreme load: {metrics.failed_tasks}/{burst_size}",
                    "details": "System cannot handle burst traffic"
                })
                
        except Exception as e:
            metrics.errors.append(str(e))
            self.defects_found.append({
                "test": "extreme_load",
                "severity": "critical",
                "issue": f"Test crashed: {e}",
                "details": traceback.format_exc()
            })
        finally:
            metrics.end_time = time.time()
            self.test_results.append(metrics)
            
    async def shutdown_system(self):
        """关闭被测系统"""
        print("\n[STRESS_TEST] Shutting down system...")
        
        await self.components['optimizer'].shutdown()
        await self.components['deployer'].shutdown()
        await self.components['executor'].shutdown()
        await self.components['knowledge'].shutdown()
        
        print("[STRESS_TEST] System shutdown complete")
        
    async def generate_test_report(self):
        """生成测试报告"""
        print("\n" + "="*70)
        print("STRESS TEST REPORT")
        print("="*70)
        
        # 汇总测试结果
        print("\n[Test Results Summary]")
        for result in self.test_results:
            print(f"\n{result.test_name}:")
            print(f"  Duration: {result.end_time - result.start_time:.2f}s")
            print(f"  Tasks: {result.total_tasks} total, {result.completed_tasks} completed, {result.failed_tasks} failed")
            if result.avg_latency_ms > 0:
                print(f"  Latency: avg={result.avg_latency_ms:.2f}ms, p99={result.p99_latency_ms:.2f}ms")
            if result.memory_peak_mb > 0:
                print(f"  Memory peak: {result.memory_peak_mb:.2f}MB")
                
        # 缺陷汇总
        print("\n" + "-"*70)
        print(f"DEFECTS FOUND: {len(self.defects_found)}")
        print("-"*70)
        
        critical = [d for d in self.defects_found if d['severity'] == 'critical']
        high = [d for d in self.defects_found if d['severity'] == 'high']
        medium = [d for d in self.defects_found if d['severity'] == 'medium']
        low = [d for d in self.defects_found if d['severity'] == 'low']
        
        print(f"  Critical: {len(critical)}")
        print(f"  High: {len(high)}")
        print(f"  Medium: {len(medium)}")
        print(f"  Low: {len(low)}")
        
        for defect in self.defects_found:
            print(f"\n[{defect['severity'].upper()}] {defect['test']}")
            print(f"  Issue: {defect['issue']}")
            print(f"  Details: {defect['details'][:200]}...")
            
        # 保存详细报告
        report = {
            "test_results": [r.to_dict() for r in self.test_results],
            "defects": self.defects_found,
            "summary": {
                "total_tests": len(self.test_results),
                "total_defects": len(self.defects_found),
                "critical_defects": len(critical),
                "high_defects": len(high)
            }
        }
        
        os.makedirs('./stress_test_reports', exist_ok=True)
        with open('./stress_test_reports/report.json', 'w') as f:
            json.dump(report, f, indent=2)
            
        print(f"\n[Report saved to ./stress_test_reports/report.json]")
        print("="*70)
        
        return report


async def main():
    tester = ComprehensiveStressTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    import traceback
    asyncio.run(main())
