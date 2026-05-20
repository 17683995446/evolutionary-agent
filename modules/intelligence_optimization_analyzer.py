#!/usr/bin/env python3
"""
Intelligence Optimization Analyzer
智能优化层 - 实时复盘执行数据，找短板、改流程、提效率、降损耗

核心职责：
1. 实时分析执行数据
2. 识别性能瓶颈和短板
3. 生成优化方案
4. 持续迭代改进策略
"""

import asyncio
import time
import statistics
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum
import json


class OptimizationType(Enum):
    """优化类型枚举"""
    BATCH_SIZE = "batch_size"           # 批处理大小
    CONCURRENCY = "concurrency"         # 并发度
    TIMEOUT = "timeout"                 # 超时设置
    PRIORITY = "priority"               # 优先级调整
    RESOURCE = "resource"               # 资源分配
    PIPELINE = "pipeline"               # 流程优化


@dataclass
class Bottleneck:
    """瓶颈分析结果"""
    type: str
    severity: float  # 0-1
    description: str
    affected_components: List[str]
    suggested_actions: List[str]
    expected_improvement: float


@dataclass
class OptimizationRule:
    """优化规则"""
    rule_id: str
    optimization_type: OptimizationType
    target_component: str
    parameters: Dict[str, Any]
    confidence: float  # 0-1
    created_at: float
    applied_count: int = 0
    success_rate: float = 0.0


class IntelligenceOptimizationAnalyzer:
    """
    智能优化分析器
    
    设计原则：
    - 数据驱动：基于实际执行数据进行分析
    - 渐进优化：小步快跑，持续改进
    - 效果验证：优化后跟踪验证效果
    - 知识沉淀：优化经验沉淀到知识图谱
    """
    
    def __init__(
        self,
        knowledge_engine: Any,
        state_manager: Any,
        analysis_interval: float = 10.0,
        min_samples_for_analysis: int = 50
    ):
        self.knowledge_engine = knowledge_engine
        self.state_manager = state_manager
        self.analysis_interval = analysis_interval
        self.min_samples_for_analysis = min_samples_for_analysis
        
        # 原始数据缓冲区
        self._execution_data_buffer: deque = deque(maxlen=10000)
        self._metrics_buffer: deque = deque(maxlen=5000)
        
        # 分析结果
        self._current_bottlenecks: List[Bottleneck] = []
        self._optimization_rules: Dict[str, OptimizationRule] = {}
        self._rule_effectiveness: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=100)
        )
        
        # 分析统计
        self._analysis_count = 0
        self._optimization_count = 0
        self._last_analysis_time = 0.0
        
        # 性能基线
        self._performance_baselines: Dict[str, float] = {
            "avg_execution_time": 0.0,
            "success_rate": 1.0,
            "throughput": 0.0
        }
        
        # 运行状态
        self._is_running = False
        self._analyzer_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """初始化优化分析器"""
        print("[OPTIMIZER] Initializing intelligence optimization analyzer...")
        
        # 加载历史优化规则
        saved_rules = await self.state_manager.load_optimization_rules()
        if saved_rules:
            for rule_data in saved_rules:
                # 修复：从 JSON 加载后 optimization_type 是字符串，需转换为枚举
                if isinstance(rule_data.get("optimization_type"), str):
                    rule_data["optimization_type"] = OptimizationType(rule_data["optimization_type"])
                rule = OptimizationRule(**rule_data)
                self._optimization_rules[rule.rule_id] = rule
        
        self._is_running = True
        
    async def start_analysis_loop(self):
        """启动分析循环"""
        print("[OPTIMIZER] Starting optimization analysis loop...")
        self._analyzer_task = asyncio.create_task(self._analysis_loop())
    
    async def _analysis_loop(self):
        """分析主循环 - 定期执行优化分析"""
        while self._is_running:
            try:
                await asyncio.sleep(self.analysis_interval)
                
                # 检查数据量是否足够
                if len(self._execution_data_buffer) >= self.min_samples_for_analysis:
                    await self._perform_analysis()
                
            except Exception as e:
                print(f"[OPTIMIZER] Analysis loop error: {e}")
    
    async def _perform_analysis(self):
        """执行优化分析"""
        self._analysis_count += 1
        self._last_analysis_time = time.time()
        
        print(f"\n[OPTIMIZER] === Analysis Cycle #{self._analysis_count} ===")
        
        # 1. 分析执行数据
        execution_stats = self._analyze_execution_data()
        
        # 2. 识别瓶颈
        bottlenecks = self._identify_bottlenecks(execution_stats)
        self._current_bottlenecks = bottlenecks
        
        # 3. 生成优化规则
        new_rules = self._generate_optimization_rules(bottlenecks, execution_stats)
        
        # 4. 评估现有规则效果
        await self._evaluate_rule_effectiveness()
        
        # 5. 记录分析结果到知识图谱
        await self._record_analysis_to_knowledge(
            execution_stats, bottlenecks, new_rules
        )
        
        # 输出分析摘要
        print(f"[OPTIMIZER] Bottlenecks found: {len(bottlenecks)}")
        print(f"[OPTIMIZER] New rules generated: {len(new_rules)}")
        print(f"[OPTIMIZER] Total active rules: {len(self._optimization_rules)}")
    
    def _analyze_execution_data(self) -> Dict[str, Any]:
        """分析执行数据"""
        data = list(self._execution_data_buffer)
        
        if not data:
            return {}
        
        # 按任务类型分组统计
        task_type_stats = defaultdict(lambda: {
            "count": 0,
            "durations": [],
            "successes": 0,
            "failures": 0
        })
        
        for record in data:
            task_type = record.get("task_type", "default")
            stats = task_type_stats[task_type]
            stats["count"] += 1
            stats["durations"].append(record.get("duration_ms", 0))
            if record.get("success"):
                stats["successes"] += 1
            else:
                stats["failures"] += 1
        
        # 计算统计指标
        analysis_result = {}
        for task_type, stats in task_type_stats.items():
            durations = stats["durations"]
            analysis_result[task_type] = {
                "total_count": stats["count"],
                "success_count": stats["successes"],
                "failure_count": stats["failures"],
                "success_rate": stats["successes"] / stats["count"] if stats["count"] > 0 else 0,
                "avg_duration_ms": statistics.mean(durations) if durations else 0,
                "median_duration_ms": statistics.median(durations) if durations else 0,
                "std_duration_ms": statistics.stdev(durations) if len(durations) > 1 else 0,
                "min_duration_ms": min(durations) if durations else 0,
                "max_duration_ms": max(durations) if durations else 0,
                "p95_duration_ms": self._percentile(durations, 95) if durations else 0,
                "p99_duration_ms": self._percentile(durations, 99) if durations else 0
            }
        
        # 更新基线
        all_durations = [r.get("duration_ms", 0) for r in data]
        self._performance_baselines["avg_execution_time"] = statistics.mean(all_durations) if all_durations else 0
        self._performance_baselines["success_rate"] = sum(1 for r in data if r.get("success")) / len(data) if data else 1.0
        
        return analysis_result
    
    def _identify_bottlenecks(self, execution_stats: Dict[str, Any]) -> List[Bottleneck]:
        """识别性能瓶颈"""
        bottlenecks = []
        
        for task_type, stats in execution_stats.items():
            # 瓶颈1: 高失败率
            if stats["success_rate"] < 0.95:
                severity = 1.0 - stats["success_rate"]
                bottlenecks.append(Bottleneck(
                    type="high_failure_rate",
                    severity=severity,
                    description=f"{task_type} has {stats['failure_count']} failures "
                               f"({(1-stats['success_rate'])*100:.1f}% failure rate)",
                    affected_components=[task_type],
                    suggested_actions=[
                        "Increase retry attempts",
                        "Add error handling",
                        "Reduce task complexity"
                    ],
                    expected_improvement=severity * 0.5
                ))
            
            # 瓶颈2: 高延迟
            if stats["avg_duration_ms"] > 100:
                severity = min(stats["avg_duration_ms"] / 1000, 1.0)
                bottlenecks.append(Bottleneck(
                    type="high_latency",
                    severity=severity,
                    description=f"{task_type} avg duration {stats['avg_duration_ms']:.1f}ms",
                    affected_components=[task_type],
                    suggested_actions=[
                        "Increase batch size",
                        "Optimize algorithm",
                        "Add caching"
                    ],
                    expected_improvement=severity * 0.3
                ))
            
            # 瓶颈3: 高方差（不稳定）
            if stats.get("std_duration_ms", 0) > stats["avg_duration_ms"] * 0.5:
                severity = min(stats["std_duration_ms"] / stats["avg_duration_ms"], 1.0) if stats["avg_duration_ms"] > 0 else 0
                bottlenecks.append(Bottleneck(
                    type="high_variance",
                    severity=severity,
                    description=f"{task_type} has high variance "
                               f"(std: {stats['std_duration_ms']:.1f}ms)",
                    affected_components=[task_type],
                    suggested_actions=[
                        "Add resource limits",
                        "Implement backpressure",
                        "Optimize resource allocation"
                    ],
                    expected_improvement=severity * 0.2
                ))
        
        # 按严重程度排序
        bottlenecks.sort(key=lambda b: b.severity, reverse=True)
        
        return bottlenecks[:5]  # 只保留前5个最严重的
    
    def _generate_optimization_rules(
        self,
        bottlenecks: List[Bottleneck],
        execution_stats: Dict[str, Any]
    ) -> List[OptimizationRule]:
        """基于瓶颈生成优化规则"""
        new_rules = []
        
        for bottleneck in bottlenecks:
            if bottleneck.severity < 0.3:
                continue  # 忽略轻微瓶颈
            
            for component in bottleneck.affected_components:
                stats = execution_stats.get(component, {})
                
                # 根据瓶颈类型生成规则
                if bottleneck.type == "high_failure_rate":
                    rule = OptimizationRule(
                        rule_id=f"retry_{component}_{int(time.time())}",
                        optimization_type=OptimizationType.PIPELINE,
                        target_component=component,
                        parameters={
                            "max_retries": 5,
                            "retry_delay_ms": 100,
                            "exponential_backoff": True
                        },
                        confidence=bottleneck.severity * 0.8,
                        created_at=time.time()
                    )
                    new_rules.append(rule)
                    
                elif bottleneck.type == "high_latency":
                    # 批处理优化
                    current_batch = self._optimization_rules.get(f"batch_{component}")
                    current_size = current_batch.parameters.get("batch_size", 1) if current_batch else 1
                    
                    rule = OptimizationRule(
                        rule_id=f"batch_{component}_{int(time.time())}",
                        optimization_type=OptimizationType.BATCH_SIZE,
                        target_component=component,
                        parameters={
                            "batch_size": min(current_size * 2, 100),
                            "batch_timeout_ms": 50
                        },
                        confidence=bottleneck.severity * 0.7,
                        created_at=time.time()
                    )
                    new_rules.append(rule)
                    
                elif bottleneck.type == "high_variance":
                    # 并发度优化
                    rule = OptimizationRule(
                        rule_id=f"concurrency_{component}_{int(time.time())}",
                        optimization_type=OptimizationType.CONCURRENCY,
                        target_component=component,
                        parameters={
                            "max_concurrent": 5,
                            "adaptive_scaling": True
                        },
                        confidence=bottleneck.severity * 0.6,
                        created_at=time.time()
                    )
                    new_rules.append(rule)
        
        # 保存新规则
        for rule in new_rules:
            self._optimization_rules[rule.rule_id] = rule
            self._optimization_count += 1
        
        return new_rules
    
    async def _evaluate_rule_effectiveness(self):
        """评估优化规则效果"""
        for rule_id, rule in list(self._optimization_rules.items()):
            if rule.applied_count == 0:
                continue
            
            # 计算规则成功率
            effectiveness_history = self._rule_effectiveness[rule_id]
            if len(effectiveness_history) >= 10:
                recent_success_rate = sum(effectiveness_history) / len(effectiveness_history)
                rule.success_rate = recent_success_rate
                
                # 如果规则效果不好，降低置信度或移除
                if recent_success_rate < 0.3 and rule.applied_count > 20:
                    print(f"[OPTIMIZER] Rule {rule_id} underperforming, removing")
                    del self._optimization_rules[rule_id]
                    # 修复(R9-CASCADE-04): 清理孤儿效果数据，防止内存泄漏
                    self._rule_effectiveness.pop(rule_id, None)
    
    async def _record_analysis_to_knowledge(
        self,
        execution_stats: Dict[str, Any],
        bottlenecks: List[Bottleneck],
        new_rules: List[OptimizationRule]
    ):
        """记录分析结果到知识图谱"""
        analysis_record = {
            "timestamp": time.time(),
            "analysis_id": self._analysis_count,
            "execution_stats": execution_stats,
            "bottlenecks": [
                {
                    "type": b.type,
                    "severity": b.severity,
                    "description": b.description,
                    "affected_components": b.affected_components
                }
                for b in bottlenecks
            ],
            "new_rules": [
                {
                    "rule_id": r.rule_id,
                    "type": r.optimization_type.value,
                    "target": r.target_component,
                    "confidence": r.confidence
                }
                for r in new_rules
            ],
            "baselines": self._performance_baselines.copy()
        }
        
        await self.knowledge_engine.add_optimization_record(analysis_record)
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """计算百分位数"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]
    
    # ============== 数据输入接口 ==============
    
    def feed_execution_data(self, data: Dict[str, Any]):
        """接收执行数据（由执行层调用）"""
        self._execution_data_buffer.append(data)
    
    def feed_metrics_data(self, metrics: Dict[str, Any]):
        """接收指标数据"""
        self._metrics_buffer.append(metrics)
    
    def record_rule_effectiveness(self, rule_id: str, was_successful: bool):
        """记录规则效果"""
        if rule_id in self._optimization_rules:
            self._optimization_rules[rule_id].applied_count += 1
            self._rule_effectiveness[rule_id].append(1.0 if was_successful else 0.0)
    
    # ============== 输出接口 ==============
    
    def get_optimization_rules(self) -> Dict[str, Dict[str, Any]]:
        """获取当前优化规则（供同步层调用）"""
        return {
            rule.target_component: rule.parameters
            for rule in self._optimization_rules.values()
            if rule.confidence > 0.5  # 只返回高置信度规则
        }
    
    def get_heartbeat(self) -> Dict[str, Any]:
        """获取心跳数据"""
        return {
            "healthy": True,
            "metrics": {
                "analysis_count": self._analysis_count,
                "optimization_count": self._optimization_count,
                "active_rules": len(self._optimization_rules),
                "data_buffer_size": len(self._execution_data_buffer),
                "current_bottlenecks": len(self._current_bottlenecks),
                "last_analysis_time": self._last_analysis_time
            }
        }
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """获取分析摘要"""
        return {
            "analysis_count": self._analysis_count,
            "optimization_count": self._optimization_count,
            "current_bottlenecks": [
                {
                    "type": b.type,
                    "severity": b.severity,
                    "description": b.description
                }
                for b in self._current_bottlenecks
            ],
            "active_rules": len(self._optimization_rules),
            "performance_baselines": self._performance_baselines.copy()
        }
    
    async def shutdown(self):
        """关闭优化分析器"""
        print("[OPTIMIZER] Shutting down optimization analyzer...")
        self._is_running = False
        
        if self._analyzer_task:
            self._analyzer_task.cancel()
            try:
                await self._analyzer_task
            except asyncio.CancelledError:
                pass
        
        # 保存优化规则
        rules_data = [
            {
                "rule_id": r.rule_id,
                "optimization_type": r.optimization_type.value,
                "target_component": r.target_component,
                "parameters": r.parameters,
                "confidence": r.confidence,
                "created_at": r.created_at,
                "applied_count": r.applied_count,
                "success_rate": r.success_rate
            }
            for r in self._optimization_rules.values()
        ]
        await self.state_manager.save_optimization_rules(rules_data)
        
        print("[OPTIMIZER] Optimization analyzer shutdown complete")
