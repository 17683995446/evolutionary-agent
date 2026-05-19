#!/usr/bin/env python3
"""
Intelligent Task Scheduler
智能任务调度器 - 基于任务特征和资源状况的自适应调度

核心特性：
1. 预测性调度：基于历史数据预测任务执行时间
2. 资源感知：动态调整调度策略基于当前资源状况
3. 公平调度：防止低优先级任务饿死
4. 亲和性调度：相关任务就近执行
"""

import asyncio
import time
import math
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import statistics


class SchedulingStrategy(Enum):
    """调度策略枚举"""
    FIFO = "fifo"                    # 先进先出
    PRIORITY = "priority"            # 优先级调度
    FAIR = "fair"                    # 公平调度
    SHORTEST_JOB_FIRST = "sjf"       # 最短任务优先
    RESOURCE_AWARE = "resource_aware"  # 资源感知调度
    ADAPTIVE = "adaptive"            # 自适应调度


@dataclass
class TaskProfile:
    """任务画像 - 描述任务特征"""
    task_type: str
    avg_execution_time: float = 0.0
    execution_time_variance: float = 0.0
    success_rate: float = 1.0
    resource_intensity: float = 1.0  # 1.0 = 平均资源消耗
    sample_count: int = 0
    last_updated: float = 0.0


@dataclass
class ResourceSnapshot:
    """资源快照"""
    timestamp: float
    cpu_utilization: float = 0.0
    memory_utilization: float = 0.0
    queue_depth: int = 0
    active_tasks: int = 0
    throughput: float = 0.0  # 任务/秒


@dataclass
class SchedulingDecision:
    """调度决策"""
    task_id: str
    priority: int
    predicted_duration: float
    scheduled_at: float
    strategy_used: SchedulingStrategy
    reasoning: str


class IntelligentTaskScheduler:
    """
    智能任务调度器
    
    设计原则：
    - 数据驱动：基于历史数据做决策
    - 自适应：根据系统状态动态调整
    - 公平性：防止任务饿死
    - 可解释：每个决策都有推理依据
    """
    
    def __init__(
        self,
        default_strategy: SchedulingStrategy = SchedulingStrategy.ADAPTIVE,
        history_window: int = 1000,
        prediction_smoothing: float = 0.3
    ):
        self.default_strategy = default_strategy
        self.history_window = history_window
        self.prediction_smoothing = prediction_smoothing
        
        # 任务画像库
        self._task_profiles: Dict[str, TaskProfile] = {}
        
        # 执行历史
        self._execution_history: deque = deque(maxlen=history_window)
        
        # 资源历史
        self._resource_history: deque = deque(maxlen=100)
        
        # 调度决策历史
        self._scheduling_decisions: deque = deque(maxlen=500)
        
        # 公平调度状态
        self._fair_queue: Dict[str, int] = defaultdict(int)  # task_type -> deficit
        self._starvation_threshold = 10  # 饥饿阈值
        
        # 统计
        self._stats = {
            "total_scheduled": 0,
            "predictions_made": 0,
            "strategy_switches": 0,
            "starvation_prevented": 0
        }
        
        # 当前策略
        self._current_strategy = default_strategy
        self._strategy_performance: Dict[SchedulingStrategy, deque] = {
            strategy: deque(maxlen=50) for strategy in SchedulingStrategy
        }
    
    def record_execution(
        self,
        task_id: str,
        task_type: str,
        duration: float,
        success: bool,
        resource_usage: Optional[Dict[str, float]] = None
    ):
        """记录任务执行结果，用于学习和预测"""
        self._execution_history.append({
            "task_id": task_id,
            "task_type": task_type,
            "duration": duration,
            "success": success,
            "resource_usage": resource_usage or {},
            "timestamp": time.time()
        })
        
        # 更新任务画像
        self._update_task_profile(task_type, duration, success)
    
    def _update_task_profile(self, task_type: str, duration: float, success: bool):
        """更新任务画像（增量更新）"""
        profile = self._task_profiles.get(task_type)
        
        if profile is None:
            profile = TaskProfile(
                task_type=task_type,
                avg_execution_time=duration,
                sample_count=1,
                last_updated=time.time()
            )
            self._task_profiles[task_type] = profile
            return
        
        # 使用 Welford 算法增量更新均值和方差
        n = profile.sample_count + 1
        old_mean = profile.avg_execution_time
        new_mean = old_mean + (duration - old_mean) / n
        old_variance = profile.execution_time_variance
        new_variance = old_variance + (duration - old_mean) * (duration - new_mean) / n
        
        profile.avg_execution_time = new_mean
        profile.execution_time_variance = new_variance
        profile.sample_count = n
        profile.last_updated = time.time()
        
        # 更新成功率（指数移动平均）
        success_value = 1.0 if success else 0.0
        alpha = self.prediction_smoothing
        profile.success_rate = alpha * success_value + (1 - alpha) * profile.success_rate
    
    def record_resource_snapshot(self, snapshot: ResourceSnapshot):
        """记录资源快照"""
        self._resource_history.append(snapshot)
    
    def predict_duration(self, task_type: str) -> Tuple[float, float]:
        """
        预测任务执行时间
        
        Returns:
            (预测时间, 置信度)
        """
        profile = self._task_profiles.get(task_type)
        
        if profile is None or profile.sample_count < 5:
            # 数据不足，使用保守估计
            self._stats["predictions_made"] += 1
            return (1.0, 0.1)  # 默认1秒，低置信度
        
        # 使用均值 + 标准差作为预测（保守估计）
        std_dev = math.sqrt(profile.execution_time_variance) if profile.execution_time_variance > 0 else 0
        predicted = profile.avg_execution_time + std_dev * 0.5
        
        # 置信度基于样本数量和方差
        confidence = min(profile.sample_count / 100, 1.0) * (1.0 - std_dev / (profile.avg_execution_time + 1e-6))
        confidence = max(0.1, min(confidence, 0.95))
        
        self._stats["predictions_made"] += 1
        return (predicted, confidence)
    
    def schedule(
        self,
        pending_tasks: List[Tuple[int, str, str]],  # (priority, task_id, task_type)
        max_batch: int = 10
    ) -> List[SchedulingDecision]:
        """
        智能调度任务
        
        Args:
            pending_tasks: 待调度任务列表 [(优先级, 任务ID, 任务类型), ...]
            max_batch: 最大批量调度数
            
        Returns:
            调度决策列表
        """
        if not pending_tasks:
            return []
        
        # 获取当前资源状态
        resource_state = self._get_current_resource_state()
        
        # 自适应选择策略
        if self.default_strategy == SchedulingStrategy.ADAPTIVE:
            strategy = self._select_adaptive_strategy(resource_state)
        else:
            strategy = self.default_strategy
        
        # 执行调度
        if strategy == SchedulingStrategy.FIFO:
            decisions = self._schedule_fifo(pending_tasks, max_batch)
        elif strategy == SchedulingStrategy.PRIORITY:
            decisions = self._schedule_priority(pending_tasks, max_batch)
        elif strategy == SchedulingStrategy.FAIR:
            decisions = self._schedule_fair(pending_tasks, max_batch)
        elif strategy == SchedulingStrategy.SHORTEST_JOB_FIRST:
            decisions = self._schedule_sjf(pending_tasks, max_batch)
        elif strategy == SchedulingStrategy.RESOURCE_AWARE:
            decisions = self._schedule_resource_aware(pending_tasks, max_batch, resource_state)
        else:
            decisions = self._schedule_priority(pending_tasks, max_batch)
        
        # 记录决策
        for decision in decisions:
            self._scheduling_decisions.append(decision)
            self._stats["total_scheduled"] += 1
        
        return decisions
    
    def _get_current_resource_state(self) -> Dict[str, Any]:
        """获取当前资源状态"""
        if not self._resource_history:
            return {
                "cpu_utilization": 0.5,
                "memory_utilization": 0.5,
                "queue_depth": 0,
                "load_level": "normal"
            }
        
        recent = list(self._resource_history)[-10:]
        avg_cpu = statistics.mean(r.cpu_utilization for r in recent)
        avg_memory = statistics.mean(r.memory_utilization for r in recent)
        avg_queue = statistics.mean(r.queue_depth for r in recent)
        
        # 判断负载级别
        if avg_cpu > 0.8 or avg_memory > 0.8:
            load_level = "high"
        elif avg_cpu > 0.5 or avg_memory > 0.5:
            load_level = "medium"
        else:
            load_level = "low"
        
        return {
            "cpu_utilization": avg_cpu,
            "memory_utilization": avg_memory,
            "queue_depth": avg_queue,
            "load_level": load_level
        }
    
    def _select_adaptive_strategy(self, resource_state: Dict[str, Any]) -> SchedulingStrategy:
        """自适应选择调度策略"""
        load_level = resource_state["load_level"]
        queue_depth = resource_state["queue_depth"]
        
        # 高负载时使用公平调度防止饿死
        if load_level == "high":
            return SchedulingStrategy.FAIR
        
        # 队列深度大时使用最短任务优先提高吞吐量
        if queue_depth > 50:
            return SchedulingStrategy.SHORTEST_JOB_FIRST
        
        # 资源紧张时使用资源感知调度
        if resource_state["cpu_utilization"] > 0.7:
            return SchedulingStrategy.RESOURCE_AWARE
        
        # 默认使用优先级调度
        return SchedulingStrategy.PRIORITY
    
    def _schedule_fifo(
        self,
        pending_tasks: List[Tuple[int, str, str]],
        max_batch: int
    ) -> List[SchedulingDecision]:
        """FIFO调度"""
        decisions = []
        now = time.time()
        
        for i, (priority, task_id, task_type) in enumerate(pending_tasks[:max_batch]):
            predicted, confidence = self.predict_duration(task_type)
            decisions.append(SchedulingDecision(
                task_id=task_id,
                priority=priority,
                predicted_duration=predicted,
                scheduled_at=now,
                strategy_used=SchedulingStrategy.FIFO,
                reasoning=f"FIFO order {i}, predicted {predicted:.3f}s (confidence: {confidence:.2f})"
            ))
        
        return decisions
    
    def _schedule_priority(
        self,
        pending_tasks: List[Tuple[int, str, str]],
        max_batch: int
    ) -> List[SchedulingDecision]:
        """优先级调度"""
        # 按优先级排序（值越小优先级越高）
        sorted_tasks = sorted(pending_tasks, key=lambda x: x[0])
        decisions = []
        now = time.time()
        
        for priority, task_id, task_type in sorted_tasks[:max_batch]:
            predicted, confidence = self.predict_duration(task_type)
            decisions.append(SchedulingDecision(
                task_id=task_id,
                priority=priority,
                predicted_duration=predicted,
                scheduled_at=now,
                strategy_used=SchedulingStrategy.PRIORITY,
                reasoning=f"Priority {priority}, predicted {predicted:.3f}s (confidence: {confidence:.2f})"
            ))
        
        return decisions
    
    def _schedule_fair(
        self,
        pending_tasks: List[Tuple[int, str, str]],
        max_batch: int
    ) -> List[SchedulingDecision]:
        """公平调度 - 防止低优先级任务饿死"""
        decisions = []
        now = time.time()
        
        # 按任务类型分组
        by_type: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)
        for task in pending_tasks:
            by_type[task[2]].append(task)
        
        # 轮询调度，优先调度饥饿的任务类型
        scheduled_count = 0
        while scheduled_count < max_batch and by_type:
            # 找到最饥饿的类型
            starved_type = max(by_type.keys(), key=lambda t: self._fair_queue[t])
            
            if by_type[starved_type]:
                priority, task_id, task_type = by_type[starved_type].pop(0)
                predicted, confidence = self.predict_duration(task_type)
                
                decisions.append(SchedulingDecision(
                    task_id=task_id,
                    priority=priority,
                    predicted_duration=predicted,
                    scheduled_at=now,
                    strategy_used=SchedulingStrategy.FAIR,
                    reasoning=f"Fair scheduling for starving type '{task_type}', deficit={self._fair_queue[task_type]}"
                ))
                
                # 更新饥饿计数
                self._fair_queue[task_type] = max(0, self._fair_queue[task_type] - 1)
                for t in by_type:
                    if t != task_type:
                        self._fair_queue[t] += 1
                
                scheduled_count += 1
                self._stats["starvation_prevented"] += 1
            
            if not by_type[starved_type]:
                del by_type[starved_type]
        
        return decisions
    
    def _schedule_sjf(
        self,
        pending_tasks: List[Tuple[int, str, str]],
        max_batch: int
    ) -> List[SchedulingDecision]:
        """最短任务优先调度"""
        # 预测所有任务的执行时间
        task_predictions = []
        for priority, task_id, task_type in pending_tasks:
            predicted, confidence = self.predict_duration(task_type)
            task_predictions.append((predicted, priority, task_id, task_type, confidence))
        
        # 按预测时间排序
        task_predictions.sort(key=lambda x: x[0])
        
        decisions = []
        now = time.time()
        
        for predicted, priority, task_id, task_type, confidence in task_predictions[:max_batch]:
            decisions.append(SchedulingDecision(
                task_id=task_id,
                priority=priority,
                predicted_duration=predicted,
                scheduled_at=now,
                strategy_used=SchedulingStrategy.SHORTEST_JOB_FIRST,
                reasoning=f"SJF: shortest predicted duration {predicted:.3f}s (confidence: {confidence:.2f})"
            ))
        
        return decisions
    
    def _schedule_resource_aware(
        self,
        pending_tasks: List[Tuple[int, str, str]],
        max_batch: int,
        resource_state: Dict[str, Any]
    ) -> List[SchedulingDecision]:
        """资源感知调度"""
        decisions = []
        now = time.time()
        
        # 计算每个任务的综合得分
        scored_tasks = []
        for priority, task_id, task_type in pending_tasks:
            predicted, confidence = self.predict_duration(task_type)
            profile = self._task_profiles.get(task_type)
            
            # 综合得分 = 优先级权重 + 资源效率权重 + 时间预测权重
            priority_score = (5 - priority) * 10  # 优先级越高得分越高
            
            # 资源效率：资源消耗低的任务在高负载时更优先
            resource_intensity = profile.resource_intensity if profile else 1.0
            if resource_state["load_level"] == "high":
                resource_score = (2.0 - resource_intensity) * 20  # 低资源消耗优先
            else:
                resource_score = 0
            
            # 时间预测：短任务优先
            time_score = max(0, 10 - predicted)
            
            total_score = priority_score + resource_score + time_score
            scored_tasks.append((total_score, priority, task_id, task_type, predicted, confidence))
        
        # 按综合得分排序
        scored_tasks.sort(key=lambda x: x[0], reverse=True)
        
        for score, priority, task_id, task_type, predicted, confidence in scored_tasks[:max_batch]:
            decisions.append(SchedulingDecision(
                task_id=task_id,
                priority=priority,
                predicted_duration=predicted,
                scheduled_at=now,
                strategy_used=SchedulingStrategy.RESOURCE_AWARE,
                reasoning=f"Resource-aware: score={score:.1f}, predicted {predicted:.3f}s, load={resource_state['load_level']}"
            ))
        
        return decisions
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取调度统计"""
        return {
            "total_scheduled": self._stats["total_scheduled"],
            "predictions_made": self._stats["predictions_made"],
            "strategy_switches": self._stats["strategy_switches"],
            "starvation_prevented": self._stats["starvation_prevented"],
            "task_profiles": len(self._task_profiles),
            "current_strategy": self._current_strategy.value
        }
    
    def get_task_profiles(self) -> Dict[str, Dict[str, Any]]:
        """获取所有任务画像"""
        return {
            task_type: {
                "avg_execution_time": profile.avg_execution_time,
                "execution_time_variance": profile.execution_time_variance,
                "success_rate": profile.success_rate,
                "sample_count": profile.sample_count
            }
            for task_type, profile in self._task_profiles.items()
        }
