#!/usr/bin/env python3
"""
Synchronization Deployment Pipeline
同步落地层 - 优化规则即时生效，直接融入下一轮任务执行

核心职责：
1. 接收优化层生成的规则
2. 实时同步到执行层
3. 验证优化效果
4. 无缝衔接执行与优化
"""

import asyncio
import hashlib
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class DeploymentStatus(Enum):
    """部署状态枚举"""
    PENDING = "pending"           # 等待部署
    DEPLOYING = "deploying"       # 部署中
    ACTIVE = "active"             # 已生效
    ROLLED_BACK = "rolled_back"   # 已回滚
    FAILED = "failed"             # 部署失败


@dataclass
class DeploymentRecord:
    """部署记录"""
    deployment_id: str
    rule_id: str
    target_component: str
    parameters: Dict[str, Any]
    status: DeploymentStatus
    deployed_at: Optional[float] = None
    validated_at: Optional[float] = None
    validation_result: Optional[bool] = None
    rollback_reason: Optional[str] = None


class SynchronizationDeploymentPipeline:
    """
    同步部署管道
    
    设计原则：
    - 零停机部署：热更新，无需重启
    - 快速回滚：效果不佳时立即回滚
    - 效果验证：部署后跟踪验证
    - 原子操作：规则要么全生效，要么全不生效
    """
    
    def __init__(
        self,
        task_worker: Any,
        optimization_analyzer: Any,
        validation_window_seconds: float = 30.0,
        deployment_cooldown_seconds: float = 5.0
    ):
        self.task_worker = task_worker
        self.optimization_analyzer = optimization_analyzer
        self.validation_window_seconds = validation_window_seconds
        self.deployment_cooldown_seconds = deployment_cooldown_seconds
        
        # 部署队列
        self._deployment_queue: asyncio.Queue = asyncio.Queue()
        
        # 部署记录（限制最大数量防止内存泄漏）
        self._deployments: deque = deque(maxlen=1000)
        self._active_rules: Dict[str, Dict[str, Any]] = {}
        
        # 部署历史
        self._deployment_history: deque = deque(maxlen=1000)
        
        # 部署统计
        self._stats = {
            "total_deployed": 0,
            "total_active": 0,
            "total_rolled_back": 0,
            "total_failed": 0,
            "avg_deployment_time_ms": 0.0
        }
        
        # 性能基线（用于验证优化效果）
        self._performance_baselines: Dict[str, Dict[str, float]] = {}
        
        # 运行状态
        self._is_running = False
        self._pipeline_task: Optional[asyncio.Task] = None
        self._validation_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """初始化部署管道"""
        print("[DEPLOYER] Initializing synchronization deployment pipeline...")
        self._is_running = True
        
    async def start_deployment_loop(self):
        """启动部署循环"""
        print("[DEPLOYER] Starting deployment pipeline...")
        self._pipeline_task = asyncio.create_task(self._deployment_loop())
        self._validation_task = asyncio.create_task(self._validation_loop())
    
    async def _deployment_loop(self):
        """部署主循环 - 持续从优化层获取规则并部署"""
        while self._is_running:
            try:
                # 从优化层获取最新规则
                rules = self.optimization_analyzer.get_optimization_rules()
                
                # 检查是否有新规则需要部署
                for target_component, parameters in rules.items():
                    # 修复：使用 hashlib 保证跨进程稳定（替代不稳定的 hash()）
                    stable_hash = hashlib.sha256(str(parameters).encode()).hexdigest()[:12]
                    rule_id = f"{target_component}_{stable_hash}"
                    
                    # 检查是否已部署
                    if rule_id in self._active_rules:
                        continue
                    
                    # 创建部署记录
                    deployment = DeploymentRecord(
                        deployment_id=f"deploy_{int(time.time() * 1000)}",
                        rule_id=rule_id,
                        target_component=target_component,
                        parameters=parameters,
                        status=DeploymentStatus.PENDING
                    )
                    
                    await self._deployment_queue.put(deployment)
                
                # 处理部署队列
                await self._process_deployment_queue()
                
                await asyncio.sleep(self.deployment_cooldown_seconds)
                
            except Exception as e:
                print(f"[DEPLOYER] Deployment loop error: {e}")
                await asyncio.sleep(1.0)
    
    async def _process_deployment_queue(self):
        """处理部署队列"""
        while not self._deployment_queue.empty():
            try:
                deployment = self._deployment_queue.get_nowait()
                await self._deploy_rule(deployment)
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                print(f"[DEPLOYER] Error processing deployment: {e}")
    
    async def _deploy_rule(self, deployment: DeploymentRecord):
        """部署单个规则"""
        print(f"[DEPLOYER] Deploying rule {deployment.rule_id} to {deployment.target_component}")
        
        deployment.status = DeploymentStatus.DEPLOYING
        deployment.deployed_at = time.time()
        
        try:
            # 修复：找到优化器中匹配的规则ID，确保 record_rule_effectiveness 有效
            matched_rule_id = None
            for rule_id, rule in self.optimization_analyzer._optimization_rules.items():
                if rule.target_component == deployment.target_component:
                    matched_rule_id = rule_id
                    break
            if matched_rule_id:
                deployment.rule_id = matched_rule_id

            # 记录当前性能基线
            self._performance_baselines[deployment.rule_id] = await self._capture_baseline()
            
            # 执行部署
            await self._apply_optimization(
                deployment.target_component,
                deployment.parameters
            )
            
            # 更新状态
            deployment.status = DeploymentStatus.ACTIVE
            self._active_rules[deployment.rule_id] = deployment.parameters
            self._stats["total_deployed"] += 1
            self._stats["total_active"] = len(self._active_rules)
            
            # 记录部署
            self._deployments.append(deployment)
            self._deployment_history.append({
                "deployment_id": deployment.deployment_id,
                "rule_id": deployment.rule_id,
                "target": deployment.target_component,
                "status": "deployed",
                "timestamp": time.time()
            })
            
            deployment_time = (time.time() - deployment.deployed_at) * 1000
            self._update_avg_deployment_time(deployment_time)
            
            print(f"[DEPLOYER] ✓ Rule {deployment.rule_id} deployed successfully ({deployment_time:.1f}ms)")
            
        except Exception as e:
            deployment.status = DeploymentStatus.FAILED
            self._stats["total_failed"] += 1
            print(f"[DEPLOYER] ✗ Rule {deployment.rule_id} deployment failed: {e}")
    
    async def _apply_optimization(self, target_component: str, parameters: Dict[str, Any]):
        """应用优化到目标组件"""
        # 更新任务执行层的优化规则
        self.task_worker.update_optimization_rules({target_component: parameters})
    
    async def _validation_loop(self):
        """验证循环 - 持续验证已部署规则的效果"""
        while self._is_running:
            try:
                await asyncio.sleep(self.validation_window_seconds)
                
                # 验证活跃规则
                for deployment in list(self._deployments):
                    if deployment.status != DeploymentStatus.ACTIVE:
                        continue
                    
                    await self._validate_deployment(deployment)
                    
            except Exception as e:
                print(f"[DEPLOYER] Validation loop error: {e}")
    
    async def _validate_deployment(self, deployment: DeploymentRecord):
        """验证部署效果"""
        deployment.validated_at = time.time()
        
        # 获取当前性能指标
        current_metrics = await self._capture_baseline()
        baseline = self._performance_baselines.get(deployment.rule_id, {})
        
        if not baseline:
            return
        
        # 计算改进
        improvements = {}
        for metric, current_value in current_metrics.items():
            baseline_value = baseline.get(metric, current_value)
            if baseline_value > 0:
                improvement = (baseline_value - current_value) / baseline_value
                improvements[metric] = improvement
        
        # 判断是否有改进
        has_improvement = any(imp > 0.1 for imp in improvements.values())
        has_degradation = any(imp < -0.2 for imp in improvements.values())
        
        deployment.validation_result = has_improvement and not has_degradation
        
        # 通知优化层规则效果
        self.optimization_analyzer.record_rule_effectiveness(
            deployment.rule_id,
            deployment.validation_result
        )
        
        if has_degradation:
            # 性能下降，回滚
            await self._rollback_deployment(deployment, "Performance degradation detected")
        elif has_improvement:
            print(f"[DEPLOYER] ✓ Rule {deployment.rule_id} validated - improvements: {improvements}")
        else:
            print(f"[DEPLOYER] ~ Rule {deployment.rule_id} neutral - no significant change")
    
    async def _rollback_deployment(self, deployment: DeploymentRecord, reason: str):
        """回滚部署"""
        print(f"[DEPLOYER] Rolling back rule {deployment.rule_id}: {reason}")
        
        try:
            # 移除优化规则
            self.task_worker.update_optimization_rules({deployment.target_component: {}})
            
            # 更新状态
            deployment.status = DeploymentStatus.ROLLED_BACK
            deployment.rollback_reason = reason
            
            if deployment.rule_id in self._active_rules:
                del self._active_rules[deployment.rule_id]
            
            self._stats["total_rolled_back"] += 1
            self._stats["total_active"] = len(self._active_rules)
            
            print(f"[DEPLOYER] ✓ Rule {deployment.rule_id} rolled back")
            
        except Exception as e:
            print(f"[DEPLOYER] ✗ Rollback failed for rule {deployment.rule_id}: {e}")
    
    async def _capture_baseline(self) -> Dict[str, float]:
        """捕获当前性能基线"""
        heartbeat = self.task_worker.get_heartbeat()
        metrics = heartbeat.get("metrics", {})
        
        return {
            "avg_execution_time_ms": metrics.get("avg_execution_time_ms", 0),
            "queue_size": float(metrics.get("queue_size", 0)),
            "success_rate": 1.0 - (metrics.get("total_failed", 0) / max(metrics.get("total_completed", 1), 1)),
            "throughput": metrics.get("total_completed", 0) / max(time.time() - metrics.get("start_time", time.time()), 1)
        }
    
    def _update_avg_deployment_time(self, deployment_time_ms: float):
        """更新平均部署时间"""
        total = self._stats["total_deployed"]
        if total > 0:
            current_avg = self._stats["avg_deployment_time_ms"]
            self._stats["avg_deployment_time_ms"] = (
                (current_avg * (total - 1) + deployment_time_ms) / total
            )
    
    # ============== 状态查询接口 ==============
    
    def get_heartbeat(self) -> Dict[str, Any]:
        """获取心跳数据"""
        return {
            "healthy": True,
            "metrics": {
                "total_deployed": self._stats["total_deployed"],
                "total_active": self._stats["total_active"],
                "total_rolled_back": self._stats["total_rolled_back"],
                "total_failed": self._stats["total_failed"],
                "avg_deployment_time_ms": self._stats["avg_deployment_time_ms"],
                "queue_size": self._deployment_queue.qsize()
            }
        }
    
    def get_deployment_summary(self) -> Dict[str, Any]:
        """获取部署摘要"""
        return {
            "stats": self._stats.copy(),
            "active_rules": len(self._active_rules),
            "recent_deployments": list(self._deployment_history)[-10:]
        }
    
    async def shutdown(self):
        """关闭部署管道"""
        print("[DEPLOYER] Shutting down deployment pipeline...")
        self._is_running = False
        
        if self._pipeline_task:
            self._pipeline_task.cancel()
            try:
                await self._pipeline_task
            except asyncio.CancelledError:
                pass
        
        if self._validation_task:
            self._validation_task.cancel()
            try:
                await self._validation_task
            except asyncio.CancelledError:
                pass
        
        print("[DEPLOYER] Deployment pipeline shutdown complete")
