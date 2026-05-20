#!/usr/bin/env python3
"""
Resident Guardian Daemon
常驻守护层 - 全程保活，异常自动修复重启

核心职责：
1. 监控系统健康状态
2. 自动检测并修复异常
3. 组件故障时自动重启
4. 保证双流程持续跑通，杜绝停机
"""

import asyncio
import time
import traceback
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class HealthStatus(Enum):
    """健康状态枚举"""
    HEALTHY = "healthy"           # 健康运行
    DEGRADED = "degraded"         # 性能降级
    UNHEALTHY = "unhealthy"       # 不健康
    CRITICAL = "critical"         # 严重故障
    RECOVERING = "recovering"     # 恢复中


@dataclass
class ComponentHealth:
    """组件健康状态数据"""
    component_name: str
    status: HealthStatus
    last_heartbeat: float
    error_count: int = 0
    recovery_attempts: int = 0
    metrics: Dict[str, Any] = field(default_factory=dict)
    last_error: Optional[str] = None


class ResidentGuardianDaemon:
    """
    常驻守护守护进程
    
    设计原则：
    - 无单点故障：守护进程自身也有自监控
    - 快速恢复：异常检测后秒级响应
    - 非侵入式：被监控组件无感知
    """
    
    def __init__(
        self,
        shutdown_event: asyncio.Event,
        health_check_interval: float = 5.0,
        heartbeat_timeout: float = 15.0,
        max_recovery_attempts: int = 3
    ):
        self.shutdown_event = shutdown_event
        self.health_check_interval = health_check_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_recovery_attempts = max_recovery_attempts
        
        # 组件注册表
        self._registered_components: Dict[str, Dict[str, Any]] = {}
        self._component_health: Dict[str, ComponentHealth] = {}
        
        # 健康历史记录（用于趋势分析）
        self._health_history: Dict[str, deque] = {}
        self._max_history_size = 100
        
        # 恢复回调注册
        self._recovery_callbacks: Dict[str, Callable] = {}
        
        # 监控任务
        self._monitor_task: Optional[asyncio.Task] = None
        self._self_monitor_task: Optional[asyncio.Task] = None

        # 修复(R8-SD-04): 跟踪恢复过程中创建的异步任务，确保关闭时能取消
        self._recovery_tasks: List[asyncio.Task] = []
        self._recovery_lock = asyncio.Lock()  # 防止并发恢复

        # 运行状态
        self._is_running = False
        self._daemon_start_time = 0.0
        
    async def initialize(self):
        """初始化守护进程"""
        print("[GUARDIAN] Initializing resident guardian daemon...")
        self._daemon_start_time = time.time()
        self._is_running = True
        
    def register_component(
        self,
        component_name: str,
        heartbeat_callback: Callable[[], Dict[str, Any]],
        recovery_callback: Optional[Callable[[], Any]] = None,
        critical: bool = True
    ):
        """
        注册需要监控的组件
        
        Args:
            component_name: 组件唯一标识
            heartbeat_callback: 返回组件健康指标的回调函数
            recovery_callback: 组件恢复回调函数
            critical: 是否为关键组件（故障时影响全局）
        """
        self._registered_components[component_name] = {
            "heartbeat_callback": heartbeat_callback,
            "recovery_callback": recovery_callback,
            "critical": critical,
            "registered_at": time.time()
        }
        
        self._component_health[component_name] = ComponentHealth(
            component_name=component_name,
            status=HealthStatus.HEALTHY,
            last_heartbeat=time.time()
        )
        
        self._health_history[component_name] = deque(maxlen=self._max_history_size)
        
        if recovery_callback:
            self._recovery_callbacks[component_name] = recovery_callback
            
        print(f"[GUARDIAN] Registered component: {component_name} (critical={critical})")
    
    def unregister_component(self, component_name: str):
        """注销组件监控"""
        self._registered_components.pop(component_name, None)
        self._component_health.pop(component_name, None)
        self._health_history.pop(component_name, None)
        self._recovery_callbacks.pop(component_name, None)
        print(f"[GUARDIAN] Unregistered component: {component_name}")
    
    async def start_monitoring(self):
        """启动监控循环"""
        print("[GUARDIAN] Starting health monitoring...")
        
        self._monitor_task = asyncio.create_task(self._monitoring_loop())
        self._self_monitor_task = asyncio.create_task(self._self_monitoring_loop())
        
    async def _monitoring_loop(self):
        """健康监控主循环"""
        while not self.shutdown_event.is_set() and self._is_running:
            try:
                await self._check_all_components()
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=self.health_check_interval
                )
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"[GUARDIAN] Monitoring loop error: {e}")
                await asyncio.sleep(1)
    
    async def _check_all_components(self):
        """检查所有注册组件的健康状态"""
        current_time = time.time()
        
        for component_name, config in self._registered_components.items():
            try:
                # 获取组件心跳
                heartbeat_data = await asyncio.wait_for(
                    self._get_heartbeat(config["heartbeat_callback"]),
                    timeout=5.0
                )
                
                # 更新健康状态
                await self._update_component_health(
                    component_name, 
                    heartbeat_data,
                    current_time
                )
                
            except asyncio.TimeoutError:
                await self._handle_component_timeout(component_name, current_time)
            except Exception as e:
                await self._handle_component_error(component_name, str(e), current_time)
    
    async def _get_heartbeat(self, callback: Callable) -> Dict[str, Any]:
        """获取组件心跳数据"""
        if asyncio.iscoroutinefunction(callback):
            return await callback()
        else:
            return callback()
    
    async def _update_component_health(
        self,
        component_name: str,
        heartbeat_data: Dict[str, Any],
        current_time: float
    ):
        """更新组件健康状态"""
        health = self._component_health[component_name]
        
        # 解析心跳数据
        is_healthy = heartbeat_data.get("healthy", True)
        metrics = heartbeat_data.get("metrics", {})
        
        # 更新状态
        health.last_heartbeat = current_time
        health.metrics = metrics
        
        if is_healthy:
            if health.status in [HealthStatus.UNHEALTHY, HealthStatus.CRITICAL]:
                health.status = HealthStatus.RECOVERING
                health.recovery_attempts = 0
                print(f"[GUARDIAN] ✓ {component_name} is recovering")
            elif health.status == HealthStatus.RECOVERING:
                # 连续健康则转为正常
                health.status = HealthStatus.HEALTHY
                health.error_count = 0
                print(f"[GUARDIAN] ✓ {component_name} is now healthy")
        else:
            health.error_count += 1
            # 修复(BUG-6)：先检查 CRITICAL（>=5），再检查 UNHEALTHY（>=3），避免判断顺序反了
            if health.error_count >= 5:
                health.status = HealthStatus.CRITICAL
            elif health.error_count >= 3:
                health.status = HealthStatus.UNHEALTHY
                await self._trigger_recovery(component_name)
        
        # 记录历史
        self._health_history[component_name].append({
            "timestamp": current_time,
            "status": health.status.value,
            "metrics": metrics
        })
    
    async def _handle_component_timeout(self, component_name: str, current_time: float):
        """处理组件心跳超时"""
        health = self._component_health[component_name]
        health.last_heartbeat = current_time
        health.error_count += 1
        health.status = HealthStatus.DEGRADED
        
        print(f"[GUARDIAN] ⚠ {component_name} heartbeat timeout")
        
        if health.error_count >= 3:
            await self._trigger_recovery(component_name)
    
    async def _handle_component_error(
        self,
        component_name: str,
        error: str,
        current_time: float
    ):
        """处理组件错误"""
        health = self._component_health[component_name]
        health.last_heartbeat = current_time
        health.error_count += 1
        health.last_error = error
        health.status = HealthStatus.UNHEALTHY
        
        print(f"[GUARDIAN] ✗ {component_name} error: {error}")
        
        await self._trigger_recovery(component_name)
    
    async def _trigger_recovery(self, component_name: str):
        """触发组件恢复流程"""
        health = self._component_health[component_name]

        if health.recovery_attempts >= self.max_recovery_attempts:
            print(f"[GUARDIAN] ✗ {component_name} max recovery attempts reached")
            health.status = HealthStatus.CRITICAL
            return

        # 修复(R8-SD-04): 恢复前检查 shutdown 状态，避免关闭期间发起恢复
        if self.shutdown_event.is_set() or not self._is_running:
            print(f"[GUARDIAN] Skipping recovery for {component_name}: shutdown in progress")
            return

        # 使用锁防止同一组件并发恢复
        async with self._recovery_lock:
            # 再次检查（可能在等待锁期间收到 shutdown）
            if self.shutdown_event.is_set() or not self._is_running:
                print(f"[GUARDIAN] Skipping recovery for {component_name}: shutdown in progress")
                return

            health.recovery_attempts += 1
            health.status = HealthStatus.RECOVERING

            print(f"[GUARDIAN] → Attempting recovery for {component_name} "
                  f"(attempt {health.recovery_attempts}/{self.max_recovery_attempts})")

            recovery_callback = self._recovery_callbacks.get(component_name)
            if recovery_callback:
                try:
                    # 将恢复过程包装为 task 以便跟踪
                    recovery_task = asyncio.ensure_future(
                        asyncio.wait_for(recovery_callback(), timeout=30.0)
                    )
                    self._recovery_tasks.append(recovery_task)

                    await recovery_task

                    # 恢复完成后从跟踪列表移除
                    self._recovery_tasks = [
                        t for t in self._recovery_tasks if t is not recovery_task
                    ]

                    print(f"[GUARDIAN] ✓ {component_name} recovery successful")
                    health.error_count = 0

                except asyncio.TimeoutError:
                    print(f"[GUARDIAN] ✗ {component_name} recovery timed out")
                    health.status = HealthStatus.UNHEALTHY
                except asyncio.CancelledError:
                    print(f"[GUARDIAN] ✗ {component_name} recovery cancelled (shutdown)")
                    health.status = HealthStatus.UNHEALTHY
                except Exception as e:
                    print(f"[GUARDIAN] ✗ {component_name} recovery failed: {e}")
                    health.status = HealthStatus.UNHEALTHY
    
    async def _self_monitoring_loop(self):
        """自监控循环 - 确保守护进程自身健康"""
        while not self.shutdown_event.is_set() and self._is_running:
            try:
                # 检查监控任务是否存活
                if self._monitor_task and self._monitor_task.done():
                    print("[GUARDIAN] Monitor task died, restarting...")
                    self._monitor_task = asyncio.create_task(self._monitoring_loop())
                
                await asyncio.sleep(10)
            except Exception as e:
                print(f"[GUARDIAN] Self-monitoring error: {e}")
                await asyncio.sleep(5)
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取整体系统健康状态"""
        total_components = len(self._component_health)
        healthy_count = sum(
            1 for h in self._component_health.values()
            if h.status == HealthStatus.HEALTHY
        )
        
        return {
            "overall_status": "healthy" if healthy_count == total_components else "degraded",
            "uptime": time.time() - self._daemon_start_time,
            "components": {
                name: {
                    "status": health.status.value,
                    "error_count": health.error_count,
                    "recovery_attempts": health.recovery_attempts,
                    "last_heartbeat": health.last_heartbeat
                }
                for name, health in self._component_health.items()
            },
            "summary": {
                "total": total_components,
                "healthy": healthy_count,
                "degraded": sum(1 for h in self._component_health.values() if h.status == HealthStatus.DEGRADED),
                "unhealthy": sum(1 for h in self._component_health.values() if h.status == HealthStatus.UNHEALTHY),
                "critical": sum(1 for h in self._component_health.values() if h.status == HealthStatus.CRITICAL)
            }
        }
    
    async def shutdown(self):
        """关闭守护进程"""
        print("[GUARDIAN] Shutting down guardian daemon...")
        self._is_running = False

        # 修复(R8-SD-04): 取消所有正在进行的恢复任务
        for recovery_task in self._recovery_tasks:
            if not recovery_task.done():
                recovery_task.cancel()

        if self._recovery_tasks:
            await asyncio.gather(*self._recovery_tasks, return_exceptions=True)
            self._recovery_tasks.clear()

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        if self._self_monitor_task:
            self._self_monitor_task.cancel()
            try:
                await self._self_monitor_task
            except asyncio.CancelledError:
                pass
        
        print("[GUARDIAN] Guardian daemon shutdown complete")
