#!/usr/bin/env python3
"""
Task Execution Worker
任务执行层 - 核心干活模块，持续输出成果

核心职责：
1. 不间断推进既定任务
2. 按标准流程落地执行
3. 保证任务稳步落地，绝不中断进度
4. 实时采集执行数据供优化层分析
"""

import asyncio
import time
import uuid
from typing import Dict, List, Optional, Callable, Any, AsyncIterator
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
import functools


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"           # 等待执行
    RUNNING = "running"           # 执行中
    COMPLETED = "completed"       # 已完成
    FAILED = "failed"             # 执行失败
    RETRYING = "retrying"         # 重试中
    CANCELLED = "cancelled"       # 已取消


class TaskPriority(Enum):
    """任务优先级"""
    CRITICAL = 0      # 关键任务
    HIGH = 1          # 高优先级
    NORMAL = 2        # 普通优先级
    LOW = 3           # 低优先级


@dataclass
class ExecutionMetrics:
    """执行指标数据"""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    io_operations: int = 0
    error_count: int = 0
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Task:
    """任务数据结构"""
    id: str
    name: str
    priority: TaskPriority
    status: TaskStatus
    created_at: float
    payload: Dict[str, Any]
    execution_count: int = 0
    metrics: ExecutionMetrics = field(default_factory=ExecutionMetrics)
    result: Optional[Any] = None
    error_message: Optional[str] = None
    optimization_rules: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "priority": self.priority.value,
            "status": self.status.value,
            "created_at": self.created_at,
            "payload": self.payload,
            "execution_count": self.execution_count,
            "metrics": self.metrics.to_dict(),
            "result": self.result,
            "error_message": self.error_message,
            "optimization_rules": self.optimization_rules
        }


class TaskExecutionWorker:
    """
    任务执行工作器
    
    设计原则：
    - 永不阻塞：异步执行确保无空闲
    - 优先级调度：关键任务优先执行
    - 故障隔离：单个任务失败不影响其他任务
    - 数据透明：执行数据实时采集
    """
    
    def __init__(
        self,
        knowledge_engine: Any,
        state_manager: Any,
        max_concurrent_tasks: int = 10,
        task_queue_size: int = 1000,
        execution_history_size: int = 1000,
        task_timeout_seconds: float = 30.0,
        max_task_retention: int = 10000
    ):
        self.knowledge_engine = knowledge_engine
        self.state_manager = state_manager
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_queue_size = task_queue_size
        self.task_timeout_seconds = task_timeout_seconds
        self.max_task_retention = max_task_retention
        
        # 背压控制：当内存中任务数超过此阈值时，拒绝新任务
        self._backpressure_threshold = int(max_task_retention * 0.8)
        
        # 任务队列（按优先级排序）
        self._task_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=task_queue_size
        )
        
        # 正在执行的任务
        self._running_tasks: Dict[str, asyncio.Task] = {}
        
        # 任务存储（使用OrderedDict支持LRU清理）
        self._tasks: Dict[str, Task] = {}
        # 修复(RL-4)：使用 set 替代 deque，使 in 操作从 O(n) 变为 O(1)
        self._completed_task_ids: set = set()
        self._completed_task_counter: int = 0  # 计数器用于 LRU 排序
        
        # 执行历史（用于优化分析）
        self._execution_history: deque = deque(maxlen=execution_history_size)
        
        # 清理任务
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # 线程池用于CPU密集型任务
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        
        # 任务处理器注册表
        self._task_handlers: Dict[str, Callable] = {}
        
        # 信号量控制并发
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # 执行统计
        self._stats = {
            "total_submitted": 0,
            "total_completed": 0,
            "total_failed": 0,
            "total_retried": 0,
            "avg_execution_time": 0.0
        }
        
        # 当前优化规则
        self._optimization_rules: Dict[str, Any] = {}
        
        # 运行状态
        self._is_running = False
        self._worker_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """初始化执行工作器"""
        print("[EXECUTOR] Initializing task execution worker...")
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        self._thread_pool = ThreadPoolExecutor(max_workers=self.max_concurrent_tasks)
        self._is_running = True
        
        # 注册内置任务处理器
        self._register_builtin_handlers()
        
        # 启动清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    def _register_builtin_handlers(self):
        """注册内置任务处理器"""
        self.register_handler("default", self._default_task_handler)
        self.register_handler("compute", self._compute_task_handler)
        self.register_handler("io", self._io_task_handler)
        
    def register_handler(self, task_type: str, handler: Callable):
        """注册任务处理器"""
        self._task_handlers[task_type] = handler
        print(f"[EXECUTOR] Registered handler for task type: {task_type}")
    
    async def submit_task(
        self,
        name: str,
        payload: Dict[str, Any],
        priority: TaskPriority = TaskPriority.NORMAL,
        task_type: str = "default"
    ) -> str:
        """
        提交任务到执行队列
        
        Args:
            name: 任务名称
            payload: 任务数据
            priority: 任务优先级
            task_type: 任务类型（决定使用哪个处理器）
            
        Returns:
            任务ID
        """
        task_id = str(uuid.uuid4())
        
        task = Task(
            id=task_id,
            name=name,
            priority=priority,
            status=TaskStatus.PENDING,
            created_at=time.time(),
            payload={**payload, "task_type": task_type},
            optimization_rules=self._optimization_rules.copy()
        )
        
        # 背压控制：检查内存中任务数是否超过阈值
        if len(self._tasks) >= self._backpressure_threshold:
            raise RuntimeError(f"Backpressure: too many tasks in memory ({len(self._tasks)}/{self._backpressure_threshold})")
        
        self._tasks[task_id] = task
        
        # 检查队列是否已满（使用put_nowait避免无限阻塞）
        if self._task_queue.qsize() >= self.task_queue_size * 0.95:
            # 队列接近满，移除任务并抛出异常
            del self._tasks[task_id]
            raise RuntimeError(f"Task queue full ({self._task_queue.qsize()}/{self.task_queue_size})")
        
        # 按优先级放入队列（priority值越小优先级越高）
        await self._task_queue.put((priority.value, task_id))
        
        self._stats["total_submitted"] += 1
        
        # 不在提交时记录到知识图谱，改为完成时记录以减少节点数量
        
        return task_id
    
    async def start_execution_loop(self):
        """启动执行循环"""
        print("[EXECUTOR] Starting task execution loop...")
        self._worker_task = asyncio.create_task(self._execution_loop())
    
    async def _execution_loop(self):
        """任务执行主循环 - 永不停止"""
        while self._is_running:
            try:
                # 获取任务（带超时以便检查关闭信号）
                try:
                    priority, task_id = await asyncio.wait_for(
                        self._task_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # 在信号量控制下执行
                asyncio.create_task(self._execute_with_semaphore(task_id))
                
            except Exception as e:
                print(f"[EXECUTOR] Execution loop error: {e}")
                await asyncio.sleep(0.1)
    
    async def _execute_with_semaphore(self, task_id: str):
        """在信号量控制下执行任务"""
        # 修复(BUG-4)：跟踪正在执行的任务，使 get_heartbeat 能正确报告
        task = asyncio.current_task()
        self._running_tasks[task_id] = task
        try:
            async with self._semaphore:
                await self._execute_task(task_id)
        finally:
            self._running_tasks.pop(task_id, None)
    
    async def _execute_task(self, task_id: str):
        """执行单个任务"""
        task = self._tasks.get(task_id)
        if not task:
            return
        
        task.status = TaskStatus.RUNNING
        task.execution_count += 1
        task.metrics.start_time = time.time()
        
        task_type = task.payload.get("task_type", "default")
        handler = self._task_handlers.get(task_type, self._default_task_handler)
        
        try:
            # 应用优化规则
            if task.optimization_rules:
                task.payload = self._apply_optimization_rules(
                    task.payload,
                    task.optimization_rules
                )
            
            # 执行任务（修复BUG-5：使用 wait_for 包裹以启用超时机制）
            result = await asyncio.wait_for(
                handler(task.payload),
                timeout=self.task_timeout_seconds
            )
            
            task.result = result
            task.status = TaskStatus.COMPLETED
            task.metrics.end_time = time.time()
            task.metrics.duration_ms = (
                task.metrics.end_time - task.metrics.start_time
            ) * 1000
            
            self._stats["total_completed"] += 1
            
            # 记录成功执行
            await self._record_execution(task, success=True)
            
            # 标记任务为可清理
            self._completed_task_ids.add(task_id)
            self._completed_task_counter += 1
            
        except asyncio.TimeoutError:
            task.error_message = f"Task timeout after {self.task_timeout_seconds}s"
            task.metrics.error_count += 1
            task.status = TaskStatus.FAILED
            self._stats["total_failed"] += 1
            await self._record_execution(task, success=False)
            
            # 记录超时错误
            await self.knowledge_engine.add_execution_record(
                task_type=task_type,
                action="timeout",
                data={
                    "task_id": task_id,
                    "timeout": self.task_timeout_seconds
                }
            )
            
        except Exception as e:
            task.error_message = str(e)
            task.metrics.error_count += 1
            
            # 检查是否需要重试
            if task.execution_count < 3:
                task.status = TaskStatus.RETRYING
                self._stats["total_retried"] += 1
                await self._task_queue.put((task.priority.value, task_id))
            else:
                task.status = TaskStatus.FAILED
                self._stats["total_failed"] += 1
                await self._record_execution(task, success=False)
                # 标记失败任务为可清理
                self._completed_task_ids.add(task_id)
                self._completed_task_counter += 1
            
            # 记录错误到知识图谱
            await self.knowledge_engine.add_execution_record(
                task_type=task_type,
                action="error",
                data={
                    "task_id": task_id,
                    "error": str(e),
                    "execution_count": task.execution_count
                }
            )
    
    async def _record_execution(self, task: Task, success: bool):
        """记录执行数据"""
        execution_record = {
            "task_id": task.id,
            "task_name": task.name,
            "task_type": task.payload.get("task_type", "default"),
            "success": success,
            "duration_ms": task.metrics.duration_ms,
            "execution_count": task.execution_count,
            "timestamp": time.time(),
            "metrics": task.metrics.to_dict()
        }
        
        self._execution_history.append(execution_record)
        
        # 更新平均执行时间
        total_completed = self._stats["total_completed"] + self._stats["total_failed"]
        if total_completed > 0:
            self._stats["avg_execution_time"] = (
                (self._stats["avg_execution_time"] * (total_completed - 1) +
                 task.metrics.duration_ms) / total_completed
            )
    
    def _apply_optimization_rules(
        self,
        payload: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """应用优化规则到任务载荷"""
        optimized_payload = payload.copy()
        
        # 应用批处理优化
        if rules.get("batch_size"):
            optimized_payload["_batch_size"] = rules["batch_size"]
        
        # 应用超时优化
        if rules.get("timeout_seconds"):
            optimized_payload["_timeout"] = rules["timeout_seconds"]
        
        # 应用并发优化
        if rules.get("concurrency_limit"):
            optimized_payload["_concurrency"] = rules["concurrency_limit"]
        
        return optimized_payload
    
    # ============== 内置任务处理器 ==============
    
    async def _default_task_handler(self, payload: Dict[str, Any]) -> Any:
        """默认任务处理器"""
        # 模拟任务执行
        await asyncio.sleep(0.01)
        return {"status": "completed", "payload": payload}
    
    def _compute_sync(self, operation: str, data: list) -> Any:
        """同步计算函数（在线程池中运行）"""
        if operation == "sum":
            return sum(data)
        elif operation == "avg":
            return sum(data) / len(data) if data else 0
        elif operation == "max":
            return max(data) if data else None
        elif operation == "min":
            return min(data) if data else None
        else:
            return {"operation": operation, "data": data}

    async def _compute_task_handler(self, payload: Dict[str, Any]) -> Any:
        """计算型任务处理器 - 使用线程池避免阻塞事件循环"""
        operation = payload.get("operation", "noop")
        data = payload.get("data", [])

        # 大数据量时使用线程池
        if len(data) > 1000:
            # 修复(RL-3)：使用 get_running_loop() 替代已弃用的 get_event_loop()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._thread_pool,
                functools.partial(self._compute_sync, operation, data)
            )
        else:
            # 小数据量直接计算
            result = self._compute_sync(operation, data)

        return {"result": result, "operation": operation}
    
    async def _io_task_handler(self, payload: Dict[str, Any]) -> Any:
        """IO型任务处理器"""
        # 模拟IO操作
        await asyncio.sleep(0.02)
        return {"io_operation": payload.get("operation"), "status": "completed"}
    
    # ============== 清理循环 ==============
    
    async def _cleanup_loop(self):
        """定期清理已完成任务，防止内存泄漏"""
        while self._is_running:
            try:
                await asyncio.sleep(10)  # 每10秒清理一次
                
                if len(self._tasks) > self.max_task_retention * 0.5:
                    # 清理已完成的任务
                    to_remove = []
                    for task_id, task in self._tasks.items():
                        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                            to_remove.append(task_id)
                    
                    # 批量删除（保留最近完成的任务）
                    # 保留最新的 max_task_retention 个任务
                    keep_count = self.max_task_retention
                    if len(to_remove) > keep_count:
                        to_remove = to_remove[:-keep_count]
                    
                    for task_id in to_remove:
                        self._tasks.pop(task_id, None)
                        self._completed_task_ids.discard(task_id)
                    
                    if to_remove:
                        print(f"[EXECUTOR] Cleaned up {len(to_remove)} completed tasks, remaining: {len(self._tasks)}")
                        
            except Exception as e:
                print(f"[EXECUTOR] Cleanup error: {e}")
    
    # ============== 优化规则接口 ==============
    
    def update_optimization_rules(self, rules: Dict[str, Any]):
        """更新优化规则（由优化层调用）"""
        self._optimization_rules.update(rules)
        print(f"[EXECUTOR] Updated optimization rules: {list(rules.keys())}")
    
    # ============== 状态查询接口 ==============
    
    def get_heartbeat(self) -> Dict[str, Any]:
        """获取心跳数据（供守护层监控）"""
        queue_size = self._task_queue.qsize()
        running_count = len(self._running_tasks)
        
        # 判断健康状态
        is_healthy = (
            queue_size < self.task_queue_size * 0.9 and
            running_count <= self.max_concurrent_tasks
        )
        
        return {
            "healthy": is_healthy,
            "metrics": {
                "queue_size": queue_size,
                "running_tasks": running_count,
                "total_submitted": self._stats["total_submitted"],
                "total_completed": self._stats["total_completed"],
                "total_failed": self._stats["total_failed"],
                "avg_execution_time_ms": self._stats["avg_execution_time"],
                "optimization_rules_count": len(self._optimization_rules)
            }
        }
    
    def get_execution_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取执行历史（供优化层分析）"""
        return list(self._execution_history)[-limit:]
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        task = self._tasks.get(task_id)
        return task.to_dict() if task else None
    
    async def shutdown(self):
        """关闭执行工作器"""
        print("[EXECUTOR] Shutting down task execution worker...")
        self._is_running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        # 取消所有正在执行的任务
        for task in self._running_tasks.values():
            task.cancel()
        
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks.values(), return_exceptions=True)
        
        # 关闭线程池
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
        
        # 保存状态
        await self.state_manager.save_executor_state({
            "stats": self._stats,
            "optimization_rules": self._optimization_rules
        })
        
        print("[EXECUTOR] Task execution worker shutdown complete")
