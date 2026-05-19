#!/usr/bin/env python3
"""
Workflow Orchestration Engine
工作流编排引擎 - 支持复杂任务依赖和DAG执行

核心特性：
1. DAG工作流：支持任务依赖关系
2. 并行执行：独立任务并行执行
3. 失败处理：支持重试、跳过、回滚
4. 状态追踪：完整的工作流执行历史
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import uuid


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowStatus(Enum):
    """工作流状态"""
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowTask:
    """工作流任务"""
    task_id: str
    name: str
    handler: Optional[Callable] = None
    dependencies: Set[str] = field(default_factory=set)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Workflow:
    """工作流定义"""
    workflow_id: str
    name: str
    tasks: Dict[str, WorkflowTask] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.CREATED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_task(
        self,
        task_id: str,
        name: str,
        handler: Optional[Callable] = None,
        dependencies: Optional[Set[str]] = None,
        **kwargs
    ):
        """添加任务"""
        task = WorkflowTask(
            task_id=task_id,
            name=name,
            handler=handler,
            dependencies=dependencies or set(),
            **kwargs
        )
        self.tasks[task_id] = task
        return task
    
    def validate(self) -> List[str]:
        """验证工作流（检查循环依赖等）"""
        errors = []
        
        # 检查依赖是否存在
        for task_id, task in self.tasks.items():
            for dep_id in task.dependencies:
                if dep_id not in self.tasks:
                    errors.append(f"Task '{task_id}' depends on non-existent task '{dep_id}'")
        
        # 检查循环依赖（修复BUG-WF-5: 跳过不存在的依赖，避免KeyError）
        visited = set()
        rec_stack = set()

        def has_cycle(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)

            for dep_id in self.tasks[task_id].dependencies:
                # 修复(BUG-WF-5): 不存在的依赖已在上面报错，跳过不参与循环检测
                if dep_id not in self.tasks:
                    continue
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True

            rec_stack.remove(task_id)
            return False
        
        for task_id in self.tasks:
            if task_id not in visited:
                if has_cycle(task_id):
                    errors.append(f"Cycle detected in workflow")
                    break
        
        return errors


@dataclass
class WorkflowExecutionResult:
    """工作流执行结果"""
    workflow_id: str
    status: WorkflowStatus
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    skipped_tasks: int
    duration: float
    task_results: Dict[str, Any]
    errors: List[str]


class WorkflowOrchestrationEngine:
    """
    工作流编排引擎
    
    设计原则：
    - DAG执行：按依赖顺序执行
    - 并行优化：独立任务并行执行
    - 容错处理：支持重试和跳过
    - 可观测：完整的执行历史
    """
    
    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        default_timeout: float = 30.0,
        default_max_retries: int = 3
    ):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.default_timeout = default_timeout
        self.default_max_retries = default_max_retries
        
        # 工作流存储
        self._workflows: Dict[str, Workflow] = {}
        
        # 执行历史
        self._execution_history: deque = deque(maxlen=100)
        
        # 统计
        self._stats = {
            "workflows_created": 0,
            "workflows_completed": 0,
            "workflows_failed": 0,
            "tasks_executed": 0,
            "tasks_failed": 0
        }
    
    def create_workflow(
        self,
        name: str,
        workflow_id: Optional[str] = None
    ) -> Workflow:
        """创建工作流"""
        workflow_id = workflow_id or str(uuid.uuid4())[:8]
        
        workflow = Workflow(
            workflow_id=workflow_id,
            name=name
        )
        
        self._workflows[workflow_id] = workflow
        self._stats["workflows_created"] += 1
        
        return workflow
    
    async def execute_workflow(
        self,
        workflow: Workflow,
        context: Optional[Dict[str, Any]] = None
    ) -> WorkflowExecutionResult:
        """
        执行工作流
        
        Args:
            workflow: 工作流对象
            context: 执行上下文
            
        Returns:
            执行结果
        """
        # 验证工作流
        errors = workflow.validate()
        if errors:
            return WorkflowExecutionResult(
                workflow_id=workflow.workflow_id,
                status=WorkflowStatus.FAILED,
                total_tasks=len(workflow.tasks),
                completed_tasks=0,
                failed_tasks=0,
                skipped_tasks=0,
                duration=0.0,
                task_results={},
                errors=errors
            )
        
        workflow.status = WorkflowStatus.RUNNING
        workflow.started_at = time.time()
        
        context = context or {}
        semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        
        try:
            # 执行DAG
            await self._execute_dag(workflow, context, semaphore)
            
            # 确定最终状态
            failed_count = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.FAILED)
            if failed_count > 0:
                workflow.status = WorkflowStatus.FAILED
                self._stats["workflows_failed"] += 1
            else:
                workflow.status = WorkflowStatus.COMPLETED
                self._stats["workflows_completed"] += 1
            
        except Exception as e:
            workflow.status = WorkflowStatus.FAILED
            self._stats["workflows_failed"] += 1
            errors.append(str(e))
        
        workflow.completed_at = time.time()
        
        # 构建结果
        return self._build_result(workflow, errors)
    
    async def _execute_dag(
        self,
        workflow: Workflow,
        context: Dict[str, Any],
        semaphore: asyncio.Semaphore
    ):
        """执行DAG工作流"""
        # 初始化：找出所有就绪任务（无依赖）
        ready_tasks: deque = deque()
        pending_tasks: Set[str] = set()
        
        for task_id, task in workflow.tasks.items():
            if not task.dependencies:
                task.status = TaskStatus.READY
                ready_tasks.append(task_id)
            else:
                pending_tasks.add(task_id)
        
        # 执行循环
        running_tasks: Dict[str, asyncio.Task] = {}
        
        while ready_tasks or running_tasks or pending_tasks:
            # 启动就绪任务
            while ready_tasks and len(running_tasks) < self.max_concurrent_tasks:
                task_id = ready_tasks.popleft()
                task = workflow.tasks[task_id]
                
                async def run_with_semaphore(t: WorkflowTask):
                    async with semaphore:
                        await self._execute_task(t, context)
                
                running_tasks[task_id] = asyncio.create_task(run_with_semaphore(task))
            
            # 等待任意任务完成
            if running_tasks:
                done, _ = await asyncio.wait(
                    running_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # 处理完成的任务
                for task_id, future in list(running_tasks.items()):
                    if future in done:
                        del running_tasks[task_id]
                        task = workflow.tasks[task_id]
                        
                        # 更新依赖此任务的其他任务状态
                        for pending_id in list(pending_tasks):
                            pending_task = workflow.tasks[pending_id]
                            
                            # 检查是否所有依赖都完成
                            if all(
                                workflow.tasks[dep_id].status == TaskStatus.COMPLETED
                                for dep_id in pending_task.dependencies
                            ):
                                pending_task.status = TaskStatus.READY
                                ready_tasks.append(pending_id)
                                pending_tasks.remove(pending_id)
                            
                            # 检查是否有依赖失败
                            elif any(
                                workflow.tasks[dep_id].status == TaskStatus.FAILED
                                for dep_id in pending_task.dependencies
                            ):
                                pending_task.status = TaskStatus.SKIPPED
                                pending_tasks.remove(pending_id)
            
            # 如果没有任务在运行且没有就绪任务，检查是否有死锁
            if not running_tasks and not ready_tasks and pending_tasks:
                # 标记所有待处理任务为跳过
                for task_id in pending_tasks:
                    workflow.tasks[task_id].status = TaskStatus.SKIPPED
                pending_tasks.clear()
    
    async def _execute_task(
        self,
        task: WorkflowTask,
        context: Dict[str, Any]
    ):
        """执行单个任务"""
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()
        
        try:
            if task.handler is None:
                # 无处理器，直接标记完成
                task.result = None
            elif asyncio.iscoroutinefunction(task.handler):
                task.result = await asyncio.wait_for(
                    task.handler(context, task),
                    timeout=task.timeout
                )
            else:
                task.result = task.handler(context, task)
            
            task.status = TaskStatus.COMPLETED
            self._stats["tasks_executed"] += 1
            
        except asyncio.TimeoutError:
            task.error = f"Timeout after {task.timeout}s"
            task.status = TaskStatus.FAILED
            self._stats["tasks_failed"] += 1
            
        except Exception as e:
            task.error = str(e)
            
            # 检查是否需要重试
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                # 重新执行
                await asyncio.sleep(2 ** task.retry_count)  # 指数退避
                await self._execute_task(task, context)
            else:
                task.status = TaskStatus.FAILED
                self._stats["tasks_failed"] += 1
        
        task.completed_at = time.time()
    
    def _build_result(
        self,
        workflow: Workflow,
        errors: List[str]
    ) -> WorkflowExecutionResult:
        """构建执行结果"""
        completed = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.FAILED)
        skipped = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.SKIPPED)
        
        duration = (workflow.completed_at - workflow.started_at) if workflow.completed_at and workflow.started_at else 0
        
        return WorkflowExecutionResult(
            workflow_id=workflow.workflow_id,
            status=workflow.status,
            total_tasks=len(workflow.tasks),
            completed_tasks=completed,
            failed_tasks=failed,
            skipped_tasks=skipped,
            duration=duration,
            task_results={
                task_id: {
                    "status": task.status.value,
                    "result": task.result,
                    "error": task.error,
                    "duration": (task.completed_at - task.started_at) if task.completed_at and task.started_at else None
                }
                for task_id, task in workflow.tasks.items()
            },
            errors=errors
        )
    
    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """获取工作流"""
        return self._workflows.get(workflow_id)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "workflows_created": self._stats["workflows_created"],
            "workflows_completed": self._stats["workflows_completed"],
            "workflows_failed": self._stats["workflows_failed"],
            "tasks_executed": self._stats["tasks_executed"],
            "tasks_failed": self._stats["tasks_failed"],
            "active_workflows": len([w for w in self._workflows.values() if w.status == WorkflowStatus.RUNNING])
        }
