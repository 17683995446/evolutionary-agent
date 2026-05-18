#!/usr/bin/env python3
"""
Dual Process Orchestrator
双流程编排器 - 执行与优化双并行运转的核心协调器

核心职责：
1. 协调任务执行层和智能优化层
2. 实现数据双向流动
3. 确保执行与优化无缝衔接
4. 处理全局异常
"""

import asyncio
import time
import traceback
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import deque


@dataclass
class CycleMetrics:
    """循环周期指标"""
    cycle_number: int
    start_time: float
    end_time: Optional[float] = None
    tasks_executed: int = 0
    optimizations_applied: int = 0
    errors_encountered: int = 0


class DualProcessOrchestrator:
    """
    双流程编排器
    
    核心循环：
    while True:
        1. 全力执行当前全部任务
        2. 实时采集执行数据
        3. 分析不足、梳理优化方向
        4. 更新任务执行规则
        5. 新规则直接投入下一轮执行
        6. 捕获异常，仅修正不终止全局流程
    """
    
    def __init__(
        self,
        task_worker: Any,
        optimization_analyzer: Any,
        deployment_pipeline: Any,
        guardian_daemon: Any,
        knowledge_engine: Any,
        cycle_report_interval: int = 10
    ):
        self.task_worker = task_worker
        self.optimization_analyzer = optimization_analyzer
        self.deployment_pipeline = deployment_pipeline
        self.guardian_daemon = guardian_daemon
        self.knowledge_engine = knowledge_engine
        self.cycle_report_interval = cycle_report_interval
        
        # 循环计数
        self.execution_cycles = 0
        self.optimization_cycles = 0
        
        # 周期历史
        self._cycle_history: deque = deque(maxlen=100)
        
        # 运行状态
        self._is_running = False
        self._shutdown_requested = False
        
        # 任务引用
        self._tasks: List[asyncio.Task] = []
        # 修复(RL-1)：跟踪各层启动的后台任务，确保关闭时能正确取消
        self._layer_tasks: List[asyncio.Task] = []
        
    async def run_dual_process_loop(self, shutdown_event: asyncio.Event):
        """
        运行双流程主循环
        
        这是整个系统的核心引擎，确保执行和优化并行运转
        """
        print("[ORCHESTRATOR] Starting dual-process orchestration...")
        self._is_running = True
        
        # 启动各层
        await self._start_all_layers()
        
        # 注册组件到守护层
        await self._register_components_to_guardian()
        
        # 启动数据流管道
        await self._start_data_flow_pipelines()
        
        # 主循环
        cycle_number = 0
        while not shutdown_event.is_set() and self._is_running:
            cycle_number += 1
            cycle_metrics = CycleMetrics(
                cycle_number=cycle_number,
                start_time=time.time()
            )
            
            try:
                # ========== 阶段1: 执行阶段 ==========
                execution_task = asyncio.create_task(
                    self._execution_phase(cycle_metrics)
                )
                
                # ========== 阶段2: 优化阶段（与执行并行） ==========
                optimization_task = asyncio.create_task(
                    self._optimization_phase(cycle_metrics)
                )
                
                # 等待两个阶段完成
                await asyncio.gather(execution_task, optimization_task)
                
                # ========== 阶段3: 同步阶段 ==========
                await self._synchronization_phase(cycle_metrics)
                
                # 记录周期完成
                cycle_metrics.end_time = time.time()
                self._cycle_history.append(cycle_metrics)
                
                # 定期报告
                if cycle_number % self.cycle_report_interval == 0:
                    await self._report_cycle_status(cycle_metrics)
                
            except Exception as e:
                cycle_metrics.errors_encountered += 1
                print(f"[ORCHESTRATOR] Cycle {cycle_number} error: {e}")
                print(traceback.format_exc())
                
                # 记录错误但不终止循环
                await self.knowledge_engine.add_error_pattern(
                    error_type="cycle_error",
                    error_message=str(e),
                    context={"cycle_number": cycle_number}
                )
                
                await asyncio.sleep(1)  # 短暂暂停后继续
        
        print("[ORCHESTRATOR] Dual-process loop ended")
    
    async def _start_all_layers(self):
        """启动所有层"""
        print("[ORCHESTRATOR] Starting all layers...")
        
        # 启动任务执行层
        await self.task_worker.start_execution_loop()
        if self.task_worker._worker_task:
            self._layer_tasks.append(self.task_worker._worker_task)
        
        # 启动智能优化层
        await self.optimization_analyzer.start_analysis_loop()
        if self.optimization_analyzer._analyzer_task:
            self._layer_tasks.append(self.optimization_analyzer._analyzer_task)
        
        # 启动同步落地层
        await self.deployment_pipeline.start_deployment_loop()
        if self.deployment_pipeline._pipeline_task:
            self._layer_tasks.append(self.deployment_pipeline._pipeline_task)
        if self.deployment_pipeline._validation_task:
            self._layer_tasks.append(self.deployment_pipeline._validation_task)
        
        # 启动守护层监控
        await self.guardian_daemon.start_monitoring()
        if self.guardian_daemon._monitor_task:
            self._layer_tasks.append(self.guardian_daemon._monitor_task)
        if self.guardian_daemon._self_monitor_task:
            self._layer_tasks.append(self.guardian_daemon._self_monitor_task)
        
        print("[ORCHESTRATOR] All layers started")
    
    async def _register_components_to_guardian(self):
        """注册组件到守护层"""
        print("[ORCHESTRATOR] Registering components to guardian...")
        
        # 注册任务执行层
        self.guardian_daemon.register_component(
            component_name="task_executor",
            heartbeat_callback=self.task_worker.get_heartbeat,
            recovery_callback=self._recover_task_executor,
            critical=True
        )
        
        # 注册优化分析层
        self.guardian_daemon.register_component(
            component_name="optimization_analyzer",
            heartbeat_callback=self.optimization_analyzer.get_heartbeat,
            recovery_callback=self._recover_optimizer,
            critical=True
        )
        
        # 注册部署管道
        self.guardian_daemon.register_component(
            component_name="deployment_pipeline",
            heartbeat_callback=self.deployment_pipeline.get_heartbeat,
            recovery_callback=self._recover_deployer,
            critical=True
        )
        
        # 注册知识图谱
        self.guardian_daemon.register_component(
            component_name="knowledge_engine",
            heartbeat_callback=self.knowledge_engine.get_heartbeat,
            recovery_callback=None,
            critical=False
        )
        
        print("[ORCHESTRATOR] Components registered")
    
    async def _start_data_flow_pipelines(self):
        """启动数据流管道"""
        print("[ORCHESTRATOR] Starting data flow pipelines...")
        
        # 启动执行数据流向优化层
        data_flow_task = asyncio.create_task(self._execution_to_optimizer_flow())
        self._tasks.append(data_flow_task)
        
        print("[ORCHESTRATOR] Data flow pipelines started")
    
    async def _execution_phase(self, cycle_metrics: CycleMetrics):
        """执行阶段 - 全力执行任务"""
        self.execution_cycles += 1
        
        # 生成测试任务（实际应用中这里应该是真实任务）
        # 这里模拟持续的任务流
        for i in range(10):  # 每周期生成10个任务
            await self.task_worker.submit_task(
                name=f"task_cycle_{cycle_metrics.cycle_number}_{i}",
                payload={
                    "operation": ["sum", "avg", "max"][i % 3],
                    "data": list(range(100))
                },
                priority=self.task_worker.TaskPriority.NORMAL,
                task_type="compute"
            )
            cycle_metrics.tasks_executed += 1
        
        # 等待任务处理（短暂时间）
        await asyncio.sleep(0.5)
    
    async def _optimization_phase(self, cycle_metrics: CycleMetrics):
        """优化阶段 - 分析并生成优化方案"""
        self.optimization_cycles += 1
        
        # 获取执行历史并喂给优化器
        execution_history = self.task_worker.get_execution_history(limit=50)
        for record in execution_history:
            self.optimization_analyzer.feed_execution_data(record)
        
        # 优化分析器在后台运行，这里只是触发数据喂养
        await asyncio.sleep(0.1)
    
    async def _synchronization_phase(self, cycle_metrics: CycleMetrics):
        """同步阶段 - 确保优化规则应用到执行层"""
        # 部署管道会自动处理同步
        # 这里检查是否有新的优化规则被应用
        deployment_summary = self.deployment_pipeline.get_deployment_summary()
        cycle_metrics.optimizations_applied = deployment_summary["stats"]["total_active"]
        
        await asyncio.sleep(0.1)
    
    async def _execution_to_optimizer_flow(self):
        """执行数据流向优化层的管道"""
        while self._is_running:
            try:
                # 定期将执行数据传递给优化层
                execution_history = self.task_worker.get_execution_history(limit=20)
                for record in execution_history:
                    self.optimization_analyzer.feed_execution_data(record)
                
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[ORCHESTRATOR] Data flow error: {e}")
                await asyncio.sleep(1)
    
    async def _report_cycle_status(self, cycle_metrics: CycleMetrics):
        """报告周期状态"""
        duration = cycle_metrics.end_time - cycle_metrics.start_time if cycle_metrics.end_time else 0
        
        guardian_health = self.guardian_daemon.get_system_health()
        knowledge_stats = self.knowledge_engine.get_statistics()
        
        print(f"\n[ORCHESTRATOR] === Cycle {cycle_metrics.cycle_number} Report ===")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Tasks executed: {cycle_metrics.tasks_executed}")
        print(f"  Active optimizations: {cycle_metrics.optimizations_applied}")
        print(f"  System health: {guardian_health['overall_status']}")
        print(f"  Knowledge entries: {knowledge_stats['total_nodes']}")
        print("=" * 50)
    
    # ============== 恢复回调 ==============
    
    async def _recover_task_executor(self):
        """恢复任务执行器"""
        print("[ORCHESTRATOR] Recovering task executor...")
        await self.task_worker.initialize()
        await self.task_worker.start_execution_loop()
    
    async def _recover_optimizer(self):
        """恢复优化分析器"""
        print("[ORCHESTRATOR] Recovering optimizer...")
        await self.optimization_analyzer.initialize()
        await self.optimization_analyzer.start_analysis_loop()
    
    async def _recover_deployer(self):
        """恢复部署管道"""
        print("[ORCHESTRATOR] Recovering deployment pipeline...")
        await self.deployment_pipeline.initialize()
        await self.deployment_pipeline.start_deployment_loop()
    
    # ============== 关闭接口 ==============
    
    async def graceful_shutdown(self):
        """优雅关闭"""
        print("[ORCHESTRATOR] Initiating graceful shutdown...")
        self._is_running = False
        self._shutdown_requested = True
        
        # 取消所有数据流任务
        for task in self._tasks:
            task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # 修复(RL-1)：取消所有层启动的后台任务
        for task in self._layer_tasks:
            task.cancel()
        
        if self._layer_tasks:
            await asyncio.gather(*self._layer_tasks, return_exceptions=True)
        
        print("[ORCHESTRATOR] Graceful shutdown complete")
