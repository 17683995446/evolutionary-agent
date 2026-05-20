#!/usr/bin/env python3
"""
Main Perpetual Execution Engine
核心主引擎 - 启动即全程闭环，无空档、无闲置

架构设计（第一性原理）：
- 单一职责：每个模块只做一件事
- 事件驱动：通过异步事件循环实现并行
- 状态透明：所有状态变更可追溯
- 故障隔离：局部故障不影响全局
"""

import asyncio
import signal
import sys
import time
from typing import Optional
from contextlib import asynccontextmanager

from core.resident_guardian_daemon import ResidentGuardianDaemon
from core.dual_process_orchestrator import DualProcessOrchestrator
from core.knowledge_graph_integration_engine import KnowledgeGraphIntegrationEngine
from modules.task_execution_worker import TaskExecutionWorker
from modules.intelligence_optimization_analyzer import IntelligenceOptimizationAnalyzer
from modules.synchronization_deployment_pipeline import SynchronizationDeploymentPipeline
from storage.persistent_state_manager import PersistentStateManager
from config.runtime_configuration_loader import RuntimeConfigurationLoader


class MainPerpetualExecutionEngine:
    """
    主永续执行引擎
    
    核心特性：
    1. 零空闲设计 - 执行与优化双线程并行
    2. 自驱进化 - 无需人工干预
    3. 故障自愈 - 局部异常自动修复
    4. 知识积累 - 经验持续沉淀到知识图谱
    """
    
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self._startup_time = time.time()
        self._execution_cycles = 0
        self._optimization_cycles = 0
        
        # 初始化配置加载器
        self.config_loader = RuntimeConfigurationLoader()
        
        # 从配置中读取参数（修复BUG-8：将配置值传递给组件，替代硬编码默认值）
        max_concurrent = self.config_loader.get("engine", "max_concurrent_tasks", 10)
        task_queue_size = self.config_loader.get("engine", "task_queue_size", 1000)
        health_check_interval = self.config_loader.get("guardian", "health_check_interval", 5.0)
        heartbeat_timeout = self.config_loader.get("guardian", "heartbeat_timeout", 15.0)
        max_recovery_attempts = self.config_loader.get("guardian", "max_recovery_attempts", 3)
        analysis_interval = self.config_loader.get("optimizer", "analysis_interval", 10.0)
        min_samples = self.config_loader.get("optimizer", "min_samples_for_analysis", 50)
        validation_window = self.config_loader.get("deployer", "validation_window_seconds", 30.0)
        deployment_cooldown = self.config_loader.get("deployer", "deployment_cooldown_seconds", 5.0)
        auto_save_interval = self.config_loader.get("knowledge", "auto_save_interval", 60.0)
        max_nodes = self.config_loader.get("knowledge", "max_nodes_in_memory", 100000)
        cycle_report_interval = self.config_loader.get("engine", "cycle_report_interval", 10)
        
        # 初始化状态管理器
        self.state_manager = PersistentStateManager()
        
        # 初始化知识图谱引擎
        self.knowledge_engine = KnowledgeGraphIntegrationEngine(
            auto_save_interval=auto_save_interval,
            max_nodes_in_memory=max_nodes
        )
        
        # 初始化常驻守护层
        self.guardian_daemon = ResidentGuardianDaemon(
            shutdown_event=self._shutdown_event,
            health_check_interval=health_check_interval,
            heartbeat_timeout=heartbeat_timeout,
            max_recovery_attempts=max_recovery_attempts
        )
        
        # 初始化任务执行层
        self.task_worker = TaskExecutionWorker(
            knowledge_engine=self.knowledge_engine,
            state_manager=self.state_manager,
            max_concurrent_tasks=max_concurrent,
            task_queue_size=task_queue_size
        )
        
        # 初始化智能优化层
        self.optimization_analyzer = IntelligenceOptimizationAnalyzer(
            knowledge_engine=self.knowledge_engine,
            state_manager=self.state_manager,
            analysis_interval=analysis_interval,
            min_samples_for_analysis=min_samples
        )
        
        # 初始化同步落地层
        self.deployment_pipeline = SynchronizationDeploymentPipeline(
            task_worker=self.task_worker,
            optimization_analyzer=self.optimization_analyzer,
            validation_window_seconds=validation_window,
            deployment_cooldown_seconds=deployment_cooldown
        )
        
        # 初始化双流程编排器
        self.orchestrator = DualProcessOrchestrator(
            task_worker=self.task_worker,
            optimization_analyzer=self.optimization_analyzer,
            deployment_pipeline=self.deployment_pipeline,
            guardian_daemon=self.guardian_daemon,
            knowledge_engine=self.knowledge_engine,
            cycle_report_interval=cycle_report_interval
        )
        
    async def initialize_system(self) -> bool:
        """系统初始化 - 按依赖顺序启动各组件"""
        print("[ENGINE] Initializing perpetual execution system...")

        init_sequence = [
            ("Configuration Loader", self.config_loader.initialize()),
            ("State Manager", self.state_manager.initialize()),
            ("Knowledge Graph Engine", self.knowledge_engine.initialize()),
            ("Guardian Daemon", self.guardian_daemon.initialize()),
            ("Task Execution Worker", self.task_worker.initialize()),
            ("Optimization Analyzer", self.optimization_analyzer.initialize()),
            ("Deployment Pipeline", self.deployment_pipeline.initialize()),
        ]

        # 修复(BUG-ENGINE-3): 跟踪已初始化的组件，失败时清理
        shutdown_sequence = [
            ("Deployment Pipeline", self.deployment_pipeline.shutdown),
            ("Optimization Analyzer", self.optimization_analyzer.shutdown),
            ("Task Execution Worker", self.task_worker.shutdown),
            ("Guardian Daemon", self.guardian_daemon.shutdown),
            ("Knowledge Graph Engine", self.knowledge_engine.shutdown),
            ("State Manager", self.state_manager.shutdown),
            ("Configuration Loader", self.config_loader.shutdown),
        ]

        initialized_count = 0
        for name, coro in init_sequence:
            try:
                await asyncio.wait_for(coro, timeout=10.0)
                print(f"[ENGINE] ✓ {name} initialized")
                initialized_count += 1
            except Exception as e:
                print(f"[ENGINE] ✗ {name} failed: {e}")
                # 修复(BUG-ENGINE-3/R8-MISC-01): 清理已初始化的组件（逆序）
                # shutdown_sequence 是逆序的，前 (7 - initialized_count) 个是未初始化的
                # 已初始化的组件对应 shutdown_sequence 中从索引 (7 - initialized_count) 开始
                num_to_skip = len(init_sequence) - initialized_count
                print(f"[ENGINE] Cleaning up {initialized_count} already-initialized components...")
                for prev_name, prev_shutdown_fn in shutdown_sequence[num_to_skip:]:
                    try:
                        await asyncio.wait_for(prev_shutdown_fn(), timeout=5.0)
                        print(f"[ENGINE] ✓ {prev_name} cleaned up")
                    except Exception as cleanup_err:
                        print(f"[ENGINE] ✗ {prev_name} cleanup failed: {cleanup_err}")
                return False

        # 注册信号处理器
        self._register_signal_handlers()

        print("[ENGINE] System initialization complete")
        return True
    
    def _register_signal_handlers(self):
        """注册系统信号处理器 - 实现优雅关闭"""
        def signal_handler(sig, frame):
            print(f"\n[ENGINE] Received signal {sig}, initiating graceful shutdown...")
            self._shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def run_perpetual_loop(self):
        """
        永续执行主循环
        
        核心逻辑：
        while True:
            1. 全力执行当前全部任务
            2. 实时采集执行数据
            3. 分析不足、梳理优化方向
            4. 更新任务执行规则
            5. 新规则直接投入下一轮执行
            6. 捕获异常，仅修正不终止全局流程
        """
        print("[ENGINE] Starting perpetual execution loop...")
        print("[ENGINE] Press Ctrl+C to initiate graceful shutdown")
        print("=" * 60)
        
        # 启动双流程编排器
        orchestrator_task = asyncio.create_task(
            self.orchestrator.run_dual_process_loop(self._shutdown_event)
        )
        
        # 等待关闭信号
        await self._shutdown_event.wait()
        
        print("\n[ENGINE] Shutdown signal received, stopping orchestrator...")
        
        # 优雅关闭编排器
        await self.orchestrator.graceful_shutdown()
        
        # 等待编排器任务完成
        try:
            await asyncio.wait_for(orchestrator_task, timeout=30.0)
        except asyncio.TimeoutError:
            print("[ENGINE] Orchestrator shutdown timeout, forcing cancellation")
            orchestrator_task.cancel()
    
    async def graceful_teardown(self):
        """优雅拆卸 - 按反向依赖顺序关闭组件"""
        print("[ENGINE] Starting graceful teardown...")
        
        teardown_sequence = [
            ("Deployment Pipeline", self.deployment_pipeline.shutdown()),
            ("Optimization Analyzer", self.optimization_analyzer.shutdown()),
            ("Task Execution Worker", self.task_worker.shutdown()),
            ("Guardian Daemon", self.guardian_daemon.shutdown()),
            ("Knowledge Graph Engine", self.knowledge_engine.shutdown()),
            ("State Manager", self.state_manager.shutdown()),
        ]
        
        for name, coro in teardown_sequence:
            try:
                await asyncio.wait_for(coro, timeout=10.0)
                print(f"[ENGINE] ✓ {name} shutdown complete")
            except Exception as e:
                print(f"[ENGINE] ✗ {name} shutdown error: {e}")
        
        # 输出运行统计
        runtime = time.time() - self._startup_time
        print(f"\n[ENGINE] Runtime Statistics:")
        print(f"  - Total runtime: {runtime:.2f}s")
        print(f"  - Execution cycles: {self.orchestrator.execution_cycles}")
        print(f"  - Optimization cycles: {self.orchestrator.optimization_cycles}")
        print(f"  - Knowledge entries: {await self.knowledge_engine.get_entry_count()}")
        print("\n[ENGINE] System shutdown complete")


async def main():
    """主入口函数"""
    engine = MainPerpetualExecutionEngine()
    
    # 初始化系统
    if not await engine.initialize_system():
        print("[ENGINE] System initialization failed, exiting")
        sys.exit(1)
    
    try:
        # 运行永续循环
        await engine.run_perpetual_loop()
    finally:
        # 优雅关闭
        await engine.graceful_teardown()


if __name__ == "__main__":
    # 设置事件循环策略（Windows兼容）
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行主程序
    asyncio.run(main())
