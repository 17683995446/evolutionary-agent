#!/usr/bin/env python3
"""
Component Interface Protocols
组件接口协议 - 定义所有组件的标准接口

设计原则：
- 依赖反转：高层模块依赖抽象，不依赖具体实现
- 接口隔离：每个接口职责单一
- 可替换性：任何实现都可以无缝替换
"""

from typing import Protocol, Dict, Any, List, Optional, Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
import asyncio


class ComponentState(Enum):
    """组件状态"""
    CREATED = "created"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    healthy: bool
    message: str
    metrics: Dict[str, Any]


# ============== 核心组件接口 ==============

class ILifecycleComponent(Protocol):
    """生命周期组件接口 - 所有组件的基础接口"""
    
    @property
    def component_name(self) -> str:
        """组件名称"""
        ...
    
    @property
    def state(self) -> ComponentState:
        """当前状态"""
        ...
    
    async def initialize(self) -> None:
        """初始化组件"""
        ...
    
    async def start(self) -> None:
        """启动组件"""
        ...
    
    async def stop(self) -> None:
        """停止组件"""
        ...
    
    async def shutdown(self) -> None:
        """关闭组件（释放所有资源）"""
        ...
    
    def get_heartbeat(self) -> HealthCheckResult:
        """获取心跳（健康检查）"""
        ...


class ITaskExecutor(Protocol):
    """任务执行器接口"""
    
    async def submit_task(
        self,
        name: str,
        payload: Dict[str, Any],
        priority: int = 0,
        task_type: str = "default"
    ) -> str:
        """提交任务，返回任务ID"""
        ...
    
    def get_task_status(self, task_id: str) -> Optional[str]:
        """获取任务状态"""
        ...
    
    def get_execution_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取执行历史"""
        ...
    
    def register_handler(self, task_type: str, handler: Callable) -> None:
        """注册任务处理器"""
        ...
    
    def update_optimization_rules(self, rules: Dict[str, Any]) -> None:
        """更新优化规则"""
        ...


class IKnowledgeEngine(Protocol):
    """知识图谱引擎接口"""
    
    async def add_execution_record(
        self,
        task_type: str,
        action: str,
        data: Dict[str, Any]
    ) -> str:
        """添加执行记录"""
        ...
    
    async def add_optimization_record(self, data: Dict[str, Any]) -> str:
        """添加优化记录"""
        ...
    
    async def add_error_pattern(
        self,
        error_type: str,
        error_message: str,
        context: Dict[str, Any]
    ) -> str:
        """添加错误模式"""
        ...
    
    def query_nodes(
        self,
        node_type: Optional[str] = None,
        tags: Optional[set] = None,
        limit: int = 100
    ) -> List[Any]:
        """查询节点"""
        ...
    
    def search_by_content(
        self,
        keywords: List[str],
        limit: int = 50
    ) -> List[Any]:
        """内容搜索"""
        ...
    
    async def get_entry_count(self) -> int:
        """获取条目数量"""
        ...


class IOptimizationAnalyzer(Protocol):
    """优化分析器接口"""
    
    def feed_execution_data(self, data: Dict[str, Any]) -> None:
        """输入执行数据"""
        ...
    
    def get_optimization_rules(self) -> Dict[str, Dict[str, Any]]:
        """获取优化规则"""
        ...
    
    def record_rule_effectiveness(self, rule_id: str, was_successful: bool) -> None:
        """记录规则效果"""
        ...
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """获取分析摘要"""
        ...


class IDeploymentPipeline(Protocol):
    """部署管道接口"""
    
    async def deploy_rule(
        self,
        target_component: str,
        parameters: Dict[str, Any]
    ) -> str:
        """部署规则"""
        ...
    
    async def rollback_deployment(self, deployment_id: str) -> bool:
        """回滚部署"""
        ...
    
    def get_deployment_summary(self) -> Dict[str, Any]:
        """获取部署摘要"""
        ...


class IGuardianDaemon(Protocol):
    """守护进程接口"""
    
    def register_component(
        self,
        component_name: str,
        heartbeat_callback: Callable[[], HealthCheckResult],
        recovery_callback: Optional[Callable[[], Awaitable[None]]] = None,
        critical: bool = True
    ) -> None:
        """注册组件"""
        ...
    
    def unregister_component(self, component_name: str) -> None:
        """注销组件"""
        ...
    
    def get_system_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        ...


class IEventBus(Protocol):
    """事件总线接口"""
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable,
        source_filter: Optional[str] = None
    ) -> str:
        """订阅事件"""
        ...
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """取消订阅"""
        ...
    
    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        source: str = "unknown"
    ) -> str:
        """发布事件"""
        ...


class ILogger(Protocol):
    """日志接口"""
    
    def debug(self, message: str, **extra) -> None:
        ...
    
    def info(self, message: str, **extra) -> None:
        ...
    
    def warning(self, message: str, **extra) -> None:
        ...
    
    def error(self, message: str, exception: Optional[Exception] = None, **extra) -> None:
        ...
    
    def critical(self, message: str, exception: Optional[Exception] = None, **extra) -> None:
        ...


class IStateManager(Protocol):
    """状态管理器接口"""
    
    async def save_key_value(self, key: str, value: Any) -> None:
        ...
    
    async def load_key_value(self, key: str) -> Optional[Any]:
        ...
    
    async def save_executor_state(self, state: Dict[str, Any]) -> None:
        ...
    
    async def load_executor_state(self) -> Optional[Dict[str, Any]]:
        ...


class IConfigurationLoader(Protocol):
    """配置加载器接口"""
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        ...
    
    def get_section(self, section: str) -> Dict[str, Any]:
        ...
    
    def get_all(self) -> Dict[str, Any]:
        ...


class IScheduler(Protocol):
    """调度器接口"""
    
    def record_execution(
        self,
        task_id: str,
        task_type: str,
        duration: float,
        success: bool
    ) -> None:
        ...
    
    def schedule(
        self,
        pending_tasks: List[tuple],
        max_batch: int = 10
    ) -> List[Any]:
        ...
    
    def predict_duration(self, task_type: str) -> tuple:
        ...


class IWorkflowEngine(Protocol):
    """工作流引擎接口"""
    
    def create_workflow(self, name: str, workflow_id: Optional[str] = None) -> Any:
        ...
    
    async def execute_workflow(
        self,
        workflow: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> Any:
        ...
    
    def get_workflow(self, workflow_id: str) -> Optional[Any]:
        ...
