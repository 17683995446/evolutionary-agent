#!/usr/bin/env python3
"""
Event Bus System
事件总线系统 - 组件间解耦通信机制

核心特性：
1. 发布/订阅模式：组件间松耦合通信
2. 异步事件处理：不阻塞主流程
3. 事件优先级：重要事件优先处理
4. 事件历史：支持事件回放和审计
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import uuid


class EventType(Enum):
    """系统事件类型"""
    # 任务事件
    TASK_SUBMITTED = "task.submitted"
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_TIMEOUT = "task.timeout"
    
    # 优化事件
    OPTIMIZATION_ANALYZED = "optimization.analyzed"
    OPTIMIZATION_RULE_CREATED = "optimization.rule_created"
    OPTIMIZATION_RULE_APPLIED = "optimization.rule_applied"
    OPTIMIZATION_RULE_ROLLBACK = "optimization.rule_rollback"
    
    # 系统事件
    SYSTEM_STARTED = "system.started"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_ERROR = "system.error"
    SYSTEM_WARNING = "system.warning"
    
    # 组件事件
    COMPONENT_REGISTERED = "component.registered"
    COMPONENT_HEALTH_CHANGED = "component.health_changed"
    COMPONENT_RECOVERY_STARTED = "component.recovery_started"
    COMPONENT_RECOVERY_COMPLETED = "component.recovery_completed"
    
    # 知识图谱事件
    KNOWLEDGE_NODE_CREATED = "knowledge.node_created"
    KNOWLEDGE_NODE_ACCESSED = "knowledge.node_accessed"
    KNOWLEDGE_CLEANUP = "knowledge.cleanup"
    
    # 自定义事件
    CUSTOM = "custom"


@dataclass
class Event:
    """事件对象"""
    event_id: str
    event_type: str
    source: str  # 事件来源组件
    data: Dict[str, Any]
    timestamp: float
    priority: int = 0  # 优先级，值越大越优先
    metadata: Dict[str, Any] = field(default_factory=dict)
    _sequence: int = 0  # 内部序列号，保证入队顺序
    
    def __lt__(self, other):
        """支持优先级队列比较（同优先级时按序列号排序）"""
        if not isinstance(other, Event):
            return NotImplemented
        return self._sequence < other._sequence
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "data": self.data,
            "timestamp": self.timestamp,
            "priority": self.priority,
            "metadata": self.metadata
        }


@dataclass
class Subscription:
    """订阅信息"""
    subscription_id: str
    event_type: str
    handler: Callable
    source_filter: Optional[str] = None  # 可选的来源过滤
    priority: int = 0
    active: bool = True
    call_count: int = 0
    last_called: Optional[float] = None


class EventBus:
    """
    事件总线
    
    设计原则：
    - 松耦合：发布者和订阅者互不依赖
    - 异步优先：所有处理都是异步的
    - 可观测：所有事件可追踪
    - 容错：处理器异常不影响其他处理器
    """
    
    def __init__(
        self,
        max_history: int = 1000,
        max_queue_size: int = 10000,
        default_timeout: float = 5.0
    ):
        self.max_history = max_history
        self.max_queue_size = max_queue_size
        self.default_timeout = default_timeout
        
        # 事件序列号生成器
        self._event_sequence = 0
        
        # 订阅表：event_type -> List[Subscription]
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        
        # 事件队列
        self._event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=max_queue_size
        )
        
        # 事件历史
        self._event_history: deque = deque(maxlen=max_history)
        
        # 统计
        self._stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_dropped": 0,
            "handler_errors": 0,
            "active_subscriptions": 0
        }
        
        # 运行状态
        self._is_running = False
        self._processor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """启动事件处理器"""
        if self._is_running:
            return
        
        self._is_running = True
        self._processor_task = asyncio.create_task(self._process_events())
        print("[EVENT_BUS] Event processor started")
    
    async def stop(self):
        """停止事件处理器"""
        self._is_running = False
        
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        
        print("[EVENT_BUS] Event processor stopped")
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable,
        source_filter: Optional[str] = None,
        priority: int = 0
    ) -> str:
        """
        订阅事件
        
        Args:
            event_type: 事件类型（支持通配符 "*"）
            handler: 事件处理函数（async 或 sync）
            source_filter: 可选的来源过滤
            priority: 处理器优先级
            
        Returns:
            订阅ID
        """
        subscription_id = str(uuid.uuid4())[:8]
        
        subscription = Subscription(
            subscription_id=subscription_id,
            event_type=event_type,
            handler=handler,
            source_filter=source_filter,
            priority=priority
        )
        
        self._subscriptions[event_type].append(subscription)
        self._stats["active_subscriptions"] += 1
        
        print(f"[EVENT_BUS] Subscribed to '{event_type}' (id={subscription_id})")
        return subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """取消订阅"""
        for event_type, subs in self._subscriptions.items():
            for i, sub in enumerate(subs):
                if sub.subscription_id == subscription_id:
                    subs.pop(i)
                    self._stats["active_subscriptions"] -= 1
                    print(f"[EVENT_BUS] Unsubscribed {subscription_id}")
                    return True
        return False
    
    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        source: str = "unknown",
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        发布事件
        
        Args:
            event_type: 事件类型
            data: 事件数据
            source: 事件来源
            priority: 事件优先级（值越大越优先）
            metadata: 可选的元数据
            
        Returns:
            事件ID
        """
        event_id = str(uuid.uuid4())[:8]
        self._event_sequence += 1
        
        event = Event(
            event_id=event_id,
            event_type=event_type,
            source=source,
            data=data,
            timestamp=time.time(),
            priority=priority,
            metadata=metadata or {},
            _sequence=self._event_sequence
        )
        
        # 加入队列（优先级队列，负号使大优先级先出）
        try:
            await asyncio.wait_for(
                self._event_queue.put((-priority, event)),
                timeout=1.0
            )
            self._stats["events_published"] += 1
        except asyncio.TimeoutError:
            self._stats["events_dropped"] += 1
            print(f"[EVENT_BUS] WARNING: Event queue full, dropped event {event_id}")
        
        return event_id
    
    def publish_sync(
        self,
        event_type: str,
        data: Dict[str, Any],
        source: str = "unknown",
        priority: int = 0
    ) -> str:
        """同步发布事件（用于非异步上下文）"""
        event_id = str(uuid.uuid4())[:8]
        self._event_sequence += 1
        
        event = Event(
            event_id=event_id,
            event_type=event_type,
            source=source,
            data=data,
            timestamp=time.time(),
            priority=priority,
            _sequence=self._event_sequence
        )
        
        try:
            self._event_queue.put_nowait((-priority, event))
            self._stats["events_published"] += 1
        except asyncio.QueueFull:
            self._stats["events_dropped"] += 1
        
        return event_id
    
    async def _process_events(self):
        """事件处理循环"""
        while self._is_running:
            try:
                # 获取事件（带超时）
                try:
                    _, event = await asyncio.wait_for(
                        self._event_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # 记录历史
                self._event_history.append(event)
                
                # 分发事件
                await self._dispatch_event(event)
                
                self._stats["events_processed"] += 1
                
            except Exception as e:
                print(f"[EVENT_BUS] Error processing event: {e}")
    
    async def _dispatch_event(self, event: Event):
        """分发事件到订阅者"""
        # 找到所有匹配的订阅
        handlers_to_call = []
        
        # 精确匹配
        for sub in self._subscriptions.get(event.event_type, []):
            if sub.active and self._matches_filter(sub, event):
                handlers_to_call.append(sub)
        
        # 通配符匹配
        for sub in self._subscriptions.get("*", []):
            if sub.active and self._matches_filter(sub, event):
                handlers_to_call.append(sub)
        
        # 按优先级排序
        handlers_to_call.sort(key=lambda s: s.priority, reverse=True)
        
        # 调用处理器
        for sub in handlers_to_call:
            try:
                if asyncio.iscoroutinefunction(sub.handler):
                    await asyncio.wait_for(
                        sub.handler(event),
                        timeout=self.default_timeout
                    )
                else:
                    sub.handler(event)
                
                sub.call_count += 1
                sub.last_called = time.time()
                
            except asyncio.TimeoutError:
                print(f"[EVENT_BUS] Handler timeout for {sub.subscription_id}")
                self._stats["handler_errors"] += 1
            except Exception as e:
                print(f"[EVENT_BUS] Handler error for {sub.subscription_id}: {e}")
                self._stats["handler_errors"] += 1
    
    def _matches_filter(self, subscription: Subscription, event: Event) -> bool:
        """检查事件是否匹配订阅过滤器"""
        if subscription.source_filter is None:
            return True
        return event.source == subscription.source_filter
    
    def get_history(
        self,
        event_type: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 100
    ) -> List[Event]:
        """获取事件历史"""
        events = list(self._event_history)
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if source:
            events = [e for e in events if e.source == source]
        
        return events[-limit:]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "events_published": self._stats["events_published"],
            "events_processed": self._stats["events_processed"],
            "events_dropped": self._stats["events_dropped"],
            "handler_errors": self._stats["handler_errors"],
            "active_subscriptions": self._stats["active_subscriptions"],
            "queue_size": self._event_queue.qsize(),
            "history_size": len(self._event_history)
        }


# 全局事件总线实例
_global_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """获取全局事件总线"""
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus


async def init_event_bus() -> EventBus:
    """初始化并启动全局事件总线"""
    bus = get_event_bus()
    await bus.start()
    return bus
