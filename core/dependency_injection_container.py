#!/usr/bin/env python3
"""
Dependency Injection Container
依赖注入容器 - 管理组件创建和依赖解析

核心特性：
1. 服务注册：注册组件实现
2. 依赖解析：自动解析和注入依赖
3. 生命周期管理：单例、瞬态等
4. 延迟初始化：按需创建组件
"""

import asyncio
from typing import Dict, Any, Type, TypeVar, Callable, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import inspect


T = TypeVar('T')


class ServiceLifetime(Enum):
    """服务生命周期"""
    SINGLETON = "singleton"      # 单例：整个应用共享一个实例
    SCOPED = "scoped"           # 作用域：每个作用域一个实例
    TRANSIENT = "transient"     # 瞬态：每次请求创建新实例


@dataclass
class ServiceDescriptor:
    """服务描述符"""
    service_type: Type
    implementation: Any  # 类型或工厂函数
    lifetime: ServiceLifetime
    instance: Optional[Any] = None
    dependencies: List[str] = field(default_factory=list)


class ServiceCollection:
    """
    服务集合 - 用于注册服务
    
    用法：
        services = ServiceCollection()
        services.add_singleton(IKnowledgeEngine, KnowledgeGraphEngine)
        services.add_transient(ITaskExecutor, TaskExecutionWorker)
    """
    
    def __init__(self):
        self._descriptors: Dict[str, ServiceDescriptor] = {}
        self._type_aliases: Dict[Type, str] = {}
    
    def _get_service_name(self, service_type: Type) -> str:
        """获取服务名称"""
        return service_type.__name__ if hasattr(service_type, '__name__') else str(service_type)
    
    def add_singleton(
        self,
        service_type: Type[T],
        implementation: Type[T] = None,
        factory: Callable = None
    ) -> 'ServiceCollection':
        """注册单例服务"""
        return self._add(
            service_type,
            implementation or factory or service_type,
            ServiceLifetime.SINGLETON
        )
    
    def add_scoped(
        self,
        service_type: Type[T],
        implementation: Type[T] = None
    ) -> 'ServiceCollection':
        """注册作用域服务"""
        return self._add(
            service_type,
            implementation or service_type,
            ServiceLifetime.SCOPED
        )
    
    def add_transient(
        self,
        service_type: Type[T],
        implementation: Type[T] = None
    ) -> 'ServiceCollection':
        """注册瞬态服务"""
        return self._add(
            service_type,
            implementation or service_type,
            ServiceLifetime.TRANSIENT
        )
    
    def add_instance(
        self,
        service_type: Type[T],
        instance: T
    ) -> 'ServiceCollection':
        """注册已有实例"""
        name = self._get_service_name(service_type)
        
        descriptor = ServiceDescriptor(
            service_type=service_type,
            implementation=type(instance),
            lifetime=ServiceLifetime.SINGLETON,
            instance=instance
        )
        
        self._descriptors[name] = descriptor
        self._type_aliases[service_type] = name
        
        return self
    
    def _add(
        self,
        service_type: Type,
        implementation: Any,
        lifetime: ServiceLifetime
    ) -> 'ServiceCollection':
        """内部添加方法"""
        name = self._get_service_name(service_type)
        
        # 分析依赖
        dependencies = []
        if callable(implementation) and not isinstance(implementation, type):
            # 工厂函数
            sig = inspect.signature(implementation)
            for param_name, param in sig.parameters.items():
                if param_name != 'self' and param.annotation != inspect.Parameter.empty:
                    dependencies.append(self._get_service_name(param.annotation))
        elif isinstance(implementation, type):
            # 类构造函数
            sig = inspect.signature(implementation.__init__)
            for param_name, param in sig.parameters.items():
                if param_name != 'self' and param.annotation != inspect.Parameter.empty:
                    dependencies.append(self._get_service_name(param.annotation))
        
        descriptor = ServiceDescriptor(
            service_type=service_type,
            implementation=implementation,
            lifetime=lifetime,
            dependencies=dependencies
        )
        
        self._descriptors[name] = descriptor
        self._type_aliases[service_type] = name
        
        return self
    
    def get_descriptor(self, name: str) -> Optional[ServiceDescriptor]:
        """获取服务描述符"""
        return self._descriptors.get(name)


class ServiceProvider:
    """
    服务提供者 - 用于解析服务
    
    用法：
        provider = services.build_provider()
        executor = provider.get_service(ITaskExecutor)
    """
    
    def __init__(self, collection: ServiceCollection):
        self._collection = collection
        self._singletons: Dict[str, Any] = {}
        self._resolving: set = set()  # 防止循环依赖
    
    @classmethod
    def from_collection(cls, collection: ServiceCollection) -> 'ServiceProvider':
        """从服务集合创建提供者"""
        return cls(collection)
    
    def get_service(self, service_type: Type[T]) -> T:
        """获取服务实例"""
        name = self._collection._type_aliases.get(service_type) or \
               self._collection._get_service_name(service_type)
        return self._get_service_internal(name)
    
    def get_service_by_name(self, name: str) -> Any:
        """按名称获取服务"""
        return self._get_service_internal(name)
    
    def _get_service_internal(self, name: str) -> Any:
        """内部获取服务"""
        descriptor = self._collection.get_descriptor(name)
        
        if descriptor is None:
            raise ValueError(f"Service '{name}' not registered")
        
        # 单例：返回已创建的实例
        if descriptor.lifetime == ServiceLifetime.SINGLETON:
            if descriptor.instance is not None:
                return descriptor.instance
            if name in self._singletons:
                return self._singletons[name]
        
        # 检查循环依赖
        if name in self._resolving:
            raise ValueError(f"Circular dependency detected for service '{name}'")
        
        self._resolving.add(name)
        
        try:
            instance = self._create_instance(descriptor)
            
            # 单例：缓存实例
            if descriptor.lifetime == ServiceLifetime.SINGLETON:
                self._singletons[name] = instance
            
            return instance
        finally:
            self._resolving.remove(name)
    
    def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """创建服务实例"""
        implementation = descriptor.implementation
        
        # 工厂函数
        if callable(implementation) and not isinstance(implementation, type):
            # 解析工厂函数参数
            kwargs = {}
            sig = inspect.signature(implementation)
            for param_name, param in sig.parameters.items():
                if param_name != 'self' and param.annotation != inspect.Parameter.empty:
                    dep_name = self._collection._get_service_name(param.annotation)
                    kwargs[param_name] = self._get_service_internal(dep_name)
            
            return implementation(**kwargs)
        
        # 类实例化
        if isinstance(implementation, type):
            # 解析构造函数参数
            kwargs = {}
            sig = inspect.signature(implementation.__init__)
            for param_name, param in sig.parameters.items():
                if param_name != 'self' and param.annotation != inspect.Parameter.empty:
                    dep_name = self._collection._get_service_name(param.annotation)
                    kwargs[param_name] = self._get_service_internal(dep_name)
            
            return implementation(**kwargs)
        
        raise ValueError(f"Invalid implementation for service '{descriptor.service_type}'")
    
    def get_all_services(self) -> Dict[str, Any]:
        """获取所有已创建的服务"""
        return self._singletons.copy()
    
    async def initialize_all(self) -> None:
        """初始化所有单例服务"""
        for name, descriptor in self._collection._descriptors.items():
            if descriptor.lifetime == ServiceLifetime.SINGLETON:
                instance = self.get_service_by_name(name)
                if hasattr(instance, 'initialize') and asyncio.iscoroutinefunction(instance.initialize):
                    await instance.initialize()
    
    async def shutdown_all(self) -> None:
        """关闭所有服务"""
        # 按依赖顺序反向关闭
        for name in reversed(list(self._singletons.keys())):
            instance = self._singletons[name]
            if hasattr(instance, 'shutdown') and asyncio.iscoroutinefunction(instance.shutdown):
                try:
                    await instance.shutdown()
                except Exception as e:
                    print(f"[DI] Error shutting down {name}: {e}")


class Container:
    """
    简化的依赖注入容器
    
    用法：
        container = Container()
        container.register_singleton('knowledge', KnowledgeGraphEngine)
        knowledge = container.resolve('knowledge')
    """
    
    def __init__(self):
        self._services: Dict[str, ServiceDescriptor] = {}
        self._instances: Dict[str, Any] = {}
    
    def register_singleton(
        self,
        name: str,
        cls: Type,
        factory: Optional[Callable] = None
    ):
        """注册单例"""
        self._services[name] = ServiceDescriptor(
            service_type=cls,
            implementation=factory or cls,
            lifetime=ServiceLifetime.SINGLETON
        )
    
    def register_transient(self, name: str, cls: Type):
        """注册瞬态"""
        self._services[name] = ServiceDescriptor(
            service_type=cls,
            implementation=cls,
            lifetime=ServiceLifetime.TRANSIENT
        )
    
    def register_instance(self, name: str, instance: Any):
        """注册实例"""
        self._instances[name] = instance
    
    def resolve(self, name: str) -> Any:
        """解析服务"""
        if name in self._instances:
            return self._instances[name]
        
        descriptor = self._services.get(name)
        if descriptor is None:
            raise ValueError(f"Service '{name}' not found")
        
        instance = self._create(descriptor)
        
        if descriptor.lifetime == ServiceLifetime.SINGLETON:
            self._instances[name] = instance
        
        return instance
    
    def _create(self, descriptor: ServiceDescriptor) -> Any:
        """创建实例"""
        impl = descriptor.implementation
        
        if callable(impl) and not isinstance(impl, type):
            return impl()
        
        if isinstance(impl, type):
            return impl()
        
        raise ValueError(f"Invalid implementation")
    
    def has(self, name: str) -> bool:
        """检查服务是否存在"""
        return name in self._services or name in self._instances
