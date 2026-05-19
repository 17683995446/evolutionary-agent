#!/usr/bin/env python3
"""
Structured Logging System
结构化日志系统 - 支持日志分析、聚合和可视化

核心特性：
1. 结构化输出：JSON格式，便于解析
2. 日志级别：DEBUG/INFO/WARNING/ERROR/CRITICAL
3. 上下文支持：自动附加组件、任务等上下文
4. 性能优化：异步写入，不阻塞主流程
"""

import asyncio
import time
import json
import sys
import os
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from collections import deque
from enum import Enum
from datetime import datetime
from pathlib import Path


class LogLevel(Enum):
    """日志级别"""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    
    def __str__(self) -> str:
        return self.name


@dataclass
class LogRecord:
    """日志记录"""
    timestamp: float
    level: str
    message: str
    component: str
    context: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[str] = None
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        record = {
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "level": self.level,
            "message": self.message,
            "component": self.component,
            "context": self.context,
            "extra": self.extra
        }
        if self.exception:
            record["exception"] = self.exception
        return json.dumps(record, ensure_ascii=False)


class StructuredLogger:
    """
    结构化日志器
    
    设计原则：
    - 结构化：所有日志都是结构化数据
    - 异步：写入不阻塞主流程
    - 可配置：日志级别、输出目标可配置
    - 上下文：自动附加上下文信息
    """
    
    def __init__(
        self,
        component: str = "system",
        min_level: LogLevel = LogLevel.INFO,
        output_dir: Optional[str] = None,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        enable_console: bool = True,
        enable_file: bool = True,
        buffer_size: int = 1000
    ):
        self.component = component
        self.min_level = min_level
        self.output_dir = Path(output_dir) if output_dir else None
        self.max_file_size = max_file_size
        self.enable_console = enable_console
        self.enable_file = enable_file
        self.buffer_size = buffer_size
        
        # 日志缓冲区
        self._buffer: deque = deque(maxlen=buffer_size)
        
        # 上下文栈
        self._context_stack: List[Dict[str, Any]] = []
        
        # 统计
        self._stats = {
            "total_logged": 0,
            "by_level": {level.name: 0 for level in LogLevel}
        }
        
        # 文件句柄
        self._current_file: Optional[Any] = None
        self._current_file_size = 0
        self._file_index = 0
        
        # 运行状态
        self._is_running = False
        self._writer_task: Optional[asyncio.Task] = None
        
        # 初始化输出目录
        if self.output_dir and enable_file:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    
    async def start(self):
        """启动日志写入器"""
        if self._is_running:
            return
        
        self._is_running = True
        if self.enable_file and self.output_dir:
            self._writer_task = asyncio.create_task(self._file_writer_loop())
    
    async def stop(self):
        """停止日志写入器"""
        self._is_running = False
        
        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
        
        # 刷新剩余日志
        await self._flush_buffer()
        
        if self._current_file:
            self._current_file.close()
    
    def push_context(self, **kwargs):
        """推送上下文"""
        self._context_stack.append(kwargs)
    
    def pop_context(self):
        """弹出上下文"""
        if self._context_stack:
            self._context_stack.pop()
    
    def _get_merged_context(self) -> Dict[str, Any]:
        """获取合并后的上下文"""
        merged = {}
        for ctx in self._context_stack:
            merged.update(ctx)
        return merged
    
    def _log(
        self,
        level: LogLevel,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None
    ):
        """内部日志方法"""
        if level.value < self.min_level.value:
            return
        
        record = LogRecord(
            timestamp=time.time(),
            level=level.name,
            message=message,
            component=self.component,
            context=self._get_merged_context(),
            extra=extra or {},
            exception=str(exception) if exception else None
        )
        
        # 加入缓冲区
        self._buffer.append(record)
        
        # 更新统计
        self._stats["total_logged"] += 1
        self._stats["by_level"][level.name] += 1
        
        # 控制台输出
        if self.enable_console:
            self._console_output(record)
    
    def _console_output(self, record: LogRecord):
        """控制台输出"""
        # 颜色映射
        colors = {
            "DEBUG": "\033[36m",     # 青色
            "INFO": "\033[32m",      # 绿色
            "WARNING": "\033[33m",   # 黄色
            "ERROR": "\033[31m",     # 红色
            "CRITICAL": "\033[35m",  # 紫色
        }
        reset = "\033[0m"
        
        color = colors.get(record.level, "")
        timestamp = datetime.fromtimestamp(record.timestamp).strftime("%H:%M:%S.%f")[:-3]
        
        # 格式化输出
        output = f"{color}[{timestamp}] [{record.level:8}] [{record.component}] {record.message}{reset}"
        
        if record.context:
            output += f" | ctx={record.context}"
        if record.extra:
            output += f" | extra={record.extra}"
        
        print(output, file=sys.stderr if record.level in ["ERROR", "CRITICAL"] else sys.stdout)
    
    async def _file_writer_loop(self):
        """文件写入循环"""
        while self._is_running:
            try:
                await asyncio.sleep(1.0)  # 每秒刷新一次
                await self._flush_buffer()
            except Exception as e:
                print(f"[LOGGER] File writer error: {e}")
    
    async def _flush_buffer(self):
        """刷新缓冲区到文件"""
        if not self.output_dir or not self._buffer:
            return
        
        # 打开或轮转文件
        if self._current_file is None or self._current_file_size > self.max_file_size:
            if self._current_file:
                self._current_file.close()
            
            self._file_index += 1
            filename = self.output_dir / f"{self.component}_{self._file_index:04d}.jsonl"
            self._current_file = open(filename, "a", encoding="utf-8")
            self._current_file_size = 0
        
        # 写入日志
        while self._buffer:
            record = self._buffer.popleft()
            line = record.to_json() + "\n"
            self._current_file.write(line)
            self._current_file_size += len(line)
        
        self._current_file.flush()
    
    # ============== 便捷方法 ==============
    
    def debug(self, message: str, **extra):
        """DEBUG级别日志"""
        self._log(LogLevel.DEBUG, message, extra)
    
    def info(self, message: str, **extra):
        """INFO级别日志"""
        self._log(LogLevel.INFO, message, extra)
    
    def warning(self, message: str, **extra):
        """WARNING级别日志"""
        self._log(LogLevel.WARNING, message, extra)
    
    def error(self, message: str, exception: Optional[Exception] = None, **extra):
        """ERROR级别日志"""
        self._log(LogLevel.ERROR, message, extra, exception)
    
    def critical(self, message: str, exception: Optional[Exception] = None, **extra):
        """CRITICAL级别日志"""
        self._log(LogLevel.CRITICAL, message, extra, exception)
    
    def exception(self, message: str, **extra):
        """异常日志（自动捕获当前异常）"""
        exc_info = sys.exc_info()
        exception = exc_info[1] if exc_info[1] else None
        self._log(LogLevel.ERROR, message, extra, exception)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_logged": self._stats["total_logged"],
            "by_level": self._stats["by_level"],
            "buffer_size": len(self._buffer),
            "component": self.component,
            "min_level": self.min_level.name
        }


# 全局日志器注册表
_loggers: Dict[str, StructuredLogger] = {}


def get_logger(component: str = "system") -> StructuredLogger:
    """获取或创建日志器"""
    if component not in _loggers:
        _loggers[component] = StructuredLogger(component=component)
    return _loggers[component]


def configure_logging(
    output_dir: Optional[str] = None,
    min_level: LogLevel = LogLevel.INFO,
    enable_console: bool = True,
    enable_file: bool = True
):
    """配置全局日志"""
    global _loggers
    
    for component, logger in _loggers.items():
        logger.min_level = min_level
        logger.enable_console = enable_console
        logger.enable_file = enable_file
        if output_dir:
            logger.output_dir = Path(output_dir)
            logger.output_dir.mkdir(parents=True, exist_ok=True)
