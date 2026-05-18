#!/usr/bin/env python3
"""
Runtime Configuration Loader
运行时配置加载器 - 管理系统运行时配置

核心职责：
1. 加载和管理配置
2. 支持配置热更新
3. 提供配置默认值
"""

import json
from typing import Dict, Any, Optional
from pathlib import Path


class RuntimeConfigurationLoader:
    """运行时配置加载器"""
    
    DEFAULT_CONFIG = {
        "engine": {
            "max_concurrent_tasks": 10,
            "task_queue_size": 1000,
            "cycle_report_interval": 10
        },
        "guardian": {
            "health_check_interval": 5.0,
            "heartbeat_timeout": 15.0,
            "max_recovery_attempts": 3
        },
        "optimizer": {
            "analysis_interval": 10.0,
            "min_samples_for_analysis": 50
        },
        "deployer": {
            "validation_window_seconds": 30.0,
            "deployment_cooldown_seconds": 5.0
        },
        "knowledge": {
            "auto_save_interval": 60.0,
            "max_nodes_in_memory": 100000
        }
    }
    
    def __init__(self, config_path: str = "./config/runtime_config.json"):
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        
    async def initialize(self):
        """初始化配置"""
        print("[CONFIG] Loading runtime configuration...")
        
        # 从默认配置开始
        self._config = self.DEFAULT_CONFIG.copy()
        
        # 尝试加载配置文件
        if self.config_path.exists():
            try:
                with open(self.config_path, "r") as f:
                    user_config = json.load(f)
                    self._merge_config(user_config)
                print(f"[CONFIG] Loaded configuration from {self.config_path}")
            except Exception as e:
                print(f"[CONFIG] Error loading config: {e}, using defaults")
        else:
            # 创建默认配置文件
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, "w") as f:
                json.dump(self.DEFAULT_CONFIG, f, indent=2)
            print(f"[CONFIG] Created default configuration at {self.config_path}")
    
    def _merge_config(self, user_config: Dict[str, Any]):
        """合并用户配置到默认配置"""
        for key, value in user_config.items():
            if key in self._config and isinstance(value, dict):
                self._config[key].update(value)
            else:
                self._config[key] = value
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self._config.get(section, {}).get(key, default)
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """获取配置节"""
        return self._config.get(section, {})
    
    def get_all(self) -> Dict[str, Any]:
        """获取所有配置"""
        return self._config.copy()
    
    async def shutdown(self):
        """关闭配置加载器"""
        print("[CONFIG] Configuration loader shutdown complete")
