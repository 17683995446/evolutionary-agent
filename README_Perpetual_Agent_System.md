# 持续进化Agent系统 (Perpetual Evolution Agent System)

## 系统概述

这是一个**零空闲、自驱进化**的Agent系统，启动后全程闭环运转，执行与优化双线程并行，永久循环不停歇。

### 核心定位

- **启动即全速运转**：不存在等待、休眠、闲置状态
- **任务越做越熟练**：流程越跑越精简
- **全自动自驱进化**：无需人工干预调整
- **全程闭环**：执行与进化深度绑定

## 架构设计

### 四层极简架构

```
┌─────────────────────────────────────────────────────────────┐
│                    常驻守护层 (Resident Guardian)            │
│  • 全程保活，异常自动修复重启                                 │
│  • 杜绝停机，保证双流程持续跑通                               │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        ▼                                           ▼
┌──────────────────┐                    ┌──────────────────┐
│   任务执行层      │◄──────────────────►│   智能优化层      │
│  (Task Executor) │    双向数据流       │  (Optimizer)     │
│                  │                    │                  │
│ • 持续输出成果   │                    │ • 实时复盘分析   │
│ • 满负荷推进     │                    │ • 找短板改流程   │
│ • 采集执行数据   │                    │ • 生成优化方案   │
└──────────────────┘                    └──────────────────┘
        │                                           │
        └─────────────────────┬─────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 同步落地层 (Sync Deployment)                 │
│  • 优化规则即时生效                                          │
│  • 直接融入下一轮任务执行                                     │
│  • 无缝衔接执行与优化                                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              知识图谱集成引擎 (Knowledge Graph)               │
│  • 经验和项目信息不断集成                                     │
│  • 支持快速准确检索                                          │
│  • 知识关联与推理                                            │
└─────────────────────────────────────────────────────────────┘
```

## 核心运转逻辑

### 永久循环流程

```
启动Agent → 持续执行任务 → 同步分析执行优劣 → 生成优化方案 → 
执行时套用新优化逻辑 → 继续执行+持续复盘优化 → 无限循环
```

### 伪代码

```python
while True:
    全力执行当前全部任务
    实时采集执行数据
    分析不足、梳理优化方向
    更新任务执行规则
    新规则直接投入下一轮执行
    捕获异常，仅修正不终止全局流程
```

## 项目结构

```
evolutionary_agent/
├── main_perpetual_execution_engine.py    # 主引擎入口
├── launch_perpetual_agent_system.py      # 启动脚本
├── README_Perpetual_Agent_System.md      # 说明文档
│
├── core/                                 # 核心层
│   ├── __init__.py
│   ├── resident_guardian_daemon.py       # 常驻守护层
│   ├── dual_process_orchestrator.py      # 双流程编排器
│   └── knowledge_graph_integration_engine.py  # 知识图谱引擎
│
├── modules/                              # 功能模块层
│   ├── __init__.py
│   ├── task_execution_worker.py          # 任务执行层
│   ├── intelligence_optimization_analyzer.py  # 智能优化层
│   └── synchronization_deployment_pipeline.py # 同步落地层
│
├── storage/                              # 存储层
│   ├── __init__.py
│   └── persistent_state_manager.py       # 状态管理器
│
└── config/                               # 配置层
    ├── __init__.py
    └── runtime_configuration_loader.py   # 配置加载器
    └── runtime_config.json               # 运行时配置
```

## 使用方法

### 启动系统

```bash
python launch_perpetual_agent_system.py
```

### 优雅关闭

按 `Ctrl+C` 触发优雅关闭流程，系统会：
1. 停止接受新任务
2. 等待正在执行的任务完成
3. 保存所有状态
4. 关闭所有组件

## 配置说明

配置文件位置：`config/runtime_config.json`

```json
{
  "engine": {
    "max_concurrent_tasks": 10,        // 最大并发任务数
    "task_queue_size": 1000,           // 任务队列大小
    "cycle_report_interval": 10        // 周期报告间隔
  },
  "guardian": {
    "health_check_interval": 5.0,      // 健康检查间隔(秒)
    "heartbeat_timeout": 15.0,         // 心跳超时(秒)
    "max_recovery_attempts": 3         // 最大恢复尝试次数
  },
  "optimizer": {
    "analysis_interval": 10.0,         // 分析间隔(秒)
    "min_samples_for_analysis": 50     // 最小分析样本数
  },
  "deployer": {
    "validation_window_seconds": 30.0, // 验证窗口(秒)
    "deployment_cooldown_seconds": 5.0 // 部署冷却(秒)
  },
  "knowledge": {
    "auto_save_interval": 60.0,        // 自动保存间隔(秒)
    "max_nodes_in_memory": 100000      // 内存中最大节点数
  }
}
```

## 核心特点

### 1. 零空闲设计
- 执行与优化双线程并行运行
- 无等待、无休眠、无闲置状态
- 任务队列永不为空

### 2. 故障自愈
- 组件异常自动检测
- 自动重启故障组件
- 局部故障不影响全局运行

### 3. 持续进化
- 执行数据实时采集
- 自动分析性能瓶颈
- 生成并应用优化规则
- 效果验证与回滚机制

### 4. 知识积累
- 执行经验自动沉淀
- 错误模式自动归纳
- 优化历史可追溯
- 支持知识检索与复用

## 扩展开发

### 添加自定义任务处理器

```python
# 在 task_execution_worker.py 中注册
async def my_custom_handler(payload: Dict[str, Any]) -> Any:
    # 实现任务处理逻辑
    result = await process(payload)
    return result

# 注册处理器
self.register_handler("my_task_type", my_custom_handler)
```

### 添加自定义优化策略

```python
# 在 intelligence_optimization_analyzer.py 中扩展
# 在 _generate_optimization_rules 方法中添加新的优化类型
```

## 技术栈

- **Python 3.8+**：核心语言
- **asyncio**：异步并发
- **标准库**：无外部依赖
  - `collections`：数据结构
  - `dataclasses`：数据模型
  - `pickle`：对象序列化
  - `json`：配置存储
  - `pathlib`：路径管理

## 运行规则

1. **全程零空闲**：执行与优化同时进行
2. **任务不结束**：优化不停止
3. **优化迭代无上限**：持续打磨完善
4. **遇故障仅暂停局部**：整体执行+优化大循环不断

## License

MIT License
