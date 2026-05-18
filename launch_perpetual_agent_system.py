#!/usr/bin/env python3
"""
Launch Perpetual Agent System
启动脚本 - 一键启动持续进化Agent系统

使用方法:
    python launch_perpetual_agent_system.py
    
环境要求:
    Python 3.8+
    无需外部依赖（使用标准库）
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_perpetual_execution_engine import main

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════╗
║           Perpetual Evolution Agent System                        ║
║           持续进化Agent系统 - 启动器                              ║
╠══════════════════════════════════════════════════════════════════╣
║  核心特性:                                                        ║
║  • 零空闲设计 - 执行与优化双线程并行                              ║
║  • 自驱进化 - 无需人工干预                                        ║
║  • 故障自愈 - 局部异常自动修复                                    ║
║  • 知识积累 - 经验持续沉淀到知识图谱                              ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        import asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[System] Shutdown requested by user")
    except Exception as e:
        print(f"\n\n[System] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
