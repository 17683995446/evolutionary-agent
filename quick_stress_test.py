#!/usr/bin/env python3
"""快速压力测试 - 快速发现问题"""

import asyncio
import time
import sys
import os
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import modules.task_execution_worker as task_module
import core.knowledge_graph_integration_engine as knowledge_module
import storage.persistent_state_manager as state_module

TaskExecutionWorker = task_module.TaskExecutionWorker
TaskPriority = task_module.TaskPriority
KnowledgeGraphIntegrationEngine = knowledge_module.KnowledgeGraphIntegrationEngine
PersistentStateManager = state_module.PersistentStateManager


async def quick_test():
    print("[QUICK_TEST] Starting quick stress test...")
    
    state_manager = PersistentStateManager(storage_path='./quick_test_storage/state')
    await state_manager.initialize()
    
    knowledge = KnowledgeGraphIntegrationEngine(storage_path='./quick_test_storage/knowledge')
    await knowledge.initialize()
    
    executor = TaskExecutionWorker(
        knowledge_engine=knowledge,
        state_manager=state_manager,
        max_concurrent_tasks=20,
        task_queue_size=1000
    )
    await executor.initialize()
    await executor.start_execution_loop()
    
    print("[QUICK_TEST] Submitting 100 compute tasks with large data...")
    tracemalloc.start()
    
    start = time.time()
    for i in range(100):
        await executor.submit_task(
            name=f"test_task_{i}",
            payload={
                "operation": "sum",
                "data": list(range(5000))  # 5000个元素
            },
            task_type="compute"
        )
    
    submit_time = time.time() - start
    print(f"[QUICK_TEST] Submitted 100 tasks in {submit_time:.2f}s")
    
    # 等待完成
    await asyncio.sleep(5)
    
    heartbeat = executor.get_heartbeat()
    print(f"[QUICK_TEST] Completed: {heartbeat['metrics']['total_completed']}")
    print(f"[QUICK_TEST] Failed: {heartbeat['metrics']['total_failed']}")
    
    current, peak = tracemalloc.get_traced_memory()
    print(f"[QUICK_TEST] Memory: {current/1024/1024:.2f}MB, Peak: {peak/1024/1024:.2f}MB")
    
    await executor.shutdown()
    await knowledge.shutdown()
    
    print("[QUICK_TEST] Test complete!")

if __name__ == "__main__":
    asyncio.run(quick_test())
