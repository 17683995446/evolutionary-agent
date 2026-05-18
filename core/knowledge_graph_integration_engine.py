#!/usr/bin/env python3
"""
Knowledge Graph Integration Engine
知识图谱集成引擎 - 经验和项目信息不断集成，方便快速准确检索

核心职责：
1. 存储执行经验
2. 存储优化历史
3. 支持快速检索
4. 知识关联与推理
"""

import asyncio
import json
import time
import hashlib
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from pathlib import Path
import pickle


@dataclass
class KnowledgeNode:
    """知识图谱节点"""
    node_id: str
    node_type: str  # task, optimization, execution, error, pattern
    content: Dict[str, Any]
    created_at: float
    tags: Set[str] = field(default_factory=set)
    embedding: Optional[List[float]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "content": self.content,
            "created_at": self.created_at,
            "tags": list(self.tags),
            "embedding": self.embedding
        }


@dataclass
class KnowledgeEdge:
    """知识图谱边（关系）"""
    edge_id: str
    source_id: str
    target_id: str
    relation_type: str  # caused_by, improved_by, similar_to, depends_on
    weight: float
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "relation_type": self.relation_type,
            "weight": self.weight,
            "created_at": self.created_at,
            "metadata": self.metadata
        }


class KnowledgeGraphIntegrationEngine:
    """
    知识图谱集成引擎
    
    设计原则：
    - 本地优先：使用本地存储，无需外部依赖
    - 快速检索：基于标签和类型的索引
    - 增量更新：支持实时知识积累
    - 关联推理：发现知识间的隐含关系
    """
    
    def __init__(
        self,
        storage_path: str = "./storage/knowledge_graph",
        auto_save_interval: float = 60.0,
        max_nodes_in_memory: int = 100000
    ):
        self.storage_path = Path(storage_path)
        self.auto_save_interval = auto_save_interval
        self.max_nodes_in_memory = max_nodes_in_memory
        
        # 知识图谱数据
        self._nodes: Dict[str, KnowledgeNode] = {}
        self._edges: Dict[str, KnowledgeEdge] = {}
        
        # 索引
        self._type_index: Dict[str, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._relation_index: Dict[str, Set[str]] = defaultdict(set)
        
        # 统计
        self._stats = {
            "total_nodes": 0,
            "total_edges": 0,
            "node_types": defaultdict(int),
            "relation_types": defaultdict(int)
        }
        
        # 运行状态
        self._is_running = False
        self._save_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._modified_since_save = False
        
        # 内存管理
        self._node_access_times: Dict[str, float] = {}
        self._max_node_age_seconds = 120  # 2分钟（执行记录快速过期）
        
    async def initialize(self):
        """初始化知识图谱引擎"""
        print("[KNOWLEDGE] Initializing knowledge graph engine...")
        
        # 创建存储目录
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # 加载已有知识
        await self._load_from_storage()
        
        self._is_running = True
        
        # 启动自动保存
        self._save_task = asyncio.create_task(self._auto_save_loop())
        
        # 启动清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
    async def _load_from_storage(self):
        """从存储加载知识图谱"""
        nodes_file = self.storage_path / "nodes.pkl"
        edges_file = self.storage_path / "edges.pkl"
        stats_file = self.storage_path / "stats.json"
        
        try:
            if nodes_file.exists():
                with open(nodes_file, "rb") as f:
                    nodes_data = pickle.load(f)
                    for node_data in nodes_data:
                        node = KnowledgeNode(**node_data)
                        # 修复：反序列化后 tags 是 list，需转换为 set
                        if isinstance(node.tags, list):
                            node.tags = set(node.tags)
                        self._nodes[node.node_id] = node
                        self._type_index[node.node_type].add(node.node_id)
                        for tag in node.tags:
                            self._tag_index[tag].add(node.node_id)
                
                print(f"[KNOWLEDGE] Loaded {len(self._nodes)} nodes from storage")
            
            if edges_file.exists():
                with open(edges_file, "rb") as f:
                    edges_data = pickle.load(f)
                    for edge_data in edges_data:
                        edge = KnowledgeEdge(**edge_data)
                        self._edges[edge.edge_id] = edge
                        self._relation_index[edge.relation_type].add(edge.edge_id)
                
                print(f"[KNOWLEDGE] Loaded {len(self._edges)} edges from storage")
            
            if stats_file.exists():
                with open(stats_file, "r") as f:
                    self._stats = json.load(f)
                    
        except Exception as e:
            print(f"[KNOWLEDGE] Error loading from storage: {e}")
    
    async def _auto_save_loop(self):
        """自动保存循环"""
        while self._is_running:
            try:
                await asyncio.sleep(self.auto_save_interval)
                
                if self._modified_since_save:
                    await self._save_to_storage()
                    
            except Exception as e:
                print(f"[KNOWLEDGE] Auto-save error: {e}")
    
    async def _save_to_storage(self):
        """保存知识图谱到存储"""
        try:
            nodes_file = self.storage_path / "nodes.pkl"
            edges_file = self.storage_path / "edges.pkl"
            stats_file = self.storage_path / "stats.json"
            
            # 保存节点
            nodes_data = [node.to_dict() for node in self._nodes.values()]
            with open(nodes_file, "wb") as f:
                pickle.dump(nodes_data, f)
            
            # 保存边
            edges_data = [edge.to_dict() for edge in self._edges.values()]
            with open(edges_file, "wb") as f:
                pickle.dump(edges_data, f)
            
            # 保存统计
            with open(stats_file, "w") as f:
                json.dump(self._stats, f, indent=2)
            
            self._modified_since_save = False
            print(f"[KNOWLEDGE] Saved {len(self._nodes)} nodes, {len(self._edges)} edges to storage")
            
        except Exception as e:
            print(f"[KNOWLEDGE] Error saving to storage: {e}")
    
    def _generate_id(self, content: str) -> str:
        """生成唯一ID"""
        return hashlib.md5(f"{content}_{time.time()}".encode()).hexdigest()[:16]
    
    # ============== 知识添加接口 ==============
    
    async def add_execution_record(
        self,
        task_type: str,
        action: str,
        data: Dict[str, Any]
    ) -> str:
        """添加执行记录"""
        node_id = self._generate_id(f"{task_type}_{action}_{time.time()}")
        
        node = KnowledgeNode(
            node_id=node_id,
            node_type="execution",
            content={
                "task_type": task_type,
                "action": action,
                "data": data
            },
            created_at=time.time(),
            tags={task_type, action, "execution"}
        )
        
        self._nodes[node_id] = node
        self._type_index["execution"].add(node_id)
        for tag in node.tags:
            self._tag_index[tag].add(node_id)
        
        self._stats["total_nodes"] += 1
        self._stats["node_types"]["execution"] += 1
        self._modified_since_save = True
        
        return node_id
    
    async def add_optimization_record(self, analysis_data: Dict[str, Any]) -> str:
        """添加优化记录"""
        node_id = self._generate_id(f"optimization_{time.time()}")
        
        node = KnowledgeNode(
            node_id=node_id,
            node_type="optimization",
            content=analysis_data,
            created_at=time.time(),
            tags={"optimization", "analysis"}
        )
        
        # 添加瓶颈标签
        for bottleneck in analysis_data.get("bottlenecks", []):
            node.tags.add(f"bottleneck:{bottleneck['type']}")
            for component in bottleneck.get("affected_components", []):
                node.tags.add(f"component:{component}")
        
        self._nodes[node_id] = node
        self._type_index["optimization"].add(node_id)
        for tag in node.tags:
            self._tag_index[tag].add(node_id)
        
        self._stats["total_nodes"] += 1
        self._stats["node_types"]["optimization"] += 1
        self._modified_since_save = True
        
        # 关联相关执行记录
        await self._create_relations_to_recent_executions(node_id)
        
        return node_id
    
    async def add_error_pattern(
        self,
        error_type: str,
        error_message: str,
        context: Dict[str, Any]
    ) -> str:
        """添加错误模式"""
        node_id = self._generate_id(f"error_{error_type}_{time.time()}")
        
        node = KnowledgeNode(
            node_id=node_id,
            node_type="error",
            content={
                "error_type": error_type,
                "error_message": error_message,
                "context": context
            },
            created_at=time.time(),
            tags={"error", error_type, "failure"}
        )
        
        self._nodes[node_id] = node
        self._type_index["error"].add(node_id)
        for tag in node.tags:
            self._tag_index[tag].add(node_id)
        
        self._stats["total_nodes"] += 1
        self._stats["node_types"]["error"] += 1
        self._modified_since_save = True
        
        return node_id
    
    async def add_success_pattern(
        self,
        pattern_type: str,
        description: str,
        effectiveness: float,
        context: Dict[str, Any]
    ) -> str:
        """添加成功模式"""
        node_id = self._generate_id(f"pattern_{pattern_type}_{time.time()}")
        
        node = KnowledgeNode(
            node_id=node_id,
            node_type="pattern",
            content={
                "pattern_type": pattern_type,
                "description": description,
                "effectiveness": effectiveness,
                "context": context
            },
            created_at=time.time(),
            tags={"pattern", "success", pattern_type}
        )
        
        self._nodes[node_id] = node
        self._type_index["pattern"].add(node_id)
        for tag in node.tags:
            self._tag_index[tag].add(node_id)
        
        self._stats["total_nodes"] += 1
        self._stats["node_types"]["pattern"] += 1
        self._modified_since_save = True
        
        return node_id
    
    async def _create_relations_to_recent_executions(self, optimization_node_id: str):
        """创建优化记录到近期执行记录的关联"""
        recent_executions = self.query_nodes(
            node_type="execution",
            time_range=(time.time() - 300, time.time())  # 最近5分钟
        )
        
        for exec_node in recent_executions[:10]:  # 只关联最近10个
            edge_id = self._generate_id(f"rel_{optimization_node_id}_{exec_node.node_id}")
            
            edge = KnowledgeEdge(
                edge_id=edge_id,
                source_id=optimization_node_id,
                target_id=exec_node.node_id,
                relation_type="analyzes",
                weight=0.8,
                created_at=time.time()
            )
            
            self._edges[edge_id] = edge
            self._relation_index["analyzes"].add(edge_id)
            self._stats["total_edges"] += 1
            self._stats["relation_types"]["analyzes"] += 1
    
    # ============== 知识检索接口 ==============
    
    def query_nodes(
        self,
        node_type: Optional[str] = None,
        tags: Optional[Set[str]] = None,
        time_range: Optional[Tuple[float, float]] = None,
        limit: int = 100
    ) -> List[KnowledgeNode]:
        """
        查询知识节点
        
        Args:
            node_type: 节点类型过滤
            tags: 标签过滤
            time_range: 时间范围过滤 (start, end)
            limit: 返回数量限制
        """
        # 获取候选节点ID
        if node_type:
            candidate_ids = self._type_index.get(node_type, set())
        else:
            candidate_ids = set(self._nodes.keys())
        
        if tags:
            tag_filtered = set()
            for tag in tags:
                tag_filtered.update(self._tag_index.get(tag, set()))
            candidate_ids = candidate_ids & tag_filtered
        
        # 过滤并排序
        results = []
        for node_id in candidate_ids:
            node = self._nodes.get(node_id)
            if not node:
                continue
            
            if time_range:
                if not (time_range[0] <= node.created_at <= time_range[1]):
                    continue
            
            results.append(node)
        
        # 按时间倒序
        results.sort(key=lambda n: n.created_at, reverse=True)
        
        return results[:limit]
    
    def find_related_nodes(
        self,
        node_id: str,
        relation_types: Optional[List[str]] = None,
        depth: int = 1
    ) -> List[KnowledgeNode]:
        """查找相关节点"""
        if node_id not in self._nodes:
            return []
        
        related_ids = set()
        current_level = {node_id}
        
        for _ in range(depth):
            next_level = set()
            for edge in self._edges.values():
                if relation_types and edge.relation_type not in relation_types:
                    continue
                
                if edge.source_id in current_level:
                    next_level.add(edge.target_id)
                if edge.target_id in current_level:
                    next_level.add(edge.source_id)
            
            related_ids.update(next_level)
            current_level = next_level
        
        related_ids.discard(node_id)  # 排除自身
        
        return [self._nodes[nid] for nid in related_ids if nid in self._nodes]
    
    def search_by_content(
        self,
        keywords: List[str],
        node_types: Optional[List[str]] = None,
        limit: int = 50
    ) -> List[KnowledgeNode]:
        """基于内容关键词搜索"""
        results = []
        
        search_nodes = self._nodes.values()
        if node_types:
            search_nodes = [
                n for n in search_nodes
                if n.node_type in node_types
            ]
        
        for node in search_nodes:
            content_str = json.dumps(node.content).lower()
            score = sum(1 for kw in keywords if kw.lower() in content_str)
            if score > 0:
                results.append((score, node))
        
        # 按相关度排序
        results.sort(key=lambda x: x[0], reverse=True)
        
        return [node for _, node in results[:limit]]
    
    def get_similar_errors(
        self,
        error_message: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """获取相似的错误记录"""
        error_nodes = self.query_nodes(node_type="error", limit=100)
        
        similarities = []
        error_lower = error_message.lower()
        
        for node in error_nodes:
            node_error = node.content.get("error_message", "").lower()
            # 简单的相似度计算
            common_words = set(error_lower.split()) & set(node_error.split())
            similarity = len(common_words) / max(len(error_lower.split()), 1)
            
            if similarity > 0.3:
                similarities.append((similarity, node))
        
        similarities.sort(key=lambda x: x[0], reverse=True)
        
        return [
            {
                "similarity": sim,
                "error_type": node.content.get("error_type"),
                "error_message": node.content.get("error_message"),
                "context": node.content.get("context"),
                "created_at": node.created_at
            }
            for sim, node in similarities[:limit]
        ]
    
    def get_optimization_suggestions(
        self,
        task_type: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """获取针对特定任务类型的优化建议"""
        # 查询相关的优化记录
        optimization_nodes = self.query_nodes(
            node_type="optimization",
            tags={f"component:{task_type}"},
            limit=20
        )
        
        suggestions = []
        for node in optimization_nodes:
            content = node.content
            for bottleneck in content.get("bottlenecks", []):
                if task_type in bottleneck.get("affected_components", []):
                    suggestions.append({
                        "bottleneck_type": bottleneck.get("type"),
                        "description": bottleneck.get("description"),
                        "suggested_actions": bottleneck.get("suggested_actions"),
                        "severity": bottleneck.get("severity"),
                        "timestamp": node.created_at
                    })
        
        # 去重并排序
        seen = set()
        unique_suggestions = []
        for s in suggestions:
            key = s["bottleneck_type"]
            if key not in seen:
                seen.add(key)
                unique_suggestions.append(s)
        
        return unique_suggestions[:limit]
    
    # ============== 统计接口 ==============
    
    async def get_entry_count(self) -> int:
        """获取知识条目总数"""
        return self._stats["total_nodes"]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取知识图谱统计信息"""
        return {
            "total_nodes": self._stats["total_nodes"],
            "total_edges": self._stats["total_edges"],
            "node_types": dict(self._stats["node_types"]),
            "relation_types": dict(self._stats["relation_types"]),
            "storage_path": str(self.storage_path)
        }
    
    def get_heartbeat(self) -> Dict[str, Any]:
        """获取心跳数据"""
        return {
            "healthy": True,
            "metrics": {
                "total_nodes": self._stats["total_nodes"],
                "total_edges": self._stats["total_edges"],
                "modified_since_save": self._modified_since_save
            }
        }
    
    async def _cleanup_loop(self):
        """定期清理旧节点，防止内存无限增长"""
        # 首次快速触发（5秒后），之后每15秒
        await asyncio.sleep(5)
        await self._do_cleanup()
        
        while self._is_running:
            try:
                await asyncio.sleep(15)  # 每15秒清理一次
                await self._do_cleanup()
            except Exception as e:
                print(f"[KNOWLEDGE] Cleanup error: {e}")
    
    async def _do_cleanup(self):
        """执行一次清理"""
        if len(self._nodes) > self.max_nodes_in_memory * 0.2:
            current_time = time.time()
            nodes_to_remove = []
            
            # 找出最旧的执行记录节点
            for node_id, node in list(self._nodes.items()):
                if node.node_type == "execution":
                    age = current_time - node.created_at
                    if age > self._max_node_age_seconds:
                        nodes_to_remove.append(node_id)
            
            # 清理节点
            for node_id in nodes_to_remove[:10000]:
                node = self._nodes.pop(node_id, None)
                if node:
                    self._type_index[node.node_type].discard(node_id)
                    for tag in node.tags:
                        self._tag_index[tag].discard(node_id)
                    self._stats["total_nodes"] -= 1
                    self._stats["node_types"][node.node_type] -= 1
            
            if nodes_to_remove:
                print(f"[KNOWLEDGE] Cleaned up {len(nodes_to_remove)} old nodes, remaining: {len(self._nodes)}")
                self._modified_since_save = True
    
    async def shutdown(self):
        """关闭知识图谱引擎"""
        print("[KNOWLEDGE] Shutting down knowledge graph engine...")
        self._is_running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass
        
        # 最终保存
        if self._modified_since_save:
            await self._save_to_storage()
        
        print("[KNOWLEDGE] Knowledge graph engine shutdown complete")
