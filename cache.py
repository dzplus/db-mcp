"""
元数据缓存模块

异步元数据缓存管理，支持后台自动更新和 DiskStore 持久化
使用 LRU 算法管理数据库缓存队列，只刷新最近访问的数据库
"""

import asyncio
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Optional

import aiomysql

from config import CacheConfig
from database import AsyncDatabasePool

logger = logging.getLogger("mysql_mcp_server")


# 缓存存储的 key 常量
CACHE_KEY_DATABASES = "databases"
CACHE_KEY_TABLES = "tables"
CACHE_KEY_COLUMNS = "columns"
CACHE_KEY_RELATIONS = "relations"
CACHE_KEY_LAST_UPDATE = "last_update"
CACHE_KEY_HISTORY = "refresh_history"
CACHE_KEY_LRU_QUEUE = "lru_queue"

# 系统数据库，默认跳过
SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


class LRUDatabaseQueue:
    """
    LRU 数据库队列

    使用 OrderedDict 实现 LRU 算法，记录最近访问的数据库
    只有在队列中的数据库才会被定时刷新
    """

    def __init__(self, max_size: int = 10, on_evict: Optional[callable] = None):
        self.max_size = max_size
        # OrderedDict 保持插入顺序，最近访问的在末尾
        self._queue: OrderedDict[str, float] = OrderedDict()
        # 当数据库被移除时的回调函数
        self._on_evict = on_evict

    def access(self, database: str) -> bool:
        """
        访问数据库，将其移到队列末尾（最近使用）

        Args:
            database: 数据库名称

        Returns:
            是否是新加入的数据库
        """
        if database in SYSTEM_DATABASES:
            return False

        is_new = database not in self._queue

        # 如果已存在，先移除再添加（移到末尾）
        if database in self._queue:
            self._queue.move_to_end(database)
        else:
            self._queue[database] = time.time()

        # 如果超出容量，移除最久未使用的
        while len(self._queue) > self.max_size:
            removed_db, _ = self._queue.popitem(last=False)
            logger.info(f"LRU 队列已满，移除最久未使用的数据库: {removed_db}")
            # 触发回调清理缓存数据
            if self._on_evict:
                self._on_evict(removed_db)

        # 更新访问时间
        self._queue[database] = time.time()

        if is_new:
            logger.info(f"数据库 '{database}' 加入 LRU 缓存队列")

        return is_new

    def remove(self, database: str) -> bool:
        """从队列中移除数据库"""
        if database in self._queue:
            del self._queue[database]
            logger.info(f"数据库 '{database}' 已从 LRU 队列移除")
            return True
        return False

    def contains(self, database: str) -> bool:
        """检查数据库是否在队列中"""
        return database in self._queue

    def get_databases(self) -> list[str]:
        """获取队列中的所有数据库（按 LRU 顺序，最近使用的在后）"""
        return list(self._queue.keys())

    def get_databases_with_time(self) -> list[dict[str, Any]]:
        """获取队列中的所有数据库及其访问时间"""
        return [
            {
                "database": db,
                "last_access": access_time,
                "last_access_formatted": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(access_time)
                ),
            }
            for db, access_time in self._queue.items()
        ]

    def clear(self):
        """清空队列"""
        self._queue.clear()

    def __len__(self) -> int:
        return len(self._queue)

    def to_dict(self) -> dict[str, float]:
        """序列化为字典"""
        return dict(self._queue)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, float],
        max_size: int = 10,
        on_evict: Optional[callable] = None
    ) -> "LRUDatabaseQueue":
        """从字典反序列化"""
        queue = cls(max_size=max_size, on_evict=on_evict)
        # 按访问时间排序后添加
        sorted_items = sorted(data.items(), key=lambda x: x[1])
        for db, access_time in sorted_items:
            if db not in SYSTEM_DATABASES:
                queue._queue[db] = access_time
        # 裁剪到最大容量
        while len(queue._queue) > max_size:
            queue._queue.popitem(last=False)
        return queue


class MetadataCache:
    """元数据缓存管理器，支持 DiskStore 持久化和 LRU 数据库队列"""

    def __init__(
        self,
        db_pool: AsyncDatabasePool,
        cache_config: Optional[CacheConfig] = None,
        update_interval_minutes: int = 5,
    ):
        self.db_pool = db_pool
        self.cache_config = cache_config or CacheConfig()
        self.update_interval = (
            self.cache_config.update_interval * 60
            if cache_config
            else update_interval_minutes * 60
        )

        # 内存缓存数据
        self._databases: list[str] = []
        # 按数据库分组的表信息: {db_name: {table_name: table_info}}
        self._tables: dict[str, dict[str, Any]] = {}
        # 按数据库分组的列信息: {db_name: {table_name: [column_info]}}
        self._columns: dict[str, dict[str, list[dict[str, Any]]]] = {}
        # 按数据库分组的外键关系: {db_name: {table_name: [relation_info]}}
        self._relations: dict[str, dict[str, list[dict[str, Any]]]] = {}
        self._last_update: float = 0

        # LRU 数据库队列（带缓存清理回调）
        self._lru_queue = LRUDatabaseQueue(
            max_size=self.cache_config.lru_max_databases,
            on_evict=self._evict_database_cache
        )

        # 刷新历史记录
        self._refresh_history: list[dict[str, Any]] = []

        # 并发控制
        self._lock = asyncio.Lock()
        self._updating = False
        self._update_task: Optional[asyncio.Task] = None

        # DiskStore 实例（延迟初始化）
        self._disk_store: Optional[Any] = None
        self._persistence_enabled = self.cache_config.enable_persistence

    def _evict_database_cache(self, database: str):
        """当数据库被移出 LRU 队列时清理其缓存数据"""
        self._tables.pop(database, None)
        self._columns.pop(database, None)
        self._relations.pop(database, None)
        logger.info(f"已清理数据库 '{database}' 的缓存数据")

    async def _init_disk_store(self) -> bool:
        """初始化 DiskStore"""
        if not self._persistence_enabled:
            return False

        if self._disk_store is not None:
            return True

        try:
            from key_value.aio.stores.disk import DiskStore

            # 确保目录存在
            storage_dir = self.cache_config.storage_dir
            os.makedirs(storage_dir, exist_ok=True)

            self._disk_store = DiskStore(directory=storage_dir)
            logger.info(f"DiskStore 初始化成功，存储目录: {storage_dir}")
            return True
        except ImportError:
            logger.warning(
                "py-key-value-aio 未安装，缓存持久化不可用。"
                "请运行: pip install py-key-value-aio"
            )
            self._persistence_enabled = False
            return False
        except Exception as e:
            logger.error(f"初始化 DiskStore 失败: {e}")
            self._persistence_enabled = False
            return False

    async def _load_from_disk(self) -> bool:
        """从磁盘加载缓存数据"""
        if not await self._init_disk_store():
            return False

        try:
            # 加载各项缓存数据（DiskStore 返回的是 dict，数据在 "data" 键中）
            databases_data = await self._disk_store.get(CACHE_KEY_DATABASES)
            if databases_data and "data" in databases_data:
                self._databases = databases_data["data"]

            tables_data = await self._disk_store.get(CACHE_KEY_TABLES)
            if tables_data and "data" in tables_data:
                self._tables = tables_data["data"]

            columns_data = await self._disk_store.get(CACHE_KEY_COLUMNS)
            if columns_data and "data" in columns_data:
                self._columns = columns_data["data"]

            relations_data = await self._disk_store.get(CACHE_KEY_RELATIONS)
            if relations_data and "data" in relations_data:
                self._relations = relations_data["data"]

            last_update_data = await self._disk_store.get(CACHE_KEY_LAST_UPDATE)
            if last_update_data and "data" in last_update_data:
                self._last_update = float(last_update_data["data"])

            history_data = await self._disk_store.get(CACHE_KEY_HISTORY)
            if history_data and "data" in history_data:
                self._refresh_history = history_data["data"]

            # 加载 LRU 队列
            lru_data = await self._disk_store.get(CACHE_KEY_LRU_QUEUE)
            if lru_data and "data" in lru_data:
                self._lru_queue = LRUDatabaseQueue.from_dict(
                    lru_data["data"],
                    max_size=self.cache_config.lru_max_databases,
                    on_evict=self._evict_database_cache
                )

            total_tables = sum(len(tables) for tables in self._tables.values())
            logger.info(
                f"从磁盘加载缓存成功: {len(self._databases)} 个数据库, "
                f"{total_tables} 个表, {len(self._lru_queue)} 个活跃数据库, "
                f"{len(self._refresh_history)} 条历史记录"
            )
            return True
        except Exception as e:
            logger.error(f"从磁盘加载缓存失败: {e}")
            return False

    async def _save_to_disk(self) -> bool:
        """保存缓存数据到磁盘"""
        if not await self._init_disk_store():
            return False

        try:
            # DiskStore.put 需要 Mapping[str, Any] 类型，所以用 {"data": ...} 包装
            await self._disk_store.put(
                CACHE_KEY_DATABASES, {"data": self._databases}
            )
            await self._disk_store.put(
                CACHE_KEY_TABLES, {"data": self._tables}
            )
            await self._disk_store.put(
                CACHE_KEY_COLUMNS, {"data": self._columns}
            )
            await self._disk_store.put(
                CACHE_KEY_RELATIONS, {"data": self._relations}
            )
            await self._disk_store.put(
                CACHE_KEY_LAST_UPDATE, {"data": self._last_update}
            )
            await self._disk_store.put(
                CACHE_KEY_HISTORY, {"data": self._refresh_history}
            )
            # 保存 LRU 队列
            await self._disk_store.put(
                CACHE_KEY_LRU_QUEUE, {"data": self._lru_queue.to_dict()}
            )

            logger.debug("缓存数据已保存到磁盘")
            return True
        except Exception as e:
            logger.error(f"保存缓存到磁盘失败: {e}")
            return False

    def _add_history_record(
        self,
        success: bool,
        duration: float,
        database_count: int,
        table_count: int,
        error: Optional[str] = None,
    ):
        """添加刷新历史记录"""
        record = {
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "success": success,
            "duration_seconds": round(duration, 3),
            "database_count": database_count,
            "table_count": table_count,
        }

        if error:
            record["error"] = error

        self._refresh_history.append(record)

        # 限制历史记录数量
        max_records = self.cache_config.max_history_records
        if len(self._refresh_history) > max_records:
            self._refresh_history = self._refresh_history[-max_records:]

    # ==================== LRU 队列操作 ====================

    def access_database(self, database: str) -> bool:
        """
        标记数据库被访问，自动加入 LRU 队列

        Args:
            database: 数据库名称

        Returns:
            是否是新加入的数据库
        """
        if not self.cache_config.lru_auto_add:
            return False
        return self._lru_queue.access(database)

    def add_database_to_queue(self, database: str) -> bool:
        """手动将数据库加入 LRU 队列"""
        return self._lru_queue.access(database)

    def remove_database_from_queue(self, database: str) -> bool:
        """从 LRU 队列中移除数据库"""
        removed = self._lru_queue.remove(database)
        if removed:
            # 同时清除该数据库的缓存数据
            self._tables.pop(database, None)
            self._columns.pop(database, None)
            self._relations.pop(database, None)
        return removed

    def get_lru_queue(self) -> list[str]:
        """获取 LRU 队列中的数据库列表"""
        return self._lru_queue.get_databases()

    def get_lru_queue_status(self) -> dict[str, Any]:
        """获取 LRU 队列状态"""
        return {
            "databases": self._lru_queue.get_databases_with_time(),
            "count": len(self._lru_queue),
            "max_size": self._lru_queue.max_size,
            "auto_add_enabled": self.cache_config.lru_auto_add,
        }

    # ==================== 属性访问器 ====================

    @property
    def databases(self) -> list[str]:
        return self._databases.copy()

    @property
    def tables(self) -> dict[str, dict[str, Any]]:
        """获取所有表（按数据库分组）"""
        return self._tables.copy()

    def get_tables_for_database(self, database: str) -> Optional[dict[str, Any]]:
        """
        获取指定数据库的表

        Returns:
            如果数据库在缓存中，返回表字典；否则返回 None
        """
        # 自动标记访问
        self.access_database(database)
        if database in self._tables:
            return self._tables[database].copy()
        return None

    @property
    def columns(self) -> dict[str, dict[str, list[dict[str, Any]]]]:
        """获取所有列（按数据库分组）"""
        return self._columns.copy()

    def get_columns_for_database(self, database: str) -> Optional[dict[str, list[dict[str, Any]]]]:
        """
        获取指定数据库的列

        Returns:
            如果数据库在缓存中，返回列字典；否则返回 None
        """
        self.access_database(database)
        if database in self._columns:
            return self._columns[database].copy()
        return None

    @property
    def relations(self) -> dict[str, dict[str, list[dict[str, Any]]]]:
        """获取所有外键关系（按数据库分组）"""
        return self._relations.copy()

    def get_relations_for_database(self, database: str) -> Optional[dict[str, list[dict[str, Any]]]]:
        """
        获取指定数据库的外键关系

        Returns:
            如果数据库在缓存中，返回外键关系字典；否则返回 None
        """
        self.access_database(database)
        if database in self._relations:
            return self._relations[database].copy()
        return None

    @property
    def last_update(self) -> float:
        return self._last_update

    @property
    def refresh_history(self) -> list[dict[str, Any]]:
        """获取刷新历史记录"""
        return self._refresh_history.copy()

    @property
    def is_stale(self) -> bool:
        """缓存是否过期"""
        if self._last_update == 0:
            return True
        return (time.time() - self._last_update) >= self.update_interval

    @property
    def total_table_count(self) -> int:
        """获取所有数据库的表总数"""
        return sum(len(tables) for tables in self._tables.values())

    def get_status(self) -> dict[str, Any]:
        """获取缓存状态"""
        return {
            "last_update": self._last_update,
            "last_update_formatted": (
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_update))
                if self._last_update > 0
                else "从未更新"
            ),
            "database_count": len(self._databases),
            "table_count": self.total_table_count,
            "is_stale": self.is_stale,
            "next_update": (
                self._last_update + self.update_interval
                if self._last_update > 0
                else 0
            ),
            "updating": self._updating,
            "persistence_enabled": self._persistence_enabled,
            "storage_dir": (
                self.cache_config.storage_dir if self._persistence_enabled else None
            ),
            "history_count": len(self._refresh_history),
            "lru_queue": self.get_lru_queue_status(),
        }

    def get_refresh_history(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        获取最近的刷新历史记录

        Args:
            limit: 返回的记录数量

        Returns:
            最近的刷新历史记录列表（倒序）
        """
        return list(reversed(self._refresh_history[-limit:]))

    async def initialize(self) -> bool:
        """
        初始化缓存，从磁盘加载已有数据

        Returns:
            是否成功加载
        """
        # 如果配置了默认数据库，自动加入 LRU 队列
        configured_db = self.db_pool.config.db.database
        if configured_db and configured_db not in SYSTEM_DATABASES:
            self._lru_queue.access(configured_db)
            logger.info(f"配置的默认数据库 '{configured_db}' 已加入 LRU 缓存队列")

        if self._persistence_enabled:
            loaded = await self._load_from_disk()
            if loaded and self._last_update > 0:
                logger.info(
                    f"从磁盘恢复缓存数据，上次更新: "
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_update))}"
                )
                return True
        return False

    async def update(self, force: bool = False) -> bool:
        """
        更新元数据缓存（只刷新 LRU 队列中的数据库）

        Args:
            force: 是否强制更新（忽略更新间隔）

        Returns:
            是否更新成功
        """
        # 检查是否需要更新
        if not force and not self.is_stale:
            logger.debug("元数据缓存仍然有效，跳过更新")
            return True

        # 使用非阻塞锁
        if not self._lock.locked():
            async with self._lock:
                return await self._do_update()
        else:
            logger.debug("另一个线程正在更新缓存，跳过本次更新")
            return False

    async def refresh_database(self, database: str) -> bool:
        """
        刷新指定数据库的元数据（会自动加入 LRU 队列）

        Args:
            database: 数据库名称

        Returns:
            是否刷新成功
        """
        if database in SYSTEM_DATABASES:
            logger.warning(f"不能刷新系统数据库: {database}")
            return False

        # 加入 LRU 队列
        self._lru_queue.access(database)

        async with self._lock:
            return await self._refresh_single_database(database)

    async def _refresh_single_database(self, database: str) -> bool:
        """刷新单个数据库的元数据"""
        start_time = time.time()
        logger.info(f"开始刷新数据库 '{database}' 的元数据")

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # 切换到目标数据库
                    await cur.execute(f"USE `{database}`")

                    # 获取表列表
                    tables = await self._get_table_list(cur, database)

                    db_tables = {}
                    db_columns = {}

                    for table in tables:
                        try:
                            table_info, columns = await self._get_table_info(
                                cur, table, database
                            )
                            if table_info:
                                db_tables[table] = table_info
                                db_columns[table] = columns
                        except Exception as e:
                            logger.error(f"更新表 {database}.{table} 信息时出错: {e}")

                    # 获取外键关系
                    db_relations = await self._get_relations(cur, database)

                    # 更新缓存
                    self._tables[database] = db_tables
                    self._columns[database] = db_columns
                    self._relations[database] = db_relations

            elapsed = time.time() - start_time
            logger.info(
                f"数据库 '{database}' 刷新完成，耗时 {elapsed:.2f} 秒，"
                f"共 {len(db_tables)} 个表"
            )

            # 保存到磁盘
            await self._save_to_disk()

            return True

        except Exception as e:
            logger.error(f"刷新数据库 '{database}' 时出错: {e}")
            return False

    async def _do_update(self) -> bool:
        """执行实际的缓存更新（只刷新 LRU 队列中的数据库）"""
        if self._updating:
            return False

        self._updating = True
        start_time = time.time()
        logger.info("开始更新元数据缓存")

        error_msg = None
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    # 1. 更新数据库列表（始终更新）
                    await self._update_databases(cur)

                    # 2. 确定要刷新的数据库列表（只刷新 LRU 队列中的）
                    databases_to_refresh = self._lru_queue.get_databases()

                    if not databases_to_refresh:
                        # 如果 LRU 队列为空，检查是否配置了默认数据库
                        configured_db = self.db_pool.config.db.database
                        if configured_db and configured_db not in SYSTEM_DATABASES:
                            databases_to_refresh = [configured_db]
                            self._lru_queue.access(configured_db)
                            logger.info(f"LRU 队列为空，使用配置的数据库: {configured_db}")
                        else:
                            logger.info("LRU 队列为空，跳过表元数据刷新")

                    if databases_to_refresh:
                        logger.info(
                            f"刷新 LRU 队列中的 {len(databases_to_refresh)} 个数据库: "
                            f"{', '.join(databases_to_refresh)}"
                        )

                    # 3. 遍历数据库获取表信息
                    new_tables: dict[str, dict[str, Any]] = {}
                    new_columns: dict[str, dict[str, list[dict[str, Any]]]] = {}
                    new_relations: dict[str, dict[str, list[dict[str, Any]]]] = {}

                    for db_name in databases_to_refresh:
                        try:
                            # 切换到目标数据库
                            await cur.execute(f"USE `{db_name}`")

                            # 获取表列表
                            tables = await self._get_table_list(cur, db_name)

                            db_tables = {}
                            db_columns = {}

                            for table in tables:
                                try:
                                    table_info, columns = await self._get_table_info(
                                        cur, table, db_name
                                    )
                                    if table_info:
                                        db_tables[table] = table_info
                                        db_columns[table] = columns
                                except Exception as e:
                                    logger.error(
                                        f"更新表 {db_name}.{table} 信息时出错: {e}"
                                    )

                            # 获取外键关系
                            db_relations = await self._get_relations(cur, db_name)

                            new_tables[db_name] = db_tables
                            new_columns[db_name] = db_columns
                            new_relations[db_name] = db_relations

                            logger.debug(f"数据库 {db_name}: {len(db_tables)} 个表")

                        except Exception as e:
                            logger.error(f"刷新数据库 {db_name} 时出错: {e}")
                            # 继续处理其他数据库

                    # 原子更新（只更新刷新的数据库，保留其他缓存）
                    for db_name in databases_to_refresh:
                        if db_name in new_tables:
                            self._tables[db_name] = new_tables[db_name]
                            self._columns[db_name] = new_columns.get(db_name, {})
                            self._relations[db_name] = new_relations.get(db_name, {})

                    self._last_update = time.time()

            elapsed = time.time() - start_time
            total_tables = sum(len(tables) for tables in new_tables.values())

            # 添加历史记录
            self._add_history_record(
                success=True,
                duration=elapsed,
                database_count=len(databases_to_refresh),
                table_count=total_tables,
            )

            # 保存到磁盘
            await self._save_to_disk()

            logger.info(
                f"元数据缓存更新完成，耗时 {elapsed:.2f} 秒，"
                f"共 {len(self._databases)} 个数据库，"
                f"刷新 {len(databases_to_refresh)} 个活跃数据库，"
                f"{total_tables} 个表"
            )
            return True

        except Exception as e:
            error_msg = str(e)
            elapsed = time.time() - start_time

            # 添加失败记录
            self._add_history_record(
                success=False,
                duration=elapsed,
                database_count=len(self._lru_queue),
                table_count=self.total_table_count,
                error=error_msg,
            )

            # 即使更新失败也保存历史记录
            await self._save_to_disk()

            logger.error(f"更新元数据缓存时出错: {e}")
            return False
        finally:
            self._updating = False

    async def _update_databases(self, cur: aiomysql.DictCursor):
        """更新数据库列表"""
        await cur.execute("SHOW DATABASES")
        rows = await cur.fetchall()
        self._databases = [row["Database"] for row in rows]

    async def _get_table_list(
        self, cur: aiomysql.DictCursor, database: str
    ) -> list[str]:
        """获取指定数据库的表列表"""
        await cur.execute(
            """
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
            """,
            (database,)
        )
        rows = await cur.fetchall()
        return [row["TABLE_NAME"] for row in rows]

    async def _get_table_info(
        self, cur: aiomysql.DictCursor, table: str, database: str
    ) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
        """获取表信息和列信息"""
        # 表基本信息
        await cur.execute(
            """
            SELECT
                TABLE_NAME as name,
                ENGINE as engine,
                TABLE_ROWS as `rows`,
                TABLE_COMMENT as comment,
                CREATE_TIME as create_time
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (database, table)
        )
        table_info = await cur.fetchone()
        if not table_info:
            return None, []

        # 处理时间字段
        result = {
            "name": table_info["name"],
            "database": database,
            "engine": table_info.get("engine"),
            "rows": table_info.get("rows"),
            "comment": table_info.get("comment"),
            "create_time": (
                table_info["create_time"].isoformat()
                if table_info.get("create_time")
                else None
            ),
        }

        # 列信息
        await cur.execute(
            """
            SELECT
                COLUMN_NAME as name,
                COLUMN_TYPE as type,
                IS_NULLABLE as `null`,
                COLUMN_KEY as `key`,
                COLUMN_DEFAULT as `default`,
                EXTRA as extra,
                COLUMN_COMMENT as comment
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (database, table)
        )
        columns = await cur.fetchall()

        # 索引信息
        await cur.execute(
            """
            SELECT
                INDEX_NAME as index_name,
                NON_UNIQUE as non_unique,
                COLUMN_NAME as column_name,
                SEQ_IN_INDEX as seq_in_index
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
            (database, table)
        )
        indexes_raw = await cur.fetchall()

        index_dict = {}
        for idx in indexes_raw:
            index_name = idx["index_name"]
            if index_name not in index_dict:
                index_dict[index_name] = {
                    "name": index_name,
                    "unique": not idx["non_unique"],
                    "columns": [],
                }
            index_dict[index_name]["columns"].append({
                "name": idx["column_name"],
                "order": idx["seq_in_index"],
            })

        result["indexes"] = list(index_dict.values())
        return result, list(columns)

    async def _get_relations(
        self, cur: aiomysql.DictCursor, database: str
    ) -> dict[str, list[dict[str, Any]]]:
        """获取指定数据库的外键关系"""
        try:
            await cur.execute(
                """
                SELECT
                    TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME,
                    REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE
                    TABLE_SCHEMA = %s
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """,
                (database,),
            )

            rows = await cur.fetchall()
            relations: dict[str, list[dict[str, Any]]] = {}
            for rel in rows:
                table_name = rel["TABLE_NAME"]
                if table_name not in relations:
                    relations[table_name] = []

                relations[table_name].append({
                    "constraint_name": rel["CONSTRAINT_NAME"],
                    "column": rel["COLUMN_NAME"],
                    "referenced_table": rel["REFERENCED_TABLE_NAME"],
                    "referenced_column": rel["REFERENCED_COLUMN_NAME"],
                })

            return relations
        except Exception as e:
            logger.error(f"获取数据库 {database} 外键关系时出错: {e}")
            return {}

    async def start_background_updater(self):
        """启动后台更新任务"""
        if self._update_task is not None and not self._update_task.done():
            logger.warning("后台更新任务已在运行")
            return

        self._update_task = asyncio.create_task(self._background_update_loop())
        logger.info("后台缓存更新任务已启动")

    async def stop_background_updater(self):
        """停止后台更新任务"""
        if self._update_task is not None:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
            self._update_task = None
            logger.info("后台缓存更新任务已停止")

    async def _background_update_loop(self):
        """后台更新循环，固定间隔执行刷新"""
        while True:
            try:
                # 固定等待 update_interval 秒后执行刷新
                await asyncio.sleep(self.update_interval)
                logger.info("定时任务开始刷新元数据缓存")
                await self.update(force=True)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"后台缓存更新出错: {e}")
                # 出错后等待一段时间再重试
                await asyncio.sleep(60)

    async def clear_cache(self) -> bool:
        """
        清除所有缓存数据（内存和磁盘）

        Returns:
            是否清除成功
        """
        self._databases = []
        self._tables = {}
        self._columns = {}
        self._relations = {}
        self._last_update = 0
        self._lru_queue.clear()
        # 保留历史记录

        if self._persistence_enabled and self._disk_store:
            try:
                await self._disk_store.delete(CACHE_KEY_DATABASES)
                await self._disk_store.delete(CACHE_KEY_TABLES)
                await self._disk_store.delete(CACHE_KEY_COLUMNS)
                await self._disk_store.delete(CACHE_KEY_RELATIONS)
                await self._disk_store.delete(CACHE_KEY_LAST_UPDATE)
                await self._disk_store.delete(CACHE_KEY_LRU_QUEUE)
                logger.info("磁盘缓存已清除")
            except Exception as e:
                logger.error(f"清除磁盘缓存失败: {e}")
                return False

        logger.info("缓存已清除")
        return True
