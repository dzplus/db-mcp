"""
MySQL Database MCP Server

主服务器模块，提供 MCP 工具和多种传输模式支持
"""

import argparse
import asyncio
import logging
import sys
import time
from typing import Any, Optional

from fastmcp import FastMCP, Context
from fastmcp.server.dependencies import get_context

from cache import MetadataCache
from config import AppConfig, load_config
from database import AsyncDatabasePool
from lineage import (
    LineageStore,
    analyze_sql_lineage,
    should_analyze_sql,
)
from middleware import (
    ErrorHandlingMiddleware,
    LoggingMiddleware,
    StatisticsMiddleware,
    TimingMiddleware,
)

__version__ = "0.2.0"

# 全局状态
_config: Optional[AppConfig] = None
_db_pool: Optional[AsyncDatabasePool] = None
_cache: Optional[MetadataCache] = None
_lineage_store: Optional[LineageStore] = None
_start_time: float = 0
_initialized: bool = False
_stats_middleware: Optional[StatisticsMiddleware] = None

logger = logging.getLogger("mysql_mcp_server")


def setup_logging(config: AppConfig) -> logging.Logger:
    """配置日志"""
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.log_file),
        ],
    )
    return logging.getLogger("mysql_mcp_server")


async def ensure_initialized():
    """确保服务器已初始化（懒加载模式）"""
    global _db_pool, _cache, _lineage_store, _initialized

    if _initialized:
        return

    logger.info(f"MySQL MCP Server v{__version__} 正在初始化...")

    # 初始化数据库连接池
    if _db_pool is None:
        _db_pool = AsyncDatabasePool(_config)

    if not await _db_pool.initialize():
        logger.warning("数据库连接池初始化失败，部分功能可能不可用")

    # 初始化元数据缓存
    if _cache is None:
        _cache = MetadataCache(
            _db_pool,
            cache_config=_config.cache,
        )

    # 尝试从磁盘加载缓存
    try:
        loaded = await _cache.initialize()
        if loaded:
            logger.info("从磁盘恢复元数据缓存成功")
        else:
            # 没有缓存数据，执行首次更新
            await _cache.update(force=True)
            logger.info("元数据缓存初始化成功")
    except Exception as e:
        logger.warning(f"初始化元数据缓存失败: {e}")

    # 启动后台更新
    await _cache.start_background_updater()

    # 初始化血缘存储
    if _lineage_store is None and _config.lineage.enabled:
        _lineage_store = LineageStore(
            storage_dir=_config.lineage.storage_dir,
            enabled=_config.lineage.enabled,
            dialect=_config.lineage.dialect,
            analyze_select=_config.lineage.analyze_select,
        )
        logger.info(f"血缘分析已启用，存储目录: {_config.lineage.storage_dir}")

    _initialized = True
    logger.info(
        f"服务器已初始化 - 传输模式: {_config.server.transport}, "
        f"数据库: {_config.db.database}@{_config.db.host}:{_config.db.port}"
    )


async def log_to_client(message: str, level: str = "info"):
    """
    向客户端发送日志消息

    Args:
        message: 日志消息
        level: 日志级别 (debug, info, warning, error)
    """
    try:
        ctx = get_context()
        if ctx:
            if level == "debug":
                await ctx.debug(message)
            elif level == "warning":
                await ctx.warning(message)
            elif level == "error":
                await ctx.error(message)
            else:
                await ctx.info(message)
    except Exception:
        # 如果无法获取 context（比如在初始化阶段），忽略错误
        pass


async def report_progress(current: int, total: int, message: str = ""):
    """
    向客户端报告进度

    Args:
        current: 当前进度
        total: 总进度
        message: 可选的进度消息
    """
    try:
        ctx = get_context()
        if ctx:
            await ctx.report_progress(progress=current, total=total)
    except Exception:
        pass


async def elicit_database(ctx: Context, database: Optional[str] = None) -> Optional[str]:
    """
    获取数据库名称，如果未指定则通过 elicitation 询问用户

    Args:
        ctx: MCP Context
        database: 可选的数据库名称

    Returns:
        数据库名称，如果用户取消则返回 None
    """
    # 如果已指定数据库，直接返回
    if database:
        return database

    # 如果配置了默认数据库，使用默认值
    if _config and _config.db.database:
        return _config.db.database

    # 如果禁用了 elicitation，直接返回 None
    if _config and not _config.server.enable_elicitation:
        logger.debug("Elicitation 已禁用，需要显式提供 database 参数")
        return None

    # 获取可用的数据库列表
    await _cache.update()
    databases = _cache.databases

    if not databases:
        return None

    # 过滤掉系统数据库
    system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
    user_databases = [db for db in databases if db not in system_dbs]

    if not user_databases:
        user_databases = databases

    # 通过 elicitation 询问用户选择数据库
    try:
        # 使用列表作为 response_type，让用户从数据库列表中选择
        result = await ctx.elicit(
            message="请选择要操作的数据库：",
            response_type=user_databases[:50],  # 限制最多50个选项
        )

        if result.action == "accept" and result.data:
            return result.data

        return None
    except Exception as e:
        logger.warning(f"Elicitation 不可用: {e}，请直接提供 database 参数")
        return None


def create_mcp_server(config: AppConfig) -> FastMCP:
    """创建并配置 MCP 服务器"""
    global _config, _start_time, _stats_middleware

    _config = config
    _start_time = time.time()

    # 创建 FastMCP 实例
    mcp = FastMCP(
        "MySQL Database MCP Server",
        version=__version__,
    )

    # 添加中间件（按顺序执行）
    _stats_middleware = StatisticsMiddleware()
    mcp.add_middleware(ErrorHandlingMiddleware())
    mcp.add_middleware(TimingMiddleware())
    mcp.add_middleware(LoggingMiddleware(log_inputs=True, log_outputs=False))
    mcp.add_middleware(_stats_middleware)

    # 注册 MCP 工具
    register_tools(mcp)

    return mcp


def register_tools(mcp: FastMCP):
    """注册所有 MCP 工具"""

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_databases() -> list[str]:
        """获取所有数据库列表"""
        await ensure_initialized()
        await log_to_client("正在获取数据库列表...")

        await _cache.update()
        result = _cache.databases

        await log_to_client(f"找到 {len(result)} 个数据库")
        return result

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_tables(
        ctx: Context,
        database: Optional[str] = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        获取数据库中的所有表信息，包括表名、引擎、行数、注释等

        Args:
            ctx: MCP Context
            database: 数据库名称，如果不指定会交互式询问用户选择
            force_refresh: 是否强制从数据库刷新（默认优先使用缓存）

        Returns:
            成功时返回 {"tables": [...], "from_cache": bool}，失败时返回 {"error": "..."}
        """
        await ensure_initialized()

        db_name = await elicit_database(ctx, database)
        if not db_name:
            return {"error": "未指定数据库，请提供 database 参数或在交互中选择"}

        # 自动将数据库加入 LRU 缓存队列
        is_new = _cache.access_database(db_name)

        # 如果是新加入的数据库，先刷新其元数据
        if is_new:
            await log_to_client(f"数据库 '{db_name}' 首次访问，正在刷新元数据...")
            await _cache.refresh_database(db_name)

        # 尝试从缓存获取（除非强制刷新）
        if not force_refresh:
            cached_tables = _cache.get_tables_for_database(db_name)
            if cached_tables is not None:
                tables = list(cached_tables.values())
                await log_to_client(f"从缓存获取 {len(tables)} 个表")
                return {"tables": tables, "from_cache": True}

        # 缓存未命中或强制刷新，从数据库查询
        await log_to_client(f"正在从数据库获取 '{db_name}' 的表列表...")

        # 回退到直接查询
        result = await _db_pool.execute_query(
            """
            SELECT
                TABLE_NAME as name,
                ENGINE as engine,
                TABLE_ROWS as `rows`,
                TABLE_COMMENT as comment,
                CREATE_TIME as create_time
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
            """,
            [db_name],
        )

        if not result.get("success", False):
            return {"error": result.get("error", "查询失败")}

        tables = result.get("rows", [])
        await log_to_client(f"找到 {len(tables)} 个表")
        return {"tables": tables, "from_cache": False}

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_table_structure(
        ctx: Context,
        table_name: str,
        database: Optional[str] = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        获取指定表的详细结构信息

        Args:
            ctx: MCP Context
            table_name: 表名
            database: 数据库名称，如果不指定会交互式询问用户选择
            force_refresh: 是否强制从数据库刷新（默认优先使用缓存）

        Returns:
            包含表信息、列信息、索引信息和外键关系的字典，以及 from_cache 标识
        """
        await ensure_initialized()

        db_name = await elicit_database(ctx, database)
        if not db_name:
            return {"error": "未指定数据库，请提供 database 参数或在交互中选择"}

        # 自动将数据库加入 LRU 缓存队列
        is_new = _cache.access_database(db_name)

        # 如果是新加入的数据库，先刷新其元数据
        if is_new:
            await log_to_client(f"正在刷新数据库 '{db_name}' 的元数据...")
            await _cache.refresh_database(db_name)

        # 尝试从缓存获取
        if not force_refresh:
            cached_tables = _cache.get_tables_for_database(db_name)
            if cached_tables is not None and table_name in cached_tables:
                cached_columns = _cache.get_columns_for_database(db_name)
                cached_relations = _cache.get_relations_for_database(db_name)

                table_info = cached_tables[table_name]
                columns = (cached_columns or {}).get(table_name, [])
                relations = (cached_relations or {}).get(table_name, [])

                await log_to_client(f"从缓存获取表 '{table_name}' 结构，{len(columns)} 个字段")
                return {
                    "table": table_info,
                    "columns": columns,
                    "relations": relations,
                    "from_cache": True,
                }

        # 缓存未命中或强制刷新，从数据库查询
        await log_to_client(f"正在从数据库获取表 '{db_name}.{table_name}' 的结构...")

        # 获取表基本信息
        table_result = await _db_pool.execute_query(
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
            [db_name, table_name],
        )

        if not table_result.get("success", False):
            return {"error": table_result.get("error", "查询失败")}

        if not table_result.get("rows"):
            await log_to_client(f"表 '{table_name}' 不存在", level="warning")
            return {"error": f"表 '{db_name}.{table_name}' 不存在"}

        table_info = table_result["rows"][0]

        # 获取列信息
        columns_result = await _db_pool.execute_query(
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
            [db_name, table_name],
        )

        columns = columns_result.get("rows", []) if columns_result.get("success") else []

        # 获取索引信息
        indexes_result = await _db_pool.execute_query(
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
            [db_name, table_name],
        )

        # 整理索引信息
        indexes = {}
        if indexes_result.get("success"):
            for idx in indexes_result.get("rows", []):
                index_name = idx["index_name"]
                if index_name not in indexes:
                    indexes[index_name] = {
                        "name": index_name,
                        "unique": not idx["non_unique"],
                        "columns": [],
                    }
                indexes[index_name]["columns"].append({
                    "name": idx["column_name"],
                    "order": idx["seq_in_index"],
                })

        table_info["indexes"] = list(indexes.values())

        # 获取外键关系
        relations_result = await _db_pool.execute_query(
            """
            SELECT
                CONSTRAINT_NAME as constraint_name,
                COLUMN_NAME as `column`,
                REFERENCED_TABLE_NAME as referenced_table,
                REFERENCED_COLUMN_NAME as referenced_column
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            """,
            [db_name, table_name],
        )

        relations = relations_result.get("rows", []) if relations_result.get("success") else []

        await log_to_client(f"表 '{table_name}' 有 {len(columns)} 个字段")
        return {
            "table": table_info,
            "columns": columns,
            "relations": relations,
            "from_cache": False,
        }

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_database_relations(
        ctx: Context,
        database: Optional[str] = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        获取数据库中的所有表关系（外键关系）

        Args:
            ctx: MCP Context
            database: 数据库名称，如果不指定会交互式询问用户选择
            force_refresh: 是否强制刷新（忽略缓存）

        Returns:
            成功时返回 {"relations": {...}}，失败时返回 {"error": "..."}
        """
        await ensure_initialized()

        db_name = await elicit_database(ctx, database)
        if not db_name:
            return {"error": "未指定数据库，请提供 database 参数或在交互中选择"}

        # 自动将数据库加入 LRU 缓存队列，返回是否是新加入的
        is_new = _cache.access_database(db_name)

        # 如果是新加入的数据库，先刷新其元数据
        if is_new:
            await log_to_client(f"数据库 '{db_name}' 首次访问，正在刷新元数据...")
            await _cache.refresh_database(db_name)

        # 优先从缓存读取（除非强制刷新）
        if not force_refresh and not is_new:
            cached_relations = _cache.get_relations_for_database(db_name)
            if cached_relations is not None:
                relation_count = sum(len(v) for v in cached_relations.values())
                await log_to_client(f"从缓存获取到 {relation_count} 个外键关系")
                return {"relations": cached_relations, "from_cache": True}

        # 缓存未命中或强制刷新，从数据库查询
        await log_to_client(f"正在从数据库获取 '{db_name}' 的外键关系...")

        relations_result = await _db_pool.execute_query(
            """
            SELECT
                TABLE_NAME as table_name,
                CONSTRAINT_NAME as constraint_name,
                COLUMN_NAME as `column`,
                REFERENCED_TABLE_NAME as referenced_table,
                REFERENCED_COLUMN_NAME as referenced_column
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY TABLE_NAME, CONSTRAINT_NAME
            """,
            [db_name],
        )

        if not relations_result.get("success", False):
            return {"error": relations_result.get("error", "查询失败")}

        # 按表名分组
        relations = {}
        for rel in relations_result.get("rows", []):
            table_name = rel.pop("table_name")
            if table_name not in relations:
                relations[table_name] = []
            relations[table_name].append(rel)

        relation_count = sum(len(v) for v in relations.values())
        await log_to_client(f"找到 {relation_count} 个外键关系")
        return {"relations": relations, "from_cache": False}

    @mcp.tool(
        annotations={"destructiveHint": True},
    )
    async def execute_query(
        sql: str,
        params: Optional[list[Any]] = None,
    ) -> dict[str, Any]:
        """
        执行 SQL 查询语句

        Args:
            sql: SQL 查询语句（支持参数化查询，使用 %s 作为占位符）
            params: 参数列表，用于参数化查询防止 SQL 注入

        Returns:
            查询结果:
            - SELECT 查询返回: columns, rows, row_count, has_more, execution_time
            - INSERT/UPDATE/DELETE 返回: affected_rows, last_insert_id, execution_time
            - 错误返回: success=False, error

        示例:
            execute_query("SELECT * FROM users WHERE id = %s", [1])
            execute_query("INSERT INTO logs (message) VALUES (%s)", ["test"])
        """
        await ensure_initialized()

        # 判断查询类型
        sql_upper = sql.strip().upper()
        is_select = sql_upper.startswith("SELECT")

        if is_select:
            await log_to_client("正在执行查询...")
        else:
            await log_to_client("正在执行更新操作...", level="warning")

        # 报告进度
        await report_progress(0, 100)

        result = await _db_pool.execute_query(sql, params)

        await report_progress(100, 100)

        # 报告结果
        if result.get("success", True):
            if "row_count" in result:
                await log_to_client(f"查询返回 {result['row_count']} 行")
            elif "affected_rows" in result:
                await log_to_client(f"影响 {result['affected_rows']} 行")

            # 后台异步分析血缘（不阻塞返回）
            if _lineage_store and _lineage_store.enabled:
                if should_analyze_sql(sql, _lineage_store.analyze_select):
                    asyncio.create_task(_analyze_lineage_background(sql))
        else:
            await log_to_client(f"执行失败: {result.get('error', '未知错误')}", level="error")

        return result

    async def _analyze_lineage_background(sql: str):
        """后台异步分析 SQL 血缘"""
        try:
            lineage_result = await analyze_sql_lineage(
                sql, dialect=_lineage_store.dialect
            )

            # 存储血缘关系
            if lineage_result["table_lineage"] or lineage_result["column_lineage"]:
                _lineage_store.add_lineage(lineage_result, sql)
                logger.debug(
                    f"血缘分析完成: {len(lineage_result['table_lineage'])} 表, "
                    f"{len(lineage_result['column_lineage'])} 列"
                )
        except ImportError:
            logger.warning("sqllineage 未安装，跳过血缘分析")
        except Exception as e:
            logger.debug(f"血缘分析失败: {e}")
            if _lineage_store:
                _lineage_store.record_analysis_failure(sql, str(e))

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_server_config() -> dict[str, Any]:
        """获取服务器当前配置信息"""
        return {
            "version": __version__,
            "database": {
                "host": _config.db.host,
                "port": _config.db.port,
                "database": _config.db.database,
            },
            "pool": {
                "min_size": _config.pool.min_size,
                "max_size": _config.pool.max_size,
                "connect_timeout": _config.pool.connect_timeout,
            },
            "server": {
                "transport": _config.server.transport,
                "host": _config.server.host,
                "port": _config.server.port,
                "cache_update_interval": _config.server.cache_update_interval,
                "max_result_rows": _config.server.max_result_rows,
                "query_timeout": _config.server.query_timeout,
            },
        }

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_server_status() -> dict[str, Any]:
        """获取服务器当前运行状态，包括运行时间、数据库连接状态、查询统计等"""
        await ensure_initialized()
        await log_to_client("正在获取服务器状态...")

        # 健康检查
        db_connected = await _db_pool.health_check() if _db_pool else False

        # 计算运行时间
        uptime = time.time() - _start_time
        days = int(uptime // 86400)
        hours = int((uptime % 86400) // 3600)
        minutes = int((uptime % 3600) // 60)

        # 获取统计信息
        db_stats = _db_pool.get_stats() if _db_pool else {}
        cache_status = _cache.get_status() if _cache else {}

        # 获取中间件统计
        tool_stats = _stats_middleware.get_stats() if _stats_middleware else {}

        return {
            "version": __version__,
            "uptime": int(uptime),
            "uptime_formatted": f"{days}天 {hours}小时 {minutes}分钟",
            "db_connected": db_connected,
            "transport": _config.server.transport,
            "statistics": db_stats,
            "cache": cache_status,
            "tool_stats": tool_stats,  # 新增: 工具调用统计
            "lineage": _lineage_store.get_statistics() if _lineage_store else {"enabled": False},
        }

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_table_relations(
        table: str,
        database: Optional[str] = None,
        depth: int = 1,
    ) -> dict[str, Any]:
        """
        查询表的关联关系

        基于历史 SELECT 查询分析，返回与指定表一起被查询过的其他表。
        例如：如果执行过 SELECT * FROM users JOIN orders ON ...
        则 users 和 orders 会建立关联关系。

        Args:
            table: 表名
            database: 数据库名
            depth: 追溯深度（1=直接关联，2=间接关联，默认1，最大5）

        Returns:
            {
                "table": "crm_enterprise",
                "related_tables": [
                    {"table": "crm_follow_record", "database": "db_crm", "depth": 1},
                    {"table": "crm_person", "database": "db_crm", "depth": 1}
                ]
            }
        """
        await ensure_initialized()

        if not _lineage_store or not _lineage_store.enabled:
            return {"error": "血缘分析未启用，请检查配置 LINEAGE_ENABLED"}

        # 验证参数
        if depth < 1:
            depth = 1
        if depth > 5:
            depth = 5

        result = _lineage_store.get_table_lineage(
            table=table,
            database=database,
            direction="both",
            depth=depth,
        )

        # 合并并去重
        seen = set()
        related_tables = []
        for item in result.get("upstream", []) + result.get("downstream", []):
            key = f"{item.get('database', '')}.{item['table']}"
            if key not in seen:
                seen.add(key)
                related_tables.append(item)

        related_tables.sort(key=lambda x: (x["depth"], x["table"]))

        await log_to_client(f"表 {table} 关联 {len(related_tables)} 个表")

        return {
            "table": table,
            "database": database,
            "related_tables": related_tables,
        }

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_column_relations(
        table: str,
        column: Optional[str] = None,
        database: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        查询列的关联关系

        基于历史 SELECT JOIN 查询分析，返回与指定列一起出现在 JOIN ON 条件中的列。
        例如：SELECT * FROM users u JOIN orders o ON u.id = o.user_id
        会记录 users.id 和 orders.user_id 的关联关系。

        Args:
            table: 表名
            column: 列名（可选，不指定则返回表的所有列关联）
            database: 数据库名

        Returns:
            {
                "table": "users",
                "column": "id",
                "related_columns": [
                    {"table": "orders", "column": "user_id", "count": 5},
                    {"table": "profiles", "column": "user_id", "count": 3}
                ]
            }
        """
        await ensure_initialized()

        if not _lineage_store or not _lineage_store.enabled:
            return {"error": "关系分析未启用，请检查配置 LINEAGE_ENABLED"}

        result = _lineage_store.get_column_lineage(
            table=table,
            column=column,
            database=database,
            direction="both",
        )

        # 合并并去重
        seen = set()
        related_columns = []
        for item in result.get("upstream", []) + result.get("downstream", []):
            key = f"{item.get('database', '')}.{item['table']}.{item['column']}"
            if key not in seen:
                seen.add(key)
                related_columns.append(item)

        await log_to_client(
            f"列 {table}.{column or '*'} 关联 {len(related_columns)} 个列"
        )

        return {
            "table": table,
            "column": column,
            "database": database,
            "related_columns": related_columns,
        }

    @mcp.tool(
        annotations={"readOnlyHint": True},
    )
    async def get_relation_statistics() -> dict[str, Any]:
        """
        获取关系分析统计信息

        Returns:
            {
                "enabled": true,
                "storage_path": ".cache/lineage/lineage.db",
                "table_relation_count": 100,
                "column_relation_count": 500,
                "sql_analyzed_count": 1000,
                "analysis_success_rate": "95.0%"
            }
        """
        await ensure_initialized()

        if not _lineage_store:
            return {"enabled": False}

        return _lineage_store.get_statistics()


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="MySQL Database MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # HTTP 模式 (默认，Streamable HTTP)
  python server.py

  # 指定端口和地址
  python server.py --port 9000 --host 127.0.0.1

  # STDIO 模式 (用于 Claude Desktop)
  python server.py --transport stdio

  # SSE 模式
  python server.py --transport sse --port 8080
        """,
    )

    parser.add_argument(
        "--transport", "-t",
        choices=["stdio", "http", "sse"],
        default=None,
        help="传输模式 (默认: http，可通过 SERVER_TRANSPORT 环境变量设置)",
    )
    parser.add_argument(
        "--host", "-H",
        default=None,
        help="HTTP/SSE 服务器绑定地址 (默认: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="HTTP/SSE 服务器端口 (默认: 8000)",
    )
    parser.add_argument(
        "--version", "-v",
        action="version",
        version=f"db-mcp {__version__}",
    )

    return parser.parse_args()


def main():
    """主入口函数"""
    # 解析命令行参数
    args = parse_args()

    # 加载配置
    config = load_config()

    # 命令行参数覆盖配置
    if args.transport:
        config.server.transport = args.transport
    if args.host:
        config.server.host = args.host
    if args.port:
        config.server.port = args.port

    # 配置日志
    global logger
    logger = setup_logging(config)

    logger.info(f"MySQL MCP Server v{__version__}")
    logger.info(
        f"配置: 数据库={config.db.database}@{config.db.host}:{config.db.port}, "
        f"传输模式={config.server.transport}"
    )

    # 创建 MCP 服务器
    mcp = create_mcp_server(config)

    # 运行服务器
    try:
        if config.server.transport == "stdio":
            logger.info("以 STDIO 模式启动服务器...")
            mcp.run(transport="stdio")
        elif config.server.transport == "http":
            logger.info(
                f"以 HTTP 模式启动服务器: "
                f"http://{config.server.host}:{config.server.port}/mcp"
            )
            # stateless_http=True: 禁用会话管理，每个请求独立处理，解决 "Missing session ID" 错误
            mcp.run(
                transport="http",
                host=config.server.host,
                port=config.server.port,
                stateless_http=True,
            )
        elif config.server.transport == "sse":
            logger.info(
                f"以 SSE 模式启动服务器: "
                f"http://{config.server.host}:{config.server.port}/sse"
            )
            mcp.run(
                transport="sse",
                host=config.server.host,
                port=config.server.port,
            )
        else:
            logger.error(f"不支持的传输模式: {config.server.transport}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("接收到中断信号")
    except Exception as e:
        logger.error(f"服务器运行时出错: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
