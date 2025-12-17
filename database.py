"""
异步数据库连接管理模块

使用 aiomysql 提供异步数据库连接池
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

import aiomysql

from config import AppConfig

logger = logging.getLogger("mysql_mcp_server")


class AsyncDatabasePool:
    """异步数据库连接池管理器"""

    def __init__(self, config: AppConfig):
        self.config = config
        self._pool: Optional[aiomysql.Pool] = None
        self._lock = asyncio.Lock()

        # 统计信息
        self._query_count = 0
        self._error_count = 0
        self._total_query_time = 0.0

    async def initialize(self) -> bool:
        """初始化连接池"""
        async with self._lock:
            if self._pool is not None:
                return True

            try:
                logger.info(
                    f"初始化异步数据库连接池: "
                    f"{self.config.db.host}:{self.config.db.port}/{self.config.db.database}"
                )
                self._pool = await aiomysql.create_pool(
                    host=self.config.db.host,
                    port=self.config.db.port,
                    user=self.config.db.user,
                    password=self.config.db.password,
                    db=self.config.db.database,
                    minsize=self.config.pool.min_size,
                    maxsize=self.config.pool.max_size,
                    connect_timeout=self.config.pool.connect_timeout,
                    autocommit=False,
                    charset="utf8mb4",
                    cursorclass=aiomysql.DictCursor,
                )
                logger.info("数据库连接池初始化成功")
                return True
            except Exception as e:
                logger.error(f"初始化数据库连接池失败: {e}")
                return False

    async def close(self):
        """关闭连接池"""
        async with self._lock:
            if self._pool is not None:
                self._pool.close()
                await self._pool.wait_closed()
                self._pool = None
                logger.info("数据库连接池已关闭")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiomysql.Connection]:
        """获取数据库连接的上下文管理器"""
        if self._pool is None:
            await self.initialize()

        if self._pool is None:
            raise RuntimeError("数据库连接池未初始化")

        async with self._pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def cursor(self, conn: Optional[aiomysql.Connection] = None) -> AsyncIterator[aiomysql.DictCursor]:
        """获取游标的上下文管理器"""
        if conn is not None:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                yield cur
        else:
            async with self.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    yield cur

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            async with self.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
            return True
        except Exception as e:
            logger.warning(f"数据库健康检查失败: {e}")
            return False

    async def execute_query(
        self,
        sql: str,
        params: Optional[list[Any]] = None,
        timeout: Optional[int] = None
    ) -> dict[str, Any]:
        """
        执行 SQL 查询

        Args:
            sql: SQL 语句
            params: 参数列表
            timeout: 超时时间(秒)

        Returns:
            查询结果字典
        """
        import time

        timeout = timeout or self.config.server.query_timeout
        max_rows = self.config.server.max_result_rows
        start_time = time.time()

        # 记录查询
        log_sql = sql.replace("\n", " ").strip()
        if len(log_sql) > 100:
            log_sql = log_sql[:97] + "..."
        logger.info(f"执行SQL查询: {log_sql}")

        self._query_count += 1

        try:
            async with asyncio.timeout(timeout):
                async with self.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        # 执行查询
                        if params:
                            await cur.execute(sql, params)
                        else:
                            await cur.execute(sql)

                        execution_time = time.time() - start_time
                        self._total_query_time += execution_time

                        # 处理结果
                        if cur.description:
                            # SELECT 查询
                            rows = await cur.fetchmany(max_rows)

                            # 检查是否有更多数据
                            extra_row = await cur.fetchone()
                            has_more = extra_row is not None

                            # 获取列信息
                            columns = [
                                {"name": col[0], "type": str(col[1])}
                                for col in cur.description
                            ]

                            result = {
                                "success": True,
                                "columns": columns,
                                "rows": rows,
                                "row_count": len(rows),
                                "has_more": has_more,
                                "execution_time": round(execution_time, 3),
                            }

                            logger.info(
                                f"查询完成，返回 {len(rows)} 行结果，"
                                f"执行时间: {execution_time:.3f}秒"
                            )
                            if has_more:
                                logger.info(f"结果集超过最大限制 {max_rows} 行，已截断")
                        else:
                            # INSERT/UPDATE/DELETE
                            await conn.commit()
                            result = {
                                "success": True,
                                "affected_rows": cur.rowcount,
                                "last_insert_id": cur.lastrowid,
                                "execution_time": round(execution_time, 3),
                            }
                            logger.info(
                                f"更新操作完成，影响 {cur.rowcount} 行，"
                                f"执行时间: {execution_time:.3f}秒"
                            )

                        return result

        except asyncio.TimeoutError:
            self._error_count += 1
            logger.error(f"查询超时 (>{timeout}秒)")
            return {
                "success": False,
                "error": f"查询执行超时 (>{timeout}秒)"
            }
        except Exception as e:
            self._error_count += 1
            logger.error(f"执行SQL查询时出错: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息"""
        avg_query_time = 0.0
        if self._query_count > 0:
            avg_query_time = self._total_query_time / self._query_count

        pool_info = {}
        if self._pool:
            pool_info = {
                "size": self._pool.size,
                "free_size": self._pool.freesize,
                "min_size": self._pool.minsize,
                "max_size": self._pool.maxsize,
            }

        return {
            "query_count": self._query_count,
            "error_count": self._error_count,
            "avg_query_time": round(avg_query_time, 3),
            "total_query_time": round(self._total_query_time, 3),
            "pool": pool_info,
        }
