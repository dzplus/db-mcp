"""
SQL 血缘分析模块

提供 SQL 血缘关系的自动分析、持久化存储和查询功能
使用 sqllineage 库解析 SQL，SQLite 存储血缘关系
"""

import asyncio
import hashlib
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Literal, Optional

logger = logging.getLogger("mysql_mcp_server")


class LineageStore:
    """
    血缘关系存储管理器

    使用 SQLite 存储表级和列级血缘关系，支持增量更新和关系查询
    """

    def __init__(
        self,
        storage_dir: str = ".cache/lineage",
        enabled: bool = True,
        dialect: str = "mysql",
        analyze_select: bool = False,
    ):
        """
        初始化血缘存储

        Args:
            storage_dir: 存储目录
            enabled: 是否启用血缘分析
            dialect: SQL 方言 (mysql, ansi, hive, sparksql 等)
            analyze_select: 是否分析 SELECT 语句（默认只分析写入语句）
        """
        self.storage_dir = storage_dir
        self.db_path = os.path.join(storage_dir, "lineage.db")
        self.enabled = enabled
        self.dialect = dialect
        self.analyze_select = analyze_select

        # 延迟初始化数据库
        self._initialized = False
        self._lock = asyncio.Lock()

    def _init_db(self):
        """初始化 SQLite 数据库表"""
        if self._initialized:
            return

        os.makedirs(self.storage_dir, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                -- 表级血缘关系
                CREATE TABLE IF NOT EXISTS table_lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_db TEXT,
                    source_table TEXT NOT NULL,
                    target_db TEXT,
                    target_table TEXT NOT NULL,
                    sql_hash TEXT,
                    created_at TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    UNIQUE(source_db, source_table, target_db, target_table)
                );

                -- 列级血缘关系
                CREATE TABLE IF NOT EXISTS column_lineage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_db TEXT,
                    source_table TEXT NOT NULL,
                    source_column TEXT NOT NULL,
                    target_db TEXT,
                    target_table TEXT NOT NULL,
                    target_column TEXT NOT NULL,
                    sql_hash TEXT,
                    created_at TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    UNIQUE(source_db, source_table, source_column,
                           target_db, target_table, target_column)
                );

                -- SQL 执行历史（用于调试和审计）
                CREATE TABLE IF NOT EXISTS sql_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sql_hash TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    executed_at TEXT NOT NULL,
                    analysis_success INTEGER DEFAULT 1,
                    error_message TEXT
                );

                -- 创建索引以加速查询
                CREATE INDEX IF NOT EXISTS idx_table_source
                    ON table_lineage(source_db, source_table);
                CREATE INDEX IF NOT EXISTS idx_table_target
                    ON table_lineage(target_db, target_table);

                CREATE INDEX IF NOT EXISTS idx_col_source
                    ON column_lineage(source_db, source_table, source_column);
                CREATE INDEX IF NOT EXISTS idx_col_target
                    ON column_lineage(target_db, target_table, target_column);

                CREATE INDEX IF NOT EXISTS idx_sql_hash
                    ON sql_history(sql_hash);
            """)

        self._initialized = True
        logger.info(f"血缘存储数据库已初始化: {self.db_path}")

    def _ensure_initialized(self):
        """确保数据库已初始化"""
        if not self._initialized:
            self._init_db()

    @staticmethod
    def _hash_sql(sql: str) -> str:
        """计算 SQL 语句的哈希值"""
        return hashlib.md5(sql.strip().encode()).hexdigest()[:16]

    def add_lineage(self, lineage_result: dict, sql: str) -> bool:
        """
        添加血缘关系到数据库

        Args:
            lineage_result: 血缘分析结果，包含 table_lineage 和 column_lineage
            sql: 原始 SQL 语句

        Returns:
            是否添加成功
        """
        if not self.enabled:
            return False

        self._ensure_initialized()

        sql_hash = self._hash_sql(sql)
        now = datetime.now().isoformat()

        try:
            with sqlite3.connect(self.db_path) as conn:
                # 记录 SQL 历史
                conn.execute(
                    """
                    INSERT INTO sql_history (sql_hash, sql_text, executed_at, analysis_success)
                    VALUES (?, ?, ?, 1)
                    """,
                    (sql_hash, sql[:2000], now)  # 限制 SQL 长度
                )

                # 表级血缘
                for edge in lineage_result.get("table_lineage", []):
                    conn.execute(
                        """
                        INSERT INTO table_lineage
                            (source_db, source_table, target_db, target_table,
                             sql_hash, created_at, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_db, source_table, target_db, target_table)
                        DO UPDATE SET last_seen = ?, sql_hash = ?
                        """,
                        (
                            edge.get("source_db", ""),
                            edge["source_table"],
                            edge.get("target_db", ""),
                            edge["target_table"],
                            sql_hash,
                            now,
                            now,
                            now,
                            sql_hash,
                        )
                    )

                # 列级血缘
                for edge in lineage_result.get("column_lineage", []):
                    conn.execute(
                        """
                        INSERT INTO column_lineage
                            (source_db, source_table, source_column,
                             target_db, target_table, target_column,
                             sql_hash, created_at, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_db, source_table, source_column,
                                   target_db, target_table, target_column)
                        DO UPDATE SET last_seen = ?, sql_hash = ?
                        """,
                        (
                            edge.get("source_db", ""),
                            edge["source_table"],
                            edge["source_column"],
                            edge.get("target_db", ""),
                            edge["target_table"],
                            edge["target_column"],
                            sql_hash,
                            now,
                            now,
                            now,
                            sql_hash,
                        )
                    )

                conn.commit()

            table_count = len(lineage_result.get("table_lineage", []))
            column_count = len(lineage_result.get("column_lineage", []))
            logger.debug(
                f"血缘关系已保存: {table_count} 个表关系, {column_count} 个列关系"
            )
            return True

        except Exception as e:
            logger.error(f"保存血缘关系失败: {e}")
            return False

    def record_analysis_failure(self, sql: str, error: str):
        """记录血缘分析失败"""
        if not self.enabled:
            return

        self._ensure_initialized()

        sql_hash = self._hash_sql(sql)
        now = datetime.now().isoformat()

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO sql_history
                        (sql_hash, sql_text, executed_at, analysis_success, error_message)
                    VALUES (?, ?, ?, 0, ?)
                    """,
                    (sql_hash, sql[:2000], now, error[:500])
                )
        except Exception as e:
            logger.warning(f"记录分析失败时出错: {e}")

    def get_column_lineage(
        self,
        table: str,
        column: Optional[str] = None,
        database: Optional[str] = None,
        direction: Literal["upstream", "downstream", "both"] = "both",
    ) -> dict[str, Any]:
        """
        查询列血缘关系

        Args:
            table: 表名
            column: 列名（可选，不指定则返回表的所有列血缘）
            database: 数据库名
            direction: 查询方向
                - upstream: 查询数据来源（谁流入我）
                - downstream: 查询数据去向（我流向谁）
                - both: 双向查询

        Returns:
            血缘关系结果
        """
        self._ensure_initialized()

        result = {
            "table": table,
            "column": column,
            "database": database,
            "upstream": [],
            "downstream": [],
        }

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row

                # 上游（谁流入我）- 查询 target 匹配的记录
                if direction in ("upstream", "both"):
                    query = """
                        SELECT source_db, source_table, source_column,
                               COUNT(*) as cnt, MAX(last_seen) as last_seen
                        FROM column_lineage
                        WHERE target_table = ?
                    """
                    params = [table]

                    if column:
                        query += " AND target_column = ?"
                        params.append(column)
                    if database:
                        query += " AND target_db = ?"
                        params.append(database)

                    query += " GROUP BY source_db, source_table, source_column"
                    query += " ORDER BY cnt DESC"

                    for row in conn.execute(query, params):
                        result["upstream"].append({
                            "database": row["source_db"] or None,
                            "table": row["source_table"],
                            "column": row["source_column"],
                            "count": row["cnt"],
                            "last_seen": row["last_seen"],
                        })

                # 下游（我流向谁）- 查询 source 匹配的记录
                if direction in ("downstream", "both"):
                    query = """
                        SELECT target_db, target_table, target_column,
                               COUNT(*) as cnt, MAX(last_seen) as last_seen
                        FROM column_lineage
                        WHERE source_table = ?
                    """
                    params = [table]

                    if column:
                        query += " AND source_column = ?"
                        params.append(column)
                    if database:
                        query += " AND source_db = ?"
                        params.append(database)

                    query += " GROUP BY target_db, target_table, target_column"
                    query += " ORDER BY cnt DESC"

                    for row in conn.execute(query, params):
                        result["downstream"].append({
                            "database": row["target_db"] or None,
                            "table": row["target_table"],
                            "column": row["target_column"],
                            "count": row["cnt"],
                            "last_seen": row["last_seen"],
                        })

        except Exception as e:
            logger.error(f"查询列血缘失败: {e}")
            result["error"] = str(e)

        return result

    def get_table_lineage(
        self,
        table: str,
        database: Optional[str] = None,
        direction: Literal["upstream", "downstream", "both"] = "both",
        depth: int = 1,
    ) -> dict[str, Any]:
        """
        查询表级血缘关系

        Args:
            table: 表名
            database: 数据库名
            direction: 查询方向
            depth: 追溯深度（1=直接关联，2=间接关联...）

        Returns:
            表级血缘关系
        """
        self._ensure_initialized()

        result = {
            "table": table,
            "database": database,
            "upstream": [],
            "downstream": [],
        }

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row

                # 上游表
                if direction in ("upstream", "both"):
                    visited = set()
                    self._traverse_table_lineage(
                        conn, table, database, "upstream", depth, 1, visited, result["upstream"]
                    )

                # 下游表
                if direction in ("downstream", "both"):
                    visited = set()
                    self._traverse_table_lineage(
                        conn, table, database, "downstream", depth, 1, visited, result["downstream"]
                    )

        except Exception as e:
            logger.error(f"查询表血缘失败: {e}")
            result["error"] = str(e)

        return result

    def _traverse_table_lineage(
        self,
        conn: sqlite3.Connection,
        table: str,
        database: Optional[str],
        direction: str,
        max_depth: int,
        current_depth: int,
        visited: set,
        results: list,
    ):
        """递归遍历表血缘（支持无向关联查询）"""
        if current_depth > max_depth:
            return

        # 构建查询 - 同时查询两个方向（支持无向关联）
        if direction == "upstream":
            # 上游：查找指向当前表的记录，或当前表作为 source 的无向关联
            query = """
                SELECT source_db as related_db, source_table as related_table, MAX(last_seen) as last_seen
                FROM table_lineage
                WHERE target_table = ?
            """
            params = [table]
            if database:
                query += " AND target_db = ?"
                params.append(database)
            query += " GROUP BY source_db, source_table"

            # 合并无向关联（当前表在 source 侧）
            query += """
                UNION
                SELECT target_db as related_db, target_table as related_table, MAX(last_seen) as last_seen
                FROM table_lineage
                WHERE source_table = ?
            """
            params.append(table)
            if database:
                query += " AND source_db = ?"
                params.append(database)
            query += " GROUP BY target_db, target_table"
        else:  # downstream
            # 下游：查找当前表指向的记录，或当前表作为 target 的无向关联
            query = """
                SELECT target_db as related_db, target_table as related_table, MAX(last_seen) as last_seen
                FROM table_lineage
                WHERE source_table = ?
            """
            params = [table]
            if database:
                query += " AND source_db = ?"
                params.append(database)
            query += " GROUP BY target_db, target_table"

            # 合并无向关联（当前表在 target 侧）
            query += """
                UNION
                SELECT source_db as related_db, source_table as related_table, MAX(last_seen) as last_seen
                FROM table_lineage
                WHERE target_table = ?
            """
            params.append(table)
            if database:
                query += " AND target_db = ?"
                params.append(database)
            query += " GROUP BY source_db, source_table"

        for row in conn.execute(query, params):
            related_db = row["related_db"]
            related_table = row["related_table"]

            # 避免循环和自引用
            key = f"{related_db}.{related_table}"
            if key in visited or related_table == table:
                continue
            visited.add(key)

            results.append({
                "database": related_db or None,
                "table": related_table,
                "depth": current_depth,
                "last_seen": row["last_seen"],
            })

            # 递归查找
            if current_depth < max_depth:
                self._traverse_table_lineage(
                    conn, related_table, related_db, direction,
                    max_depth, current_depth + 1, visited, results
                )

    def get_statistics(self) -> dict[str, Any]:
        """获取血缘统计信息"""
        self._ensure_initialized()

        try:
            with sqlite3.connect(self.db_path) as conn:
                table_count = conn.execute(
                    "SELECT COUNT(*) FROM table_lineage"
                ).fetchone()[0]

                column_count = conn.execute(
                    "SELECT COUNT(*) FROM column_lineage"
                ).fetchone()[0]

                sql_count = conn.execute(
                    "SELECT COUNT(*) FROM sql_history"
                ).fetchone()[0]

                success_count = conn.execute(
                    "SELECT COUNT(*) FROM sql_history WHERE analysis_success = 1"
                ).fetchone()[0]

                # 获取涉及的表数量
                unique_tables = conn.execute(
                    """
                    SELECT COUNT(DISTINCT source_table) + COUNT(DISTINCT target_table)
                    FROM table_lineage
                    """
                ).fetchone()[0]

                return {
                    "enabled": self.enabled,
                    "storage_path": self.db_path,
                    "table_lineage_count": table_count,
                    "column_lineage_count": column_count,
                    "sql_analyzed_count": sql_count,
                    "analysis_success_rate": (
                        f"{success_count / sql_count * 100:.1f}%"
                        if sql_count > 0 else "N/A"
                    ),
                    "unique_tables_involved": unique_tables,
                }
        except Exception as e:
            return {
                "enabled": self.enabled,
                "error": str(e),
            }

    def clear(self) -> bool:
        """清除所有血缘数据"""
        self._ensure_initialized()

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM table_lineage")
                conn.execute("DELETE FROM column_lineage")
                conn.execute("DELETE FROM sql_history")
                conn.commit()
            logger.info("血缘数据已清除")
            return True
        except Exception as e:
            logger.error(f"清除血缘数据失败: {e}")
            return False


def _parse_join_conditions(sql: str, source_tables: list) -> list[dict]:
    """
    解析 SQL 中 JOIN ON 条件的列关联关系

    Args:
        sql: SQL 语句
        source_tables: sqllineage 解析出的源表列表

    Returns:
        列关联关系列表，每项包含 table1, column1, table2, column2
    """
    import re

    relations = []

    # 构建表名/别名映射
    # 匹配: FROM db.table alias, JOIN db.table alias, FROM table alias 等
    table_pattern = r'(?:FROM|JOIN)\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?'
    table_matches = re.findall(table_pattern, sql, re.IGNORECASE)

    # 别名 -> 表名映射
    alias_map = {}
    for db, table, alias in table_matches:
        if alias:
            alias_map[alias.lower()] = (db, table)
        alias_map[table.lower()] = (db, table)

    # 匹配 ON 条件中的列比较: a.col1 = b.col2
    # 支持格式: alias.column, table.column, db.table.column
    on_pattern = r'ON\s+([^\s]+)\s*=\s*([^\s,\)]+)'
    on_matches = re.findall(on_pattern, sql, re.IGNORECASE)

    for left, right in on_matches:
        # 解析左侧列
        left_parts = left.split('.')
        right_parts = right.split('.')

        # 解析 alias.column 或 db.table.column 格式
        def parse_column_ref(parts):
            if len(parts) == 2:
                # alias.column 或 table.column
                ref, col = parts
                if ref.lower() in alias_map:
                    db, table = alias_map[ref.lower()]
                    return {'db': db, 'table': table, 'column': col}
                return {'db': '', 'table': ref, 'column': col}
            elif len(parts) == 3:
                # db.table.column
                return {'db': parts[0], 'table': parts[1], 'column': parts[2]}
            return None

        left_col = parse_column_ref(left_parts)
        right_col = parse_column_ref(right_parts)

        if left_col and right_col and left_col['table'] != right_col['table']:
            relations.append({
                'db1': left_col['db'],
                'table1': left_col['table'],
                'column1': left_col['column'],
                'db2': right_col['db'],
                'table2': right_col['table'],
                'column2': right_col['column'],
            })

    return relations


async def analyze_sql_lineage(sql: str, dialect: str = "mysql") -> dict[str, Any]:
    """
    分析 SQL 语句的血缘关系

    Args:
        sql: SQL 语句
        dialect: SQL 方言

    Returns:
        血缘分析结果，包含 table_lineage 和 column_lineage
    """
    result = {
        "table_lineage": [],
        "column_lineage": [],
    }

    try:
        from sqllineage.runner import LineageRunner

        # 使用 sqllineage 解析
        runner = LineageRunner(sql, dialect=dialect)

        # 表级血缘 (source_tables 和 target_tables 是属性，不是方法)
        source_tables = runner.source_tables
        target_tables = runner.target_tables

        if target_tables:
            # 有目标表（INSERT/CREATE等），记录 source -> target 关系
            for src in source_tables:
                for tgt in target_tables:
                    src_str = str(src)
                    tgt_str = str(tgt)

                    # 解析 <default>.table 或 database.table 格式
                    src_parts = src_str.replace("<default>.", "").split(".")
                    tgt_parts = tgt_str.replace("<default>.", "").split(".")

                    result["table_lineage"].append({
                        "source_db": src_parts[0] if len(src_parts) > 1 else "",
                        "source_table": src_parts[-1],
                        "target_db": tgt_parts[0] if len(tgt_parts) > 1 else "",
                        "target_table": tgt_parts[-1],
                    })
        elif len(source_tables) > 1:
            # SELECT 多表 JOIN 查询（无目标表），记录表之间的无向关联关系
            source_list = list(source_tables)

            # 记录所有表两两之间的关联（只存一条，按字母序排列保证唯一性）
            for i, tbl1 in enumerate(source_list):
                for tbl2 in source_list[i + 1:]:
                    tbl1_str = str(tbl1).replace("<default>.", "")
                    tbl2_str = str(tbl2).replace("<default>.", "")

                    # 按字母序排列，确保 A-B 和 B-A 存储为同一条记录
                    if tbl1_str > tbl2_str:
                        tbl1_str, tbl2_str = tbl2_str, tbl1_str

                    tbl1_parts = tbl1_str.split(".")
                    tbl2_parts = tbl2_str.split(".")

                    result["table_lineage"].append({
                        "source_db": tbl1_parts[0] if len(tbl1_parts) > 1 else "",
                        "source_table": tbl1_parts[-1],
                        "target_db": tbl2_parts[0] if len(tbl2_parts) > 1 else "",
                        "target_table": tbl2_parts[-1],
                    })

            # 解析 JOIN ON 条件中的列关联关系
            join_column_relations = _parse_join_conditions(sql, source_tables)
            for rel in join_column_relations:
                # 按字母序排列列关系，确保无向存储
                col1 = f"{rel['table1']}.{rel['column1']}"
                col2 = f"{rel['table2']}.{rel['column2']}"
                if col1 > col2:
                    rel['table1'], rel['table2'] = rel['table2'], rel['table1']
                    rel['column1'], rel['column2'] = rel['column2'], rel['column1']
                    rel['db1'], rel['db2'] = rel.get('db2', ''), rel.get('db1', '')

                result["column_lineage"].append({
                    "source_db": rel.get('db1', ''),
                    "source_table": rel['table1'],
                    "source_column": rel['column1'],
                    "target_db": rel.get('db2', ''),
                    "target_table": rel['table2'],
                    "target_column": rel['column2'],
                })

        # 列级血缘
        try:
            # get_column_lineage 返回 set of (Column, Column) tuples
            # Column 对象的字符串格式是 "<default>.table.column" 或 "db.table.column"
            column_lineage = runner.get_column_lineage()
            for src_col, tgt_col in column_lineage:
                # 解析 Column 字符串: <default>.table.column 或 db.table.column
                src_str = str(src_col).replace("<default>.", "")
                tgt_str = str(tgt_col).replace("<default>.", "")

                # 分割: 可能是 "table.column" 或 "db.table.column"
                src_parts = src_str.split(".")
                tgt_parts = tgt_str.split(".")

                # 解析源
                if len(src_parts) >= 3:  # db.table.column
                    src_db, src_table, src_column = src_parts[0], src_parts[1], ".".join(src_parts[2:])
                elif len(src_parts) == 2:  # table.column
                    src_db, src_table, src_column = "", src_parts[0], src_parts[1]
                else:  # 只有 column
                    src_db, src_table, src_column = "", "", src_parts[0]

                # 解析目标
                if len(tgt_parts) >= 3:  # db.table.column
                    tgt_db, tgt_table, tgt_column = tgt_parts[0], tgt_parts[1], ".".join(tgt_parts[2:])
                elif len(tgt_parts) == 2:  # table.column
                    tgt_db, tgt_table, tgt_column = "", tgt_parts[0], tgt_parts[1]
                else:  # 只有 column
                    tgt_db, tgt_table, tgt_column = "", "", tgt_parts[0]

                result["column_lineage"].append({
                    "source_db": src_db,
                    "source_table": src_table,
                    "source_column": src_column,
                    "target_db": tgt_db,
                    "target_table": tgt_table,
                    "target_column": tgt_column,
                })
        except Exception as e:
            # 列级分析可能失败（如 SELECT * 等情况），忽略
            logger.debug(f"列级血缘分析跳过: {e}")

    except ImportError:
        logger.warning(
            "sqllineage 未安装，血缘分析不可用。"
            "请运行: pip install sqllineage"
        )
        raise
    except Exception as e:
        logger.debug(f"SQL 血缘分析失败: {e}")
        raise

    return result


def should_analyze_sql(sql: str, analyze_select: bool = False) -> bool:
    """
    判断是否应该分析该 SQL 的血缘

    Args:
        sql: SQL 语句
        analyze_select: 是否分析 SELECT 语句

    Returns:
        是否应该分析
    """
    sql_upper = sql.strip().upper()

    # 始终分析写入类语句
    write_keywords = (
        "INSERT", "CREATE", "MERGE", "UPDATE", "DELETE",
        "ALTER", "DROP", "TRUNCATE", "REPLACE"
    )

    if any(sql_upper.startswith(kw) for kw in write_keywords):
        return True

    # 如果启用了 SELECT 分析
    if analyze_select and sql_upper.startswith("SELECT"):
        # 排除简单查询（如 SHOW, DESCRIBE 等）
        if "INTO" in sql_upper:  # SELECT INTO
            return True
        return True

    return False
