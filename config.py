"""
配置管理模块

使用 Pydantic Settings 进行配置验证和环境变量加载
"""

from typing import Literal
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """数据库连接配置"""

    model_config = SettingsConfigDict(
        env_prefix="MYSQL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    host: str = Field(default="localhost", description="数据库主机地址")
    port: int = Field(default=3306, ge=1, le=65535, description="数据库端口")
    database: str = Field(default="mysql", description="数据库名称")
    user: str = Field(default="root", description="数据库用户名")
    password: str = Field(default="", description="数据库密码")


class PoolConfig(BaseSettings):
    """连接池配置"""

    model_config = SettingsConfigDict(
        env_prefix="POOL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    size: int = Field(default=10, ge=1, le=100, alias="POOL_SIZE", description="连接池大小")
    min_size: int = Field(default=1, ge=1, le=50, description="最小连接数")
    max_size: int = Field(default=20, ge=1, le=100, description="最大连接数")
    connect_timeout: int = Field(default=30, ge=1, le=300, alias="CONNECT_TIMEOUT", description="连接超时时间(秒)")

    @field_validator("max_size")
    @classmethod
    def max_size_must_be_greater_than_min(cls, v, info):
        min_size = info.data.get("min_size", 1)
        if v < min_size:
            return min_size
        return v


class LineageConfig(BaseSettings):
    """血缘分析配置"""

    model_config = SettingsConfigDict(
        env_prefix="LINEAGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    enabled: bool = Field(
        default=True,
        description="是否启用血缘分析"
    )
    storage_dir: str = Field(
        default=".cache/lineage",
        description="血缘数据存储目录"
    )
    dialect: str = Field(
        default="mysql",
        description="SQL 方言 (mysql, ansi, hive, sparksql 等)"
    )
    analyze_select: bool = Field(
        default=False,
        description="是否分析 SELECT 语句（默认只分析写入语句）"
    )


class CacheConfig(BaseSettings):
    """缓存配置"""

    model_config = SettingsConfigDict(
        env_prefix="CACHE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # 缓存存储配置
    enable_persistence: bool = Field(
        default=True,
        description="是否启用缓存持久化（使用 DiskStore）"
    )
    storage_dir: str = Field(
        default=".cache/metadata",
        description="缓存存储目录路径"
    )
    update_interval: int = Field(
        default=10, ge=1, le=60,
        description="元数据缓存定时刷新间隔(分钟)"
    )
    max_history_records: int = Field(
        default=100, ge=1, le=1000,
        description="保留的刷新历史记录数量"
    )

    # LRU 缓存队列配置
    lru_max_databases: int = Field(
        default=10, ge=1, le=100,
        description="LRU 缓存队列最大数据库数量，只有队列中的数据库会被定时刷新"
    )
    lru_auto_add: bool = Field(
        default=True,
        description="访问数据库时是否自动加入 LRU 队列"
    )


class ServerConfig(BaseSettings):
    """服务器配置"""

    model_config = SettingsConfigDict(
        env_prefix="SERVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # 传输模式配置
    transport: Literal["stdio", "http", "sse"] = Field(
        default="http",
        description="传输模式: stdio, http, sse"
    )

    # Elicitation 配置
    enable_elicitation: bool = Field(
        default=True,
        description="是否启用 elicitation 交互式询问（设为 False 时需要显式提供参数）"
    )
    host: str = Field(default="0.0.0.0", description="HTTP/SSE 服务器绑定地址")
    port: int = Field(default=8000, ge=1, le=65535, description="HTTP/SSE 服务器端口")

    # 性能配置（已迁移到 CacheConfig，保留兼容性）
    cache_update_interval: int = Field(
        default=5, ge=1, le=60,
        alias="CACHE_UPDATE_INTERVAL",
        description="元数据缓存更新间隔(分钟)，已弃用，请使用 CACHE_UPDATE_INTERVAL"
    )
    max_result_rows: int = Field(
        default=1000, ge=1, le=100000,
        alias="MAX_RESULT_ROWS",
        description="查询结果最大行数"
    )
    query_timeout: int = Field(
        default=60, ge=1, le=3600,
        alias="QUERY_TIMEOUT",
        description="查询超时时间(秒)"
    )


class AppConfig(BaseSettings):
    """应用总配置"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    db: DatabaseConfig = Field(default_factory=DatabaseConfig)
    pool: PoolConfig = Field(default_factory=PoolConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    lineage: LineageConfig = Field(default_factory=LineageConfig)

    # 日志配置
    log_level: str = Field(default="INFO", description="日志级别")
    log_file: str = Field(default="mysql_mcp_server.log", description="日志文件路径")


def load_config() -> AppConfig:
    """加载配置"""
    return AppConfig()
