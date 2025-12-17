# MySQL 数据库 MCP 服务器

这是一个基于 FastMCP 框架的 MySQL 数据库 MCP 服务器，用于为 AI 提供 MySQL 数据库的元数据信息以及执行 SQL 的能力，支持 AI 基于数据库数据进行分析、处理等操作。

## ✨ v0.2.0 新特性

- **多传输模式支持**：支持 STDIO、HTTP、SSE 三种传输模式
- **完全异步架构**：使用 `aiomysql` 实现真正的异步数据库操作
- **Pydantic 配置**：类型安全的配置管理和验证
- **模块化设计**：代码拆分为多个模块，便于维护和扩展
- **Streamable HTTP**：支持 HTTP 传输的 Streamable HTTP 协议

## 功能特点

- **元数据信息提供**：获取数据库中的库名、表名、字段名、数据类型、注释、索引等表结构相关信息
- **关系信息获取**：获取外键约束、表关联关系等数据关系信息
- **元数据缓存**：对获取的元数据信息进行缓存，提升效率
- **SQL 执行**：支持执行任何 SQL 语句，并返回格式化结果
- **参数化查询**：采用参数化查询方式，防止 SQL 注入
- **健康检查**：定期检查数据库连接状态，确保服务可用性
- **服务器状态监控**：提供服务器运行状态、查询统计等信息
- **异步连接池**：使用 aiomysql 异步连接池管理数据库连接
- **非阻塞缓存更新**：后台任务自动更新缓存，不影响正常查询
- **查询超时控制**：支持设置查询超时时间，避免长时间运行的查询占用资源

## 安装

### 使用 uv (推荐)

```bash
uv sync
```

### 使用 pip

```bash
pip install -e .
```

## 配置说明

服务器支持通过环境变量或 `.env` 文件进行配置。复制示例配置文件：

```bash
cp .env.example .env
```

### 配置项

#### 数据库连接配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `MYSQL_HOST` | 数据库主机地址 | localhost |
| `MYSQL_PORT` | 数据库端口号 | 3306 |
| `MYSQL_DATABASE` | 数据库名称 | mysql |
| `MYSQL_USER` | 数据库用户名 | root |
| `MYSQL_PASSWORD` | 数据库密码 | (空) |

#### 连接池配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `POOL_SIZE` | 连接池大小 | 10 |
| `POOL_MIN_SIZE` | 最小连接数 | 1 |
| `POOL_MAX_SIZE` | 最大连接数 | 20 |
| `CONNECT_TIMEOUT` | 连接超时时间(秒) | 30 |

#### 服务器配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `SERVER_TRANSPORT` | 传输模式 (stdio/http/sse) | stdio |
| `SERVER_HOST` | HTTP/SSE 绑定地址 | 127.0.0.1 |
| `SERVER_PORT` | HTTP/SSE 端口 | 8000 |
| `CACHE_UPDATE_INTERVAL` | 缓存更新间隔(分钟) | 5 |
| `MAX_RESULT_ROWS` | 查询结果最大行数 | 1000 |
| `QUERY_TIMEOUT` | 查询超时时间(秒) | 60 |

#### 日志配置

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| `LOG_LEVEL` | 日志级别 | INFO |
| `LOG_FILE` | 日志文件路径 | mysql_mcp_server.log |

## 使用方法

### 启动服务器

#### STDIO 模式 (默认，用于 Claude Desktop)

```bash
# 直接运行
python server.py

# 或使用命令行工具
db-mcp
```

#### HTTP 模式 (用于网络访问)

```bash
python server.py --transport http --host 0.0.0.0 --port 8000
```

服务器将在 `http://0.0.0.0:8000/mcp` 提供 MCP 端点。

#### SSE 模式

```bash
python server.py --transport sse --port 8080
```

### 命令行参数

```
usage: server.py [-h] [--transport {stdio,http,sse}] [--host HOST] [--port PORT] [--version]

MySQL Database MCP Server

options:
  -h, --help            显示帮助信息
  --transport, -t       传输模式 (stdio/http/sse)
  --host, -H            HTTP/SSE 服务器绑定地址
  --port, -p            HTTP/SSE 服务器端口
  --version, -v         显示版本信息
```

### 客户端示例

#### STDIO 模式客户端

```python
import asyncio
from fastmcp import Client

client = Client("python server.py")

async def main():
    async with client:
        # 获取数据库列表
        databases = await client.call_tool("get_databases", {})
        print(databases)

        # 执行 SQL 查询
        result = await client.call_tool("execute_query", {
            "sql": "SELECT * FROM users LIMIT 5"
        })
        print(result)

asyncio.run(main())
```

#### HTTP 模式客户端

```python
import asyncio
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport

async def main():
    transport = StreamableHttpTransport(url="http://localhost:8000/mcp")
    client = Client(transport)

    async with client:
        # 获取服务器状态
        status = await client.call_tool("get_server_status", {})
        print(status)

        # 执行参数化查询
        result = await client.call_tool("execute_query", {
            "sql": "SELECT * FROM users WHERE id > %s LIMIT %s",
            "params": [10, 5]
        })
        print(result)

asyncio.run(main())
```

## API 说明

### 获取数据库列表

```python
await client.call_tool("get_databases", {})
```

### 获取表列表

```python
await client.call_tool("get_tables", {})
```

### 获取表结构

```python
await client.call_tool("get_table_structure", {"table_name": "users"})
```

### 获取数据库关系

```python
await client.call_tool("get_database_relations", {})
```

### 执行 SQL 查询

```python
# 简单查询
await client.call_tool("execute_query", {"sql": "SELECT * FROM users LIMIT 5"})

# 参数化查询 (防止 SQL 注入)
await client.call_tool("execute_query", {
    "sql": "SELECT * FROM users WHERE id > %s LIMIT %s",
    "params": [10, 5]
})
```

### 获取服务器配置

```python
config = await client.call_tool("get_server_config", {})
```

### 获取服务器状态

```python
status = await client.call_tool("get_server_status", {})
print(f"运行时间: {status['uptime_formatted']}")
print(f"数据库连接: {'正常' if status['db_connected'] else '异常'}")
print(f"查询次数: {status['statistics']['query_count']}")
```

### 刷新元数据缓存

```python
await client.call_tool("refresh_metadata_cache", {})
```

## Claude Desktop 配置

在 Claude Desktop 的配置文件中添加：

### STDIO 模式 (推荐)

```json
{
  "mcpServers": {
    "mysql": {
      "command": "python",
      "args": ["server.py"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_DATABASE": "your_database",
        "MYSQL_USER": "your_user",
        "MYSQL_PASSWORD": "your_password"
      }
    }
  }
}
```

### 使用 uv

```json
{
  "mcpServers": {
    "mysql": {
      "command": "uv",
      "args": ["run", "python", "server.py"],
      "cwd": "/path/to/db-mcp",
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_DATABASE": "your_database",
        "MYSQL_USER": "your_user",
        "MYSQL_PASSWORD": "your_password"
      }
    }
  }
}
```

## 架构设计

### 异步架构

v0.2.0 采用完全异步架构：

- **aiomysql**: 异步 MySQL 连接池
- **asyncio**: Python 异步 I/O 框架
- **FastMCP**: 异步 MCP 服务器框架

### 项目结构

```
db-mcp/
├── server.py            # MCP 服务器主模块
├── config.py            # Pydantic 配置管理
├── database.py          # 异步数据库连接池
├── cache.py             # 元数据缓存管理
├── middleware.py        # 中间件（计时、日志、统计）
├── .env.example         # 配置示例
├── pyproject.toml       # 项目配置
└── README.md            # 文档
```

### 性能优化

- **连接池复用**：避免频繁创建和关闭连接的开销
- **非阻塞 I/O**：使用 asyncio 实现真正的非阻塞操作
- **后台缓存更新**：独立的后台任务定期更新缓存
- **查询超时控制**：使用 asyncio.timeout 实现查询超时

## 注意事项

- 请确保数据库连接信息正确，否则服务器将无法正常工作
- HTTP 模式绑定 `0.0.0.0` 时需注意安全风险，建议配置防火墙或使用身份验证
- 对于大型数据库，建议适当增加缓存更新间隔，减少数据库压力
- 查询超时时间过短可能导致复杂查询失败，请根据实际情况调整

## 许可证

MIT License
