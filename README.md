# MySQL数据库MCP服务器

这是一个基于FastMCP框架的MySQL数据库MCP服务器，用于为AI提供MySQL数据库的元数据信息以及执行SQL的能力，支持AI基于数据库数据进行分析、处理等操作。

## 功能特点

- **元数据信息提供**：获取数据库中的库名、表名、字段名、数据类型、注释、索引等表结构相关信息
- **关系信息获取**：获取外键约束、表关联关系等数据关系信息
- **元数据缓存**：对获取的元数据信息进行缓存，提升效率
- **SQL执行**：支持执行任何SQL语句，并返回格式化结果
- **参数化查询**：采用参数化查询方式，防止SQL注入
- **健康检查**：定期检查数据库连接状态，确保服务可用性
- **服务器状态监控**：提供服务器运行状态、查询统计等信息
- **完善的日志记录**：详细记录服务器运行、查询执行等信息，便于调试和监控
- **配置验证**：启动时验证配置的有效性，确保服务正常运行
- **数据库连接池**：使用连接池管理数据库连接，提高连接复用效率
- **并发查询处理**：基于工作线程池和查询队列的并发查询处理机制
- **非阻塞缓存更新**：后台线程自动更新缓存，不影响正常查询
- **查询超时控制**：支持设置查询超时时间，避免长时间运行的查询占用资源

## 安装依赖

```bash
pip install -r requirements.txt
```

或者使用uv：

```bash
uv pip install -e .
```

## 配置说明

服务器支持通过环境变量或.env文件进行配置，主要配置项包括：

- **数据库连接配置**
  - `MYSQL_HOST`：数据库主机地址，默认为localhost
  - `MYSQL_PORT`：数据库端口号，默认为3306
  - `MYSQL_DATABASE`：数据库名称
  - `MYSQL_USER`：数据库用户名
  - `MYSQL_PASSWORD`：数据库密码

- **服务器配置**
  - `MAX_THREADS`：允许并发执行的线程数，默认为10
  - `CACHE_UPDATE_INTERVAL`：缓存更新时间间隔（分钟），默认为5
  - `MAX_RESULT_ROWS`：查询结果最大返回行数，默认为1000
  - `QUERY_TIMEOUT`：查询超时时间（秒），默认为60

- **连接池配置**
  - `POOL_SIZE`：连接池大小，默认为10
  - `POOL_NAME`：连接池名称，默认为"mysql_pool"
  - `CONNECT_TIMEOUT`：连接超时时间（秒），默认为5

## 使用方法

### 启动服务器

```bash
python my_server.py
```

### 客户端示例

```python
import asyncio
from fastmcp import Client

client = Client("my_server.py")

async def main():
    async with client:
        # 获取数据库列表
        databases = await client.call_tool("get_databases", {})
        print(databases)
        
        # 执行SQL查询
        result = await client.call_tool("execute_query", {
            "sql": "SELECT * FROM users LIMIT 5"
        })
        print(result)

asyncio.run(main())
```

## API说明

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

### 执行SQL查询

```python
# 简单查询
await client.call_tool("execute_query", {"sql": "SELECT * FROM users LIMIT 5"})

# 参数化查询
await client.call_tool("execute_query", {
    "sql": "SELECT * FROM users WHERE id > %s LIMIT %s",
    "params": [10, 5]
})
```

### 获取服务器配置

```python
# 获取服务器配置信息
config = await client.call_tool("get_server_config", {})

# 数据库配置
print(f"数据库: {config['database']}")
print(f"主机: {config['host']}")
print(f"端口: {config['port']}")

# 连接池配置
print(f"连接池大小: {config['pool']['pool_size']}")
print(f"连接池名称: {config['pool']['pool_name']}")
print(f"连接超时: {config['pool']['connect_timeout']}秒")

# 服务器配置
print(f"最大线程数: {config['max_threads']}")
print(f"缓存更新间隔: {config['cache_update_interval']}分钟")
print(f"最大结果行数: {config['max_result_rows']}")
print(f"查询超时: {config['query_timeout']}秒")
```

### 获取服务器状态

```python
# 获取服务器运行状态、查询统计和缓存信息
status = await client.call_tool("get_server_status", {})

# 基本状态信息
print(f"服务器运行时间: {status['uptime_formatted']}")
print(f"数据库连接状态: {'正常' if status['db_connected'] else '异常'}")

# 查询统计信息
print(f"查询次数: {status['query_count']}")
print(f"错误次数: {status['error_count']}")
print(f"平均查询时间: {status['avg_query_time']}秒")
print(f"平均连接等待时间: {status['avg_connection_wait']}秒")

# 连接池状态
print(f"活跃连接数: {status['active_connections']}")

# 查询队列状态
print(f"队列大小: {status['query_queue']['size']}")
print(f"队列是否为空: {status['query_queue']['is_empty']}")
print(f"工作线程数: {status['query_queue']['worker_count']}")

# 缓存状态
print(f"缓存上次更新: {status['cache_status']['last_update_formatted']}")
print(f"数据库数量: {status['cache_status']['database_count']}")
print(f"表数量: {status['cache_status']['table_count']}")
```

### 刷新元数据缓存

```python
await client.call_tool("refresh_metadata_cache", {})
```

## 配置文件示例

可以使用以下格式的配置文件来配置MCP服务器：

```json
{
  "mcpServers": {
    "mysql": {
      "command": "python",
      "args": ["-m", "db_mcp.mcp_server"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_DATABASE": "your_database",
        "MYSQL_USER": "your_user",
        "MYSQL_PASSWORD": "your_password",
        "MAX_THREADS": "10",
        "CACHE_UPDATE_INTERVAL": "5",
        "MAX_RESULT_ROWS": "1000",
        "QUERY_TIMEOUT": "60",
        "POOL_SIZE": "10",
        "POOL_NAME": "mysql_pool",
        "CONNECT_TIMEOUT": "5"
      }
    }
  }
}
```

## 性能优化设计

### 数据库连接池

系统使用MySQL连接池技术，有效管理数据库连接资源：

- **连接复用**：避免频繁创建和关闭连接的开销
- **连接数控制**：限制最大连接数，防止数据库连接耗尽
- **连接健康检查**：自动检测和替换失效连接
- **连接超时控制**：设置获取连接的超时时间，避免长时间等待
- **降级机制**：连接池初始化失败时，自动降级为直接连接模式

### 并发查询处理

采用基于队列和工作线程池的并发处理机制：

- **查询队列**：所有查询请求先进入队列，避免直接占用线程资源
- **工作线程池**：固定数量的工作线程从队列获取查询任务并执行
- **查询超时控制**：设置查询执行的最大时间，防止长时间运行的查询占用资源
- **结果异步返回**：查询结果通过异步方式返回，不阻塞工作线程
- **查询统计**：记录查询执行时间、等待时间等统计信息，便于性能分析

### 元数据缓存优化

优化元数据缓存机制，提高缓存效率：

- **非阻塞更新**：缓存更新不阻塞查询操作，使用读写锁分离读写操作
- **后台自动更新**：独立的后台线程定期更新缓存，无需手动触发
- **原子性更新**：缓存更新过程保证原子性，避免部分更新导致的数据不一致
- **细粒度错误处理**：缓存更新过程中的错误不影响整体缓存，只影响出错部分
- **强制刷新机制**：提供手动强制刷新接口，用于特殊情况下的缓存更新

## 注意事项

- 请确保数据库连接信息正确，否则服务器将无法正常工作
- 对于大型数据库，建议适当增加缓存更新间隔，减少数据库压力
- 对于高并发场景，可以适当增加MAX_THREADS值和POOL_SIZE值，但需要注意：
  - MAX_THREADS值不应超过POOL_SIZE，以避免线程等待连接
  - POOL_SIZE不应过大，以免占用过多数据库资源
- 对于查询结果较大的情况，可以适当增加MAX_RESULT_ROWS值，但会增加内存占用
- 对于复杂查询，可以适当增加QUERY_TIMEOUT值，但过长的超时时间可能导致资源长时间占用
- 连接池初始化失败时，系统会自动降级为直接连接模式，但性能会受到影响
- 后台缓存更新线程会自动根据CACHE_UPDATE_INTERVAL设置的时间间隔更新缓存，无需手动刷新