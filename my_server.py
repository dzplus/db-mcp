import os
import json
import time
import logging
import threading
import queue
import mysql.connector
from mysql.connector import pooling
from typing import Dict, List, Any, Optional, Union
from fastmcp import FastMCP
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mysql_mcp_server.log')
    ]
)
logger = logging.getLogger('mysql_mcp_server')

# 加载环境变量
load_dotenv()

# 加载配置
def load_config():
    config = {
        'db': {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', '3306')),
            'database': os.getenv('MYSQL_DATABASE', 'your_database'),
            'user': os.getenv('MYSQL_USER', 'your_user'),
            'password': os.getenv('MYSQL_PASSWORD', 'your_password'),
        },
        'pool': {
            'pool_size': int(os.getenv('POOL_SIZE', '10')),
            'pool_name': os.getenv('POOL_NAME', 'mysql_pool'),
            'pool_reset_session': os.getenv('POOL_RESET_SESSION', 'true').lower() == 'true',
            'connect_timeout': int(os.getenv('CONNECT_TIMEOUT', '30')),
        },
        'server': {
            'max_threads': int(os.getenv('MAX_THREADS', '10')),
            'cache_update_interval': int(os.getenv('CACHE_UPDATE_INTERVAL', '5')),  # 分钟
            'max_result_rows': int(os.getenv('MAX_RESULT_ROWS', '1000')),
            'query_timeout': int(os.getenv('QUERY_TIMEOUT', '60')),  # 秒
        }
    }
    
    # 验证配置
    validate_config(config)
    
    return config

# 验证配置
def validate_config(config):
    # 验证数据库配置
    db_config = config['db']
    if not db_config['host']:
        logger.warning("数据库主机未配置，使用默认值: localhost")
    
    if db_config['port'] <= 0 or db_config['port'] > 65535:
        logger.warning(f"数据库端口 {db_config['port']} 无效，使用默认值: 3306")
        db_config['port'] = 3306
    
    # 验证连接池配置
    pool_config = config['pool']
    if pool_config['pool_size'] <= 0:
        logger.warning(f"连接池大小 {pool_config['pool_size']} 无效，使用默认值: 10")
        pool_config['pool_size'] = 10
    
    if pool_config['connect_timeout'] <= 0:
        logger.warning(f"连接超时时间 {pool_config['connect_timeout']} 无效，使用默认值: 30")
        pool_config['connect_timeout'] = 30
    
    # 验证服务器配置
    server_config = config['server']
    if server_config['max_threads'] <= 0:
        logger.warning(f"最大线程数 {server_config['max_threads']} 无效，使用默认值: 10")
        server_config['max_threads'] = 10
    
    if server_config['cache_update_interval'] <= 0:
        logger.warning(f"缓存更新间隔 {server_config['cache_update_interval']} 无效，使用默认值: 5")
        server_config['cache_update_interval'] = 5
    
    if server_config['max_result_rows'] <= 0:
        logger.warning(f"最大结果行数 {server_config['max_result_rows']} 无效，使用默认值: 1000")
        server_config['max_result_rows'] = 1000
        
    if server_config['query_timeout'] <= 0:
        logger.warning(f"查询超时时间 {server_config['query_timeout']} 无效，使用默认值: 60")
        server_config['query_timeout'] = 60

# 加载配置
config = load_config()

# 数据库配置
DB_CONFIG = config['db']
POOL_CONFIG = config['pool']

# 其他配置
MAX_THREADS = config['server']['max_threads']
CACHE_UPDATE_INTERVAL = config['server']['cache_update_interval']
MAX_RESULT_ROWS = config['server']['max_result_rows']
QUERY_TIMEOUT = config['server']['query_timeout']

# 创建MCP服务器
mcp = FastMCP("MySQL Database MCP Server")

# 元数据缓存
metadata_cache = {
    'databases': [],
    'tables': {},
    'columns': {},
    'relations': {},
    'last_update': 0
}

# 服务器状态
server_status = {
    'start_time': time.time(),
    'db_connected': False,
    'last_health_check': 0,
    'query_count': 0,
    'error_count': 0,
    'active_connections': 0,
    'connection_wait_time': 0,
    'query_time_total': 0
}

# 线程锁，用于控制并发访问
cache_lock = threading.Lock()
status_lock = threading.Lock()

# 连接池
db_pool = None

# 初始化连接池
def init_connection_pool():
    global db_pool
    try:
        pool_params = {
            **DB_CONFIG,
            'pool_name': POOL_CONFIG['pool_name'],
            'pool_size': POOL_CONFIG['pool_size'],
            'pool_reset_session': POOL_CONFIG['pool_reset_session'],
            'connect_timeout': POOL_CONFIG['connect_timeout']
        }
        logger.info(f"初始化数据库连接池，大小: {POOL_CONFIG['pool_size']}")
        db_pool = pooling.MySQLConnectionPool(**pool_params)
        return True
    except Exception as e:
        logger.error(f"初始化连接池失败: {e}")
        return False

# 健康检查函数
def health_check():
    with status_lock:
        current_time = time.time()
        # 每分钟检查一次
        if server_status['last_health_check'] > 0 and \
           (current_time - server_status['last_health_check']) < 60:
            return server_status['db_connected']
        
        logger.debug("执行数据库健康检查")
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            server_status['db_connected'] = True
            logger.debug("数据库连接正常")
        except Exception as e:
            server_status['db_connected'] = False
            logger.warning(f"数据库连接异常: {e}")
        finally:
            if conn:
                release_connection(conn)
        
        server_status['last_health_check'] = current_time
        return server_status['db_connected']

# 数据库连接函数
def get_db_connection():
    global db_pool
    start_time = time.time()
    
    try:
        # 如果连接池未初始化或不可用，尝试重新初始化
        if db_pool is None:
            init_connection_pool()
            if db_pool is None:
                raise Exception("连接池初始化失败")
        
        # 从连接池获取连接
        conn = db_pool.get_connection()
        
        # 更新连接统计信息
        with status_lock:
            server_status['active_connections'] += 1
            server_status['connection_wait_time'] += time.time() - start_time
        
        logger.debug("从连接池获取连接成功")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"从连接池获取连接错误: {err}")
        raise Exception(f"数据库连接错误: {err}")
    except Exception as e:
        logger.error(f"获取数据库连接时发生错误: {e}")
        # 如果连接池出错，尝试直接连接
        try:
            logger.warning("连接池不可用，尝试直接连接数据库")
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
        except mysql.connector.Error as err:
            logger.error(f"直接连接数据库失败: {err}")
            raise Exception(f"数据库连接错误: {err}")

# 释放数据库连接
def release_connection(conn):
    try:
        # 检查连接是否来自连接池
        if hasattr(conn, '_pool_name') and conn._pool_name == POOL_CONFIG['pool_name']:
            # 归还到连接池
            conn.close()
            with status_lock:
                server_status['active_connections'] -= 1
            logger.debug("连接已归还到连接池")
        else:
            # 直接关闭连接
            conn.close()
            with status_lock:
                server_status['active_connections'] -= 1
            logger.debug("直接连接已关闭")
    except Exception as e:
        logger.error(f"释放连接时出错: {e}")
        # 尝试强制关闭
        try:
            conn.close()
        except:
            pass

# 更新元数据缓存
def update_metadata_cache(force=False):
    # 使用非阻塞锁尝试获取缓存锁
    if not cache_lock.acquire(blocking=False):
        logger.debug("另一个线程正在更新缓存，跳过本次更新")
        return
    
    try:
        current_time = time.time()
        # 检查是否需要更新缓存
        if not force and metadata_cache['last_update'] > 0 and \
           (current_time - metadata_cache['last_update']) < (CACHE_UPDATE_INTERVAL * 60):
            logger.debug("元数据缓存仍然有效，跳过更新")
            return
        
        logger.info("开始更新元数据缓存")
        start_time = time.time()
        
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # 使用异步方式更新缓存，先更新基本信息
            update_basic_metadata(cursor)
            
            # 获取当前数据库的所有表
            cursor.execute("SHOW TABLES")
            tables = [row[f'Tables_in_{DB_CONFIG["database"]}'] for row in cursor.fetchall()]
            
            # 创建新的缓存数据结构，避免更新过程中的不一致
            new_tables = {}
            new_columns = {}
            
            # 批量获取表信息，减少查询次数
            update_table_info(cursor, tables, new_tables, new_columns)
            
            # 获取外键关系
            update_relations(cursor)
            
            # 原子性地更新缓存
            metadata_cache['tables'] = new_tables
            metadata_cache['columns'] = new_columns
            
            # 更新缓存时间
            metadata_cache['last_update'] = current_time
            
            elapsed_time = time.time() - start_time
            logger.info(f"元数据缓存更新完成，耗时 {elapsed_time:.2f} 秒，共 {len(metadata_cache['databases'])} 个数据库，{len(metadata_cache['tables'])} 个表")
            
        except Exception as e:
            logger.error(f"更新元数据缓存时出错: {e}")
            # 不抛出异常，避免影响正常业务
        finally:
            if cursor:
                cursor.close()
            if conn:
                release_connection(conn)
    finally:
        cache_lock.release()

# 更新基本元数据信息
def update_basic_metadata(cursor):
    # 获取所有数据库
    cursor.execute("SHOW DATABASES")
    metadata_cache['databases'] = [row['Database'] for row in cursor.fetchall()]

# 更新表信息
def update_table_info(cursor, tables, new_tables, new_columns):
    for table in tables:
        try:
            # 表信息
            cursor.execute(f"SHOW TABLE STATUS LIKE '{table}'")
            table_info = cursor.fetchone()
            if not table_info:
                continue
                
            new_tables[table] = {
                'name': table,
                'engine': table_info['Engine'],
                'rows': table_info['Rows'],
                'comment': table_info['Comment'],
                'create_time': table_info['Create_time'].isoformat() if table_info['Create_time'] else None,
            }
            
            # 列信息
            cursor.execute(f"SHOW FULL COLUMNS FROM `{table}`")
            columns = cursor.fetchall()
            new_columns[table] = [{
                'name': col['Field'],
                'type': col['Type'],
                'null': col['Null'],
                'key': col['Key'],
                'default': col['Default'],
                'extra': col['Extra'],
                'comment': col['Comment']
            } for col in columns]
            
            # 索引信息
            cursor.execute(f"SHOW INDEX FROM `{table}`")
            indexes = cursor.fetchall()
            index_dict = {}
            for idx in indexes:
                index_name = idx['Key_name']
                if index_name not in index_dict:
                    index_dict[index_name] = {
                        'name': index_name,
                        'unique': not idx['Non_unique'],
                        'columns': []
                    }
                index_dict[index_name]['columns'].append({
                    'name': idx['Column_name'],
                    'order': idx['Seq_in_index']
                })
            new_tables[table]['indexes'] = list(index_dict.values())
        except Exception as e:
            logger.error(f"更新表 {table} 信息时出错: {e}")
            # 继续处理其他表

# 更新关系信息
def update_relations(cursor):
    try:
        cursor.execute("""
            SELECT 
                TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, 
                REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE 
                REFERENCED_TABLE_SCHEMA = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (DB_CONFIG['database'],))
        
        relations = cursor.fetchall()
        new_relations = {}
        for rel in relations:
            table_name = rel['TABLE_NAME']
            if table_name not in new_relations:
                new_relations[table_name] = []
            
            new_relations[table_name].append({
                'constraint_name': rel['CONSTRAINT_NAME'],
                'column': rel['COLUMN_NAME'],
                'referenced_table': rel['REFERENCED_TABLE_NAME'],
                'referenced_column': rel['REFERENCED_COLUMN_NAME']
            })
        
        # 原子性地更新关系缓存
        metadata_cache['relations'] = new_relations
    except Exception as e:
        logger.error(f"更新关系信息时出错: {e}")

# MCP工具：获取数据库列表
@mcp.tool
def get_databases() -> List[str]:
    """获取所有数据库列表"""
    update_metadata_cache()
    return metadata_cache['databases']

# MCP工具：获取表列表
@mcp.tool
def get_tables() -> List[Dict[str, Any]]:
    """获取当前数据库中的所有表信息"""
    update_metadata_cache()
    return list(metadata_cache['tables'].values())

# MCP工具：获取表结构
@mcp.tool
def get_table_structure(table_name: str) -> Dict[str, Any]:
    """获取指定表的结构信息，包括列、索引等"""
    update_metadata_cache()
    if table_name not in metadata_cache['tables']:
        raise Exception(f"表 '{table_name}' 不存在")
    
    return {
        'table': metadata_cache['tables'][table_name],
        'columns': metadata_cache['columns'][table_name],
        'relations': metadata_cache['relations'].get(table_name, [])
    }

# MCP工具：获取数据库关系
@mcp.tool
def get_database_relations() -> Dict[str, List[Dict[str, Any]]]:
    """获取数据库中的所有表关系（外键关系）"""
    update_metadata_cache()
    return metadata_cache['relations']

# 查询执行队列和工作线程
query_queue = queue.Queue()
query_workers = []
query_results = {}
query_lock = threading.Lock()
query_id_counter = 0

# 初始化查询工作线程
def init_query_workers():
    global query_workers
    for i in range(MAX_THREADS):
        worker = threading.Thread(target=query_worker_thread, args=(i,), daemon=True)
        worker.start()
        query_workers.append(worker)
        logger.info(f"启动查询工作线程 #{i}")

# 查询工作线程函数
def query_worker_thread(worker_id):
    logger.debug(f"查询工作线程 #{worker_id} 已启动")
    while True:
        try:
            # 从队列获取查询任务
            query_id, sql, params = query_queue.get()
            logger.debug(f"工作线程 #{worker_id} 处理查询 #{query_id}")
            
            # 执行查询
            result = execute_query_internal(sql, params)
            
            # 存储结果
            with query_lock:
                query_results[query_id] = result
                
            # 标记任务完成
            query_queue.task_done()
            logger.debug(f"工作线程 #{worker_id} 完成查询 #{query_id}")
        except Exception as e:
            logger.error(f"查询工作线程 #{worker_id} 处理查询时出错: {e}")

# MCP工具：执行SQL查询
@mcp.tool
def execute_query(sql: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
    """执行SQL查询语句，返回查询结果
    
    Args:
        sql: SQL查询语句
        params: 参数化查询的参数列表
        
    Returns:
        包含查询结果的字典
    """
    global query_id_counter
    
    # 记录查询信息（不记录可能包含敏感信息的参数）
    log_sql = sql.replace('\n', ' ').strip()
    if len(log_sql) > 100:
        log_sql = log_sql[:97] + '...'
    logger.info(f"执行SQL查询: {log_sql}")
    
    # 更新查询计数
    with status_lock:
        server_status['query_count'] += 1
    
    # 生成唯一查询ID
    with query_lock:
        query_id = query_id_counter
        query_id_counter += 1
    
    # 将查询放入队列
    query_queue.put((query_id, sql, params))
    logger.debug(f"查询 #{query_id} 已加入队列")
    
    # 等待查询完成
    start_wait = time.time()
    while True:
        with query_lock:
            if query_id in query_results:
                result = query_results[query_id]
                del query_results[query_id]  # 清理结果缓存
                return result
        
        # 检查是否超时
        if time.time() - start_wait > QUERY_TIMEOUT:
            logger.error(f"查询 #{query_id} 等待超时")
            with status_lock:
                server_status['error_count'] += 1
            return {
                'success': False,
                'error': f"查询执行超时 (>{QUERY_TIMEOUT}秒)"
            }
        
        # 短暂休眠，避免CPU占用过高
        time.sleep(0.01)

# 内部查询执行函数
def execute_query_internal(sql: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
    conn = None
    cursor = None
    start_time = time.time()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 执行查询
        if params:
            logger.debug(f"使用参数化查询，参数数量: {len(params)}")
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        # 处理结果
        if cursor.description:  # 如果有结果集
            rows = cursor.fetchmany(MAX_RESULT_ROWS)
            has_more = cursor.fetchone() is not None
            
            # 获取列信息
            columns = []
            for i, col in enumerate(cursor.description):
                col_type = cursor.description[i][1]
                type_name = None
                
                # 处理不同类型的情况
                if hasattr(col_type, '__name__'):
                    type_name = col_type.__name__
                elif isinstance(col_type, int):
                    # MySQL-Connector 有时会返回整数类型代码
                    type_name = f"type_{col_type}"
                else:
                    type_name = str(col_type)
                
                columns.append({
                    'name': col[0],
                    'type': type_name
                })
            
            result = {
                'success': True,
                'columns': columns,
                'rows': rows,
                'row_count': len(rows),
                'has_more': has_more,
                'execution_time': time.time() - start_time,
                'affected_rows': cursor.rowcount if not has_more else None
            }
        else:  # 如果是更新/插入/删除等操作
            conn.commit()
            result = {
                'success': True,
                'affected_rows': cursor.rowcount,
                'execution_time': time.time() - start_time,
                'last_insert_id': cursor.lastrowid
            }
        
        execution_time = time.time() - start_time
        with status_lock:
            server_status['query_time_total'] += execution_time
            
        if 'rows' in result:
            logger.info(f"查询完成，返回 {result['row_count']} 行结果，执行时间: {execution_time:.3f}秒")
            if result['has_more']:
                logger.info(f"结果集超过最大限制 {MAX_RESULT_ROWS} 行，已截断")
        else:
            logger.info(f"更新操作完成，影响 {result['affected_rows']} 行，执行时间: {execution_time:.3f}秒")
        return result
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        logger.error(f"执行SQL查询时出错: {e}")
        
        # 更新错误计数
        with status_lock:
            server_status['error_count'] += 1
            
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)

# MCP工具：获取服务器配置信息
@mcp.tool
def get_server_config() -> Dict[str, Any]:
    """获取服务器当前配置信息"""
    return {
        'database': DB_CONFIG['database'],
        'host': DB_CONFIG['host'],
        'port': DB_CONFIG['port'],
        'pool': {
            'pool_size': POOL_CONFIG['pool_size'],
            'pool_name': POOL_CONFIG['pool_name'],
            'connect_timeout': POOL_CONFIG['connect_timeout']
        },
        'max_threads': MAX_THREADS,
        'cache_update_interval': CACHE_UPDATE_INTERVAL,
        'max_result_rows': MAX_RESULT_ROWS,
        'query_timeout': QUERY_TIMEOUT
    }

# MCP工具：获取服务器状态
@mcp.tool
def get_server_status() -> Dict[str, Any]:
    """获取服务器当前运行状态"""
    # 执行健康检查
    db_connected = health_check()
    
    # 获取查询队列状态
    queue_size = query_queue.qsize()
    queue_empty = query_queue.empty()
    
    with status_lock:
        uptime = time.time() - server_status['start_time']
        avg_query_time = 0
        if server_status['query_count'] > 0:
            avg_query_time = server_status['query_time_total'] / server_status['query_count']
            
        avg_connection_wait = 0
        if server_status['query_count'] > 0:
            avg_connection_wait = server_status['connection_wait_time'] / server_status['query_count']
            
        status = {
            'version': '0.1.0',
            'uptime': int(uptime),  # 秒
            'uptime_formatted': f"{int(uptime // 86400)}天 {int((uptime % 86400) // 3600)}小时 {int((uptime % 3600) // 60)}分钟",
            'db_connected': db_connected,
            'query_count': server_status['query_count'],
            'error_count': server_status['error_count'],
            'active_connections': server_status['active_connections'],
            'avg_query_time': round(avg_query_time, 3),
            'avg_connection_wait': round(avg_connection_wait, 3),
            'query_queue': {
                'size': queue_size,
                'is_empty': queue_empty,
                'worker_count': len(query_workers)
            },
            'cache_status': {
                'last_update': metadata_cache['last_update'],
                'last_update_formatted': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(metadata_cache['last_update'])) if metadata_cache['last_update'] > 0 else 'Never',
                'database_count': len(metadata_cache['databases']),
                'table_count': len(metadata_cache['tables']),
                'next_update': metadata_cache['last_update'] + (CACHE_UPDATE_INTERVAL * 60) if metadata_cache['last_update'] > 0 else 0
            }
        }
    
    return status

# MCP工具：刷新元数据缓存
@mcp.tool
def refresh_metadata_cache() -> Dict[str, Any]:
    """强制刷新元数据缓存"""
    # 使用非阻塞方式尝试获取锁
    if not cache_lock.acquire(blocking=False):
        return {
            'success': False,
            'error': "另一个刷新操作正在进行中",
            'last_update': metadata_cache['last_update']
        }
    
    try:
        # 重置上次更新时间，强制更新
        metadata_cache['last_update'] = 0
        update_metadata_cache(force=True)
        return {
            'success': True,
            'last_update': metadata_cache['last_update']
        }
    finally:
        cache_lock.release()

# 后台缓存更新线程
def background_cache_updater():
    logger.info("启动后台缓存更新线程")
    while True:
        try:
            # 检查是否需要更新缓存
            current_time = time.time()
            if metadata_cache['last_update'] > 0 and \
               (current_time - metadata_cache['last_update']) >= (CACHE_UPDATE_INTERVAL * 60):
                logger.debug("后台线程开始更新缓存")
                update_metadata_cache()
            
            # 等待一段时间再检查
            time.sleep(60)  # 每分钟检查一次
        except Exception as e:
            logger.error(f"后台缓存更新线程出错: {e}")
            time.sleep(60)  # 出错后等待一分钟再继续

if __name__ == "__main__":
    logger.info(f"启动MySQL数据库MCP服务器，版本: 0.1.0")
    logger.info(f"配置信息: 数据库={DB_CONFIG['database']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}, 连接池大小={POOL_CONFIG['pool_size']}, 最大线程数={MAX_THREADS}, 缓存更新间隔={CACHE_UPDATE_INTERVAL}分钟")
    
    # 初始化连接池
    pool_initialized = init_connection_pool()
    if not pool_initialized:
        logger.warning("初始化连接池失败，将使用直接连接模式")
    else:
        logger.info("连接池初始化成功")
    
    # 初始化查询工作线程
    init_query_workers()
    logger.info(f"已启动 {MAX_THREADS} 个查询工作线程")
    
    # 初始化缓存
    try:
        update_metadata_cache()
        logger.info(f"已成功连接到数据库并初始化元数据缓存")
    except Exception as e:
        logger.warning(f"初始化元数据缓存失败: {e}")
        logger.warning("服务器将继续启动，但某些功能可能不可用，直到数据库连接恢复。")
    
    # 启动后台缓存更新线程
    cache_updater = threading.Thread(target=background_cache_updater, daemon=True)
    cache_updater.start()
    logger.info("后台缓存更新线程已启动")
    
    # 注册工具信息
    tool_count = len([attr for attr in dir(mcp) if callable(getattr(mcp, attr)) and not attr.startswith('_')])
    logger.info(f"已注册 {tool_count} 个MCP工具")
    
    # 启动MCP服务器
    logger.info("MCP服务器启动中...")
    try:
        mcp.run()
    except KeyboardInterrupt:
        logger.info("接收到中断信号，服务器正在关闭...")
    except Exception as e:
        logger.error(f"服务器运行时出错: {e}")
    finally:
        logger.info("MCP服务器已关闭")