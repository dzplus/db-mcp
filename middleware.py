"""
FastMCP 中间件模块

提供统一的日志记录、性能计时和错误处理
"""

import logging
import time
from typing import Any

from fastmcp.server.middleware import Middleware

logger = logging.getLogger("mysql_mcp_server")


class TimingMiddleware(Middleware):
    """
    性能计时中间件

    记录每个工具调用的执行时间
    """

    async def on_call_tool(self, context: Any, call_next) -> Any:
        tool_name = getattr(context, "tool_name", "unknown")
        start_time = time.time()

        try:
            result = await call_next(context)
            elapsed = time.time() - start_time

            # 记录执行时间
            if elapsed > 1.0:
                logger.warning(f"[SLOW] Tool '{tool_name}' took {elapsed:.3f}s")
            else:
                logger.debug(f"Tool '{tool_name}' completed in {elapsed:.3f}s")

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Tool '{tool_name}' failed after {elapsed:.3f}s: {e}")
            raise


class ErrorHandlingMiddleware(Middleware):
    """
    错误处理中间件

    统一捕获和处理工具执行中的错误
    """

    def __init__(self, mask_details: bool = False):
        """
        Args:
            mask_details: 是否隐藏内部错误详情（生产环境建议开启）
        """
        self.mask_details = mask_details
        self._error_count = 0

    @property
    def error_count(self) -> int:
        return self._error_count

    async def on_call_tool(self, context: Any, call_next) -> Any:
        tool_name = getattr(context, "tool_name", "unknown")

        try:
            return await call_next(context)

        except Exception as e:
            self._error_count += 1
            logger.error(f"Tool '{tool_name}' error: {type(e).__name__}: {e}")

            # 重新抛出，让 FastMCP 处理
            raise


class LoggingMiddleware(Middleware):
    """
    日志记录中间件

    记录所有工具调用的输入和输出
    """

    def __init__(self, log_inputs: bool = True, log_outputs: bool = False):
        """
        Args:
            log_inputs: 是否记录输入参数
            log_outputs: 是否记录输出结果
        """
        self.log_inputs = log_inputs
        self.log_outputs = log_outputs

    async def on_call_tool(self, context: Any, call_next) -> Any:
        tool_name = getattr(context, "tool_name", "unknown")

        # 记录输入
        if self.log_inputs:
            arguments = getattr(context, "arguments", {})
            # 过滤敏感信息
            safe_args = self._sanitize_args(arguments)
            logger.info(f"Tool '{tool_name}' called with: {safe_args}")

        result = await call_next(context)

        # 记录输出
        if self.log_outputs:
            logger.debug(f"Tool '{tool_name}' returned: {type(result).__name__}")

        return result

    def _sanitize_args(self, args: dict) -> dict:
        """过滤敏感参数"""
        sensitive_keys = {"password", "secret", "token", "key", "credential"}
        sanitized = {}

        for k, v in args.items():
            if any(s in k.lower() for s in sensitive_keys):
                sanitized[k] = "***"
            elif isinstance(v, str) and len(v) > 100:
                sanitized[k] = v[:100] + "..."
            else:
                sanitized[k] = v

        return sanitized


class StatisticsMiddleware(Middleware):
    """
    统计中间件

    收集工具调用的统计信息
    """

    def __init__(self):
        self._call_counts: dict[str, int] = {}
        self._total_time: dict[str, float] = {}
        self._error_counts: dict[str, int] = {}

    async def on_call_tool(self, context: Any, call_next) -> Any:
        tool_name = getattr(context, "tool_name", "unknown")
        start_time = time.time()

        # 增加调用计数
        self._call_counts[tool_name] = self._call_counts.get(tool_name, 0) + 1

        try:
            result = await call_next(context)

            # 记录执行时间
            elapsed = time.time() - start_time
            self._total_time[tool_name] = self._total_time.get(tool_name, 0) + elapsed

            return result

        except Exception as e:
            # 记录错误
            self._error_counts[tool_name] = self._error_counts.get(tool_name, 0) + 1
            raise

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息"""
        stats = {}
        for tool_name in self._call_counts:
            calls = self._call_counts[tool_name]
            total_time = self._total_time.get(tool_name, 0)
            errors = self._error_counts.get(tool_name, 0)

            stats[tool_name] = {
                "calls": calls,
                "errors": errors,
                "total_time": round(total_time, 3),
                "avg_time": round(total_time / calls, 3) if calls > 0 else 0,
                "error_rate": round(errors / calls * 100, 1) if calls > 0 else 0,
            }

        return stats
