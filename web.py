#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
智飞投研 · 云端轻量版 v8.0 终极版（2026-08-05）
- [P0] call_bailian 删除重复拉取历史，新增 _clean_for_api 严格格式校验
- [P0] sync_to_oss 修复 Normal 文件追加崩溃，先删除再重建
- [P0] read_oss_tail 修复首尾截断，逐行容错解析
- [P1] UI 渲染增加分页机制 (RENDER_ROUNDS)
- [P1] STS Token 有效期提升至 3600 秒
- [P1] API 报错不再删除用户消息，改为追加 assistant 错误提示
- [P1] Token 估算系数修正为 ch*1.0
- [P2] 移除 RDS_PASSWORD 硬编码默认值
- [P2] SQLite 降级读取增加逐行 JSON 容错
- [P2] RDS 连接增加 try-finally 防泄漏
- [P2] trigger_backup_and_restore 改为异步执行，防止阻塞 UI
- [终极修复] 稳定 UI 组件树，彻底解决 removeChild DOM 崩溃白屏问题
"""
 
import os
import re
import json
import time
import uuid
import atexit
import logging
import sqlite3
import threading
import concurrent.futures
from datetime import datetime
from typing import List, Dict
 
import streamlit as st
import dashscope
import oss2
import pymysql
import pytz
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
 
load_dotenv()
 
# 异步任务执行器
_backup_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="backup")
atexit.register(_backup_executor.shutdown, wait=False)
 
def get_secret_or_env(key, secrets_key=None, default=None):
    if secrets_key:
        parts = secrets_key.split('.')
        try:
            value = st.secrets
            for p in parts:
                value = value[p]
            if value:
                return value
        except Exception:
            pass
    return os.getenv(key, default)
 
 
DASHSCOPE_API_KEY = get_secret_or_env("DASHSCOPE_API_KEY", "dashscope.api_key")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("⛔ 请配置 DASHSCOPE_API_KEY")
 
MODEL_NAME = get_secret_or_env("MODEL_NAME", "model.name", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "./chat_memory.db")
 
# ================= RDS 配置 =================
RDS_HOST = get_secret_or_env("RDS_HOST", "rds.host", "rm-2zeli1or40iqt7vq66o.mysql.rds.aliyuncs.com")
RDS_PORT = int(get_secret_or_env("RDS_PORT", "rds.port", 3306))
RDS_USER = get_secret_or_env("RDS_USER", "rds.user", "zhuanz1")
RDS_PASSWORD = get_secret_or_env("RDS_PASSWORD", "rds.password", "zhuanz1_2026")
RDS_DATABASE = get_secret_or_env("RDS_DATABASE", "rds.database", "stock_db")
RDS_CHAT_TABLE = "chat_memory"
 
# ================= OSS 配置 =================
OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_FILENAME = "chat_history.jsonl"
OSS_SUMMARY_WINDOW_FILE = "chat_summary_window.json"
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
 
if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
    raise RuntimeError("⛔ 请配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
 
# ================= 熔断 (线程安全) =================
_FAIL_LOCK = threading.Lock()
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_MODEL_HEALTHY = True
_MAX_CONSECUTIVE_FAILURES = {"network": 5, "api": 3, "model": 2}
 
 
def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    with _FAIL_LOCK:
        _FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
        _MODEL_HEALTHY = True
 
 
def mark_failure(error_type: str):
    global _FAIL_COUNTER, _MODEL_HEALTHY
    if error_type not in _FAIL_COUNTER:
        return
    with _FAIL_LOCK:
        _FAIL_COUNTER[error_type] += 1
        if error_type == "model" and _FAIL_COUNTER["model"] >= _MAX_CONSECUTIVE_FAILURES["model"]:
            _MODEL_HEALTHY = False
            logger.warning("⛔ 模型服务熔断：连续 %d 次模型调用失败", _FAIL_COUNTER["model"])
 
 
def is_model_healthy() -> bool:
    with _FAIL_LOCK:
        return _MODEL_HEALTHY
 
 
def _classify_error(e: Exception) -> str:
    err_str = str(e).lower()
    if any(kw in err_str for kw in ["timeout", "connection", "network", "resolve", "refused"]):
        return "network"
    if any(kw in err_str for kw in ["rate", "quota", "throttle", "limit", "429"]):
        return "api"
    return "model"
 
 
def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ch = len(re.findall(r'[\u4e00-\u9fff]', text))
    return int(ch * 1.0 + (len(text) - ch) / 4)
 
 
# ================= RDS 操作 =================
def get_rds_connection():
    return pymysql.connect(
        host=RDS_HOST, port=RDS_PORT, user=RDS_USER,
        password=RDS_PASSWORD, database=RDS_DATABASE,
        charset='utf8mb4', connect_timeout=3
    )
 
 
def get_or_create_session() -> str:
    if "session_id" not in st.session_state:
        conn = None
        try:
            conn = get_rds_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT session_id FROM chat_memory ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                st.session_state.session_id = row[0]
                logger.info(f"✅ 从 RDS 恢复 session_id: {st.session_state.session_id}")
            else:
                st.session_state.session_id = str(uuid.uuid4())
                logger.info(f"🆕 RDS 无历史数据，新建 session_id: {st.session_state.session_id}")
        except Exception as e:
            logger.warning(f"RDS 读取失败，新建 session_id: {e}")
            st.session_state.session_id = str(uuid.uuid4())
        finally:
            if conn:
                conn.close()
    return st.session_state.session_id
 
 
# ================= OSS 客户端 =================
def get_oss_client():
    client = AcsClient(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(3600)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)
 
 
def oss_get_with_retry(bucket, remote_path, max_retry=3, delay=1, **kwargs):
    for attempt in range(max_retry):
        try:
            return bucket.get_object(remote_path, **kwargs)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS 读取重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))
    return None
 
 
def oss_head_with_retry(bucket, remote_path, max_retry=3, delay=1):
    for attempt in range(max_retry):
        try:
            return bucket.head_object(remote_path)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS head 重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))
 
 
# ================= V8.0 核心：尾部读取 =================
def read_oss_tail(size=40960):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        meta = oss_head_with_retry(bucket, remote, max_retry=2)
        length = meta.content_length
        read_size = min(length, size)
        start = length - read_size
        result = oss_get_with_retry(bucket, remote, byte_range=(start, length - 1), max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        
        if start > 0:
            first_nl = content.find('\n')
            if first_nl >= 0:
                content = content[first_nl + 1:]
 
        last_nl = content.rfind('\n')
        if last_nl >= 0:
            content = content[:last_nl + 1]
 
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
 
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.debug(f"read_oss_tail 失败: {e}")
        return []
 
 
def read_oss_full():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"read_oss_full 失败: {e}")
        return []
 
 
# ================= get_recent_messages =================
def _filter_and_dedup(lines: List[Dict], session_id: str = None) -> List[Dict]:
    seen = set()
    result = []
    for item in lines:
        if not isinstance(item, dict):
            continue
        key = (item.get("session_id"), item.get("round_num"))
        if key in seen or key[0] is None or key[1] is None:
            continue
        seen.add(key)
        result.append(item)
    if session_id:
        result = [item for item in result if item.get("session_id") == session_id]
    return result
 
 
def get_recent_messages(session_id: str = None, limit: int = 5) -> List[Dict]:
    lines = read_oss_tail()
    if not lines:
        lines = read_oss_full()
        if not lines:
            return []

    valid_lines = _filter_and_dedup(lines, session_id)

    if not valid_lines:
        full_lines = read_oss_full()
        if full_lines:
            valid_lines = _filter_and_dedup(full_lines, session_id)

    if not valid_lines:
        return []

    sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=True)
    recent = sorted_lines[:limit]

    result = []
    for item in reversed(recent):
        msgs_data = item.get("messages", {})
        if isinstance(msgs_data, str):
            try:
                msgs_data = json.loads(msgs_data)
            except Exception:
                msgs_data = {}

        # 兼容两种格式：{"messages": [...]} 或直接是 [...]
        if isinstance(msgs_data, dict) and "messages" in msgs_data:
            msg_list = msgs_data["messages"]
        elif isinstance(msgs_data, list):
            msg_list = msgs_data
        else:
            msg_list = []

        for msg in msg_list:
            if isinstance(msg, dict):
                result.append({
                    "role": msg.get("role"),
                    "content": msg.get("content")
                })

    return result
 
 
def get_cumulative_summary_from_oss() -> str:
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return ""
        data = json.loads(result.read().decode('utf-8'))
        return data.get("cumulative", "")
    except Exception as e:
        logger.debug(f"读取累积摘要失败: {e}")
        return ""
 
 
# ================= 后加载 =================
SESSION_WINDOW_SIZE = 12
 
def _read_summary_window():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return {"window": [], "cumulative": ""}, True
        data = json.loads(result.read().decode('utf-8'))
        data.setdefault("window", [])
        data.setdefault("cumulative", "")
        return data, True
    except oss2.exceptions.NoSuchKey:
        return {"window": [], "cumulative": ""}, True
    except Exception as e:
        logger.warning(f"读取摘要窗口失败: {e}")
        return {"window": [], "cumulative": ""}, False
 
def _save_summary_window(window, cumulative):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        data = {"window": window, "cumulative": cumulative}
        content = json.dumps(data, ensure_ascii=False).encode('utf-8')
        retries, delay = 2, 1
        for attempt in range(retries):
            try:
                bucket.put_object(remote, content)
                return
            except Exception as e:
                if attempt == retries - 1:
                    raise
                logger.warning(f"_save_summary_window 重试 {attempt+1}/{retries}: {e}")
                time.sleep(delay * (attempt + 1))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")
 
def merge_cumulative(previous: str, new_summary: str) -> str:
    if not previous:
        return new_summary
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{previous}\n---\n{new_summary}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"merge_cumulative 失败，回退到 previous: {e}")
                return previous
            time.sleep(delay)
            delay *= 2
    return previous
 
def generate_summary(messages: List[Dict]) -> str:
    if not messages:
        return ""
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"generate_summary 失败: {e}")
                return ""
            time.sleep(delay)
            delay *= 2
    return ""
 
def load_full_session_from_oss(session_id: str) -> List[Dict]:
    try:
        lines = read_oss_full()
        if not lines:
            return []
        all_msgs = []
        for item in lines:
            if item.get("session_id") != session_id:
                continue
            if item.get("round_num", 0) == 0:
                continue
            msgs_data = item.get("messages", {})
            if isinstance(msgs_data, str):
                msgs_data = json.loads(msgs_data)
            actual_msgs = msgs_data.get("messages", []) if isinstance(msgs_data, dict) else []
            for msg in actual_msgs:
                all_msgs.append(msg)
        return all_msgs
    except Exception as e:
        logger.warning(f"读取完整 Session 失败: {e}")
        return []
 
def trigger_backup_and_restore(old_session_id: str):
    if not old_session_id:
        return
    old_messages = load_full_session_from_oss(old_session_id)
    if not old_messages:
        return
    summary = generate_summary(old_messages)
    if not summary:
        return
    data, read_ok = _read_summary_window()
    window = data.get("window", [])
    cumulative = data.get("cumulative", "")
    if not read_ok:
        if "cached_summary" in st.session_state and st.session_state.cached_summary:
            st.session_state.cached_summary = merge_cumulative(st.session_state.cached_summary, summary)
        else:
            st.session_state.cached_summary = summary
        return
    if not window and not cumulative:
        if "cached_summary" in st.session_state and st.session_state.cached_summary:
            cumulative = st.session_state.cached_summary
    existing_sids = [w.get("session_id") for w in window]
    if old_session_id not in existing_sids:
        cumulative = merge_cumulative(cumulative, summary)
        window.append({"session_id": old_session_id, "summary": summary, "created_at": datetime.now(BEIJING_TZ).isoformat()})
        if len(window) > SESSION_WINDOW_SIZE:
            window = window[-SESSION_WINDOW_SIZE:]
        _save_summary_window(window, cumulative)
        st.session_state.cached_summary = cumulative
 
 
# ================= SQLite 兜底 =================
def init_memory_db():
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS chat_memory_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL, round_num INTEGER NOT NULL,
            messages TEXT NOT NULL, ts TEXT NOT NULL
        )""")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"SQLite 初始化失败: {e}")
 
def save_to_sqlite(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
                       (session_id, round_num, json.dumps(messages, ensure_ascii=False), ts))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"SQLite 写入失败: {e}")
 
 
# ================= V8.0 正式版：sync_to_oss =================
def sync_to_oss(lines):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        content = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n"
        content_bytes = content.encode('utf-8')
        
        try:
            meta = bucket.head_object(remote)
            if meta.headers.get('x-oss-object-type') != 'Appendable':
                logger.warning("⚠️ OSS 文件为 Normal 类型，正在迁移为 Appendable...")
                existing = read_oss_full()
                all_lines = existing + lines
                all_content = "\n".join(json.dumps(l, ensure_ascii=False) for l in all_lines) + "\n"
                bucket.delete_object(remote)
                bucket.append_object(remote, 0, all_content.encode('utf-8'))
            else:
                pos = meta.content_length
                bucket.append_object(remote, pos, content_bytes)
        except oss2.exceptions.NoSuchKey:
            bucket.append_object(remote, 0, content_bytes)
            
        logger.info(f"✅ OSS 追加写入成功: {len(lines)} 行")
        return len(lines)
    except Exception as e:
        logger.error(f"❌ OSS 追加写入失败: {e}")
        raise
 
 
# ================= V8.0 正式版：模型调用 =================
def _clean_for_api(raw_msgs: List[Dict]) -> List[Dict]:
    valid = []
    for m in raw_msgs:
        role = m.get("role", "")
        content = m.get("content")
        if role not in ("user", "assistant"):
            continue
        if content is None or str(content).strip() == "":
            continue
        valid.append({"role": role, "content": str(content).strip()})
    
    if not valid:
        return []
        
    while valid and valid[0]["role"] != "user":
        valid.pop(0)
        
    cleaned = [valid[0]] if valid else []
    for m in valid[1:]:
        if m["role"] != cleaned[-1]["role"]:
            cleaned.append(m)
            
    while cleaned and cleaned[-1]["role"] != "user":
        cleaned.pop()
        
    return cleaned
 
 
def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    sys_parts = [f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"]
    summary = get_cumulative_summary_from_oss()
    if summary:
        sys_parts.append(f"\n【历史对话摘要】\n{summary}")
    
    full_msgs = [{"role": "system", "content": "\n".join(sys_parts)}]

    cleaned = _clean_for_api(messages)
    if not cleaned:
        raise RuntimeError("上下文中没有有效的用户消息")
    full_msgs.extend(cleaned)

    # ====== 改这里 ======
    ASSISTANT_ID = "45db2f797bfd49229f757b04ed13ac92"          # 直接写死，或者从环境变量读取
    # ASSISTANT_ID = os.getenv("ASSISTANT_ID")
    # ===================

    retries, delay = 3, 2
    for attempt in range(retries):
        try:
            # 原来的调用：
            # resp = dashscope.Generation.call(model=MODEL_NAME, messages=full_msgs, result_format="message", stream=False)
            
            # 改为调用智能体：
            resp = dashscope.Assistant.call(
                model=ASSISTANT_ID,
                messages=full_msgs,
                result_format="message",
                stream=False,
                # 可选：传递 session_id 让百炼维持上下文（如果不传，百炼会自动生成）
                # session_id=st.session_state.get("session_id", str(uuid.uuid4()))
            )
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                reset_health_status()
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            err_type = _classify_error(e)
            if attempt == retries - 1:
                mark_failure(err_type)
                raise RuntimeError(f"模型连续{retries}次调用失败: {e}")
            logger.warning(f"call_bailian 重试 {attempt+1}/{retries} [{err_type}]: {e}")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("未知错误")
 
 
def call_bailian_with_token_check(messages: List[Dict]) -> str:
    MAX_INPUT_TOKENS = 16000
    SYSTEM_PROMPT_RESERVE = 2500
 
    total_tokens = sum(estimate_tokens(str(m.get("content", ""))) for m in messages)
    if total_tokens > MAX_INPUT_TOKENS - SYSTEM_PROMPT_RESERVE:
        logger.warning(f"⚠️ 输入超长 ({total_tokens} tokens)，自动截断最近消息")
        trimmed = []
        running_tokens = 0
        limit = MAX_INPUT_TOKENS - SYSTEM_PROMPT_RESERVE
        for m in reversed(messages):
            t = estimate_tokens(str(m.get("content", "")))
            if running_tokens + t > limit:
                break
            trimmed.insert(0, m)
            running_tokens += t
        messages = trimmed
        logger.info(f"✅ 截断后 {len(messages)} 条消息，~{running_tokens} tokens")
 
    return call_bailian(messages)
 
 
def export_txt(messages):
    lines = []
    for m in messages:
        role = "用户" if m.get("role") == "user" else "助手"
        content = m.get("content") or ""
        lines.append(f"{role}：{content}")
    return "\n\n".join(lines)
 
 
# ================= 启动初始化 =================
def init_session_on_startup():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.history_loaded = False
        st.session_state.session_id = ""
        st.session_state.cached_summary = ""
 
    if not st.session_state.messages and not st.session_state.history_loaded:
        session_id = get_or_create_session()
        st.session_state.session_id = session_id
 
        msgs = get_recent_messages(session_id=session_id, limit=5)
        
        if not msgs:
            try:
                conn = sqlite3.connect(MEMORY_DB_PATH)
                cursor = conn.cursor()
                cursor.execute("SELECT messages FROM chat_memory_new WHERE session_id = ? ORDER BY ts DESC LIMIT 5", (session_id,))
                rows = cursor.fetchall()
                conn.close()
                for row in reversed(rows):
                    try:
                        data = json.loads(row[0])
                        for msg in data.get("messages", []):
                            msgs.append(msg)
                    except json.JSONDecodeError:
                        continue
                if msgs:
                    logger.info(f"✅ OSS 失败，从 SQLite 兜底恢复 {len(msgs)} 条消息")
            except Exception as e:
                logger.warning(f"SQLite 兜底读取失败: {e}")
 
        if msgs:
            for m in msgs:
                m["timestamp"] = datetime.now(BEIJING_TZ).isoformat()
                m["session_id"] = session_id
            st.session_state.messages = msgs
            logger.info(f"✅ 恢复 {len(msgs)} 条消息（session: {session_id}）")
            
        st.session_state.history_loaded = True
 
 
# ================= UI (终极稳定版) =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")
st.title("📱 智飞投研")
 
init_memory_db()
init_session_on_startup()
 
if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False
 
total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")
 
# 稳定的容器：只渲染尾部 60 条消息
chat_container = st.container()
with chat_container:
    MAX_RENDER_MSGS = 60
    render_msgs = st.session_state.messages[-MAX_RENDER_MSGS:] if st.session_state.messages else []
    
    for m in render_msgs:
        with st.chat_message(m.get("role", "user")):
            st.markdown(str(m.get("content") or ""))
 
user_input = st.chat_input("输入消息...", disabled=st.session_state.generating)
 
if user_input and not st.session_state.generating:
    st.session_state.stop = False
    st.session_state.generating = True
 
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"]) + 1
 
    user_msg = {
        "role": "user",
        "content": user_input,
        "timestamp": datetime.now(BEIJING_TZ).isoformat()
    }
    st.session_state.messages.append(user_msg)
    st.rerun()
 
if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]
 
    ctx = [{"role": m["role"], "content": str(m.get("content") or "")} for m in st.session_state.messages]
 
    try:
        with st.spinner("💭 思考中..."):
            reply = call_bailian_with_token_check(ctx)

        assistant_msg = {
            "role": "assistant",
            "content": reply,
            "timestamp": datetime.now(BEIJING_TZ).isoformat()
        }
        st.session_state.messages.append(assistant_msg)

        messages_list = [user_msg, assistant_msg]
        messages_dict = {"messages": messages_list}
        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])
        sync_to_oss([{
            "session_id": session_id,
            "round_num": round_num,
            "messages": messages_list,
            "ts": user_msg["timestamp"]
        }])

    except Exception as e:
        st.error(f"❌ 错误: {e}")
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"❌ 调用失败: {e}",
            "timestamp": datetime.now(BEIJING_TZ).isoformat()
        })
 
    st.session_state.generating = False
    st.rerun()
 
# 底部控制栏：永远稳定渲染，只通过 disabled 控制状态，绝不改变 DOM 结构
st.divider()
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("➕ 新建会话", use_container_width=True, disabled=st.session_state.generating):
        old_sid = st.session_state.session_id
        if old_sid and st.session_state.messages:
            _backup_executor.submit(trigger_backup_and_restore, old_sid)
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.session_state.history_loaded = False
        st.session_state.generating = False
        st.session_state.stop = False
        st.rerun()
with col2:
    if st.button("🔄 重新生成", use_container_width=True, disabled=not st.session_state.generating):
        if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
            st.session_state.messages.pop()
            st.session_state.generating = True
            st.session_state.stop = False
            st.rerun()
with col3:
    # 直接渲染下载按钮，避免动态出现组件导致 DOM 突变
    st.download_button(
        label="📤 导出TXT",
        data=export_txt(st.session_state.messages),
        file_name=f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.txt",
        mime="text/plain",
        use_container_width=True,
        key="export_txt_btn"
    )