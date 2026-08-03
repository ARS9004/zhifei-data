#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v8.8v4（2026-08-03）
- 手机端专用：OSS 实时同步，累积摘要 + 3轮对话恢复
- 极简 UI：无侧边栏，支持新建会话和分页加载
- ✅ 修复 OSS 回退全量读取内存隐患（改 byte_range）
- ✅ 修复同步失败游标推进导致的数据静默丢失
- ✅ 修复 OSS 回退 JSON 解析一行坏全崩的问题
- ✅ 修复 OSS 回退首行数据误丢弃问题
- ✅ 优化游标推进逻辑防回环，清理冗余代码
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
import io
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Optional, Tuple

import streamlit as st
import dashscope
import pytz
import oss2
import pymysql
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

load_dotenv()

# ================= 环境变量 =================
def get_secret_or_env(key, secrets_key=None, default=None):
    if secrets_key:
        parts = secrets_key.split('.')
        try:
            value = st.secrets
            for p in parts:
                value = value[p]
            if value: return value
        except: pass
    return os.getenv(key, default)

DASHSCOPE_API_KEY = get_secret_or_env("DASHSCOPE_API_KEY", "dashscope.api_key")
if not DASHSCOPE_API_KEY: raise RuntimeError("⛔ 请配置 DASHSCOPE_API_KEY")

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
OSS_FILENAME = get_secret_or_env("OSS_FILENAME", "oss.filename", "chat_history.jsonl")
OSS_MAX_BACKUPS = 20

# ================= 后加载与渲染配置 =================
SESSION_WINDOW_SIZE = 12
RECOVER_ROUNDS = 3
RENDER_ROUNDS = 5
SESSION_GAP_SECONDS = 3600

# ================= 统一时间格式 =================
TS_FORMAT = '%Y-%m-%d %H:%M:%S'

def now_ts() -> str:
    return datetime.now(BEIJING_TZ).strftime(TS_FORMAT)

# ================= 日志 =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 熔断 =================
_FAIL_COUNTER = 0
_MODEL_HEALTHY = True
MAX_CONSECUTIVE_FAILURES = 3

def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER = 0
    _MODEL_HEALTHY = True

def mark_failure():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER += 1
    if _FAIL_COUNTER >= MAX_CONSECUTIVE_FAILURES:
        _MODEL_HEALTHY = False
        logger.warning("🚨 模型服务熔断已触发，连续失败 %d 次", _FAIL_COUNTER)

def is_model_healthy() -> bool:
    return _MODEL_HEALTHY

# ================= 辅助函数 =================
def estimate_tokens(text: str) -> int:
    if not text: return 0
    ch = len(re.findall(r'[\u4e00-\u9fff]', text))
    return int(ch / 1.5 + (len(text) - ch) / 4)

# ================= RDS 操作 =================
def get_rds_connection():
    return pymysql.connect(
        host=RDS_HOST, port=RDS_PORT, user=RDS_USER,
        password=RDS_PASSWORD, database=RDS_DATABASE, charset='utf8mb4',
        connect_timeout=3
    )

def get_or_create_session() -> str:
    if "session_id" not in st.session_state:
        conn = None
        try:
            conn = get_rds_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT session_id, ts FROM chat_memory ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                last_session_id, last_ts = row
                last_dt = datetime.strptime(str(last_ts), TS_FORMAT)
                last_dt = BEIJING_TZ.localize(last_dt) if last_dt.tzinfo is None else last_dt
                now = datetime.now(BEIJING_TZ)
                if (now - last_dt).total_seconds() >= SESSION_GAP_SECONDS:
                    st.session_state.session_id = str(uuid.uuid4())
                    trigger_backup_and_restore(last_session_id)
                else:
                    st.session_state.session_id = last_session_id
            else:
                st.session_state.session_id = str(uuid.uuid4())
        except Exception as e:
            logger.warning(f"session 获取失败: {e}")
            st.session_state.session_id = str(uuid.uuid4())
        finally:
            if conn: conn.close()
    return st.session_state.session_id

def save_to_rds(session_id: str, round_num: int, round_messages: dict, ts: str):
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        messages_json = json.dumps(round_messages, ensure_ascii=False)
        cursor.execute(f"INSERT IGNORE INTO {RDS_CHAT_TABLE} (session_id, round_num, messages, ts) VALUES (%s, %s, %s, %s)",
            (session_id, round_num, messages_json, ts))
        conn.commit()
    except Exception as e:
        logger.warning(f"RDS 写入失败: {e}")
    finally:
        if conn: conn.close()

def load_recent_rounds_from_rds(session_id: str, limit: int = 3, conn: pymysql.Connection = None) -> List[Dict]:
    own_conn = None
    try:
        if conn is None:
            own_conn = get_rds_connection()
            conn = own_conn
        cursor = conn.cursor()
        cursor.execute("SELECT messages, ts FROM chat_memory WHERE session_id = %s ORDER BY ts DESC LIMIT %s",
            (session_id, limit))
        rows = cursor.fetchall()
        result = []
        for messages_json, ts in reversed(rows):
            data = json.loads(messages_json)
            result.append({"round_messages": data.get("messages", []), "timestamp": str(ts)})
        return result
    except Exception as e:
        logger.warning(f"读取最近对话失败: {e}")
        return []
    finally:
        if own_conn: own_conn.close()

def save_summary_to_rds(session_id: str, summary: str, conn: pymysql.Connection = None):
    own_conn = None
    try:
        if conn is None:
            own_conn = get_rds_connection()
            conn = own_conn
        cursor = conn.cursor()
        cursor.execute("INSERT INTO chat_summary (session_id, summary, created_at) VALUES (%s, %s, %s)",
            (session_id, summary, now_ts()))
        conn.commit()
    except Exception as e:
        logger.warning(f"RDS 摘要写入失败: {e}")
    finally:
        if own_conn: own_conn.close()

# ================= 表初始化 =================
_INIT_WARNINGS = []

def init_chat_summary_table():
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS chat_summary (
                id INT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(36) NOT NULL, summary TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP, INDEX idx_session (session_id), INDEX idx_created (created_at))""")
        conn.commit()
    except Exception as e:
        logger.error(f"chat_summary 表初始化失败: {e}")
    finally:
        if conn: conn.close()

def init_session_backup_table():
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS session_backup (
                id INT AUTO_INCREMENT PRIMARY KEY, session_id VARCHAR(36) NOT NULL UNIQUE, summary TEXT,
                cumulative_summary TEXT, round_count INT DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_session (session_id), INDEX idx_created (created_at))""")
        conn.commit()
    except Exception as e:
        logger.error(f"session_backup 表初始化失败: {e}")
    finally:
        if conn: conn.close()

def init_chat_memory_index():
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE chat_memory ADD UNIQUE INDEX idx_session_round (session_id, round_num)")
        conn.commit()
    except pymysql.err.MySQLError as e:
        if e.args[0] == 1061: pass
        elif e.args[0] == 1062:
            msg = "⚠️ chat_memory 存在重复数据，无法建立唯一索引。系统仍可运行，但建议在非高峰期手动执行去重。"
            logger.warning(msg)
            _INIT_WARNINGS.append(msg)
        else:
            logger.warning(f"chat_memory 索引初始化失败: {e}")
    except Exception as e:
        logger.warning(f"chat_memory 索引初始化异常: {e}")
    finally:
        if conn: conn.close()

def save_session_backup(session_id: str, summary: str, round_count: int, cumulative_summary: str = None, conn: pymysql.Connection = None):
    own_conn = None
    try:
        if conn is None:
            own_conn = get_rds_connection()
            conn = own_conn
        cursor = conn.cursor()
        now = now_ts()
        if cumulative_summary:
            cursor.execute("INSERT INTO session_backup (session_id, summary, cumulative_summary, round_count, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE summary = %s, cumulative_summary = %s, round_count = %s, updated_at = %s",
                (session_id, summary, cumulative_summary, round_count, now, now, summary, cumulative_summary, round_count, now))
        else:
            cursor.execute("INSERT INTO session_backup (session_id, summary, round_count, created_at, updated_at) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE summary = %s, round_count = %s, updated_at = %s",
                (session_id, summary, round_count, now, now, summary, round_count, now))
        conn.commit()
    except Exception as e:
        logger.warning(f"保存 Session 备份失败: {e}")
    finally:
        if own_conn: own_conn.close()

def get_rolling_window(window_size: int = SESSION_WINDOW_SIZE, conn: pymysql.Connection = None) -> List[Dict]:
    own_conn = None
    try:
        if conn is None:
            own_conn = get_rds_connection()
            conn = own_conn
        cursor = conn.cursor()
        cursor.execute("SELECT session_id, summary, cumulative_summary, round_count, created_at FROM session_backup ORDER BY created_at DESC LIMIT %s", (window_size,))
        rows = cursor.fetchall()
        result = []
        for session_id, summary, cumulative_summary, round_count, created_at in reversed(rows):
            result.append({"session_id": session_id, "summary": summary, "cumulative_summary": cumulative_summary, "round_count": round_count, "created_at": str(created_at)})
        return result
    except Exception as e:
        logger.warning(f"获取滚动窗口失败: {e}")
        return []
    finally:
        if own_conn: own_conn.close()

def update_rolling_window(session_id: str, summary: str, round_count: int, cumulative_summary: str = None, conn: pymysql.Connection = None):
    save_session_backup(session_id, summary, round_count, cumulative_summary, conn=conn)
    own_conn = None
    try:
        if conn is None:
            own_conn = get_rds_connection()
            conn = own_conn
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM session_backup")
        count = cursor.fetchone()[0]
        if count > SESSION_WINDOW_SIZE:
            cursor.execute("DELETE FROM session_backup ORDER BY created_at ASC LIMIT %s", (count - SESSION_WINDOW_SIZE,))
            conn.commit()
    except Exception as e:
        logger.warning(f"淘汰窗口失败: {e}")
    finally:
        if own_conn: own_conn.close()

# ================= 累积摘要 =================
def get_cumulative_summary(conn: pymysql.Connection = None) -> str:
    own_conn = False
    if conn is None:
        conn = get_rds_connection()
        own_conn = True
    try:
        window = get_rolling_window(SESSION_WINDOW_SIZE, conn=conn)
        if not window: return ""
        cached = window[-1].get("cumulative_summary")
        if cached: return cached
        if len(window) == 1: return window[0].get("summary", "")
        prev_cumulative = window[-2].get("cumulative_summary") or window[-2].get("summary", "")
        latest_summary = window[-1].get("summary", "")
        if not latest_summary: return prev_cumulative
        combined = f"{prev_cumulative}\n\n---\n\n{latest_summary}"
        try:
            prompt = f"将以下两段对话摘要合并成一段200字以内的整体摘要（保留核心事实和结论）：\n\n{combined}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
            if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0:
                new_cumulative = resp.output.choices[0].message.content
                save_session_backup(window[-1]["session_id"], latest_summary, window[-1]["round_count"], cumulative_summary=new_cumulative, conn=conn)
                return new_cumulative
        except Exception as e:
            logger.warning(f"累积摘要更新失败: {e}")
        return prev_cumulative if prev_cumulative else latest_summary
    finally:
        if own_conn and conn: conn.close()

def _generate_new_cumulative(previous_cumulative: str, new_summary: str) -> str:
    combined = f"{previous_cumulative}\n\n---\n\n{new_summary}"
    try:
        prompt = f"将以下多个对话摘要合并成一个200字以内的整体摘要：\n\n{combined}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0:
            return resp.output.choices[0].message.content
    except Exception as e:
        logger.warning(f"累积摘要生成失败: {e}")
    return previous_cumulative if previous_cumulative else new_summary

def trigger_backup_and_restore(old_session_id: str):
    logger.info(f"🔄 开始后加载: {old_session_id}")
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT round_num, messages, ts FROM chat_memory WHERE session_id = %s ORDER BY ts ASC", (old_session_id,))
        rows = cursor.fetchall()
        if not rows: return
        all_messages = []
        for round_num, messages_json, ts in rows:
            data = json.loads(messages_json)
            for msg in data.get("messages", []): all_messages.append(msg)
        summary = generate_summary(all_messages)
        new_cumulative = ""
        if summary:
            save_summary_to_rds(old_session_id, summary, conn=conn)
            existing_cumulative = get_cumulative_summary(conn=conn)
            new_cumulative = _generate_new_cumulative(existing_cumulative, summary) if existing_cumulative else summary
            update_rolling_window(old_session_id, summary, len(rows), cumulative_summary=new_cumulative, conn=conn)
        _backup_rows_to_oss(old_session_id, rows)
        recent_rounds = load_recent_rounds_from_rds(old_session_id, RECOVER_ROUNDS, conn=conn)
        st.session_state._recovery_context = {"summary": new_cumulative if summary else get_cumulative_summary(conn=conn), "recent_rounds": recent_rounds, "source_session": old_session_id}
    except Exception as e:
        logger.error(f"❌ 后加载失败: {e}")
    finally:
        if conn: conn.close()

def _backup_rows_to_oss(session_id: str, rows: List[Tuple]):
    try:
        if not rows: return
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"backup_{session_id}_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M%S')}.jsonl"
        content = ""
        for round_num, messages_json, ts in rows:
            content += json.dumps({"session_id": session_id, "round_num": round_num, "messages": json.loads(messages_json), "ts": str(ts)}, ensure_ascii=False) + '\n'
        if content:
            bucket.put_object(remote_path, content.encode('utf-8'))
        _cleanup_oss_backups(bucket)
    except Exception as e:
        logger.warning(f"Session 备份失败: {e}")

def _cleanup_oss_backups(bucket):
    try:
        backup_files = []
        for obj in oss2.ObjectIterator(bucket, prefix=OSS_PREFIX):
            if obj.key.startswith(OSS_PREFIX + "backup_") and obj.key.endswith(".jsonl"):
                backup_files.append((obj.key, obj.last_modified))
        if len(backup_files) <= OSS_MAX_BACKUPS: return
        backup_files.sort(key=lambda x: x[1], reverse=True)
        for key, _ in backup_files[OSS_MAX_BACKUPS:]: bucket.delete_object(key)
    except Exception as e:
        logger.warning(f"OSS 备份清理失败: {e}")

def inject_memory(prompt: str, force_latest: bool = False) -> str:
    hint_parts = []
    recovery_ctx = st.session_state.get("_recovery_context", {})
    if recovery_ctx.get("summary") or recovery_ctx.get("recent_rounds"):
        if recovery_ctx.get("summary"): hint_parts.append(f"【历史对话摘要】{recovery_ctx['summary']}")
        if recovery_ctx.get("recent_rounds"):
            recent_text = []
            for item in recovery_ctx["recent_rounds"]:
                for msg in item.get("round_messages", []):
                    role = "用户" if msg.get("role") == "user" else "助手"
                    recent_text.append(f"{role}: {str(msg.get('content', ''))[:200]}")
            if recent_text: hint_parts.append("【最近对话】\n" + "\n".join(recent_text))
        st.session_state._recovery_context = {}
        return "\n\n".join(hint_parts)
    conn = None
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        target_session = st.session_state.get("session_id")
        if force_latest or not target_session:
            cursor.execute("SELECT session_id FROM chat_memory ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            if row: target_session = row[0]
        if not target_session: return ""
        cursor.execute("SELECT summary FROM chat_summary WHERE session_id = %s ORDER BY created_at DESC LIMIT 1", (target_session,))
        row = cursor.fetchone()
        summary = row[0] if row else ""
        if summary: hint_parts.append(f"【历史对话摘要】{summary}")
        recent_rounds = load_recent_rounds_from_rds(target_session, 3, conn=conn)
        if recent_rounds:
            recent_text = []
            for item in recent_rounds:
                for msg in item.get("round_messages", []):
                    role = "用户" if msg.get("role") == "user" else "助手"
                    recent_text.append(f"{role}: {str(msg.get('content', ''))[:200]}")
            if recent_text: hint_parts.append("【最近对话】\n" + "\n".join(recent_text))
        return "\n\n".join(hint_parts)
    except Exception as e:
        logger.warning(f"记忆注入失败: {e}")
        return ""
    finally:
        if conn: conn.close()

# ================= OSS 操作 =================
def get_oss_client():
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
    if not access_key_id or not access_key_secret: raise RuntimeError("⛔ 请配置环境变量 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
    client = AcsClient(access_key_id, access_key_secret, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

# ================= 双向增量同步 =================
SYNC_CURSOR_FILE = os.getenv("SYNC_CURSOR_FILE", "./sync_cursor.json")

def _get_sync_cursor() -> dict:
    default = {"rds_last_ts": "1970-01-01 00:00:00", "oss_last_offset": 0}
    try:
        if os.path.exists(SYNC_CURSOR_FILE):
            with open(SYNC_CURSOR_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data.setdefault("rds_last_ts", default["rds_last_ts"])
                data.setdefault("oss_last_offset", 0)
                return data
    except Exception as e:
        logger.warning(f"读取游标失败: {e}")
    return default

def _save_sync_cursor(rds_ts: str, oss_offset: int):
    try:
        with open(SYNC_CURSOR_FILE, 'w', encoding='utf-8') as f:
            json.dump({"rds_last_ts": rds_ts, "oss_last_offset": oss_offset}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存游标失败: {e}")

def _ensure_appendable(bucket, remote_path: str) -> int:
    tmp_path = remote_path + ".tmp_append"
    try:
        head = bucket.head_object(remote_path)
        pos = head.content_length
        try:
            bucket.append_object(remote_path, pos, b'')
            return pos
        except oss2.exceptions.ObjectNotAppendable:
            logger.info("🔄 检测到 Normal 类型 OSS 文件，开始安全迁移...")
            try:
                use_tmp = False
                try:
                    tmp_head = bucket.head_object(tmp_path)
                    tmp_size = tmp_head.content_length
                    if tmp_size > 0 and tmp_size >= pos:
                        use_tmp = True
                except oss2.exceptions.NoSuchKey:
                    pass
                
                if use_tmp:
                    old_content = bucket.get_object(tmp_path).read()
                else:
                    old_content = bucket.get_object(remote_path).read()
                    try: bucket.delete_object(tmp_path)
                    except: pass
                    bucket.append_object(tmp_path, 0, old_content)
                
                bucket.delete_object(remote_path)
                bucket.append_object(remote_path, 0, old_content)
                bucket.delete_object(tmp_path)
                return len(old_content)
            except Exception as e:
                logger.error(f"OSS 文件安全迁移失败: {e}")
                raise
    except oss2.exceptions.NoSuchKey:
        try:
            tmp_head = bucket.head_object(tmp_path)
            if tmp_head.content_length > 0:
                logger.warning("⚠️ 检测到残留的临时文件，正在恢复...")
                old_content = bucket.get_object(tmp_path).read()
                bucket.append_object(remote_path, 0, old_content)
                bucket.delete_object(tmp_path)
                return len(old_content)
        except oss2.exceptions.NoSuchKey:
            pass
        return 0
    except Exception as e:
        logger.error(f"检查 Appendable 失败: {e}")
        raise

def sync_bidirectional():
    cursor_data = _get_sync_cursor()
    rds_last_ts = cursor_data["rds_last_ts"]
    oss_last_offset = cursor_data["oss_last_offset"]
    max_rds_ts = rds_last_ts
    new_oss_offset = oss_last_offset

    try:
        bucket = get_oss_client()
    except Exception as e:
        logger.warning(f"OSS client 创建失败: {e}")
        return

    remote_path = OSS_PREFIX + OSS_FILENAME
    rds_data = {}

    # ---------- 步骤1：RDS → OSS ----------
    conn = None
    try:
        conn = get_rds_connection()
        cursor_db = conn.cursor()
        cursor_db.execute("SELECT session_id, round_num, messages, ts FROM chat_memory WHERE ts > %s ORDER BY ts ASC", (rds_last_ts,))
        rows = cursor_db.fetchall()
        if rows:
            pos = _ensure_appendable(bucket, remote_path)
            content_to_append = ""
            pending_max_ts = max_rds_ts
            for session_id, round_num, messages_json, ts in rows:
                ts_str = str(ts)
                rds_data[(session_id, round_num)] = ts_str
                content_to_append += json.dumps({"session_id": session_id, "round_num": round_num, "messages": json.loads(messages_json), "ts": ts_str}, ensure_ascii=False) + '\n'
                if ts_str > pending_max_ts: pending_max_ts = ts_str
            if content_to_append:
                try:
                    bucket.append_object(remote_path, pos, content_to_append.encode('utf-8'))
                    logger.info(f"✅ RDS → OSS: 追加 {len(rows)} 行")
                    # v8.8v4 修复：成功后才推进游标
                    max_rds_ts = pending_max_ts
                except Exception as append_ex:
                    logger.error(f"RDS → OSS 追加失败，游标不推进，等待下次重试: {append_ex}")
    except Exception as e:
        logger.warning(f"RDS → OSS 同步失败: {e}")
    finally:
        if conn: conn.close()

    # ---------- 步骤2：OSS → RDS / SQLite ----------
    try:
        try:
            head = bucket.head_object(remote_path)
            current_size = head.content_length
            if current_size > oss_last_offset:
                resp = bucket.get_object(remote_path, byte_range=(oss_last_offset, current_size - 1))
                new_content = resp.read().decode('utf-8')
                
                if oss_last_offset > 0:
                    first_newline = new_content.find('\n')
                    if first_newline >= 0:
                        first_line = new_content[:first_newline].strip()
                        if first_line:
                            try:
                                json.loads(first_line)
                            except:
                                new_content = new_content[first_newline + 1:]
                
                new_rows = []
                for line in new_content.strip().split('\n'):
                    if not line.strip(): continue
                    try:
                        new_rows.append(json.loads(line))
                    except Exception as e:
                        logger.warning(f"解析 OSS 行失败: {e}")
                
                if new_rows:
                    rds_conn = None
                    sqlite_conn = None
                    try:
                        rds_conn = get_rds_connection()
                        rds_cursor = rds_conn.cursor()
                        sqlite_conn = sqlite3.connect(MEMORY_DB_PATH)
                        sqlite_cursor = sqlite_conn.cursor()
                        rds_ok, sqlite_ok = 0, 0
                        
                        for item in new_rows:
                            key = (item.get("session_id", ""), item.get("round_num", 0))
                            if key in rds_data: continue
                            
                            sid, rn = key
                            mj = json.dumps(item.get("messages", {}), ensure_ascii=False)
                            tv = item.get("ts", "")
                            try:
                                rds_cursor.execute(f"INSERT IGNORE INTO {RDS_CHAT_TABLE} (session_id, round_num, messages, ts) VALUES (%s, %s, %s, %s)", (sid, rn, mj, tv))
                                rds_conn.commit()
                                rds_ok += rds_cursor.rowcount
                            except Exception:
                                rds_conn.rollback()
                            try:
                                sqlite_cursor.execute("INSERT OR IGNORE INTO messages (session_id, round_num, messages, timestamp) VALUES (?, ?, ?, ?)", (sid, rn, mj, tv))
                                sqlite_conn.commit()
                                sqlite_ok += sqlite_cursor.rowcount
                            except Exception:
                                sqlite_conn.rollback()
                        if rds_ok > 0 or sqlite_ok > 0:
                            logger.info(f"✅ OSS → RDS/SQLite: 恢复 RDS{rds_ok}条/SQLite{sqlite_ok}条")
                    except Exception as e:
                        logger.warning(f"OSS → RDS/SQLite 写入失败: {e}")
                    finally:
                        if rds_conn: rds_conn.close()
                        if sqlite_conn: sqlite_conn.close()
                new_oss_offset = current_size
            else:
                new_oss_offset = oss_last_offset
        except oss2.exceptions.NoSuchKey:
            if oss_last_offset > 0:
                logger.warning("⚠️ OSS 文件不存在，但游标偏移量不为 0！已重置游标，下次将全量拉取。")
            new_oss_offset = 0
    except Exception as e:
        logger.warning(f"OSS → RDS 读取失败: {e}")

    _save_sync_cursor(max_rds_ts, new_oss_offset)

# ================= SQLite 操作 =================
def init_memory_db():
    conn = None
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, round_num INTEGER NOT NULL,
                messages TEXT NOT NULL, timestamp TEXT NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_messages_round ON messages(round_num DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC)")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_session_round ON messages(session_id, round_num)")
        conn.commit()
    except Exception as e:
        logger.error(f"SQLite初始化失败: {e}")
    finally:
        if conn: conn.close()

def save_to_sqlite(session_id: str, round_num: int, round_messages: dict, ts: str):
    conn = None
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO messages (session_id, round_num, messages, timestamp) VALUES (?, ?, ?, ?)",
            (session_id, round_num, json.dumps(round_messages, ensure_ascii=False), ts))
        conn.commit()
    except Exception as e:
        logger.warning(f"SQLite写入失败: {e}")
    finally:
        if conn: conn.close()

def load_from_sqlite(limit: int = 3) -> List[Dict]:
    conn = None
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        rows = conn.execute("SELECT session_id, messages, timestamp FROM messages ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        results = []
        for session_id, messages_json, timestamp in rows:
            data = json.loads(messages_json)
            for msg in data.get("messages", []):
                results.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": timestamp, "session_id": session_id})
        return results
    except Exception as e:
        logger.warning(f"SQLite读取失败: {e}")
        return []
    finally:
        if conn: conn.close()

def load_recent_from_oss(limit: int = 3) -> List[Dict]:
    """v8.8v4: 彻底修复尾部读取丢数据与一行坏全崩问题"""
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME
        try:
            head = bucket.head_object(remote_path)
            file_size = head.content_length
            if file_size == 0: return []
            
            tail_size = min(50 * 1024, file_size)
            resp = bucket.get_object(remote_path, byte_range=(file_size - tail_size, file_size - 1))
            content = resp.read().decode('utf-8')
            
            # v8.8v4 修复：尝试解析首行，成功则保留，失败才丢弃
            first_newline = content.find('\n')
            if first_newline >= 0:
                first_line = content[:first_newline]
                try:
                    json.loads(first_line)
                except:
                    content = content[first_newline + 1:]
            
            lines = []
            for line in content.strip().split('\n'):
                if not line.strip(): continue
                try:
                    lines.append(json.loads(line))
                except:
                    continue
            
            valid_lines = [item for item in lines if isinstance(item, dict)]
            sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=True)
            recent = sorted_lines[:limit]
            
            msgs = []
            for item in reversed(recent):
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try: msgs_data = json.loads(msgs_data)
                    except: msgs_data = {}
                
                actual_msgs = msgs_data.get("messages", []) if isinstance(msgs_data, dict) else []
                for msg in actual_msgs:
                    msgs.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("ts"), "session_id": item.get("session_id")})
            return msgs
        except oss2.exceptions.NoSuchKey:
            return []
    except:
        return []

# ================= 百炼调用 =================
def call_bailian_once(messages: List[Dict], mvars: Dict, hint: str) -> Tuple[str, int]:
    global _MODEL_HEALTHY
    if not is_model_healthy(): raise RuntimeError("🔴 服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY
    sys_p = f"你是智飞投研助手。当前时间:{mvars['CURRENT_DATE']} | 时段:{mvars['MARKET_SESSION']} | 指数:{mvars['INDEX_STATUS']} | 量能:{mvars['VOLUME_TREND']}\n规则:直接输出结论+关键数据+风险提示。不展示工具调用过程。"
    if hint: sys_p += f"\n\n{hint}"
    full_msgs = [{"role": "system", "content": sys_p}]
    for m in messages:
        content = str(m.get("content", ""))
        if m["role"] == "user": content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
        full_msgs.append({"role": m["role"], "content": content})
    base_tok = estimate_tokens(sys_p) + sum(estimate_tokens(str(m.get("content", ""))) for m in messages)
    for attempt in range(2):
        try:
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=full_msgs, result_format="message", stream=False)
            if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0:
                full_text = resp.output.choices[0].message.content
                if not full_text or not full_text.strip(): raise RuntimeError("模型返回内容为空")
                reset_health_status()
                return full_text, base_tok + estimate_tokens(full_text)
            else:
                raise RuntimeError(f"API Error: {resp.code} {resp.message}")
        except Exception as e:
            if attempt == 1:
                mark_failure()
                raise RuntimeError(f"❌ 模型调用失败：{e}")
            logger.warning(f"第{attempt+1}次调用失败，2秒后重试: {e}")
            time.sleep(2)
    raise RuntimeError("❌ 模型调用失败")

def generate_summary(messages: List[Dict]) -> str:
    if not messages: return ""
    try:
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": f"将以下对话压缩成300字摘要，突出核心主题和结论：\n{json.dumps(messages[-30:], ensure_ascii=False)[:5000]}"}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0: return resp.output.choices[0].message.content
    except Exception as e:
        logger.warning(f"摘要生成失败: {e}")
    return ""

# ================= 会话初始化 =================
def init_session_on_startup() -> List[Dict]:
    try:
        session_id = get_or_create_session()
        recent_rounds = load_recent_rounds_from_rds(session_id, RECOVER_ROUNDS)
        recovery = st.session_state.get("_recovery_context", {})
        if not recent_rounds and recovery.get("recent_rounds"): recent_rounds = recovery["recent_rounds"]
        if recent_rounds:
            messages = []
            for item in recent_rounds:
                for msg in item.get("round_messages", []):
                    messages.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("timestamp"), "session_id": session_id})
            if not recovery.get("summary"):
                cumulative = get_cumulative_summary()
                if cumulative:
                    st.session_state._recovery_context = {"summary": cumulative, "recent_rounds": recent_rounds, "source_session": session_id}
            return messages
        
        sqlite_msgs = load_from_sqlite(limit=RECOVER_ROUNDS)
        if sqlite_msgs: return sqlite_msgs
            
        oss_msgs = load_recent_from_oss(limit=RECOVER_ROUNDS)
        if oss_msgs: return oss_msgs
        
        st.session_state.session_id = str(uuid.uuid4())
        return []
    except Exception as e:
        logger.warning(f"会话初始化失败: {e}")
        st.session_state.session_id = str(uuid.uuid4())
        return []

def get_market_vars() -> Dict[str, str]:
    return {"CURRENT_DATE": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d"), "MARKET_SESSION": get_market_session(), "INDEX_STATUS": "正常", "VOLUME_TREND": "平稳"}

def get_market_session() -> str:
    now = datetime.now(BEIJING_TZ).time()
    if now < dt_time(9, 15): return "盘前"
    elif now < dt_time(11, 30): return "上午"
    elif now < dt_time(13, 0): return "午间"
    elif now < dt_time(15, 0): return "下午"
    elif now < dt_time(15, 30): return "收盘"
    else: return "盘后"

def export_txt(messages: List[Dict]) -> str:
    out = ""
    for m in messages:
        role = "用户" if m["role"] == "user" else "助手"
        out += f"{role}：{m.get('content', '')}\n\n"
    return out

# ================= UI =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")

for w in _INIT_WARNINGS:
    st.warning(w)

st.markdown("""<style>.stApp, section.main, .main, [data-testid="stAppViewContainer"] { background: #ffffff !important; transition: none !important; animation: none !important; will-change: auto !important; }[aria-label="Loading..."], [data-testid="stLoadingIndicator"] { opacity: 0 !important; pointer-events: none !important; }.stChatInputContainer { position: sticky !important; bottom: 0 !important; background: #ffffff !important; padding: 12px 0 8px 0 !important; z-index: 999 !important; border-top: 1px solid #e5e7eb !important; box-shadow: 0 -4px 10px rgba(0,0,0,0.03) !important; }.stChatMessage { margin-bottom: 8px; }</style>""", unsafe_allow_html=True)

st.title("📱 智飞投研")

init_memory_db()
init_chat_summary_table()
init_session_backup_table()
init_chat_memory_index()

if "messages" not in st.session_state:
    with st.spinner("🔄 正在恢复历史记忆..."):
        st.session_state.messages = init_session_on_startup()
        if not st.session_state.messages:
            st.session_state.messages = []
            st.session_state.session_id = str(uuid.uuid4())
        if "_recovery_context" not in st.session_state: st.session_state._recovery_context = {}

for k, v in {"generating": False, "pending_generation": False, "render_offset": 0}.items():
    st.session_state.setdefault(k, v)

total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"共 {total_rounds} 轮对话")

total_msgs = len(st.session_state.messages)
render_start = max(0, total_msgs - (st.session_state.render_offset + RENDER_ROUNDS) * 2)
render_messages = st.session_state.messages[render_start:]

for m in render_messages:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

if render_start > 0:
    if st.button("📥 加载更早对话", key="load_earlier", use_container_width=True):
        st.session_state.render_offset += RENDER_ROUNDS
        st.rerun()

if st.session_state.generating:
    st.info("⏳ 正在处理上一条消息，请稍候...")
    
user_input = st.chat_input("输入消息...")

if user_input and not st.session_state.generating:
    st.session_state.generating = True
    st.session_state.pending_generation = True
    st.session_state.messages.append({"role": "user", "content": user_input, "timestamp": now_ts()})
    st.rerun()

if st.session_state.pending_generation:
    st.session_state.pending_generation = False
    st.session_state.generating = True
    
    session_id = st.session_state.session_id
    
    mh = inject_memory(st.session_state.messages[-1].get("content", ""), force_latest=False) if st.session_state.messages else inject_memory("", force_latest=True)
    
    with st.chat_message("assistant"):
        with st.spinner("💭 思考中..."):
            try:
                full_txt, _ = call_bailian_once(st.session_state.messages[-30:], get_market_vars(), mh)
                st.markdown(full_txt)
                st.session_state.messages.append({"role": "assistant", "content": full_txt, "timestamp": now_ts()})
                
                current_round = len([m for m in st.session_state.messages if m["role"] == "user"])
                current_time = now_ts()
                round_messages = {"messages": [st.session_state.messages[-2], st.session_state.messages[-1]]}
                
                save_to_rds(session_id, current_round, round_messages, current_time)
                save_to_sqlite(session_id, current_round, round_messages, current_time)
                
                if current_round % 5 == 0:
                    sync_bidirectional()
                    
                if current_round % 10 == 0:
                    summary = generate_summary(st.session_state.messages)
                    if summary:
                        save_summary_to_rds(session_id, summary)
                        cumulative = get_cumulative_summary()
                        new_cumulative = _generate_new_cumulative(cumulative, summary) if cumulative else summary
                        update_rolling_window(session_id, summary, current_round, cumulative_summary=new_cumulative)
                        sync_bidirectional()
            except Exception as e:
                st.error(f"❌ 发生错误：{e}")
                if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
                    st.session_state.messages.pop()
    
    st.session_state.generating = False
    st.rerun()

if st.session_state.messages:
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("➕ 新建会话", use_container_width=True):
            old_session_id = st.session_state.get("session_id")
            if old_session_id and st.session_state.messages:
                with st.spinner("🔄 正在备份当前会话..."):
                    trigger_backup_and_restore(old_session_id)
            st.session_state.messages = []
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state._recovery_context = {}
            st.session_state.generating = False
            st.session_state.pending_generation = False
            st.session_state.render_offset = 0
            st.rerun()
    with col2:
        txt = export_txt(st.session_state.messages)
        st.download_button("📤 导出TXT", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.txt", "text/plain", key="dl", use_container_width=True)