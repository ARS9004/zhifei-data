#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v7.3（2026-08-01）
- 手机端专用：OSS 实时同步（每轮写），摘要 + 3轮对话恢复
- 双写：RDS + SQLite + OSS（实时三写）
- ✅ 新增：百炼长期记忆（每次对话后自动写入）
- ✅ 用户消息立即渲染，不等模型回复
- ✅ 熔断分级 & 摘要本地缓存
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple

import streamlit as st
import dashscope
import oss2
import pymysql
import pytz
import requests
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

load_dotenv()

# ================= 环境变量（兼容 .env 和 st.secrets） =================
def get_secret_or_env(key, secrets_key=None, default=None):
    if secrets_key:
        parts = secrets_key.split('.')
        try:
            value = st.secrets
            for p in parts:
                value = value[p]
            if value:
                return value
        except:
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
OSS_SUMMARY_FILE = "chat_summary.json"
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")

if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
    raise RuntimeError("⛔ 请配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")

# ================= 长期记忆配置 =================
MEMORY_LIBRARY_ID = get_secret_or_env(
    "MEMORY_LIBRARY_ID",
    "memory.library_id",
    "5e8360f1efbf4759a2a3d80d126fd77b"
)
MEMORY_USER_ID = get_secret_or_env(
    "MEMORY_USER_ID",
    "memory.user_id",
    "zhifei_user"
)

# ================= 日志 =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 熔断 =================
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_MODEL_HEALTHY = True
_MAX_CONSECUTIVE_FAILURES = {"network": 5, "api": 3, "model": 2}

def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
    _MODEL_HEALTHY = True

def mark_failure(error_type: str):
    global _FAIL_COUNTER, _MODEL_HEALTHY
    if error_type not in _FAIL_COUNTER:
        return
    _FAIL_COUNTER[error_type] += 1
    if error_type == "model" and _FAIL_COUNTER["model"] >= _MAX_CONSECUTIVE_FAILURES["model"]:
        _MODEL_HEALTHY = False
        logger.warning("⛔ 模型服务熔断：连续 %d 次模型调用失败", _FAIL_COUNTER["model"])

def is_model_healthy() -> bool:
    return _MODEL_HEALTHY

# ================= RDS 操作 =================
def get_rds_connection():
    return pymysql.connect(host=RDS_HOST, port=RDS_PORT, user=RDS_USER, password=RDS_PASSWORD, database=RDS_DATABASE, charset='utf8mb4')

def get_or_create_session() -> str:
    if "session_id" not in st.session_state:
        try:
            conn = get_rds_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT session_id FROM chat_memory ORDER BY ts DESC LIMIT 1")
            row = cursor.fetchone()
            conn.close()
            if row:
                st.session_state.session_id = row[0]
            else:
                st.session_state.session_id = str(uuid.uuid4())
        except:
            st.session_state.session_id = str(uuid.uuid4())
    return st.session_state.session_id

def save_to_rds(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = get_rds_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO {RDS_CHAT_TABLE} (session_id, round_num, messages, ts) VALUES (%s, %s, %s, %s)",
            (session_id, round_num, json.dumps(messages, ensure_ascii=False), ts)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"RDS 写入失败: {e}")
        mark_failure("database")

# ================= OSS 操作 =================
def get_oss_client():
    client = AcsClient(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

def read_oss():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        result = bucket.get_object(remote)
        content = result.read().decode('utf-8')
        lines = [json.loads(line) for line in content.strip().split('\n') if line.strip()]
        return lines
    except:
        return []

def write_oss(lines):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        content = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n"
        bucket.put_object(remote, content.encode('utf-8'))
        return True
    except Exception as e:
        logger.warning(f"OSS 写入失败: {e}")
        mark_failure("network")
        return False

def sync_to_oss(lines):
    existing = read_oss()
    existing_ids = {(m.get("session_id"), m.get("round_num")) for m in existing if isinstance(m, dict)}
    new_lines = []
    for m in lines:
        if not isinstance(m, dict):
            continue
        key = (m.get("session_id"), m.get("round_num"))
        if key not in existing_ids:
            new_lines.append(m)
            existing_ids.add(key)
    if new_lines:
        write_oss(existing + new_lines)
        logger.info(f"✅ OSS 同步: {len(new_lines)} 条")
    return len(new_lines)

def get_recent_messages(limit=5):
    lines = read_oss()
    if not lines:
        return []
    valid_lines = [item for item in lines if isinstance(item, dict)]
    if not valid_lines:
        return []
    sorted_lines = sorted(valid_lines, key=lambda x: x.get("round_num", 0), reverse=True)
    recent = sorted_lines[:limit]
    result = []
    for item in reversed(recent):
        msgs_data = item.get("messages", {})
        if isinstance(msgs_data, str):
            try:
                msgs_data = json.loads(msgs_data)
            except:
                msgs_data = {}
        for msg in msgs_data.get("messages", []):
            result.append({
                "role": msg.get("role"),
                "content": msg.get("content"),
                "session_id": item.get("session_id"),
                "round_num": item.get("round_num")
            })
    return result

# ================= OSS 摘要缓存 =================
_SUMMARY_CACHE = {"content": "", "ts": 0}
_SUMMARY_CACHE_TTL = 60 * 5

def get_summary() -> str:
    global _SUMMARY_CACHE
    now = time.time()
    if _SUMMARY_CACHE["ts"] > now - _SUMMARY_CACHE_TTL:
        return _SUMMARY_CACHE["content"]

    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_FILE
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        content = data.get("summary", "")
        _SUMMARY_CACHE = {"content": content, "ts": now}
        return content
    except Exception as e:
        logger.warning(f"OSS 摘要读取失败，返回空摘要: {e}")
        return ""

# ================= 百炼长期记忆 =================
def write_to_memory(user_msg: str, assistant_msg: str, user_id: str = None) -> bool:
    """
    将本轮对话写入百炼长期记忆库
    """
    if not DASHSCOPE_API_KEY:
        logger.warning("DASHSCOPE_API_KEY 未配置，跳过记忆写入")
        return False

    if user_id is None:
        user_id = MEMORY_USER_ID

    try:
        url = "https://dashscope.aliyuncs.com/api/v2/apps/memory/add"
        headers = {
            "Authorization": f"Bearer {DASHSCOPE_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "messages": [
                {"role": "user", "content": user_msg},
                {"role": "assistant", "content": assistant_msg}
            ],
            "user_id": user_id,
            "memory_library_id": MEMORY_LIBRARY_ID
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ 长期记忆写入成功")
            return True
        else:
            logger.warning(f"⚠️ 长期记忆写入失败: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        logger.warning(f"⚠️ 长期记忆写入异常: {e}")
        return False

# ================= SQLite 操作 =================
def init_memory_db():
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS chat_memory_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                round_num INTEGER NOT NULL,
                messages TEXT NOT NULL,
                ts TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"SQLite 初始化失败: {e}")

def save_to_sqlite(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
            (session_id, round_num, json.dumps(messages, ensure_ascii=False), ts)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"SQLite 写入失败: {e}")
        mark_failure("database")

def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    sys_parts = [
        f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"
    ]

    summary = get_summary()
    if summary:
        sys_parts.append(f"\n【历史对话摘要】\n{summary}")

    recent = get_recent_messages(limit=3)
    if recent:
        sys_parts.append("\n【最近对话（用于接续上文）】")
        for m in recent:
            role = "用户" if m["role"] == "user" else "助手"
            content = m.get("content", "")[:300]
            sys_parts.append(f"{role}：{content}")

    sys_p = "\n".join(sys_parts)
    full_msgs = [{"role": "system", "content": sys_p}] + messages

    retries, delay = 3, 2
    for attempt in range(retries):
        try:
            resp = dashscope.Generation.call(
                model=MODEL_NAME, messages=full_msgs,
                result_format="message", stream=False
            )
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                reset_health_status()
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            logger.warning(f"第{attempt+1}次调用失败: {e}")
            if attempt == retries - 1:
                mark_failure("api")
                raise RuntimeError(f"模型连续{retries}次调用失败")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("未知错误")

def export_txt(messages):
    out = ""
    for m in messages:
        role = "用户" if m["role"] == "user" else "助手"
        out += f"{role}：{m.get('content', '')}\n\n"
    return out

# ================= UI =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")

st.title("📱 智飞投研")

init_memory_db()

if "messages" not in st.session_state:
    st.session_state.messages = get_recent_messages(limit=3)
if "session_id" not in st.session_state:
    st.session_state.session_id = get_or_create_session()
if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False

total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")

# ===== 渲染所有已存在的消息 =====
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

# ===== 输入 =====
user_input = st.chat_input("输入消息...")

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

# ===== 如果有待处理的用户消息 =====
if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]

    ctx = st.session_state.messages

    try:
        with st.spinner("💭 思考中..."):
            reply = call_bailian(ctx)

        assistant_msg = {
            "role": "assistant",
            "content": reply,
            "timestamp": datetime.now(BEIJING_TZ).isoformat()
        }
        st.session_state.messages.append(assistant_msg)

        messages_dict = {"messages": [user_msg, assistant_msg]}
        save_to_rds(session_id, round_num, messages_dict, user_msg["timestamp"])
        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])
        sync_to_oss([{
            "session_id": session_id,
            "round_num": round_num,
            "messages": messages_dict,
            "ts": user_msg["timestamp"]
        }])

        # ===== 写入百炼长期记忆 =====
        write_to_memory(user_msg["content"], assistant_msg["content"], MEMORY_USER_ID)

    except Exception as e:
        st.error(f"❌ 错误: {e}")

    st.session_state.generating = False
    st.rerun()

# ---- 生成中的暂停 ----
if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] != "user":
    with st.chat_message("assistant"):
        st.markdown("⏳ 生成中...")
    if st.button("⏹ 暂停", use_container_width=True):
        st.session_state.stop = True
        st.rerun()

# ---- 操作按钮 ----
if not st.session_state.generating and st.session_state.messages:
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔄 重新生成", use_container_width=True):
            if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
                st.session_state.messages.pop()
                st.session_state.generating = True
                st.session_state.stop = False
                st.rerun()
    with col2:
        if st.button("📤 导出TXT", use_container_width=True):
            txt = export_txt(st.session_state.messages)
            st.download_button("📥 下载", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.txt", "text/plain", key="dl")
    with col3:
        if st.button("📤 同步OSS", use_container_width=True):
            with st.spinner("同步中..."):
                session_id = st.session_state.session_id
                round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
                messages_dict = {"messages": st.session_state.messages[-2:] if len(st.session_state.messages) >= 2 else st.session_state.messages}
                sync_to_oss([{
                    "session_id": session_id,
                    "round_num": round_num,
                    "messages": messages_dict,
                    "ts": datetime.now(BEIJING_TZ).isoformat()
                }])
                st.success("✅ 同步完成")