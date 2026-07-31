#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v7.0（2026-07-31）
- 手机端专用：OSS 同步，摘要 + 3轮对话恢复
- 双写：RDS + SQLite，每10轮同步 OSS
- 无侧边栏、无历史会话、无编辑删除
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

# ================= 日志 =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
        logger.warning("熔断已触发，连续失败 %d 次", _FAIL_COUNTER)

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
        return False

def sync_to_oss(messages):
    existing = read_oss()
    existing_ids = {(m.get("session_id"), m.get("round_num")) for m in existing if isinstance(m, dict)}
    new_lines = []
    for m in messages:
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

# ===== 改动1：get_recent_messages 排序从 round_num 改为 ts =====
def get_recent_messages(limit=5):
    lines = read_oss()
    if not lines:
        return []
    valid_lines = [item for item in lines if isinstance(item, dict)]
    if not valid_lines:
        return []
    # 按 ts 降序取最近 N 条
    sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=True)
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

# ===== 改动2：新增 get_summary() =====
def get_summary() -> str:
    """从 OSS 读取 chat_summary.json 获取摘要"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_FILE
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("summary", "")
    except:
        return ""

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

# ===== 改动3 + 改动4：call_bailian system prompt =====
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
                mark_failure()
                raise RuntimeError(f"模型连续{retries}次调用失败")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("未知错误")

# ================= 导出 TXT =================
def export_txt(messages):
    out = ""
    for m in messages:
        role = "用户" if m["role"] == "user" else "助手"
        out += f"{role}：{m.get('content', '')}\n\n"
    return out

# ================= UI =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")

st.title("📱 智飞投研")

# 初始化
init_memory_db()

if "messages" not in st.session_state:
    st.session_state.messages = get_recent_messages(limit=3)
if "session_id" not in st.session_state:
    st.session_state.session_id = get_or_create_session()
if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False

# 显示状态
total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")

# 渲染最近3轮
for m in st.session_state.messages[-6:]:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

# ---- 输入 ----
user_input = st.chat_input("输入消息...")

if user_input and not st.session_state.generating:
    st.session_state.stop = False
    st.session_state.generating = True

    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"]) + 1

    user_msg = {"role": "user", "content": user_input, "timestamp": datetime.now(BEIJING_TZ).isoformat()}
    st.session_state.messages.append(user_msg)

    ctx = st.session_state.messages

    try:
        reply = call_bailian(ctx)
        assistant_msg = {"role": "assistant", "content": reply, "timestamp": datetime.now(BEIJING_TZ).isoformat()}
        st.session_state.messages.append(assistant_msg)

        messages_dict = {"messages": [user_msg, assistant_msg]}
        save_to_rds(session_id, round_num, messages_dict, user_msg["timestamp"])
        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])

        if round_num % 10 == 0:
            sync_to_oss([{
                "session_id": session_id,
                "round_num": round_num,
                "messages": messages_dict,
                "ts": user_msg["timestamp"]
            }])

    except Exception as e:
        st.error(f"❌ 错误: {e}")

    st.session_state.generating = False
    st.rerun()

# ---- 生成中的暂停 ----
if st.session_state.generating:
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