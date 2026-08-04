#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v7（2026-08-04）
- 统一读写路径：按 Session 隔离的独立文件 `{session_id}.jsonl`
- `latest_session.json` 作为最新 Session 指针，读写完整
- 新建会话自动更新指针，页面刷新不丢上下文
- 纯 OSS 架构，无 RDS 依赖
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
from datetime import datetime
from typing import List, Dict

import streamlit as st
import dashscope
import oss2
import pytz
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

load_dotenv()


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
    raise RuntimeError("请配置 DASHSCOPE_API_KEY")

MODEL_NAME = get_secret_or_env("MODEL_NAME", "model.name", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "./chat_memory.db")

OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")

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


def is_model_healthy() -> bool:
    return _MODEL_HEALTHY


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ch = len(re.findall(r'[\u4e00-\u9fff]', text))
    return int(ch / 1.5 + (len(text) - ch) / 4)


# ================= OSS 客户端 =================
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


# ================= v7 核心：统一按 Session 隔离读写 =================

def _ensure_appendable(bucket, remote_path: str) -> int:
    """确保文件是 Appendable 类型，返回当前可追加位置"""
    try:
        meta = bucket.head_object(remote_path)
        if meta.headers.get('x-oss-object-type') == 'Appendable':
            return meta.content_length
        else:
            # Normal 类型 → 读出来重新用 Appendable 写入
            result = bucket.get_object(remote_path)
            content = result.read()
            bucket.delete_object(remote_path)
            bucket.append_object(remote_path, 0, content)
            return len(content)
    except oss2.exceptions.NoSuchKey:
        return 0


def write_session_to_oss(session_id: str, round_num: int, round_messages: dict, ts: str):
    """按 Session 隔离写入：直接追加到 {session_id}.jsonl"""
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        pos = _ensure_appendable(bucket, remote_path)

        content = json.dumps({
            "session_id": session_id,
            "round_num": round_num,
            "messages": round_messages,
            "ts": ts
        }, ensure_ascii=False) + '\n'

        bucket.append_object(remote_path, pos, content.encode('utf-8'))
        logger.info(f"✅ 写入 Session {session_id}, round {round_num}")

        # 更新最新 Session 指针
        _update_latest_session(session_id, ts)

    except Exception as e:
        logger.warning(f"写入 OSS 失败: {e}")
        raise


def _update_latest_session(session_id: str, ts: str):
    """更新 latest_session.json 指针文件"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "latest_session.json"
        data = {"session_id": session_id, "ts": ts}
        bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
        logger.info(f"✅ 更新最新 Session 指针: {session_id}")
    except Exception as e:
        logger.warning(f"更新最新 Session 指针失败: {e}")


def load_session_from_oss(session_id: str, num_rounds: int = 3) -> List[Dict]:
    """从 {session_id}.jsonl 读取最近 N 轮对话"""
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        try:
            meta = bucket.head_object(remote_path)
            length = meta.content_length
            read_size = min(length, 20480)
            start = length - read_size
            result = bucket.get_object(remote_path, byte_range=(start, length - 1))
            content = result.read().decode('utf-8')
            if start > 0:
                content = content[content.find('\n') + 1:]

            all_msgs = []
            for line in content.strip().split('\n'):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                    if item.get("round_num", 0) == 0:
                        continue
                    msgs_data = item.get("messages", {})
                    if isinstance(msgs_data, str):
                        msgs_data = json.loads(msgs_data)
                    actual_msgs = msgs_data.get("messages", []) if isinstance(msgs_data, dict) else []
                    for msg in actual_msgs:
                        all_msgs.append({
                            "role": msg.get("role"),
                            "content": msg.get("content")
                        })
                except:
                    continue

            return all_msgs[-(num_rounds * 2):] if len(all_msgs) > num_rounds * 2 else all_msgs
        except oss2.exceptions.NoSuchKey:
            return []
    except Exception as e:
        logger.warning(f"读取 Session {session_id} 失败: {e}")
        return []


def get_latest_session_id_from_oss() -> tuple:
    """从 latest_session.json 读取最新 Session ID 和 ts"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "latest_session.json"
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("session_id", ""), data.get("ts", "")
    except Exception:
        return "", ""


# ================= 累积摘要读取 =================

def get_cumulative_summary_from_oss() -> str:
    """从 OSS 读取累积摘要"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("cumulative", "")
    except Exception as e:
        logger.debug(f"读取累积摘要失败: {e}")
        return ""


# ================= 后加载（保留） =================

SESSION_WINDOW_SIZE = 12


def _read_summary_window():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        data.setdefault("window", [])
        data.setdefault("cumulative", "")
        return data
    except:
        return {"window": [], "cumulative": ""}


def _save_summary_window(window, cumulative):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        data = {"window": window, "cumulative": cumulative}
        bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")


def merge_cumulative(previous: str, new_summary: str) -> str:
    if not previous:
        return new_summary
    try:
        prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{previous}\n---\n{new_summary}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
    except:
        pass
    return previous


def generate_summary(messages: List[Dict]) -> str:
    if not messages:
        return ""
    try:
        prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
    except:
        pass
    return ""


def load_full_session_from_oss(session_id: str) -> List[Dict]:
    """后加载专用：读取旧 Session 的完整对话"""
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        try:
            resp = bucket.get_object(remote_path)
            content = resp.read().decode('utf-8')
            all_msgs = []
            for line in content.strip().split('\n'):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                    if item.get("round_num", 0) == 0:
                        continue
                    msgs_data = item.get("messages", {})
                    if isinstance(msgs_data, str):
                        msgs_data = json.loads(msgs_data)
                    actual_msgs = msgs_data.get("messages", []) if isinstance(msgs_data, dict) else []
                    for msg in actual_msgs:
                        all_msgs.append(msg)
                except:
                    continue
            return all_msgs
        except oss2.exceptions.NoSuchKey:
            return []
    except Exception as e:
        logger.warning(f"读取完整 Session 失败: {e}")
        return []


def trigger_backup_and_restore(old_session_id: str):
    """新建会话时触发：固化旧 Session 摘要到窗口"""
    if not old_session_id:
        return

    old_messages = load_full_session_from_oss(old_session_id)
    if not old_messages:
        return

    summary = generate_summary(old_messages)
    if not summary:
        return

    data = _read_summary_window()
    window = data["window"]
    cumulative = data["cumulative"]

    existing_sids = [w.get("session_id") for w in window]
    if old_session_id not in existing_sids:
        cumulative = merge_cumulative(cumulative, summary)
        window.append({
            "session_id": old_session_id,
            "summary": summary,
            "created_at": datetime.now(BEIJING_TZ).isoformat()
        })
        if len(window) > SESSION_WINDOW_SIZE:
            window = window[-SESSION_WINDOW_SIZE:]
        _save_summary_window(window, cumulative)


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
    except:
        pass


def save_to_sqlite(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
                       (session_id, round_num, json.dumps(messages, ensure_ascii=False), ts))
        conn.commit()
        conn.close()
    except:
        pass


# ================= 模型调用 =================

def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    sys_parts = [f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"]

    session_id = st.session_state.get("session_id", "")

    # 1. 读累积摘要
    cumulative = get_cumulative_summary_from_oss()
    if cumulative:
        sys_parts.append(f"\n【历史对话摘要】\n{cumulative}")

    # 2. 读最近 5 轮对话
    if session_id:
        recent = load_session_from_oss(session_id, num_rounds=5)
        if recent:
            lines = []
            for m in recent:
                role = "用户" if m.get("role") == "user" else "助手"
                content = m.get("content", "")[:300]
                lines.append(f"{role}：{content}")
            if lines:
                sys_parts.append("\n【最近对话（用于接续上文）】\n" + "\n".join(lines))

    sys_p = "\n".join(sys_parts)
    full_msgs = [{"role": "system", "content": sys_p}] + messages

    retries, delay = 3, 2
    for attempt in range(retries):
        try:
            resp = dashscope.Generation.call(model=MODEL_NAME, messages=full_msgs, result_format="message", stream=False)
            if resp.status_code == HTTPStatus.OK and resp.output.choices:
                reset_health_status()
                return resp.output.choices[0].message.content
            else:
                raise RuntimeError(f"API Error: {resp.code} - {resp.message}")
        except Exception as e:
            if attempt == retries - 1:
                mark_failure("api")
                raise RuntimeError(f"模型连续{retries}次调用失败")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("未知错误")


def export_txt(messages):
    return "\n\n".join([f"{'用户' if m['role']=='user' else '助手'}：{m.get('content', '')}" for m in messages])


# ================= 启动初始化 =================

def init_session_on_startup():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.history_loaded = False
        st.session_state.session_id = ""

    if not st.session_state.messages and not st.session_state.history_loaded:
        # 1. 从 latest_session.json 获取最新 Session ID
        session_id, ts = get_latest_session_id_from_oss()
        if session_id:
            st.session_state.session_id = session_id
        else:
            st.session_state.session_id = str(uuid.uuid4())

        # 2. 直接读最近 3 轮
        msgs = load_session_from_oss(st.session_state.session_id, num_rounds=3)
        if msgs:
            for m in msgs:
                m["timestamp"] = datetime.now(BEIJING_TZ).isoformat()
                m["session_id"] = st.session_state.session_id
            st.session_state.messages = msgs


# ================= UI =================

st.set_page_config(page_title="智飞投研·云端", layout="centered")
st.markdown("""<style>.stApp { background: #ffffff !important; }.stChatInputContainer { position: sticky !important; bottom: 0 !important; background: #ffffff !important; z-index: 999 !important; border-top: 1px solid #e5e7eb !important; }</style>""", unsafe_allow_html=True)

st.title("📱 智飞投研")
init_memory_db()
init_session_on_startup()

if "generating" not in st.session_state:
    st.session_state.generating = False

if not st.session_state.history_loaded:
    loading_ph = st.empty()
    loading_ph.info("⏳ 上文加载中...")

total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

if not st.session_state.history_loaded:
    st.session_state.history_loaded = True
    if 'loading_ph' in locals():
        loading_ph.empty()
    st.rerun()

user_input = st.chat_input("输入消息...")

if user_input and not st.session_state.generating:
    st.session_state.generating = True
    ts = datetime.now(BEIJING_TZ).isoformat()
    msg = {"role": "user", "content": user_input, "timestamp": ts}
    st.session_state.messages.append(msg)
    st.rerun()

if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]

    try:
        with st.spinner("💭 思考中..."):
            reply = call_bailian([{"role": m["role"], "content": m["content"]} for m in st.session_state.messages])

        assistant_msg = {"role": "assistant", "content": reply, "timestamp": datetime.now(BEIJING_TZ).isoformat()}
        st.session_state.messages.append(assistant_msg)

        messages_dict = {"messages": [user_msg, assistant_msg]}
        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])
        write_session_to_oss(session_id, round_num, messages_dict, user_msg["timestamp"])

    except Exception as e:
        st.error(f"❌ 错误: {e}")

    st.session_state.generating = False
    st.rerun()

if not st.session_state.generating and st.session_state.messages:
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("➕ 新建会话", use_container_width=True):
            old_sid = st.session_state.session_id
            if old_sid and st.session_state.messages:
                trigger_backup_and_restore(old_sid)
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.messages = []
            st.session_state.generating = False
            st.session_state.history_loaded = True
            st.rerun()
    with col2:
        if st.button("🔄 重新生成", use_container_width=True):
            if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
                st.session_state.messages.pop()
                st.session_state.generating = True
                st.rerun()
    with col3:
        if st.button("📤 导出TXT", use_container_width=True):
            txt = export_txt(st.session_state.messages)
            st.download_button("📥 下载", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.txt", "text/plain", key="dl")