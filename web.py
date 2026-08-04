#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v11（2026-08-04）
- 基于 V10，恢复 V7 稳定的 load_session_from_oss
- 新增 get_valid_latest_session：指针失效时自动扫描 OSS 目录兜底
- 模型自主从 OSS 恢复上文（call_bailian 告诉路径）
- 纯 OSS 架构，无 RDS 依赖
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
import threading
from datetime import datetime
from typing import List, Dict, Tuple, Optional

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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEMORY_DB_PATH = os.path.join(BASE_DIR, "chat_memory.db")

OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================= 熔断（线程安全） =================
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_FAIL_LOCK = threading.Lock()
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
    session_name = f"web-{uuid.uuid4().hex[:8]}"
    req.set_RoleSessionName(session_name)
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)


def oss_get_with_retry(bucket, remote_path, max_retry=3, delay=1, **kwargs):
    for attempt in range(max_retry):
        try:
            return bucket.get_object(remote_path, **kwargs)
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS 读取重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))
    return None


# ================= 核心读写 =================

def _ensure_appendable_with_lock(bucket, remote_path: str) -> int:
    try:
        meta = bucket.head_object(remote_path)
        if meta.headers.get('x-oss-object-type') == 'Appendable':
            return meta.content_length
        else:
            result = bucket.get_object(remote_path)
            content = result.read()
            bucket.put_object(remote_path, content)
            return len(content)
    except oss2.exceptions.NoSuchKey:
        return 0


def write_session_to_oss(session_id: str, round_num: int, round_messages: dict, ts: str):
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        pos = _ensure_appendable_with_lock(bucket, remote_path)

        content = json.dumps({
            "session_id": session_id,
            "round_num": round_num,
            "messages": round_messages,
            "ts": ts
        }, ensure_ascii=False) + '\n'

        bucket.append_object(remote_path, pos, content.encode('utf-8'))
        logger.info(f"✅ 写入 Session {session_id}, round {round_num}")

        _update_latest_session(session_id, ts)

    except Exception as e:
        logger.warning(f"写入 OSS 失败: {e}")
        raise


def _update_latest_session(session_id: str, ts: str):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "latest_session.json"
        data = {"session_id": session_id, "ts": ts}
        bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
        logger.info(f"✅ 更新最新 Session 指针: {session_id}")
    except Exception as e:
        logger.warning(f"更新最新 Session 指针失败: {e}")


def get_latest_session_id_from_oss() -> tuple:
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "latest_session.json"
        result = oss_get_with_retry(bucket, remote)
        if result is None:
            return "", ""
        data = json.loads(result.read().decode('utf-8'))
        return data.get("session_id", ""), data.get("ts", "")
    except Exception as e:
        logger.warning(f"读取最新 Session 失败: {e}")
        return "", ""


# V11 新增：指针失效时自动扫描 OSS 目录兜底
def get_valid_latest_session() -> tuple:
    """获取有效的最新 Session：优先读 latest_session.json，失败则扫描 OSS 目录"""
    # 1. 优先读指针文件
    sid, ts = get_latest_session_id_from_oss()
    if sid:
        # 验证这个 session 是否有数据
        msgs = load_session_from_oss(sid, num_rounds=1)
        if msgs:
            return sid, ts
        logger.warning(f"⚠️ latest_session.json 指向 {sid}，但该 session 无数据，回退扫描 OSS 目录")

    # 2. 指针文件无效，扫描 OSS 目录找最新的有数据的 session
    try:
        bucket = get_oss_client()
        latest_file = None
        latest_time = 0
        for obj in oss2.ObjectIterator(bucket, prefix=OSS_PREFIX):
            if obj.key.endswith('.jsonl') and not obj.key.endswith('.tmp_append'):
                if obj.last_modified > latest_time:
                    latest_time = obj.last_modified
                    latest_file = obj.key
        if latest_file:
            sid = latest_file.split('/')[-1].replace('.jsonl', '')
            logger.info(f"✅ 扫描 OSS 目录找到有效 session: {sid}")
            return sid, datetime.fromtimestamp(latest_time).isoformat()
    except Exception as e:
        logger.warning(f"扫描 OSS 目录失败: {e}")

    return "", ""


# V11 核心：load_session_from_oss 恢复 V7 稳定版
def load_session_from_oss(session_id: str, num_rounds: int = 3) -> List[Dict]:
    """从 {session_id}.jsonl 读取最近 N 轮对话（V7 稳定版）"""
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


def get_cumulative_summary_from_oss() -> str:
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = oss_get_with_retry(bucket, remote)
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
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = oss_get_with_retry(bucket, remote)
        if result is None:
            return {"window": [], "cumulative": ""}
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
        combined = f"{previous}\n\n---\n\n{new_summary}"
        if len(combined) > 2000:
            combined_bytes = combined.encode('utf-8')[:2000]
            while combined_bytes and (combined_bytes[-1] & 0xC0) == 0x80:
                combined_bytes = combined_bytes[:-1]
            combined = combined_bytes.decode('utf-8', errors='ignore')
        prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{combined}"
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
        content = json.dumps(messages, ensure_ascii=False)
        if len(content) > 5000:
            content_bytes = content.encode('utf-8')[:5000]
            while content_bytes and (content_bytes[-1] & 0xC0) == 0x80:
                content_bytes = content_bytes[:-1]
            content = content_bytes.decode('utf-8', errors='ignore')
        prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{content}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
    except:
        pass
    return ""


def load_full_session_from_oss(session_id: str) -> List[Dict]:
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        try:
            meta = bucket.head_object(remote_path)
            file_size = meta.content_length
            if file_size > 10 * 1024 * 1024:
                logger.warning(f"⚠️ Session {session_id} 文件过大 ({file_size} bytes)，仅读取尾部 1MB")
                read_size = min(file_size, 1024 * 1024)
                start = file_size - read_size
                result = oss_get_with_retry(bucket, remote_path, byte_range=(start, file_size - 1))
                if result is None:
                    return []
                content = result.read().decode('utf-8')
                if start > 0:
                    newline_pos = content.find('\n')
                    if newline_pos >= 0:
                        content = content[newline_pos + 1:]
                    else:
                        return []
            else:
                result = oss_get_with_retry(bucket, remote_path)
                if result is None:
                    return []
                content = result.read().decode('utf-8')

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
    if not old_session_id:
        logger.warning("⚠️ 后加载跳过：old_session_id 为空")
        return

    old_messages = load_full_session_from_oss(old_session_id)
    if not old_messages:
        logger.warning(f"⚠️ 后加载跳过：旧 Session {old_session_id} 无数据")
        return

    summary = generate_summary(old_messages)
    if not summary:
        logger.warning(f"⚠️ 后加载跳过：摘要生成失败")
        return

    data = _read_summary_window()
    window = data.get("window", [])
    cumulative = data.get("cumulative", "")

    if not window and not cumulative:
        logger.warning("⚠️ 摘要窗口读取失败，尝试保留旧缓存")
        if "cached_summary" in st.session_state and st.session_state.cached_summary:
            cumulative = st.session_state.cached_summary
            logger.info(f"✅ 使用缓存摘要替代空窗口")
        else:
            logger.info(f"🆕 无历史摘要，以当前摘要作为初始累积摘要")

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
        st.session_state.cached_summary = cumulative
        logger.info(f"✅ 后加载完成：{old_session_id} 已固化")
    else:
        logger.info(f"ℹ️ 后加载跳过：{old_session_id} 已存在摘要")


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
        messages_json = json.dumps(messages, ensure_ascii=False)
        cursor.execute("INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
                       (session_id, round_num, messages_json, ts))
        conn.commit()
        conn.close()
    except:
        pass


# ================= 模型调用 =================

MAX_INPUT_TOKENS = 8000


def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    session_id = st.session_state.get("session_id", "")
    current_time = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')
    prefix_raw = OSS_PREFIX.rstrip('/')

    total_tokens = sum(estimate_tokens(str(m.get("content", ""))) for m in messages)
    if total_tokens > MAX_INPUT_TOKENS:
        raise RuntimeError(f"输入过长（~{total_tokens} tokens），请缩短消息后重试")

    oss_instruction = f"""
你是智飞投研助手。当前时间：{current_time}

你有能力通过百炼的 getOSSFile 工具直接读取 OSS 上的文件。
当前会话的上下文存储在以下路径：
- 当前 Session 对话文件：{prefix_raw}/{session_id}.jsonl
- 全局摘要文件：{prefix_raw}/chat_summary_window.json

请按以下步骤操作：
1. 使用 getOSSFile 工具读取全局摘要文件，获取历史对话摘要。
2. 使用 getOSSFile 工具读取当前 Session 对话文件的尾部内容，获取最近几轮对话。
3. 基于读取到的上下文，回答用户的问题。

规则：直接输出结论+关键数据+风险提示。不展示工具调用过程。
"""

    sys_p = oss_instruction
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
        st.session_state.cached_summary = ""

    if not st.session_state.messages and not st.session_state.history_loaded:
        # V11 修复：使用 get_valid_latest_session，指针失效时自动扫描 OSS 目录兜底
        session_id, ts = get_valid_latest_session()
        if session_id:
            st.session_state.session_id = session_id
        else:
            st.session_state.session_id = str(uuid.uuid4())
            logger.info(f"🆕 未找到任何历史 Session，新建: {st.session_state.session_id}")

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

    except RuntimeError as e:
        st.error(f"❌ {str(e)}")
    except Exception as e:
        st.error(f"❌ 错误: {str(e)}")

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