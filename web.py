#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端轻量版 v7.3（2026-08-04）
- 基于 V7.2，修复 5 个缺陷
- 1. trigger_backup_and_restore：_read_summary_window 失败时不再覆盖 OSS 数据
- 2. call_bailian_with_token_check：预留系统提示词 token 余量（2000 tokens）
- 3. call_bailian：区分网络错误 vs API 错误，正确分类 mark_failure
- 4. generate_summary / merge_cumulative：增加重试机制
- 5. export_txt：修复 content 为 None 时的字符串拼接
- 6. 下载按钮 key 改为动态避免重复
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


def _classify_error(e: Exception) -> str:
    """V7.3 新增：根据异常类型区分 network / api / model"""
    err_str = str(e).lower()
    if any(kw in err_str for kw in ["timeout", "connection", "network", "resolve", "refused"]):
        return "network"
    if any(kw in err_str for kw in ["rate", "quota", "throttle", "limit", "429"]):
        return "api"
    return "model"


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


# ================= 1：OSS 读取重试 =================
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


# ================= 2：有效 Session 扫描兜底（V7.2 修复：删掉多余验证） =================
def get_valid_latest_session() -> tuple:
    """获取有效的最新 Session：优先读 latest_session.json，失败则扫描 OSS 目录兜底"""
    try:
        bucket = get_oss_client()
    except Exception as e:
        logger.warning(f"获取 OSS client 失败: {e}")
        return "", ""

    # 1. 优先读指针文件（直接信任，不验证数据）
    try:
        remote = OSS_PREFIX + "latest_session.json"
        result = oss_get_with_retry(bucket, remote)
        if result is not None:
            data = json.loads(result.read().decode('utf-8'))
            sid = data.get("session_id", "")
            ts = data.get("ts", "")
            if sid:
                logger.info(f"✅ 从 latest_session.json 读取 session: {sid}")
                return sid, ts
    except Exception as e:
        logger.warning(f"读取 latest_session.json 失败: {e}")

    # 2. 指针文件不存在或读取失败，扫描 OSS 目录找最新的 .jsonl
    try:
        latest_file = None
        latest_time = 0
        for obj in oss2.ObjectIterator(bucket, prefix=OSS_PREFIX):
            if obj.key.endswith('.jsonl') and not obj.key.endswith('.tmp_append'):
                if obj.last_modified > latest_time:
                    latest_time = obj.last_modified
                    latest_file = obj.key
        if latest_file:
            sid = latest_file.split('/')[-1].replace('.jsonl', '')
            logger.info(f"✅ 扫描 OSS 目录找到 session: {sid}")
            return sid, datetime.fromtimestamp(latest_time).isoformat()
    except Exception as e:
        logger.warning(f"扫描 OSS 目录失败: {e}")

    return "", ""


# ================= V7 原版代码 =================

def _ensure_appendable(bucket, remote_path: str) -> int:
    try:
        meta = bucket.head_object(remote_path)
        if meta.headers.get('x-oss-object-type') == 'Appendable':
            return meta.content_length
        else:
            result = bucket.get_object(remote_path)
            content = result.read()
            bucket.delete_object(remote_path)
            bucket.append_object(remote_path, 0, content)
            return len(content)
    except oss2.exceptions.NoSuchKey:
        return 0


def write_session_to_oss(session_id: str, round_num: int, round_messages: dict, ts: str):
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


def load_session_from_oss(session_id: str, num_rounds: int = 3) -> List[Dict]:
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
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "latest_session.json"
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("session_id", ""), data.get("ts", "")
    except Exception:
        return "", ""


def get_cumulative_summary_from_oss() -> str:
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("cumulative", "")
    except Exception as e:
        logger.debug(f"读取累积摘要失败: {e}")
        return ""


SESSION_WINDOW_SIZE = 12


def _read_summary_window():
    """V7.3 修复：返回 (data, success) 元组，success=False 表示读取失败"""
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + "chat_summary_window.json"
        result = bucket.get_object(remote)
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
        remote = OSS_PREFIX + "chat_summary_window.json"
        data = {"window": window, "cumulative": cumulative}
        bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")


def merge_cumulative(previous: str, new_summary: str) -> str:
    """V7.3 修复：增加重试机制"""
    if not previous:
        return new_summary
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{previous}\n---\n{new_summary}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                result_format="message"
            )
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
    """V7.3 修复：增加重试机制"""
    if not messages:
        return ""
    retries, delay = 2, 1
    for attempt in range(retries):
        try:
            prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"
            dashscope.api_key = DASHSCOPE_API_KEY
            resp = dashscope.Generation.call(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                result_format="message"
            )
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


# ================= 4：后加载缓存兜底（V7.3 修复） =================
def trigger_backup_and_restore(old_session_id: str):
    """V7.3 修复：_read_summary_window 失败时不覆盖 OSS 数据"""
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

    data, read_ok = _read_summary_window()
    window = data.get("window", [])
    cumulative = data.get("cumulative", "")

    if not read_ok:
        # V7.3：读取失败，不覆盖 OSS，仅用 session_state 缓存兜底
        logger.warning("⚠️ 摘要窗口读取失败，跳过 OSS 写入，仅保留内存缓存")
        if "cached_summary" in st.session_state and st.session_state.cached_summary:
            st.session_state.cached_summary = merge_cumulative(
                st.session_state.cached_summary, summary
            )
        else:
            st.session_state.cached_summary = summary
        return

    # 读取成功，正常处理
    if not window and not cumulative:
        if "cached_summary" in st.session_state and st.session_state.cached_summary:
            cumulative = st.session_state.cached_summary
            logger.info(f"✅ 使用缓存摘要替代空窗口")

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


# ================= V7.3 修复：call_bailian（区分错误类型） =================
def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY

    sys_parts = [f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"]

    session_id = st.session_state.get("session_id", "")

    cumulative = get_cumulative_summary_from_oss()
    if cumulative:
        sys_parts.append(f"\n【历史对话摘要】\n{cumulative}")

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
            resp = dashscope.Generation.call(
                model=MODEL_NAME, messages=full_msgs, result_format="message", stream=False
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


def export_txt(messages):
    """V7.3 修复：处理 content 为 None 的情况"""
    lines = []
    for m in messages:
        role = "用户" if m.get("role") == "user" else "助手"
        content = m.get("content") or ""
        lines.append(f"{role}：{content}")
    return "\n\n".join(lines)


# ================= 5：启动初始化替换 =================
def init_session_on_startup():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.history_loaded = False
        st.session_state.session_id = ""
        st.session_state.cached_summary = ""

    if not st.session_state.messages and not st.session_state.history_loaded:
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


# ================= 3：Token 自动截断（V7.3 修复：预留系统提示词余量） =================
def call_bailian_with_token_check(messages: List[Dict]) -> str:
    """V7.3 修复：预留 2500 tokens 给系统提示词（时间+累积摘要+最近对话）"""
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
        st.markdown(str(m.get("content") or ""))

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
            reply = call_bailian_with_token_check(
                [{"role": m["role"], "content": str(m.get("content") or "")} for m in st.session_state.messages]
            )

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
            download_key = f"dl_{datetime.now(BEIJING_TZ).strftime('%Y%m%d%H%M%S')}"
            st.download_button(
                "📥 下载", txt,
                f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.txt",
                "text/plain", key=download_key
            )
