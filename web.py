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
            if value: return value
        except: pass
    return os.getenv(key, default)
 
DASHSCOPE_API_KEY = get_secret_or_env("DASHSCOPE_API_KEY", "dashscope.api_key")
if not DASHSCOPE_API_KEY: raise RuntimeError("请配置 DASHSCOPE_API_KEY")
 
MODEL_NAME = get_secret_or_env("MODEL_NAME", "model.name", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "./chat_memory.db")
 
OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_FILENAME = "chat_history.jsonl"
OSS_SUMMARY_FILE = "chat_summary.json"
OSS_SUMMARY_WINDOW_FILE = "chat_summary_window.json"
SESSION_WINDOW_SIZE = 12
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_MODEL_HEALTHY = True
_MAX_CONSECUTIVE_FAILURES = {"network": 5, "api": 3, "model": 2}
 
def reset_health_status():
    global _FAIL_COUNTER, _MODEL_HEALTHY
    _FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
    _MODEL_HEALTHY = True
 
def mark_failure(error_type: str):
    global _FAIL_COUNTER, _MODEL_HEALTHY
    if error_type not in _FAIL_COUNTER: return
    _FAIL_COUNTER[error_type] += 1
    if error_type == "model" and _FAIL_COUNTER["model"] >= _MAX_CONSECUTIVE_FAILURES["model"]:
        _MODEL_HEALTHY = False
 
def is_model_healthy() -> bool:
    return _MODEL_HEALTHY
 
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
 
def read_oss_tail(size=40960):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        try:
            meta = bucket.head_object(remote)
            length = meta.content_length
            read_size = min(length, size)
            start = length - read_size
            result = bucket.get_object(remote, byte_range=(start, length - 1))
            content = result.read().decode('utf-8')
            if start > 0:
                content = content[content.find('\n')+1:]
            lines = [json.loads(line) for line in content.strip().split('\n') if line.strip()]
            if len(lines) < 6:
                raise Exception("尾部数据不足")
            return lines
        except:
            logger.info("执行全量读取保底...")
            result = bucket.get_object(remote)
            content = result.read().decode('utf-8')
            return [json.loads(line) for line in content.strip().split('\n') if line.strip()]
    except:
        return []
 
def read_oss_full():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        result = bucket.get_object(remote)
        content = result.read().decode('utf-8')
        return [json.loads(line) for line in content.strip().split('\n') if line.strip()]
    except:
        return []
 
def write_oss(lines):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        content = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n"
        bucket.put_object(remote, content.encode('utf-8'))
        return True
    except:
        return False
 
def sync_to_oss(lines):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        
        if "oss_pos" not in st.session_state:
            meta = bucket.head_object(remote)
            if meta.headers.get('x-oss-object-type') == 'Appendable':
                st.session_state.oss_pos = meta.content_length
            else:
                raise Exception("Normal 文件，降级全量写")
        
        for line in lines:
            content = json.dumps(line, ensure_ascii=False) + '\n'
            content_bytes = content.encode('utf-8')
            bucket.append_object(remote, st.session_state.oss_pos, content_bytes)
            st.session_state.oss_pos += len(content_bytes)
            
    except Exception as e:
        if "oss_pos" in st.session_state:
            del st.session_state.oss_pos
        existing = read_oss_full()
        existing_ids = {(m.get("session_id"), m.get("round_num")) for m in existing if isinstance(m, dict)}
        new_lines = [m for m in lines if isinstance(m, dict) and (m.get("session_id"), m.get("round_num")) not in existing_ids]
        if new_lines:
            write_oss(existing + new_lines)
 
def get_recent_messages(limit=3):
    lines = read_oss_tail()
    if not lines: return []
    valid_lines = [item for item in lines if isinstance(item, dict)]
    if not valid_lines: return []
    
    sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=False)
    recent = sorted_lines[-limit:]
    
    result = []
    for item in recent:
        msgs_data = item.get("messages", {})
        if isinstance(msgs_data, str):
            try: msgs_data = json.loads(msgs_data)
            except: msgs_data = {}
        for msg in msgs_data.get("messages", []):
            result.append({
                "role": msg.get("role"),
                "content": msg.get("content"),
                "session_id": item.get("session_id"),
                "round_num": item.get("round_num")
            })
    return result
 
def get_summary():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_FILE
        result = bucket.get_object(remote)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("summary", "")
    except:
        return ""
 
def _read_summary_window():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
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
        remote = OSS_PREFIX + OSS_SUMMARY_WINDOW_FILE
        data = {"window": window, "cumulative": cumulative}
        bucket.put_object(remote, json.dumps(data, ensure_ascii=False).encode('utf-8'))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")
 
def merge_cumulative(previous: str, new_summary: str) -> str:
    if not previous: return new_summary
    try:
        prompt = f"将以下两份摘要合并为一份200字以内的整体摘要：\n{previous}\n---\n{new_summary}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role":"user","content":prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
    except: pass
    return previous
 
def generate_summary(messages: List[Dict]) -> str:
    if not messages: return ""
    try:
        prompt = f"将以下对话压缩成300字摘要，突出核心标的、数据和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role":"user","content":prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices:
            return resp.output.choices[0].message.content
    except: pass
    return ""
 
def update_summary(old_summary, recent_msgs):
    dashscope.api_key = DASHSCOPE_API_KEY
    prompt = f"你是投研对话摘要助手。请根据以下已有摘要和最新对话，更新生成一份精炼摘要，保留核心标的、数据与逻辑推理，不超过500字。\n已有摘要：{old_summary}\n最新对话：{json.dumps(recent_msgs, ensure_ascii=False)}"
    try:
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role":"user","content":prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK:
            new_summary = resp.output.choices[0].message.content
            bucket = get_oss_client()
            remote = OSS_PREFIX + OSS_SUMMARY_FILE
            bucket.put_object(remote, json.dumps({"summary": new_summary}, ensure_ascii=False).encode('utf-8'))
            return new_summary
    except: pass
    return old_summary
 
def trigger_backup_and_restore(old_session_id: str):
    if not old_session_id: return
    
    all_lines = read_oss_full()
    old_lines = [line for line in all_lines if line.get("session_id") == old_session_id]
    if not old_lines: return
    
    old_messages = []
    for item in old_lines:
        msgs_data = item.get("messages", {})
        if isinstance(msgs_data, str):
            try: msgs_data = json.loads(msgs_data)
            except: msgs_data = {}
        for msg in msgs_data.get("messages", []):
            old_messages.append(msg)
            
    if not old_messages: return
    
    summary = generate_summary(old_messages)
    if not summary: return
    
    data = _read_summary_window()
    window = data["window"]
    cumulative = data["cumulative"]
    
    existing_sids = [w.get("session_id") for w in window]
    if old_session_id not in existing_sids:
        cumulative = merge_cumulative(cumulative, summary)
        window.append({"session_id": old_session_id, "summary": summary, "created_at": datetime.now(BEIJING_TZ).isoformat()})
        if len(window) > SESSION_WINDOW_SIZE:
            window = window[-SESSION_WINDOW_SIZE:]
        _save_summary_window(window, cumulative)
    
    recent_msgs = old_messages[-6:] if len(old_messages) >= 6 else old_messages
    st.session_state._recovery_context = {
        "summary": cumulative,
        "recent_msgs": recent_msgs,
        "source_session": old_session_id
    }
 
def init_session_on_startup():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.history_loaded = False
        st.session_state.summary = ""
        st.session_state.full_context = []
        st.session_state._recovery_context = {}
 
    if not st.session_state.messages and not st.session_state.history_loaded:
        st.session_state.messages = get_recent_messages(limit=3)
 
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
    except: pass
 
def save_to_sqlite(session_id: str, round_num: int, messages: dict, ts: str):
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO chat_memory_new (session_id, round_num, messages, ts) VALUES (?, ?, ?, ?)",
            (session_id, round_num, json.dumps(messages, ensure_ascii=False), ts))
        conn.commit()
        conn.close()
    except: pass
 
def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy(): raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY
 
    sys_parts = [f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"]
    
    recovery_ctx = st.session_state.get("_recovery_context", {})
    if recovery_ctx.get("summary") or recovery_ctx.get("recent_msgs"):
        if recovery_ctx.get("summary"):
            sys_parts.append(f"\n【跨会话历史摘要】\n{recovery_ctx['summary']}")
        if recovery_ctx.get("recent_msgs"):
            sys_parts.append("\n【上一会话最近对话（用于接续上文）】")
            for m in recovery_ctx["recent_msgs"]:
                role = "用户" if m.get("role") == "user" else "助手"
                content = str(m.get("content", ""))[:300]
                sys_parts.append(f"{role}：{content}")
        st.session_state._recovery_context = {}
    else:
        if st.session_state.summary:
            sys_parts.append(f"\n【历史对话摘要】\n{st.session_state.summary}")
        recent_ctx = st.session_state.full_context[-20:]
        if recent_ctx:
            sys_parts.append("\n【最近对话原话】")
            for m in recent_ctx:
                role = "用户" if m["role"] == "user" else "助手"
                content = m.get("content", "")[:500]
                sys_parts.append(f"{role}：{content}")
 
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
    return "\n\n".join([f"{'用户' if m['role']=='user' else '助手'}：{m.get('content','')}" for m in messages])
 
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
    st.session_state.summary = get_summary()
    st.session_state.full_context = get_recent_messages(limit=10)
    if st.session_state.full_context:
        st.session_state.session_id = st.session_state.full_context[-1].get("session_id", str(uuid.uuid4()))
    else:
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.full_context = []
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
    st.session_state.full_context.append(msg)
    st.rerun()
 
if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    session_id = st.session_state.session_id
    round_num = len([m for m in st.session_state.full_context if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]
    ctx = [{"role": m["role"], "content": m["content"]} for m in st.session_state.full_context]
 
    try:
        with st.spinner("💭 思考中..."):
            reply = call_bailian(ctx)
        assistant_msg = {"role": "assistant", "content": reply, "timestamp": datetime.now(BEIJING_TZ).isoformat()}
        st.session_state.messages.append(assistant_msg)
        st.session_state.full_context.append(assistant_msg)
 
        messages_dict = {"messages": [user_msg, assistant_msg]}
        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])
        sync_to_oss([{
            "session_id": session_id,
            "round_num": round_num,
            "messages": messages_dict,
            "ts": user_msg["timestamp"]
        }])
 
        if round_num % 5 == 0:
            recent_5 = st.session_state.full_context[-10:]
            st.session_state.summary = update_summary(st.session_state.summary, recent_5)
 
        if len(st.session_state.messages) > 6:
            st.session_state.messages = st.session_state.messages[-6:]
 
    except Exception as e:
        st.error(f"❌ 错误: {e}")
    
    st.session_state.generating = False
    st.rerun()
 
if not st.session_state.generating and st.session_state.messages:
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("➕ 新建会话", use_container_width=True):
            old_sid = st.session_state.session_id
            if old_sid and st.session_state.full_context:
                trigger_backup_and_restore(old_sid)
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.messages = []
            st.session_state.full_context = []
            st.session_state.summary = ""
            st.session_state.generating = False
            st.session_state.history_loaded = True
            st.rerun()
    with col2:
        if st.button("🔄 重新生成", use_container_width=True):
            if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
                st.session_state.messages.pop()
                st.session_state.full_context.pop()
                st.session_state.generating = True
                st.rerun()
    with col3:
        if st.button("📤 导出TXT", use_container_width=True):
            txt = export_txt(st.session_state.full_context)
            st.download_button("📥 下载", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.txt", "text/plain", key="dl")