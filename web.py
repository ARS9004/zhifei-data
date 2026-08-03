	#!/usr/bin/env python3
	# -*- coding: utf-8 -*-
	"""
	智飞投研 · 网端 V1.1（2026-08-03）
	🚀 全局时间线整合版
	- ✅ 强制接管：死咬最后一条数据的 session_id，杜绝乱建会话
	- ✅ 全局排序：把所有碎片化对话按 ts 严格排成一条时间线
	- ✅ 上下文扩容：从只看最后 3 轮扩到最后 10 轮，接续更稳
	- ✅ 写入/读取优化：保留 O(1) 尾部读与纯追加写
	"""
	import os
	import re
	import json
	import time
	import uuid
	import logging
	import sqlite3
	from datetime import datetime, time as dt_time
	from typing import List, Dict, Tuple
	import streamlit as st
	import dashscope
	import oss2
	import pytz
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
	OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
	OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
	OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
	OSS_FILENAME = "chat_history.jsonl"
	OSS_SUMMARY_FILE = "chat_summary.json"
	OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
	OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	logger = logging.getLogger(__name__)
	# ================= 熔断机制 =================
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
	# ================= OSS 读写优化 =================
	def read_oss():
	    try:
	        bucket = get_oss_client()
	        remote = OSS_PREFIX + OSS_FILENAME
	        try:
	            meta = bucket.head_object(remote)
	            length = meta.content_length
	            read_size = min(length, 40960)
	            start = length - read_size
	            result = bucket.get_object(remote, byte_range=(start, length - 1))
	            content = result.read().decode('utf-8')
	            if start > 0:
	                content = content[content.find('\n')+1:]
	            lines = [json.loads(line) for line in content.strip().split('\n') if line.strip()]
	            if len(lines) < 10:
	                raise Exception("尾部数据不足，降级全量")
	            return lines
	        except:
	            logger.info("⚠️ 执行全量读取保底...")
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
	                result = bucket.get_object(remote)
	                content = result.read()
	                bucket.delete_object(remote)
	                bucket.append_object(remote, 0, content)
	                st.session_state.oss_pos = len(content)
	        for line in lines:
	            content = json.dumps(line, ensure_ascii=False) + '\n'
	            content_bytes = content.encode('utf-8')
	            bucket.append_object(remote, st.session_state.oss_pos, content_bytes)
	            st.session_state.oss_pos += len(content_bytes)
	    except Exception as e:
	        if "oss_pos" in st.session_state:
	            del st.session_state.oss_pos
	        existing = read_oss()
	        existing_ids = {(m.get("session_id"), m.get("round_num")) for m in existing if isinstance(m, dict)}
	        new_lines = [m for m in lines if isinstance(m, dict) and (m.get("session_id"), m.get("round_num")) not in existing_ids]
	        if new_lines:
	            write_oss(existing + new_lines)
	# ================= Session 恢复核心 =================
	def get_recent_messages(limit=10):
	    lines = read_oss()
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
	def init_session_on_startup():
	    if "session_id" not in st.session_state:
	        recent_msgs = get_recent_messages(limit=10)
	        if recent_msgs:
	            st.session_state.session_id = recent_msgs[-1].get("session_id", str(uuid.uuid4()))
	            st.session_state.messages = recent_msgs
	        else:
	            st.session_state.session_id = str(uuid.uuid4())
	            st.session_state.messages = []
	    return st.session_state.messages
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
	# ================= 百炼调用 =================
	def call_bailian(messages: List[Dict]) -> str:
	    if not is_model_healthy(): raise RuntimeError("服务暂时不可用")
	    dashscope.api_key = DASHSCOPE_API_KEY
	    sys_parts = [f"你是智飞投研助手。当前时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M')}"]
	    recent = st.session_state.get("messages", [])[-20:]
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
	# ================= UI =================
	st.set_page_config(page_title="智飞投研·云端", layout="centered")
	st.markdown("""<style>.stApp { background: #ffffff !important; }.stChatInputContainer { position: sticky !important; bottom: 0 !important; background: #ffffff !important; z-index: 999 !important; border-top: 1px solid #e5e7eb !important; }</style>""", unsafe_allow_html=True)
	st.title("📱 智飞投研")
	init_memory_db()
	if "messages" not in st.session_state:
	    init_session_on_startup()
	if "generating" not in st.session_state:
	    st.session_state.generating = False
	total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
	st.caption(f"{total_rounds} 轮对话")
	for m in st.session_state.messages:
	    with st.chat_message(m["role"]):
	        st.markdown(m.get("content", ""))
	user_input = st.chat_input("输入消息...")
	if user_input and not st.session_state.generating:
	    st.session_state.generating = True
	    st.session_state.messages.append({"role": "user", "content": user_input, "timestamp": datetime.now(BEIJING_TZ).isoformat()})
	    st.rerun()
	if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
	    session_id = st.session_state.session_id
	    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
	    user_msg = st.session_state.messages[-1]
	    ctx = st.session_state.messages
	    try:
	        with st.spinner("💭 思考中..."):
	            reply = call_bailian(ctx)
	        assistant_msg = {"role": "assistant", "content": reply, "timestamp": datetime.now(BEIJING_TZ).isoformat()}
	        st.session_state.messages.append(assistant_msg)
	        messages_dict = {"messages": [user_msg, assistant_msg]}
	        save_to_sqlite(session_id, round_num, messages_dict, user_msg["timestamp"])
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
	if not st.session_state.generating and st.session_state.messages:
	    col1, col2, col3 = st.columns(3)
	    with col1:
	        if st.button("➕ 新建会话", use_container_width=True):
	            st.session_state.session_id = str(uuid.uuid4())
	            st.session_state.messages = []
	            st.session_state.generating = False
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