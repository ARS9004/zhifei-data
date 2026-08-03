#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 云端纯OSS版 v10.2（2026-08-03）
- 手机端专用：彻底剔除 RDS 依赖，纯 OSS 交互
- ✅ P1 优化：引入 latest_session.json 指针文件，彻底解决 ObjectIterator 遍历性能瓶颈
- ✅ P2 修复：get_or_create_session 和手动新建会话时主动刷新 cached_cumulative，防跨 Session 记忆污染
- ✅ P0/P1/P2 历史修复继承自 v10.1
"""

import os
import re
import json
import time
import uuid
import logging
import sqlite3
from datetime import datetime, time as dt_time
from typing import List, Dict, Any, Tuple

import streamlit as st
import dashscope
import pytz
import oss2
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

# ================= OSS 配置 =================
OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_SUMMARY_FILE = "chat_summary_window.json"
# v10.2 新增：最新 Session 指针文件
OSS_LATEST_SESSION_FILE = "latest_session.json"

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

# ================= OSS 操作 =================
def get_oss_client():
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
    if not access_key_id or not access_key_secret: raise RuntimeError("⛔ 请配置 OSS 密钥")
    client = AcsClient(access_key_id, access_key_secret, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

def _ensure_appendable(bucket, remote_path: str) -> int:
    tmp_path = remote_path + ".tmp_append"
    try:
        head = bucket.head_object(remote_path)
        pos = head.content_length
        try:
            bucket.append_object(remote_path, pos, b'')
            return pos
        except oss2.exceptions.ObjectNotAppendable:
            logger.info("🔄 检测到 Normal 类型，开始流式迁移...")
            try:
                try: bucket.delete_object(tmp_path)
                except: pass
                stream = bucket.get_object(remote_path)
                curr_pos = 0
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk: break
                    bucket.append_object(tmp_path, curr_pos, chunk)
                    curr_pos += len(chunk)
                bucket.delete_object(remote_path)
                stream = bucket.get_object(tmp_path)
                curr_pos = 0
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk: break
                    bucket.append_object(remote_path, curr_pos, chunk)
                    curr_pos += len(chunk)
                bucket.delete_object(tmp_path)
                return curr_pos
            except Exception as e:
                logger.error(f"OSS 文件流式迁移失败: {e}")
                raise
    except oss2.exceptions.NoSuchKey:
        return 0

def _update_latest_session_pointer(bucket, session_id: str, ts: str):
    """v10.2 新增：更新最新 Session 指针文件"""
    try:
        remote_path = OSS_PREFIX + OSS_LATEST_SESSION_FILE
        data = {"session_id": session_id, "ts": ts}
        bucket.put_object(remote_path, json.dumps(data, ensure_ascii=False).encode('utf-8'))
    except Exception as e:
        logger.warning(f"更新最新Session指针失败: {e}")

def save_to_oss(session_id: str, round_num: int, round_messages: dict, ts: str):
    try:
        bucket = get_oss_client()
        remote_path = f"{OSS_PREFIX}{session_id}.jsonl"
        cache_key = f"oss_pos_{session_id}"
        if cache_key not in st.session_state:
            st.session_state[cache_key] = _ensure_appendable(bucket, remote_path)
        
        pos = st.session_state[cache_key]
        content = json.dumps({"session_id": session_id, "round_num": round_num, "messages": round_messages, "ts": ts}, ensure_ascii=False) + '\n'
        content_bytes = content.encode('utf-8')
        bucket.append_object(remote_path, pos, content_bytes)
        st.session_state[cache_key] += len(content_bytes)
        
        # v10.2 优化：如果是有效对话（round_num > 0），顺便更新指针文件
        if round_num > 0:
            _update_latest_session_pointer(bucket, session_id, ts)
            
        logger.info(f"✅ 写入 OSS 成功: session={session_id}, round={round_num}")
    except Exception as e:
        logger.warning(f"写入 OSS 失败: {e}")
        cache_key = f"oss_pos_{session_id}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]

def load_full_session_from_oss(session_id: str) -> List[Dict]:
    try:
        bucket = get_oss_client()
        remote_path = f"{OSS_PREFIX}{session_id}.jsonl"
        try:
            resp = bucket.get_object(remote_path)
            content = resp.read().decode('utf-8')
            msgs = []
            for line in content.strip().split('\n'):
                if not line.strip(): continue
                try:
                    item = json.loads(line)
                    if item.get("round_num", 0) == 0: continue
                    msgs_data = item.get("messages", {})
                    if isinstance(msgs_data, str): msgs_data = json.loads(msgs_data)
                    actual_msgs = []
                    if isinstance(msgs_data, dict):
                        actual_msgs = msgs_data.get("messages", [])
                    elif isinstance(msgs_data, list):
                        actual_msgs = msgs_data
                    for msg in actual_msgs:
                        msgs.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("ts"), "session_id": item.get("session_id")})
                except: continue
            logger.info(f"📊 全量恢复 Session {session_id}: 共 {len(msgs)} 条消息")
            return msgs
        except oss2.exceptions.NoSuchKey:
            return []
    except Exception as e:
        logger.warning(f"全量读取失败: {e}")
        return []

def get_latest_session_id() -> Tuple[str, int]:
    """v10.2 优化：优先读指针文件，无指针文件时回退遍历"""
    try:
        bucket = get_oss_client()
        
        # 优先读指针文件
        try:
            resp = bucket.get_object(OSS_PREFIX + OSS_LATEST_SESSION_FILE)
            data = json.loads(resp.read().decode('utf-8'))
            session_id = data.get("session_id", "")
            ts_str = data.get("ts", "")
            if session_id and ts_str:
                # 转换为 Unix 时间戳
                dt = datetime.strptime(ts_str, TS_FORMAT)
                return session_id, int(dt.timestamp())
        except oss2.exceptions.NoSuchKey:
            pass
        except Exception as e:
            logger.warning(f"读取指针文件失败，回退遍历: {e}")
            
        # 回退遍历（兼容旧数据）
        latest_file = None
        latest_time = 0
        for obj in oss2.ObjectIterator(bucket, prefix=OSS_PREFIX):
            if obj.key.endswith('.jsonl') and not obj.key.endswith('.tmp_append'):
                if obj.last_modified > latest_time:
                    latest_time = obj.last_modified
                    latest_file = obj.key
        if latest_file:
            return latest_file.split('/')[-1].replace('.jsonl', ''), latest_time
        return "", 0
    except Exception as e:
        logger.warning(f"获取 OSS 列表失败: {e}")
        return "", 0

# ================= OSS 滑动窗口与累积摘要 =================
def _read_summary_window(bucket) -> dict:
    remote_path = OSS_PREFIX + OSS_SUMMARY_FILE
    try:
        resp = bucket.get_object(remote_path)
        data = json.loads(resp.read().decode('utf-8'))
        data.setdefault("window", [])
        data.setdefault("cumulative", "")
        return data
    except oss2.exceptions.NoSuchKey:
        return {"window": [], "cumulative": ""}
    except Exception as e:
        logger.warning(f"读取摘要窗口失败: {e}")
        return {"window": [], "cumulative": ""}

def _save_summary_window(bucket, window: List[Dict], cumulative: str):
    remote_path = OSS_PREFIX + OSS_SUMMARY_FILE
    data = {"window": window, "cumulative": cumulative}
    try:
        try:
            head = bucket.head_object(remote_path)
            etag = head.etag
            bucket.put_object(remote_path, json.dumps(data, ensure_ascii=False).encode('utf-8'), headers={'If-Match': etag})
        except oss2.exceptions.NoSuchKey:
            bucket.put_object(remote_path, json.dumps(data, ensure_ascii=False).encode('utf-8'))
    except Exception as e:
        logger.warning(f"保存摘要窗口失败: {e}")

def generate_summary(messages: List[Dict]) -> str:
    if not messages: return ""
    try:
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": f"将以下对话压缩成300字摘要，突出核心主题和结论：\n{json.dumps(messages[-30:], ensure_ascii=False)[:5000]}"}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0: return resp.output.choices[0].message.content
    except: pass
    return ""

def _generate_new_cumulative(previous_cumulative: str, new_summary: str) -> str:
    if not previous_cumulative: return new_summary
    combined = f"{previous_cumulative}\n\n---\n\n{new_summary}"
    try:
        prompt = f"将以下多个对话摘要合并成一个200字以内的整体摘要：\n\n{combined}"
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": prompt}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0:
            return resp.output.choices[0].message.content
    except: pass
    return previous_cumulative

def trigger_backup_and_restore(old_session_id: str):
    logger.info(f"🔄 开始网端后加载: {old_session_id}")
    try:
        bucket = get_oss_client()
        old_messages = load_full_session_from_oss(old_session_id)
        if not old_messages: return
        
        summary = generate_summary(old_messages)
        if not summary: return
        
        data = _read_summary_window(bucket)
        window = data.get("window", [])
        cumulative = data.get("cumulative", "")
        
        new_cumulative = _generate_new_cumulative(cumulative, summary)
        
        window.append({"session_id": old_session_id, "summary": summary, "created_at": now_ts()})
        if len(window) > SESSION_WINDOW_SIZE:
            window = window[-SESSION_WINDOW_SIZE:]
            
        _save_summary_window(bucket, window, new_cumulative)
        
        recent_msgs = old_messages[-(RECOVER_ROUNDS * 2):]
        recent_rounds = []
        for i in range(0, len(recent_msgs), 2):
            pair = recent_msgs[i:i+2]
            recent_rounds.append({
                "round_messages": [{"role": m["role"], "content": m["content"]} for m in pair],
                "timestamp": pair[0].get("timestamp", "") if pair else ""
            })
        
        st.session_state._recovery_context = {
            "summary": new_cumulative,
            "recent_rounds": recent_rounds,
            "source_session": old_session_id
        }
        st.session_state.cached_cumulative = new_cumulative
        logger.info(f"✅ 后加载完成，摘要已生成并持久化")
    except Exception as e:
        logger.error(f"❌ 网端后加载失败: {e}")

# ================= 网端上下文恢复 =================
def get_or_create_session() -> str:
    if "session_id" not in st.session_state:
        latest_sid, latest_time = get_latest_session_id()
        if latest_sid:
            now = time.time()
            if (now - latest_time) < SESSION_GAP_SECONDS:
                st.session_state.session_id = latest_sid
            else:
                st.session_state.session_id = str(uuid.uuid4())
                trigger_backup_and_restore(latest_sid)
                save_to_oss(st.session_state.session_id, 0, {"type": "session_init", "messages": []}, now_ts())
        else:
            st.session_state.session_id = str(uuid.uuid4())
            # v10.2 P2 修复：OSS 无任何 Session 时，清空缓存防跨 Session 污染
            st.session_state.cached_cumulative = ""
    return st.session_state.session_id

def inject_memory(prompt: str) -> str:
    hint_parts = []
    
    if "cached_cumulative" not in st.session_state:
        try:
            bucket = get_oss_client()
            data = _read_summary_window(bucket)
            st.session_state.cached_cumulative = data.get("cumulative", "")
        except:
            st.session_state.cached_cumulative = ""
    
    if st.session_state.cached_cumulative:
        hint_parts.append(f"【全局历史摘要】{st.session_state.cached_cumulative}")
    
    recovery_ctx = st.session_state.get("_recovery_context", {})
    if recovery_ctx.get("recent_rounds"):
        recent_text = []
        for item in recovery_ctx["recent_rounds"]:
            for msg in item.get("round_messages", []):
                role = "用户" if msg.get("role") == "user" else "助手"
                recent_text.append(f"{role}: {str(msg.get('content', ''))[:200]}")
        if recent_text:
            hint_parts.append("【最近对话】\n" + "\n".join(recent_text))
        recovery_ctx["recent_rounds"] = []
        st.session_state._recovery_context = recovery_ctx
    
    if hint_parts:
        return "\n\n".join(hint_parts)
    
    msgs = st.session_state.get("messages", [])
    if msgs:
        recent = msgs[-6:]
        lines = []
        for m in recent:
            role = "用户" if m["role"] == "user" else "助手"
            lines.append(f"{role}: {str(m.get('content', ''))[:200]}")
        if lines:
            hint_parts.append("【最近对话】\n" + "\n".join(lines))
    return "\n\n".join(hint_parts)

def init_session_on_startup() -> List[Dict]:
    try:
        session_id = get_or_create_session()
        recovery = st.session_state.get("_recovery_context", {})
        if recovery.get("recent_rounds"):
            messages = []
            for item in recovery["recent_rounds"]:
                for msg in item.get("round_messages", []):
                    messages.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("timestamp"), "session_id": session_id})
            return messages
            
        return load_full_session_from_oss(session_id)
    except Exception as e:
        logger.warning(f"会话初始化失败: {e}")
        return []

# ================= SQLite 缓存兜底 =================
def init_memory_db():
    conn = None
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, round_num INTEGER NOT NULL,
                messages TEXT NOT NULL, timestamp TEXT NOT NULL)""")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_session_round ON messages(session_id, round_num)")
        conn.commit()
    except: pass
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
    except: pass
    finally:
        if conn: conn.close()

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
            time.sleep(2)
    raise RuntimeError("❌ 模型调用失败")

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
st.markdown("""<style>.stApp, section.main, .main, [data-testid="stAppViewContainer"] { background: #ffffff !important; transition: none !important; }.stChatInputContainer { position: sticky !important; bottom: 0 !important; background: #ffffff !important; padding: 12px 0 8px 0 !important; z-index: 999 !important; border-top: 1px solid #e5e7eb !important; }.stChatMessage { margin-bottom: 8px; }</style>""", unsafe_allow_html=True)

st.title("📱 智飞投研")
init_memory_db()

if "messages" not in st.session_state:
    with st.spinner("🔄 正在从云端恢复历史记忆..."):
        st.session_state.messages = init_session_on_startup()
        if not st.session_state.messages:
            st.session_state.messages = []
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
    mh = inject_memory(st.session_state.messages[-1].get("content", "")) if st.session_state.messages else ""
    
    with st.chat_message("assistant"):
        with st.spinner("💭 思考中..."):
            try:
                full_text, _ = call_bailian_once(st.session_state.messages[-30:], get_market_vars(), mh)
                st.markdown(full_text)
                st.session_state.messages.append({"role": "assistant", "content": full_text, "timestamp": now_ts()})
                
                current_round = len([m for m in st.session_state.messages if m["role"] == "user"])
                current_time = now_ts()
                round_messages = {"messages": [st.session_state.messages[-2], st.session_state.messages[-1]]}
                
                save_to_oss(session_id, current_round, round_messages, current_time)
                save_to_sqlite(session_id, current_round, round_messages, current_time)
                
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
            else:
                # v10.2 P2 修复：无旧 session 时清空缓存，防跨 Session 记忆污染
                st.session_state.cached_cumulative = ""
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