#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
智飞投研 · 全新网端 1.3（2026-08-03）
基于 Session ID 生命周期管理的终极架构
- ✅ 环境变量统一：OSS 密钥等配置全走 get_secret_or_env，适配 Streamlit Secrets，免手动填 KEY
- ✅ 偏差1彻底修复：接管旧 ID 时，若无累积摘要，临时读取最后30轮生成摘要塞入内存，绝不写回 OSS
- ✅ 偏差2保持修复：采用 OSS Range 请求仅读取文件尾部 20KB，解决全量读取性能瓶颈
- ✅ 偏差3保持修复：极简校验 Appendable 类型，非追加类型直接报错，摒弃复杂流式迁移
- ✅ 哲学坚守：接管旧 ID 只读不写；用户点“新建”才集中触发后加载写入固化
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

# ================= 环境变量与配置 =================
def get_secret_or_env(key, secrets_key=None, default=None):
    """统一读取配置，优先从 Streamlit Secrets 读取，其次本地环境变量"""
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
OSS_LATEST_SESSION_FILE = "latest_session.json"

# ================= 后加载与渲染配置 =================
SESSION_WINDOW_SIZE = 12
RECOVER_ROUNDS = 3  # 恢复最近 3 轮
RENDER_ROUNDS = 3   # UI 渲染最近 3 轮
MODEL_CONTEXT_ROUNDS = 5  # 模型上下文带最近 5 轮

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
    # 统一使用 get_secret_or_env 读取配置，兼容本地 .env 和 Streamlit Secrets
    access_key_id = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
    access_key_secret = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
    
    if not access_key_id or not access_key_secret: 
        raise RuntimeError("⛔ 请在环境变量或 Streamlit Secrets 中配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
        
    client = AcsClient(access_key_id, access_key_secret, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

def _get_oss_append_pos(bucket, remote_path: str) -> int:
    """极简校验：获取追加位置，非 Appendable 直接报错"""
    try:
        meta = bucket.head_object(remote_path)
        if meta.headers.get('x-oss-object-type') == 'Appendable':
            return meta.content_length
        else:
            raise RuntimeError(f"⛔ OSS 文件 {remote_path} 不是 Appendable 类型，无法追加写入")
    except oss2.exceptions.NoSuchKey:
        return 0

def _update_latest_session_pointer(bucket, session_id: str, ts: str):
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
            st.session_state[cache_key] = _get_oss_append_pos(bucket, remote_path)
        
        pos = st.session_state[cache_key]
        content = json.dumps({"session_id": session_id, "round_num": round_num, "messages": round_messages, "ts": ts}, ensure_ascii=False) + '\n'
        content_bytes = content.encode('utf-8')
        bucket.append_object(remote_path, pos, content_bytes)
        st.session_state[cache_key] += len(content_bytes)
        
        if round_num > 0:
            _update_latest_session_pointer(bucket, session_id, ts)
        logger.info(f"✅ 写入 OSS 成功: session={session_id}, round={round_num}")
    except Exception as e:
        logger.warning(f"写入 OSS 失败: {e}")
        cache_key = f"oss_pos_{session_id}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]
        raise

def load_recent_msgs_from_oss(session_id: str, num_rounds: int = 5) -> List[Dict]:
    """采用 OSS Range 请求仅读取文件尾部 20KB，解决全量读取性能瓶颈"""
    try:
        bucket = get_oss_client()
        remote_path = f"{OSS_PREFIX}{session_id}.jsonl"
        try:
            meta = bucket.head_object(remote_path)
            length = meta.content_length
            read_size = min(length, 20480)  # 读最后 20KB
            start = length - read_size
            
            result = bucket.get_object(remote_path, byte_range=(start, length - 1))
            content = result.read().decode('utf-8')
            
            if start > 0:
                content = content[content.find('\n')+1:]
                
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
            
            return msgs[-(num_rounds * 2):]
        except oss2.exceptions.NoSuchKey:
            return []
    except Exception as e:
        logger.warning(f"尾部读取失败: {e}")
        return []

def get_latest_session_id() -> str:
    try:
        bucket = get_oss_client()
        try:
            resp = bucket.get_object(OSS_PREFIX + OSS_LATEST_SESSION_FILE)
            data = json.loads(resp.read().decode('utf-8'))
            return data.get("session_id", "")
        except oss2.exceptions.NoSuchKey:
            pass
        except Exception as e:
            logger.warning(f"读取指针文件失败: {e}")
        return ""
    except Exception as e:
        logger.warning(f"获取 OSS 列表失败: {e}")
        return ""

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
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": f"将以下对话压缩成300字摘要，突出核心主题和结论：\n{json.dumps(messages, ensure_ascii=False)[:5000]}"}], result_format="message")
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
    """后加载核心：仅在 new_session_id 创建时触发，从 OSS 恢复上文"""
    if not old_session_id: return
    logger.info(f"🔄 触发后加载，准备恢复上文: {old_session_id}")
    try:
        bucket = get_oss_client()
        old_messages = load_recent_msgs_from_oss(old_session_id, num_rounds=30)
        if not old_messages: 
            logger.warning("⚠️ 旧Session无数据，跳过后加载")
            return
        
        summary = generate_summary(old_messages)
        if not summary: 
            logger.warning("⚠️ 摘要生成失败，跳过")
            return
        
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
            if len(pair) < 2: continue
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
        logger.info(f"✅ 后加载完成，_recovery_context 已就绪")
    except Exception as e:
        logger.error(f"❌ 网端后加载失败: {e}")

# ================= 网端上下文恢复与注入 =================
def init_session_on_startup() -> List[Dict]:
    """v1.2/v1.3 核心修复：接管旧 ID 时，若无摘要则临时生成（只读不写），严防历史记忆丢失"""
    if "session_id" not in st.session_state:
        latest_sid = get_latest_session_id()
        if latest_sid:
            st.session_state.session_id = latest_sid
            logger.info(f"▶️ 接管旧 Session: {latest_sid}")
            
            # 读取最近 3 轮用于 UI 渲染
            recent_msgs = load_recent_msgs_from_oss(latest_sid, num_rounds=RENDER_ROUNDS)
            
            # 尝试读取全局累积摘要
            try:
                bucket = get_oss_client()
                data = _read_summary_window(bucket)
                cumulative = data.get("cumulative", "")
            except:
                cumulative = ""
            
            # ✅ 核心修复：如果累积摘要为空，用旧 ID 的最后 30 轮临时生成一个摘要
            # 只塞进 _recovery_context，不写回 OSS（不违背“新建才写”的原则）
            if not cumulative:
                logger.info(f"📋 旧 Session {latest_sid} 无累积摘要，临时生成（只读不写）")
                temp_msgs = load_recent_msgs_from_oss(latest_sid, num_rounds=30)
                if temp_msgs and len(temp_msgs) >= 4:
                    temp_summary = generate_summary(temp_msgs)
                    if temp_summary:
                        cumulative = temp_summary
                        logger.info(f"✅ 临时摘要已生成（仅本次使用，不写入 OSS）")
            
            # 组装 _recovery_context
            recent_rounds = []
            for i in range(0, len(recent_msgs), 2):
                pair = recent_msgs[i:i+2]
                if len(pair) < 2: continue
                recent_rounds.append({
                    "round_messages": [{"role": m["role"], "content": m["content"]} for m in pair],
                    "timestamp": pair[0].get("timestamp", "") if pair else ""
                })
            
            st.session_state._recovery_context = {
                "summary": cumulative,
                "recent_rounds": recent_rounds,
                "source_session": latest_sid
            }
            st.session_state.cached_cumulative = cumulative
            
            return recent_msgs
        else:
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.cached_cumulative = ""
            save_to_oss(st.session_state.session_id, 0, {"type": "session_init", "messages": []}, now_ts())
            logger.info(f"🆕 首次使用，已开新 Session: {st.session_state.session_id}")
    return []

def inject_memory(prompt: str) -> str:
    hint_parts = []
    
    # 1. 优先注入 _recovery_context (首次调用或接管旧 ID 时)
    recovery_ctx = st.session_state.get("_recovery_context", {})
    if recovery_ctx.get("summary") or recovery_ctx.get("recent_rounds"):
        if recovery_ctx.get("summary"):
            hint_parts.append(f"【历史对话摘要】\n{recovery_ctx['summary']}")
        if recovery_ctx.get("recent_rounds"):
            recent_text = []
            for item in recovery_ctx["recent_rounds"]:
                for msg in item.get("round_messages", []):
                    role = "用户" if msg.get("role") == "user" else "助手"
                    recent_text.append(f"{role}: {str(msg.get('content', ''))[:200]}")
            if recent_text:
                hint_parts.append("【最近对话】\n" + "\n".join(recent_text))
        
        # 注入后立即清空，绝不重复
        st.session_state._recovery_context = {}
        logger.info("🧹 _recovery_context 已注入并清空")
        return "\n\n".join(hint_parts)
    
    # 2. 兜底：注入缓存的累积摘要 + 内存中最近5轮对话
    if "cached_cumulative" not in st.session_state:
        try:
            bucket = get_oss_client()
            data = _read_summary_window(bucket)
            st.session_state.cached_cumulative = data.get("cumulative", "")
        except:
            st.session_state.cached_cumulative = ""
    
    if st.session_state.cached_cumulative:
        hint_parts.append(f"【历史对话摘要】\n{st.session_state.cached_cumulative}")
    
    msgs = st.session_state.get("messages", [])
    if len(msgs) > 2:
        recent = msgs[-(MODEL_CONTEXT_ROUNDS * 2):]
        lines = []
        for m in recent:
            role = "用户" if m["role"] == "user" else "助手"
            lines.append(f"{role}: {str(m.get('content', ''))[:200]}")
        if lines:
            hint_parts.append("【最近对话】\n" + "\n".join(lines))
        
    return "\n\n".join(hint_parts)

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
    
    context_msgs = messages[-(MODEL_CONTEXT_ROUNDS * 2):] if len(messages) > MODEL_CONTEXT_ROUNDS * 2 else messages
    for m in context_msgs:
        content = str(m.get("content", ""))
        if m["role"] == "user": content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
        full_msgs.append({"role": m["role"], "content": content})
    base_tok = estimate_tokens(sys_p) + sum(estimate_tokens(str(m.get("content", ""))) for m in context_msgs)
    
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
    with st.spinner("🔄 正在恢复上文..."):
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
                full_text, _ = call_bailian_once(st.session_state.messages, get_market_vars(), mh)
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

if st.session_state.messages or st.session_state.get("_recovery_context"):
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("➕ 新建会话", use_container_width=True):
            old_session_id = st.session_state.get("session_id")
            if old_session_id and st.session_state.messages:
                with st.spinner("🔄 正在备份并恢复上文..."):
                    trigger_backup_and_restore(old_session_id)
            else:
                st.session_state.cached_cumulative = ""
            
            st.session_state.messages = []
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.generating = False
            st.session_state.pending_generation = False
            st.session_state.render_offset = 0
            save_to_oss(st.session_state.session_id, 0, {"type": "session_init", "messages": []}, now_ts())
            st.rerun()
    with col2:
        txt = export_txt(st.session_state.messages)
        st.download_button("📤 导出TXT", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.txt", "text/plain", key="dl", use_container_width=True)