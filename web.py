	#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
智飞投研 · 网端 v9.1（2026-08-03）
🚀 极致稳定性修正版
- ✅ 三段式防崩溃迁移：增加完整性校验与启动恢复机制
- ✅ 位置缓存校准：OSS 写入失败时主动核对真实位置
- ✅ SQLite 唯一约束：补全 UNIQUE 防止冗余兜底数据
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
LATEST_SESSION_FILE = "latest_session.json"
OSS_SUMMARY_FILE = "chat_summary_window.json"
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
 
RECOVER_ROUNDS = 3
RENDER_ROUNDS = 3
MODEL_CONTEXT_ROUNDS = 5
SESSION_WINDOW_SIZE = 12
TS_FORMAT = '%Y-%m-%d %H:%M:%S'
 
def now_ts() -> str:
    return datetime.now(BEIJING_TZ).strftime(TS_FORMAT)
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# ================= 熔断机制 =================
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
 
def estimate_tokens(text: str) -> int:
    if not text: return 0
    ch = len(re.findall(r'[\u4e00-\u9fff]', text))
    return int(ch / 1.5 + (len(text) - ch) / 4)
 
# ================= OSS 纯净操作 =================
def get_oss_client():
    if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET: 
        raise RuntimeError("⛔ 请配置 OSS 密钥")
    client = AcsClient(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)
 
def _migrate_to_appendable(bucket, remote_path: str):
    """P1 修复：真正的三段式防崩溃迁移"""
    logger.warning(f"⚠️ 文件 {remote_path} 为 Normal 类型，启动安全迁移...")
    tmp_path = remote_path + ".tmp"
    
    # 1. 检查是否有历史遗留的临时文件（上次崩溃未完成）
    try:
        bucket.head_object(tmp_path)
        logger.info("ℹ️ 检测到遗留临时文件，尝试从临时文件恢复...")
        result = bucket.get_object(tmp_path)
        content = result.read()
    except oss2.exceptions.NoSuchKey:
        # 2. 没有遗留，从原文件读取
        result = bucket.get_object(remote_path)
        content = result.read()
        bucket.append_object(tmp_path, 0, content)  # 写入临时文件
        
    # 3. 验证临时文件完整性
    meta = bucket.head_object(tmp_path)
    if meta.content_length != len(content):
        raise RuntimeError("临时文件写入不完整，迁移中止")
        
    # 4. 删除原文件
    bucket.delete_object(remote_path)
    
    # 5. 从内存重建为 Appendable
    bucket.append_object(remote_path, 0, content)
    
    # 6. 清理临时文件
    bucket.delete_object(tmp_path)
    logger.info(f"✅ 迁移完成，已安全转为 Appendable")
    return len(content)
 
def _get_oss_append_pos(bucket, remote_path: str) -> int:
    try:
        meta = bucket.head_object(remote_path)
        if meta.headers.get('x-oss-object-type') == 'Appendable':
            return meta.content_length
        else:
            return _migrate_to_appendable(bucket, remote_path)
    except oss2.exceptions.NoSuchKey:
        return 0
 
def save_to_oss(session_id: str, round_num: int, round_messages: dict, ts: str):
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + f"{session_id}.jsonl"
        cache_key = f"oss_pos_{session_id}"
        if cache_key not in st.session_state:
            st.session_state[cache_key] = _get_oss_append_pos(bucket, remote_path)
        
        pos = st.session_state[cache_key]
        content = json.dumps({"session_id": session_id, "round_num": round_num, "messages": round_messages, "ts": ts}, ensure_ascii=False) + '\n'
        content_bytes = content.encode('utf-8')
        bucket.append_object(remote_path, pos, content_bytes)
        st.session_state[cache_key] += len(content_bytes)
    except Exception as e:
        logger.warning(f"OSS 追加写入失败: {e}")
        # P2 修复：失败时主动核对 OSS 真实位置进行校准
        try:
            bucket = get_oss_client()
            meta = bucket.head_object(remote_path)
            if meta.headers.get('x-oss-object-type') == 'Appendable':
                st.session_state[cache_key] = meta.content_length
                logger.info(f"ℹ️ 位置已校准为 OSS 真实长度: {meta.content_length}")
        except Exception as inner_e:
            logger.error(f"位置校准失败: {inner_e}")
            if f"oss_pos_{session_id}" in st.session_state:
                del st.session_state[f"oss_pos_{session_id}"]
        raise
 
def update_latest_session(session_id: str):
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + LATEST_SESSION_FILE
        bucket.put_object(remote_path, json.dumps({"session_id": session_id, "ts": now_ts()}).encode('utf-8'))
    except Exception as e:
        logger.warning(f"更新最新会话索引失败: {e}")
 
def get_latest_session_id_from_oss() -> str:
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + LATEST_SESSION_FILE
        result = bucket.get_object(remote_path)
        data = json.loads(result.read().decode('utf-8'))
        return data.get("session_id", "")
    except oss2.exceptions.NoSuchKey:
        return ""
    except Exception as e:
        logger.warning(f"读取最新会话失败: {e}")
        return ""
 
def load_recent_msgs_from_oss(session_id: str, num_rounds: int = 5) -> List[Dict]:
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
                content = content[content.find('\n')+1:]
                
            msgs = []
            for line in content.strip().split('\n'):
                if not line.strip(): continue
                try:
                    item = json.loads(line)
                    if item.get("round_num", 0) == 0: continue
                    msgs_data = item.get("messages", {})
                    if isinstance(msgs_data, str): msgs_data = json.loads(msgs_data)
                    actual_msgs = msgs_data.get("messages", []) if isinstance(msgs_data, dict) else []
                    for msg in actual_msgs:
                        msgs.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("ts")})
                except: continue
            
            return msgs[-(num_rounds * 2):]
        except oss2.exceptions.NoSuchKey:
            return []
    except Exception as e:
        logger.warning(f"尾部读取失败: {e}")
        return []
 
# ================= OSS 摘要窗口 =================
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
 
# ================= 后加载核心机制 =================
def trigger_backup_and_restore(old_session_id: str):
    if not old_session_id: return
    logger.info(f"🔄 点击新建，立即触发后加载，备份旧 Session: {old_session_id}")
    try:
        bucket = get_oss_client()
        old_messages = load_recent_msgs_from_oss(old_session_id, num_rounds=30)
        
        if not old_messages: 
            logger.warning("⚠️ 旧 Session 无数据，跳过摘要生成")
            return
        
        summary = generate_summary(old_messages)
        if summary: 
            data = _read_summary_window(bucket)
            window = data.get("window", [])
            cumulative = data.get("cumulative", "")
            
            existing_sids = [w.get("session_id") for w in window]
            if old_session_id not in existing_sids:
                new_cumulative = _generate_new_cumulative(cumulative, summary)
                window.append({"session_id": old_session_id, "summary": summary, "created_at": now_ts()})
                if len(window) > SESSION_WINDOW_SIZE: window = window[-SESSION_WINDOW_SIZE:]
                _save_summary_window(bucket, window, new_cumulative)
                st.session_state.cached_cumulative = new_cumulative
                logger.info(f"✅ 旧 Session 摘要已固化写入窗口")
            else:
                st.session_state.cached_cumulative = cumulative
                logger.info(f"ℹ️ 旧 Session 摘要已存在，跳过生成")
        
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
            "summary": st.session_state.cached_cumulative,
            "recent_rounds": recent_rounds,
            "source_session": old_session_id
        }
    except Exception as e:
        logger.error(f"❌ 后加载失败: {e}")
 
# ================= 网端上下文恢复 =================
def init_session_on_startup() -> List[Dict]:
    if "session_id" not in st.session_state:
        latest_sid = get_latest_session_id_from_oss()
        if latest_sid:
            st.session_state.session_id = latest_sid
            st.session_state.is_new_session = False
            logger.info(f"▶️ 接管旧 Session: {latest_sid}")
            
            try:
                bucket = get_oss_client()
                data = _read_summary_window(bucket)
                st.session_state.cached_cumulative = data.get("cumulative", "")
            except:
                st.session_state.cached_cumulative = ""
                
            return load_recent_msgs_from_oss(latest_sid, num_rounds=RENDER_ROUNDS)
        else:
            new_sid = str(uuid.uuid4())
            st.session_state.session_id = new_sid
            st.session_state.cached_cumulative = ""
            st.session_state.is_new_session = True
            save_to_oss(new_sid, 0, {"type": "session_init", "messages": []}, now_ts())
            update_latest_session(new_sid)
            logger.info(f"🆕 首次使用，已开新 Session: {new_sid}")
    return []
 
def inject_memory(prompt: str) -> str:
    hint_parts = []
    
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
        
        st.session_state._recovery_context = {}
        logger.info("🧹 _recovery_context 已注入并清空")
        return "\n\n".join(hint_parts)
    
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
 
# ================= SQLite 兜底 =================
def init_memory_db():
    conn = None
    try:
        conn = sqlite3.connect(MEMORY_DB_PATH)
        c = conn.cursor()
        # P2 修复：增加 UNIQUE 联合约束
        c.execute("""CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            session_id TEXT NOT NULL, 
            round_num INTEGER NOT NULL, 
            messages TEXT NOT NULL, 
            timestamp TEXT NOT NULL,
            UNIQUE(session_id, round_num)
        )""")
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
 
for k, v in {"generating": False, "pending_generation": False, "render_offset": 0, "is_new_session": st.session_state.get("is_new_session", False)}.items():
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
            old_session_id = st.session_state.session_id
            trigger_backup_and_restore(old_session_id)
            st.session_state._recovery_context = {} 
            
            new_sid = str(uuid.uuid4())
            st.session_state.session_id = new_sid
            st.session_state.messages = []
            st.session_state.generating = False
            st.session_state.pending_generation = False
            st.session_state.render_offset = 0
            st.session_state.is_new_session = True  
            
            save_to_oss(new_sid, 0, {"type": "session_init", "messages": []}, now_ts())
            update_latest_session(new_sid)
            st.rerun()
    with col2:
        txt = export_txt(st.session_state.messages)
        st.download_button("📤 导出TXT", txt, f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d_%H%M')}.txt", "text/plain", key="dl", use_container_width=True)