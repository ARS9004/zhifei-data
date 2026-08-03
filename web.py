#!/usr/bin/env python3
# -*- coding: utf-8 -*-
 
"""
智飞投研 · 云端纯OSS版 v9.12（2026-08-03）
- 手机端专用：彻底剔除 RDS 依赖，纯 OSS 交互
- ✅ P0 修复：inject_memory 不再清空整个 _recovery_context，只清 recent_rounds 保留 summary，解决跨 session 失忆
- ✅ P1 修复：trigger_backup_and_restore 加全链路日志，定位 OSS 读取/摘要生成失败点
- ✅ P1 修复：_ensure_appendable 改为流式分块迁移，防大文件 OOM
- ✅ P2 修复：摘要窗口写入采用 If-Match 乐观锁，防多端并发覆盖
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
OSS_FILENAME = get_secret_or_env("OSS_FILENAME", "oss.filename", "chat_history.jsonl")
OSS_SUMMARY_FILE = "chat_summary_window.json"
 
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
            logger.info("🔄 检测到 Normal 类型 OSS 文件，开始流式安全迁移...")
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
        try:
            tmp_head = bucket.head_object(tmp_path)
            if tmp_head.content_length > 0:
                logger.warning("⚠️ 检测到残留临时文件，正在流式恢复...")
                stream = bucket.get_object(tmp_path)
                curr_pos = 0
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk: break
                    bucket.append_object(remote_path, curr_pos, chunk)
                    curr_pos += len(chunk)
                bucket.delete_object(tmp_path)
                return curr_pos
        except oss2.exceptions.NoSuchKey:
            pass
        return 0
    except Exception as e:
        logger.error(f"检查 Appendable 失败: {e}")
        raise
 
def save_to_oss_directly(session_id: str, round_num: int, round_messages: dict, ts: str):
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME
        if "oss_append_pos" not in st.session_state:
            st.session_state.oss_append_pos = _ensure_appendable(bucket, remote_path)
        pos = st.session_state.oss_append_pos
        content = json.dumps({"session_id": session_id, "round_num": round_num, "messages": round_messages, "ts": ts}, ensure_ascii=False) + '\n'
        content_bytes = content.encode('utf-8')
        bucket.append_object(remote_path, pos, content_bytes)
        st.session_state.oss_append_pos += len(content_bytes)
        logger.info(f"✅ 网端直接写入 OSS 成功: session={session_id}, round={round_num}")
    except Exception as e:
        logger.warning(f"网端直接写入 OSS 失败: {e}")
        if "oss_append_pos" in st.session_state:
            del st.session_state.oss_append_pos
 
def load_recent_from_oss(limit: int = 3) -> List[Dict]:
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME
        try:
            head = bucket.head_object(remote_path)
            file_size = head.content_length
            if file_size == 0: return []
            tail_size = min(100 * 1024, file_size)
            resp = bucket.get_object(remote_path, byte_range=(file_size - tail_size, file_size - 1))
            content = resp.read().decode('utf-8')
            first_newline = content.find('\n')
            if first_newline >= 0:
                first_line = content[:first_newline]
                try:
                    json.loads(first_line)
                except:
                    content = content[first_newline + 1:]
            lines = []
            for line in content.strip().split('\n'):
                if not line.strip(): continue
                try:
                    lines.append(json.loads(line))
                except:
                    continue
            valid_lines = []
            for item in lines:
                if not isinstance(item, dict): continue
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try: msgs_data = json.loads(msgs_data)
                    except: msgs_data = {}
                actual_msgs = []
                if isinstance(msgs_data, dict):
                    actual_msgs = msgs_data.get("messages", [])
                elif isinstance(msgs_data, list):
                    actual_msgs = msgs_data
                if actual_msgs:
                    valid_lines.append(item)
            sorted_lines = sorted(valid_lines, key=lambda x: x.get("ts", ""), reverse=True)
            recent = sorted_lines[:limit]
            msgs = []
            for item in reversed(recent):
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try: msgs_data = json.loads(msgs_data)
                    except: msgs_data = {}
                actual_msgs = []
                if isinstance(msgs_data, dict):
                    actual_msgs = msgs_data.get("messages", [])
                elif isinstance(msgs_data, list):
                    actual_msgs = msgs_data
                for msg in actual_msgs:
                    msgs.append({"role": msg.get("role"), "content": msg.get("content"), "timestamp": item.get("ts"), "session_id": item.get("session_id")})
            logger.info(f"📊 load_recent_from_oss: 读取到 {len(msgs)} 条消息")
            return msgs
        except oss2.exceptions.NoSuchKey:
            return []
    except:
        return []
 
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
        logger.warning(f"保存摘要窗口失败(可能并发冲突): {e}")
 
def generate_summary(messages: List[Dict]) -> str:
    if not messages: return ""
    try:
        dashscope.api_key = DASHSCOPE_API_KEY
        resp = dashscope.Generation.call(model=MODEL_NAME, messages=[{"role": "user", "content": f"将以下对话压缩成300字摘要，突出核心主题和结论：\n{json.dumps(messages[-30:], ensure_ascii=False)[:5000]}"}], result_format="message")
        if resp.status_code == HTTPStatus.OK and resp.output.choices and len(resp.output.choices) > 0: return resp.output.choices[0].message.content
    except Exception as e:
        logger.warning(f"摘要生成失败: {e}")
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
    except Exception as e:
        logger.warning(f"累积摘要生成失败: {e}")
    return previous_cumulative
 
def trigger_backup_and_restore(old_session_id: str):
    logger.info(f"🔄 开始网端后加载: {old_session_id}")
    try:
        bucket = get_oss_client()
        remote_path = OSS_PREFIX + OSS_FILENAME
        
        try:
            head = bucket.head_object(remote_path)
            file_size = head.content_length
            logger.info(f"📊 OSS文件大小: {file_size} bytes")
            if file_size == 0:
                logger.warning("⚠️ OSS文件为空，跳过后加载")
                return
            
            tail_size = min(200 * 1024, file_size)
            content = ""
            while True:
                resp = bucket.get_object(remote_path, byte_range=(file_size - tail_size, file_size - 1))
                content = resp.read().decode('utf-8')
                valid_count = 0
                for line in content.strip().split('\n'):
                    if not line.strip(): continue
                    try:
                        row = json.loads(line)
                        if row.get("session_id") == old_session_id and row.get("round_num", 0) > 0:
                            valid_count += 1
                    except: continue
                if valid_count >= RECOVER_ROUNDS or tail_size >= file_size:
                    break
                tail_size = min(tail_size * 2, file_size)
            logger.info(f"📊 读取到 {len(content)} 字符的OSS数据，匹配到 {valid_count} 轮旧对话")
        except oss2.exceptions.NoSuchKey:
            logger.warning("⚠️ OSS文件不存在，跳过后加载")
            return
            
        old_messages = []
        old_rows = []
        for line in content.strip().split('\n'):
            if not line.strip(): continue
            try:
                row = json.loads(line)
                if row.get("session_id") == old_session_id and row.get("round_num", 0) > 0:
                    old_rows.append(row)
                    msgs_data = row.get("messages", {})
                    if isinstance(msgs_data, str): msgs_data = json.loads(msgs_data)
                    actual_msgs = []
                    if isinstance(msgs_data, dict):
                        actual_msgs = msgs_data.get("messages", [])
                    elif isinstance(msgs_data, list):
                        actual_msgs = msgs_data
                    for msg in actual_msgs:
                        old_messages.append(msg)
            except: continue
            
        logger.info(f"📊 匹配到旧Session {old_session_id}: {len(old_rows)}轮, {len(old_messages)}条消息")
        
        if not old_messages:
            logger.warning(f"⚠️ 旧Session {old_session_id} 无消息，跳过后加载")
            return
        
        summary = generate_summary(old_messages)
        if not summary:
            logger.warning("⚠️ 摘要生成失败，跳过后加载")
            return
        logger.info(f"✅ 摘要生成成功，长度: {len(summary)}")
        
        data = _read_summary_window(bucket)
        window = data.get("window", [])
        cumulative = data.get("cumulative", "")
        
        new_cumulative = _generate_new_cumulative(cumulative, summary)
        
        window.append({
            "session_id": old_session_id,
            "summary": summary,
            "round_count": len(old_rows),
            "created_at": now_ts()
        })
        if len(window) > SESSION_WINDOW_SIZE:
            window = window[-SESSION_WINDOW_SIZE:]
            
        _save_summary_window(bucket, window, new_cumulative)
        
        recent_rounds = []
        for row in old_rows[-RECOVER_ROUNDS:]:
            msgs_data = row.get("messages", {})
            if isinstance(msgs_data, str): msgs_data = json.loads(msgs_data)
            actual_msgs = []
            if isinstance(msgs_data, dict):
                actual_msgs = msgs_data.get("messages", [])
            elif isinstance(msgs_data, list):
                actual_msgs = msgs_data
            recent_rounds.append({"round_messages": actual_msgs, "timestamp": row.get("ts", "")})
            
        st.session_state._recovery_context = {
            "summary": new_cumulative,
            "recent_rounds": recent_rounds,
            "source_session": old_session_id
        }
        logger.info(f"✅ _recovery_context 已设置: summary长度={len(new_cumulative)}, recent_rounds数量={len(recent_rounds)}")
    except Exception as e:
        logger.error(f"❌ 网端后加载失败: {e}")
 
# ================= 网端上下文恢复 =================
def get_or_create_session() -> str:
    if "session_id" not in st.session_state:
        oss_msgs = load_recent_from_oss(limit=1)
        if oss_msgs and oss_msgs[-1].get("session_id"):
            last_session_id = oss_msgs[-1]["session_id"]
            last_ts = oss_msgs[-1].get("timestamp", "")
            try:
                last_dt = datetime.strptime(last_ts, TS_FORMAT)
                last_dt = BEIJING_TZ.localize(last_dt) if last_dt.tzinfo is None else last_dt
                now = datetime.now(BEIJING_TZ)
                if (now - last_dt).total_seconds() >= SESSION_GAP_SECONDS:
                    st.session_state.session_id = str(uuid.uuid4())
                    trigger_backup_and_restore(last_session_id)
                    save_to_oss_directly(st.session_state.session_id, 0, {"messages": []}, now_ts())
                else:
                    st.session_state.session_id = last_session_id
            except:
                st.session_state.session_id = str(uuid.uuid4())
        else:
            st.session_state.session_id = str(uuid.uuid4())
    return st.session_state.session_id
 
def inject_memory(prompt: str, force_latest: bool = False) -> str:
    hint_parts = []
    recovery_ctx = st.session_state.get("_recovery_context", {})
    logger.info(f"🔍 inject_memory: _recovery_context 存在? {bool(recovery_ctx)}, summary长度={len(recovery_ctx.get('summary', ''))}, recent_rounds数量={len(recovery_ctx.get('recent_rounds', []))}")
    
    # v9.12 核心 P0 修复：摘要每轮都注入，不清空
    if recovery_ctx.get("summary"):
        hint_parts.append(f"【历史对话摘要】{recovery_ctx['summary']}")
    
    # 最近对话：首次注入后清空（因为后续轮次已经在 messages 里了）
    if recovery_ctx.get("recent_rounds"):
        recent_text = []
        for item in recovery_ctx["recent_rounds"]:
            for msg in item.get("round_messages", []):
                role = "用户" if msg.get("role") == "user" else "助手"
                recent_text.append(f"{role}: {str(msg.get('content', ''))[:200]}")
        if recent_text:
            hint_parts.append("【最近对话】\n" + "\n".join(recent_text))
        # 只清空 recent_rounds，保留 summary
        recovery_ctx["recent_rounds"] = []
        st.session_state._recovery_context = recovery_ctx
    
    if hint_parts:
        logger.info(f"✅ inject_memory 返回 hint 长度: {len('\n\n'.join(hint_parts))}")
        return "\n\n".join(hint_parts)
    
    # 兜底：从内存取最近3轮
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
            logger.info(f"📊 init_session_on_startup: 从 _recovery_context 恢复了 {len(messages)} 条消息")
            return messages
            
        oss_msgs = load_recent_from_oss(limit=RECOVER_ROUNDS)
        if oss_msgs:
            logger.info(f"📊 init_session_on_startup: 从 OSS 恢复了 {len(oss_msgs)} 条消息")
            return oss_msgs
        
        logger.warning("⚠️ init_session_on_startup: 未恢复到任何消息")
        return []
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
    except Exception as e:
        logger.error(f"SQLite初始化失败: {e}")
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
    except Exception as e:
        logger.warning(f"SQLite写入失败: {e}")
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
    logger.info(f"📤 call_bailian_once: 发送 {len(full_msgs)} 条消息，hint长度={len(hint)}")
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
            logger.warning(f"第{attempt+1}次调用失败，2秒后重试: {e}")
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
    logger.info(f"🔍 即将调用模型，hint长度={len(mh)}")
    
    with st.chat_message("assistant"):
        with st.spinner("💭 思考中..."):
            try:
                full_txt, _ = call_bailian_once(st.session_state.messages[-30:], get_market_vars(), mh)
                st.markdown(full_txt)
                st.session_state.messages.append({"role": "assistant", "content": full_txt, "timestamp": now_ts()})
                
                current_round = len([m for m in st.session_state.messages if m["role"] == "user"])
                current_time = now_ts()
                round_messages = {"messages": [st.session_state.messages[-2], st.session_state.messages[-1]]}
                
                save_to_oss_directly(session_id, current_round, round_messages, current_time)
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