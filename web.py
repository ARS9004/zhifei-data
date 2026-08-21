	#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智飞投研 · 本地完整版 v2.1 (2026-08-21)
- [v2.1] 整合侧边栏业务功能：分析方案、快捷工具、Token监控
- [v2.1] 增加侧边栏快捷指令的快捷输入响应
- [v2.0] 分离 display_messages(渲染) 和 messages(模型上下文)
- [v2.0] 聊天框历史对话永久保留，新建会话不清空
- [v2.0] 新会话只发恢复指令给模型，不携带历史消息
"""
 
import os
import re
import json
import time
import uuid
import logging
import io
from datetime import datetime
from typing import List, Dict
 
import streamlit as st
import dashscope
from dashscope import Application
import oss2
import pytz
from http import HTTPStatus
from dotenv import load_dotenv
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
from docx import Document
from docx.shared import Pt
from docx.oxml.ns import qn
 
import core_engine
 
load_dotenv()
 
_CHINESE_CHAR_RE = re.compile(r'[\u4e00-\u9fff]')
 
def get_secret_or_env(key, secrets_key=None, default=None):
    if secrets_key:
        parts = secrets_key.split('.')
        try:
            value = st.secrets
            for p in parts:
                value = value[p]
            if value:
                return value
        except Exception:
            pass
    return os.getenv(key, default)
 
DASHSCOPE_API_KEY = get_secret_or_env("DASHSCOPE_API_KEY", "dashscope.api_key")
if not DASHSCOPE_API_KEY:
    raise RuntimeError("请配置 DASHSCOPE_API_KEY")
 
MODEL_NAME = get_secret_or_env("MODEL_NAME", "model.name", "qwen-plus")
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
 
OSS_BUCKET = get_secret_or_env("OSS_BUCKET", "oss.bucket", "zfai-date-oss")
OSS_REGION = get_secret_or_env("OSS_REGION", "oss.region", "cn-beijing")
OSS_PREFIX = get_secret_or_env("OSS_PREFIX", "oss.prefix", "chat_history/")
OSS_FILENAME = "chat_memory.jsonl"
 
OSS_ACCESS_KEY_ID = get_secret_or_env("OSS_ACCESS_KEY_ID", "oss.access_key_id")
OSS_ACCESS_KEY_SECRET = get_secret_or_env("OSS_ACCESS_KEY_SECRET", "oss.access_key_secret")
if not OSS_ACCESS_KEY_ID or not OSS_ACCESS_KEY_SECRET:
    raise RuntimeError("请配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# ================= 辅助函数 =================
def now_ts_display():
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
 
def is_one_click_scheme(scheme_name):
    # 简易判断逻辑，可根据实际业务扩充
    return scheme_name in ["A盘前快速分析", "B产业链扫描", "每日简报", "投研周报"]
 
def get_scheme_prompt(scheme_name):
    # 占位实现，返回对应的 Prompt 模板
    return f"请执行【{scheme_name}】分析方案"
 
# ================= 前端熔断机制 =================
_FAIL_LOCK = __import__('threading').Lock()
_FAIL_COUNTER = {"network": 0, "api": 0, "model": 0}
_MODEL_HEALTHY = True
_MAX_CONSECUTIVE_FAILURES = {"network": 5, "api": 3, "model": 3}
 
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
            logger.warning("模型服务熔断：连续 %d 次模型调用失败", _FAIL_COUNTER["model"])
 
def is_model_healthy() -> bool:
    with _FAIL_LOCK:
        return _MODEL_HEALTHY
 
def _classify_error(e: Exception) -> str:
    err_str = str(e).lower()
    if not err_str:
        return "network"
    if any(kw in err_str for kw in ["timeout", "connection", "network", "resolve", "refused"]):
        return "network"
    if any(kw in err_str for kw in ["rate", "quota", "throttle", "limit", "429"]):
        return "api"
    return "model"
 
def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ch = len(_CHINESE_CHAR_RE.findall(text))
    return int(ch * 1.0 + (len(text) - ch) / 4)
 
# ================= OSS 客户端 (前端使用 STS 临时凭证) =================
def get_oss_client():
    client = AcsClient(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_REGION)
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(3600)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)
 
def oss_get_with_retry(bucket, remote_path, max_retry=3, delay=1, **kwargs):
    for attempt in range(max_retry):
        try:
            return bucket.get_object(remote_path, **kwargs)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS 读取重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))
 
def oss_head_with_retry(bucket, remote_path, max_retry=3, delay=1):
    for attempt in range(max_retry):
        try:
            return bucket.head_object(remote_path)
        except oss2.exceptions.NoSuchKey:
            raise
        except Exception as e:
            if attempt == max_retry - 1:
                raise
            logger.warning(f"OSS head 重试 {attempt+1}/{max_retry}: {e}")
            time.sleep(delay * (attempt + 1))
 
def read_oss_full():
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        result = oss_get_with_retry(bucket, remote, max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"read_oss_full 失败: {e}")
        return []
 
def read_oss_tail(size=40960):
    try:
        bucket = get_oss_client()
        remote = OSS_PREFIX + OSS_FILENAME
        meta = oss_head_with_retry(bucket, remote, max_retry=2)
        length = meta.content_length
        read_size = min(length, size)
        start = length - read_size
        result = oss_get_with_retry(bucket, remote, byte_range=(start, length - 1), max_retry=2)
        if result is None:
            return []
        content = result.read().decode('utf-8')
        if start > 0:
            first_nl = content.find('\n')
            if first_nl >= 0:
                content = content[first_nl + 1:]
        last_nl = content.rfind('\n')
        if last_nl >= 0:
            content = content[:last_nl + 1]
        lines = []
        for line in content.strip().split('\n'):
            if not line.strip():
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return lines
    except oss2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.debug(f"read_oss_tail 失败: {e}")
        return []
 
def get_history_sessions():
    try:
        lines = read_oss_full()
        if not lines:
            lines = read_oss_tail()
        if not lines:
            return []
        sessions = {}
        for item in lines:
            if not isinstance(item, dict):
                continue
            sid = item.get("session_id", "")
            if sid not in sessions:
                ts = item.get("ts", "")
                msgs_data = item.get("messages", {})
                if isinstance(msgs_data, str):
                    try:
                        msgs_data = json.loads(msgs_data)
                    except Exception:
                        msgs_data = {}
                title = ""
                if isinstance(msgs_data, dict) and "messages" in msgs_data:
                    for m in msgs_data["messages"]:
                        if m.get("role") == "user":
                            title = str(m.get("content", ""))[:30]
                            break
                sessions[sid] = {"session_id": sid, "title": title, "ts": ts, "rounds": 0}
            sessions[sid]["rounds"] += 1
        return sorted(sessions.values(), key=lambda x: x.get("ts", ""), reverse=True)
    except Exception:
        return []
 
# ================= 百炼调用 =================
def _clean_for_api(raw_msgs: List[Dict]) -> List[Dict]:
    valid = []
    for m in raw_msgs:
        role = m.get("role", "")
        content = m.get("content")
        if role not in ("user", "assistant"):
            continue
        if content is None or str(content).strip() == "":
            continue
        valid.append({"role": role, "content": str(content).strip()})
    if not valid:
        return []
    while valid and valid[0]["role"] != "user":
        valid.pop(0)
    cleaned = [valid[0]] if valid else []
    for m in valid[1:]:
        if m["role"] != cleaned[-1]["role"]:
            cleaned.append(m)
    while cleaned and cleaned[-1]["role"] != "user":
        cleaned.pop()
    return cleaned
 
def _extract_text_from_response(resp) -> str:
    output = resp.output
    text = getattr(output, 'text', None)
    if text and isinstance(text, str) and text.strip():
        return text
    choices = getattr(output, 'choices', None)
    if choices and isinstance(choices, list) and len(choices) > 0:
        msg = getattr(choices[0], 'message', None)
        if msg:
            content = getattr(msg, 'content', None)
            if content and isinstance(content, str) and content.strip():
                return content
        assistant = getattr(choices[0], 'assistant', None)
        if assistant:
            if isinstance(assistant, dict):
                content = assistant.get('content') or assistant.get('text')
                if content and isinstance(content, str) and content.strip():
                    return content
            elif isinstance(assistant, str) and assistant.strip():
                return assistant
    if isinstance(output, str) and output.strip():
        return output
    if isinstance(output, dict):
        text = output.get('text')
        if not (text and isinstance(text, str) and text.strip()):
            text = output.get('content')
        if text and isinstance(text, str) and text.strip():
            return text
    assistant = getattr(output, 'assistant', None)
    if assistant:
        if isinstance(assistant, dict):
            text = assistant.get('content') or assistant.get('text')
            if text and isinstance(text, str) and text.strip():
                return text
        elif isinstance(assistant, str) and assistant.strip():
            return assistant
    return ""
 
def call_bailian(messages: List[Dict]) -> str:
    if not is_model_healthy():
        raise RuntimeError("服务暂时不可用")
    dashscope.api_key = DASHSCOPE_API_KEY
    cleaned = _clean_for_api(messages)
    if not cleaned:
        raise RuntimeError("上下文中没有有效的用户消息")
    full_msgs = cleaned
    BAILIAN_APP_ID = "45db2f797bfd49229f757b04ed13ac92"
    retries, delay = 3, 2
    for attempt in range(retries):
        try:
            resp = Application.call(
                app_id=BAILIAN_APP_ID,
                messages=full_msgs,
                stream=False
            )
            if resp.status_code == HTTPStatus.OK:
                full_text = _extract_text_from_response(resp)
                if not full_text or not full_text.strip():
                    raise RuntimeError("模型返回内容为空")
                reset_health_status()
                return full_text
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
 
def call_bailian_with_token_check(messages: List[Dict]) -> str:
    MAX_INPUT_TOKENS = 16000
    SYSTEM_PROMPT_RESERVE = 2500
    total_tokens = sum(estimate_tokens(str(m.get("content", ""))) for m in messages)
    if total_tokens > MAX_INPUT_TOKENS - SYSTEM_PROMPT_RESERVE:
        logger.warning(f"输入超长 ({total_tokens} tokens)，自动截断最近消息")
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
        logger.info(f"截断后 {len(messages)} 条消息，~{running_tokens} tokens")
    return call_bailian(messages)
 
# ================= 工具 =================
def export_docx(messages):
    doc = Document()
    style = doc.styles['Normal']
    style.font.name = '仿宋'
    style.element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')
    title = doc.add_heading('', level=1)
    title_run = title.add_run("智飞投研对话记录")
    title_run.font.name = '仿宋'
    title_run._element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')
    title_run.font.size = Pt(16)
    title_run.font.bold = True
    for m in messages:
        role = "用户" if m.get("role") == "user" else "助手"
        content = m.get("content") or ""
        ts = m.get("timestamp", "")[:16]
        prefix = f"[{ts}] " if ts else ""
        para = doc.add_paragraph()
        run = para.add_run(f"{prefix}{role}：{content}")
        run.font.name = '仿宋'
        run._element.rPr.rFonts.set(qn('w:eastAsia'), '仿宋')
        run.font.size = Pt(14)
    buffer = io.BytesIO()
    doc.save(buffer)
    buffer.seek(0)
    return buffer
 
# ================= 启动初始化 =================
def init_session_on_startup():
    """启动初始化：分离 display_messages(渲染) 和 messages(模型上下文)"""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "display_messages" not in st.session_state:
        st.session_state.display_messages = []
    if "history_loaded" not in st.session_state:
        st.session_state.history_loaded = False
    if "session_id" not in st.session_state:
        st.session_state.session_id = ""
    if "render_offset" not in st.session_state:
        st.session_state.render_offset = 0
    if "model_name" not in st.session_state:
        st.session_state.model_name = MODEL_NAME
    if "scheme" not in st.session_state:
        st.session_state.scheme = "日常对话"

    if not st.session_state.session_id:
        st.session_state.session_id = str(uuid.uuid4())

    if not st.session_state.history_loaded:
        # 模型上下文：只放恢复指令，不加载历史消息
        if not st.session_state.messages:
            st.session_state.messages.append({
                "role": "user",
                "content": (
                    "[系统] 请恢复上文：\n"
                    "1. 调用 searchOSSFileByFileName 搜索 chat_memory/chat_summary/history_summary（不传 mediaType）\n"
                    "2. 将三个 URL 写入 /tmp/oss_urls.json\n"
                    "3. 执行 python3 /root/workspace/context_manager.py restore\n"
                    "4. 根据恢复结果，用以下格式回复：\n"
                    "   - 恢复成功：✅ 上文恢复成功 | 摘要：xxx | 最近N轮对话 | 历史摘要N条\n"
                    "   - OSS无数据：⚠️ 上文恢复失败：OSS暂无对话数据，本次为全新会话\n"
                    "   - 脚本不存在：⚠️ 上文恢复失败：沙箱工具未部署\n"
                    "   - 执行报错：⚠️ 上文恢复失败：具体错误信息"
                ),
                "timestamp": now_ts_display()
            })
            st.session_state.generating = True

        st.session_state.history_loaded = True

    return {"success": True}

 
# ================= UI =================
st.set_page_config(page_title="智飞投研·云端", layout="centered")
st.title("智飞投研")
 
if "generating" not in st.session_state:
    st.session_state.generating = False
if "stop" not in st.session_state:
    st.session_state.stop = False
if "messages" not in st.session_state:
    st.session_state.messages = []
if "display_messages" not in st.session_state:
    st.session_state.display_messages = []
 
restore_result = init_session_on_startup()
 
total_rounds = len([m for m in st.session_state.messages if m["role"] == "user"])
st.caption(f"{total_rounds} 轮对话")
 
chat_container = st.container()
with chat_container:
    MAX_RENDER_MSGS = 60
    total_msgs = len(st.session_state.display_messages)
    render_count = (st.session_state.get("render_offset", 0) + 1) * MAX_RENDER_MSGS
    render_start = max(0, total_msgs - render_count)
    render_msgs = st.session_state.display_messages[render_start:] if st.session_state.display_messages else []
 
    if render_start > 0:
        if st.button("加载更早的对话", use_container_width=True):
            st.session_state.render_offset = st.session_state.get("render_offset", 0) + 1
            st.rerun()
 
    for m in render_msgs:
        with st.chat_message(m.get("role", "user")):
            content = str(m.get("content") or "")
            if content.startswith("❌"):
                st.error(content)
            else:
                st.markdown(content)
 
# 捕获侧边栏触发的快捷输入
_pending_prompt = st.session_state.pop("_pending_prompt", None)
user_input = st.chat_input("输入消息...", disabled=st.session_state.generating)
if _pending_prompt and not st.session_state.generating:
    user_input = _pending_prompt
 
if user_input and not st.session_state.generating:
    st.session_state.stop = False
    st.session_state.generating = True
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"]) + 1
    user_msg = {
        "role": "user",
        "content": user_input,
        "timestamp": now_ts_display()
    }
    st.session_state.messages.append(user_msg)
    st.session_state.display_messages.append(user_msg)
    st.rerun()
 
if st.session_state.generating and st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    round_num = len([m for m in st.session_state.messages if m["role"] == "user"])
    user_msg = st.session_state.messages[-1]
    ctx = [{"role": m["role"], "content": str(m.get("content") or "")} for m in st.session_state.messages]
    try:
        with st.spinner("思考中..."):
            reply = call_bailian_with_token_check(ctx)
        assistant_msg = {
            "role": "assistant",
            "content": reply,
            "timestamp": now_ts_display()
        }
        st.session_state.messages.append(assistant_msg)
        st.session_state.display_messages.append(assistant_msg)
        messages_list = [user_msg, assistant_msg]
        messages_dict = {"messages": messages_list}
        ts_str = now_ts_display()
        core_engine.save_round(
            session_id=st.session_state.session_id,
            round_num=round_num,
            messages=messages_dict,
            ts=ts_str
        )
    except Exception as e:
        st.error(f"错误: {e}")
        err_msg = {
            "role": "assistant",
            "content": f"调用失败: {e}",
            "timestamp": now_ts_display()
        }
        st.session_state.messages.append(err_msg)
        st.session_state.display_messages.append(err_msg)
    st.session_state.generating = False
    st.rerun()
 
st.divider()
 
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("新建会话", use_container_width=True, disabled=st.session_state.generating):
        old_sid = st.session_state.session_id
        if old_sid and st.session_state.messages:
            try:
                core_engine.archive_session(old_sid)
            except Exception as e:
                logger.warning(f"归档旧会话失败: {e}")
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.history_loaded = False
        st.session_state.generating = False
        st.session_state.stop = False
        st.session_state.render_offset = 0
        st.session_state.messages = []
        # display_messages 不清空，历史对话保留在聊天框
        st.rerun()
with col2:
    if st.button("重新生成", use_container_width=True, disabled=st.session_state.generating):
        if st.session_state.messages and st.session_state.messages[-1]["role"] == "assistant":
            st.session_state.messages.pop()
            st.session_state.display_messages.pop()
            st.session_state.generating = True
            st.session_state.stop = False
            st.rerun()
with col3:
    st.download_button(
        label="导出DOCX",
        data=export_docx(st.session_state.display_messages),
        file_name=f"对话_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}.docx",
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        use_container_width=True,
        key="export_docx_btn"
    )
 
# ================= 侧边栏 =================
with st.sidebar:
    st.title("🚀 智飞投研")
    st.caption(f"当前时间: {now_ts_display()}")
    st.info(f"当前模型: **{st.session_state.model_name}**")
    st.divider()
 
    # === 分析方案 ===
    st.subheader("📊 分析方案")
    scheme_cols_1 = st.columns(3)
    scheme_cols_2 = st.columns(3)
    schemes = ["A盘前快速分析", "B产业链扫描", "C卡脖子扫描", "D市场行情判断", "E资金全景动态", "F个股深度分析"]
    for i, scheme_name in enumerate(schemes):
        col = scheme_cols_1[i] if i < 3 else scheme_cols_2[i - 3]
        with col:
            if st.button(scheme_name, key=f"scheme_{i}", use_container_width=True):
                st.session_state.scheme = scheme_name
                if is_one_click_scheme(scheme_name):
                    st.session_state._pending_prompt = get_scheme_prompt(scheme_name)
                    st.rerun()
                else:
                    st.toast(f"已切换至: {scheme_name}，请输入标的/事件", icon="ℹ️")
    st.divider()
 
    # === 快捷工具 ===
    st.subheader("⚡ 快捷工具")
    tool_cols = st.columns(3)
    with tool_cols[0]:
        if st.button("📋 每日简报", use_container_width=True, key="daily_brief"):
            st.session_state.scheme = "每日简报"
            st.session_state._pending_prompt = "今日简报"
            st.rerun()
    with tool_cols[1]:
        if st.button("📊 投研周报", use_container_width=True, key="weekly_report"):
            st.session_state.scheme = "投研周报"
            st.session_state._pending_prompt = "本周周报"
            st.rerun()
    with tool_cols[2]:
        if st.button("🔍 个股速看", use_container_width=True, key="quick_stock"):
            st.session_state.scheme = "个股速看"
            st.toast("请输入股票代码或名称，如：300308", icon="ℹ️")
    st.success(f"当前方案: **{st.session_state.scheme}**")
    st.divider()
 
    # === Token 监测 ===
    st.subheader("📊 Token 容量监控")
    total_used = st.session_state.get("total_tokens_used", 0)
    BUDGET = 1000000
    usage_ratio = min(total_used / BUDGET, 1.0)
    if usage_ratio >= 0.8:
        st.warning(f"⚠️ Token使用量已达 {usage_ratio*100:.1f}%")
    elif usage_ratio >= 0.6:
        st.info(f"ℹ️ Token使用量: {usage_ratio*100:.1f}%")
    st.progress(usage_ratio, text=f"{total_used:,} / {BUDGET:,} ({usage_ratio*100:.1f}%)")
    st.caption(f"💰 本会话累计消耗: `{total_used:,}` Tokens")
    st.divider()
 
    # === 系统状态 ===
    st.subheader("系统状态")
    st.caption("系统就绪")
 
    st.divider()
 
    if st.button("OSS同步到本地SQLite", use_container_width=True):
        try:
            core_engine.startup_align()
            st.success("同步完成")
        except Exception as e:
            st.error(f"同步失败: {e}")
 
    st.divider()
    st.caption("存储状态")
    st.caption("已通过 core_engine 自动写入 SQLite 和 OSS")
 
    st.divider()
    st.subheader("历史对话")
    history_sessions = get_history_sessions()
    if history_sessions:
        for s in history_sessions:
            title = s["title"] if s["title"] else "（无标题）"
            st.caption(f" {title} ({s['rounds']}轮)")
    else:
        st.caption("暂无历史对话")
 
    st.divider()
    if st.button("清空显示", use_container_width=True):
        st.session_state.display_messages = []
        st.session_state.render_offset = 0
        st.rerun()