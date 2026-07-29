# web.py —— 智飞投研 · 云端轻量版（只读OSS + 写OSS + 调模型）
import streamlit as st
import json
import oss2
from datetime import datetime
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
import dashscope
from http import HTTPStatus
import re

# ===== 配置 =====
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"
MODEL_NAME = "qwen-plus"

# ===== 读取 OSS =====
def get_oss_client():
    client = AcsClient(
        st.secrets["oss"]["access_key_id"],
        st.secrets["oss"]["access_key_secret"],
        OSS_REGION
    )
    req = AssumeRoleRequest.AssumeRoleRequest()
    req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
    req.set_RoleSessionName("web-oss-session")
    req.set_DurationSeconds(900)
    resp = client.do_action_with_exception(req)
    creds = json.loads(resp)["Credentials"]
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

def read_oss(week_key):
    bucket = get_oss_client()
    remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
    try:
        result = bucket.get_object(remote_path)
        return json.loads(result.read().decode("utf-8"))
    except:
        return []

def write_oss(week_key, data):
    bucket = get_oss_client()
    remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
    local_temp = f"temp_{week_key}.json"
    with open(local_temp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    with open(local_temp, "rb") as f:
        bucket.put_object(remote_path, f)
    import os
    os.remove(local_temp)

def get_current_week():
    now = datetime.now()
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"

# ===== 调用百炼 =====
def call_bailian(messages):
    dashscope.api_key = st.secrets["dashscope"]["api_key"]
    sys_p = f"当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    full_msgs = [{"role": "system", "content": sys_p}] + messages
    resp = dashscope.Generation.call(
        model=MODEL_NAME,
        messages=full_msgs,
        result_format="message",
        stream=False
    )
    if resp.status_code == HTTPStatus.OK:
        return resp.output.choices[0].message.content
    raise Exception(f"API错误: {resp.code} - {resp.message}")

# ===== 页面 =====
st.set_page_config(page_title="智飞投研", layout="centered")
st.title("📦 智飞投研 · 云端")

week_key = get_current_week()
if "history" not in st.session_state:
    st.session_state.history = read_oss(week_key) or []
if "write_status" not in st.session_state:
    st.session_state.write_status = ""

# ===== 显示加载状态和最后一条消息 =====
msg_count = len(st.session_state.history)
round_count = msg_count // 2
st.caption(f"已加载 {msg_count} 条消息（{round_count} 轮对话）")

# 显示最新一轮对话（如果有）
if msg_count >= 2:
    last_two = st.session_state.history[-2:]
    for msg in last_two:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
elif msg_count == 1:
    # 只有一条消息（不完整的一轮）
    with st.chat_message(st.session_state.history[0]["role"]):
        st.markdown(st.session_state.history[0]["content"])
else:
    st.info("💬 开始新对话")

# ===== 输入框 =====
user_input = st.chat_input("输入消息...")
if user_input:
    new_msg = {"role": "user", "content": user_input, "timestamp": datetime.now().isoformat()}
    st.session_state.history.append(new_msg)

    # 取最近15轮（30条）作为上下文
    ctx = st.session_state.history[-30:] if len(st.session_state.history) > 30 else st.session_state.history

    with st.spinner("思考中..."):
        try:
            reply = call_bailian(ctx)
        except Exception as e:
            st.error(f"模型调用失败: {e}")
            st.stop()
        assistant_msg = {"role": "assistant", "content": reply, "timestamp": datetime.now().isoformat()}
        st.session_state.history.append(assistant_msg)

        try:
            write_oss(week_key, st.session_state.history)
            st.session_state.write_status = "✅ 已保存到 OSS"
        except Exception as e:
            st.session_state.write_status = f"⚠️ 写入失败: {e}"

    st.rerun()

# ===== 底部按钮 =====
col1, col2, col3 = st.columns([4, 1, 1])
with col2:
    if st.button("📥 一键写入"):
        try:
            write_oss(week_key, st.session_state.history)
            st.session_state.write_status = "✅ 手动写入成功"
        except Exception as e:
            st.session_state.write_status = f"❌ 写入失败: {e}"
        st.rerun()
with col3:
    if st.button("📤 导出TXT") and st.session_state.history:
        txt = "\n\n".join([f"{m['role']}: {m['content']}" for m in st.session_state.history[-20:]])
        st.download_button("📥 下载", txt, file_name=f"chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt", mime="text/plain", key="dl")

if st.session_state.write_status:
    st.caption(st.session_state.write_status)