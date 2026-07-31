# -*- coding: utf-8 -*-
import streamlit as st
import json
import oss2
from datetime import datetime
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"

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

def get_week():
    y, w, _ = datetime.now().isocalendar()
    return f"{y}-W{w:02d}"

def load_dialogues():
    try:
        bucket = get_oss_client()
        remote = f"{OSS_PREFIX}chat_history_{get_week()}.jsonl"
        result = bucket.get_object(remote)
        content = result.read().decode('utf-8')
        msgs = []
        for line in content.strip().split('\n'):
            if line.strip():
                msgs.append(json.loads(line))
        return msgs
    except Exception as e:
        return []

st.set_page_config(page_title="调试", layout="centered")
st.title("📱 调试版 · 云端前端")

# 加载数据
msgs = load_dialogues()
st.write(f"📊 从OSS读取到 {len(msgs)} 条消息")

# 显示前3条消息的预览
if msgs:
    st.write("前3条消息预览:")
    for i, m in enumerate(msgs[:3]):
        st.write(f"  {i+1}. {m.get('role')}: {str(m.get('content', ''))[:50]}...")

# 渲染最近3轮
RENDER_ROUNDS = 3
render = msgs[-RENDER_ROUNDS*2:] if len(msgs) > RENDER_ROUNDS*2 else msgs
st.write(f"🎨 渲染最近 {len(render)} 条消息")

for m in render:
    with st.chat_message(m["role"]):
        st.markdown(m.get("content", ""))

if not msgs:
    st.info("📭 暂无对话数据")