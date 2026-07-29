# web.py —— 从OSS读取最近20轮对话并显示
import streamlit as st
import json
import oss2
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
from datetime import datetime, timedelta

# ===== 配置（写死） =====
OSS_BUCKET = "zfai-date-oss"
OSS_REGION = "cn-beijing"
OSS_PREFIX = "chat_history/"

# ===== 获取STS凭证 =====
def get_sts_token():
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
    return json.loads(resp)["Credentials"]

# ===== 获取OSS客户端 =====
def get_oss_client():
    creds = get_sts_token()
    auth = oss2.StsAuth(creds["AccessKeyId"], creds["AccessKeySecret"], creds["SecurityToken"])
    return oss2.Bucket(auth, f"oss-{OSS_REGION}.aliyuncs.com", OSS_BUCKET)

# ===== 获取当前周的文件名（如 2026-W30） =====
def get_current_week_key():
    now = datetime.now()
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"

# ===== 查找最近一个有数据的周文件（向前搜索最多5周） =====
def find_recent_week():
    bucket = get_oss_client()
    for i in range(5):
        # 计算目标周的日期（从当前周往前推 i 周）
        now = datetime.now()
        start_of_week = now - timedelta(days=now.weekday())  # 本周一
        target_date = start_of_week - timedelta(weeks=i)
        year, week, _ = target_date.isocalendar()
        week_key = f"{year}-W{week:02d}"
        remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
        try:
            bucket.head_object(remote_path)
            return week_key
        except:
            continue
    return None

# ===== 读取OSS文件 =====
def read_oss_file(week_key):
    bucket = get_oss_client()
    remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
    try:
        result = bucket.get_object(remote_path)
        content = result.read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        st.error(f"读取失败: {e}")
        return None

# ===== 页面 =====
st.set_page_config(page_title="智飞投研 · 最近对话", layout="centered")
st.title("💬 最近20轮对话")

with st.spinner("加载中..."):
    week_key = find_recent_week()
    if week_key is None:
        st.warning("未找到任何对话记录")
        st.stop()
    
    messages = read_oss_file(week_key)
    if not messages:
        st.info("暂无对话")
        st.stop()

    # 按时间排序（有 timestamp 字段）
    def get_time(m):
        ts = m.get("timestamp") or m.get("time") or m.get("date")
        if isinstance(ts, str):
            try:
                if 'T' in ts:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except:
                return datetime.min
        return datetime.min

    messages_sorted = sorted(messages, key=get_time)
    # 取最近40条（20轮）
    recent = messages_sorted[-40:] if len(messages_sorted) >= 40 else messages_sorted

st.caption(f"📅 来源：{week_key} ｜ 共 {len(recent)} 条消息")

for msg in recent:
    role = msg.get("role", "")
    content = msg.get("content", "")
    if role == "user":
        with st.chat_message("user"):
            st.markdown(content)
    elif role == "assistant":
        with st.chat_message("assistant"):
            st.markdown(content)

if st.button("🔄 刷新"):
    st.rerun()