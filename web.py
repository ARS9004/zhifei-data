# web.py —— OSS 读取测试（显示调试信息 + 最近5轮）
import streamlit as st
import json
import oss2
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
from datetime import datetime, timedelta

# ===== 配置 =====
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

# ===== 查找最近一个有数据的周文件（向前搜索5周） =====
def find_recent_week():
    bucket = get_oss_client()
    for i in range(5):
        now = datetime.now()
        start_of_week = now - timedelta(days=now.weekday())
        target_date = start_of_week - timedelta(weeks=i)
        year, week, _ = target_date.isocalendar()
        week_key = f"{year}-W{week:02d}"
        remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
        st.write(f"🔍 检查: {remote_path}")
        try:
            bucket.head_object(remote_path)
            st.write(f"✅ 找到: {remote_path}")
            return week_key
        except Exception as e:
            st.write(f"❌ 不存在: {remote_path} ({e})")
            continue
    return None

# ===== 读取OSS文件 =====
def read_oss_file(week_key):
    bucket = get_oss_client()
    remote_path = OSS_PREFIX + f"chat_history_{week_key}.json"
    st.write(f"📂 读取: {remote_path}")
    result = bucket.get_object(remote_path)
    content = result.read().decode("utf-8")
    return json.loads(content)

# ===== 页面 =====
st.set_page_config(page_title="智飞投研 · 最近对话(测试)", layout="centered")
st.title("💬 最近5轮对话（测试版）")

with st.spinner("加载中..."):
    week_key = find_recent_week()
    if week_key is None:
        st.error("❌ 未找到任何周文件（已检查最近5周）")
        st.stop()

    messages = read_oss_file(week_key)
    if not messages:
        st.info("📭 文件为空")
        st.stop()

    # 按时间排序
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
    # 取最近10条（5轮）
    recent = messages_sorted[-10:] if len(messages_sorted) >= 10 else messages_sorted

st.caption(f"📅 来源: {week_key} ｜ 共 {len(recent)} 条消息（最近5轮）")

for msg in recent:
    role = msg.get("role", "")
    content = msg.get("content", "")
    with st.chat_message(role):
        st.markdown(content)

if st.button("🔄 刷新"):
    st.rerun()