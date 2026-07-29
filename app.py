# app.py —— 入口（含 STS 接口 + 加载 web.py）
import streamlit as st
from streamlit.starlette import App
from starlette.routing import Route
from starlette.responses import JSONResponse
import json
from aliyunsdkcore.client import AcsClient
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

async def get_sts_token(request):
    try:
        client = AcsClient(
            st.secrets["oss"]["access_key_id"],
            st.secrets["oss"]["access_key_secret"],
            "cn-beijing"
        )
        req = AssumeRoleRequest.AssumeRoleRequest()
        req.set_RoleArn("acs:ram::1045482798819953:role/STS-OSS-Read")
        req.set_RoleSessionName("web-oss-session")
        req.set_DurationSeconds(900)
        resp = client.do_action_with_exception(req)
        creds = json.loads(resp)["Credentials"]
        return JSONResponse({
            "AccessKeyId": creds["AccessKeyId"],
            "AccessKeySecret": creds["AccessKeySecret"],
            "SecurityToken": creds["SecurityToken"]
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

app = App("web.py", routes=[Route("/api/sts", get_sts_token)])