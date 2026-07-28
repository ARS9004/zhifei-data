import streamlit as st
import pymysql

st.set_page_config(page_title="RDS 测试", layout="centered")
st.title("RDS 连接测试")

# 从 secrets 读取配置
try:
    host = st.secrets["connections"]["rds"]["host"]
    port = st.secrets["connections"]["rds"]["port"]
    user = st.secrets["connections"]["rds"]["username"]
    password = st.secrets["connections"]["rds"]["password"]
    database = st.secrets["connections"]["rds"]["database"]
    st.info(f"配置: {host}:{port}/{database}")
except Exception as e:
    st.error(f"读取 Secrets 失败: {e}")
    st.stop()

if st.button("测试连接"):
    with st.spinner("连接中..."):
        try:
            conn = pymysql.connect(
                host=host, port=port,
                user=user, password=password,
                database=database,
                charset='utf8mb4',
                connect_timeout=10
            )
            st.success("✅ 连接成功！")
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM chat_memory")
                count = cur.fetchone()[0]
                st.write(f"记录数: {count}")
                cur.execute("SELECT id, user_msg, ts FROM chat_memory ORDER BY id DESC LIMIT 3")
                for row in cur.fetchall():
                    st.write(f"id={row[0]}, msg={row[1][:30]}..., ts={row[2]}")
            conn.close()
        except Exception as e:
            st.error(f"❌ 失败: {e}")